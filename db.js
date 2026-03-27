const Database = require("better-sqlite3");
const path = require("path");
const fs = require("fs");

const DB_PATH = path.join(__dirname, "data.db");

let _db;

function getDb() {
  if (!_db) {
    _db = new Database(DB_PATH);
    _db.pragma("journal_mode = WAL");
    _db.pragma("foreign_keys = ON");
    migrate(_db);
  }
  return _db;
}

function migrate(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS charges (
      id TEXT PRIMARY KEY,
      amount INTEGER NOT NULL,
      currency TEXT NOT NULL,
      status TEXT NOT NULL,
      state TEXT,
      country TEXT,
      customer_email TEXT,
      created INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS refunds (
      id TEXT PRIMARY KEY,
      charge_id TEXT NOT NULL,
      amount INTEGER NOT NULL,
      currency TEXT NOT NULL,
      status TEXT NOT NULL,
      created INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS sync_meta (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_charges_created ON charges(created);
    CREATE INDEX IF NOT EXISTS idx_charges_state ON charges(state);
    CREATE INDEX IF NOT EXISTS idx_refunds_created ON refunds(created);
  `);
}

// --- Sync meta helpers ---

function getLastSync(type) {
  const db = getDb();
  const row = db.prepare("SELECT value FROM sync_meta WHERE key = ?").get(`last_${type}_sync`);
  return row ? parseInt(row.value, 10) : null;
}

function setLastSync(type, timestamp) {
  const db = getDb();
  db.prepare(
    "INSERT INTO sync_meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value"
  ).run(`last_${type}_sync`, String(timestamp));
}

// --- Upsert helpers ---

const upsertCharge = (db) =>
  db.prepare(`
    INSERT INTO charges (id, amount, currency, status, state, country, customer_email, created)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      amount = excluded.amount,
      status = excluded.status,
      state = excluded.state,
      country = excluded.country,
      customer_email = excluded.customer_email
  `);

const upsertRefund = (db) =>
  db.prepare(`
    INSERT INTO refunds (id, charge_id, amount, currency, status, created)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      amount = excluded.amount,
      status = excluded.status
  `);

function bulkUpsertCharges(charges) {
  const db = getDb();
  const stmt = upsertCharge(db);
  const tx = db.transaction((rows) => {
    for (const r of rows) {
      stmt.run(r.id, r.amount, r.currency, r.status, r.state, r.country, r.email, r.created);
    }
  });
  tx(charges);
}

function bulkUpsertRefunds(refunds) {
  const db = getDb();
  const stmt = upsertRefund(db);
  const tx = db.transaction((rows) => {
    for (const r of rows) {
      stmt.run(r.id, r.charge_id, r.amount, r.currency, r.status, r.created);
    }
  });
  tx(refunds);
}

// --- Filters ---

const FILTERS_PATH = path.join(__dirname, "filters.json");

function loadFilters() {
  try {
    const data = JSON.parse(fs.readFileSync(FILTERS_PATH, "utf-8"));
    return {
      excluded_emails: data.excluded_emails || [],
      excluded_keywords: data.excluded_keywords || [],
    };
  } catch {
    return { excluded_emails: [], excluded_keywords: [] };
  }
}

function saveFilters(filters) {
  fs.writeFileSync(FILTERS_PATH, JSON.stringify(filters, null, 2));
}

function getExcludedEmails() {
  return loadFilters().excluded_emails;
}

function getExcludedKeywords() {
  return loadFilters().excluded_keywords;
}

function addExcludedEmail(email) {
  const filters = loadFilters();
  const lower = email.toLowerCase().trim();
  if (!filters.excluded_emails.includes(lower)) {
    filters.excluded_emails.push(lower);
    saveFilters(filters);
  }
  return filters;
}

function removeExcludedEmail(email) {
  const filters = loadFilters();
  const lower = email.toLowerCase().trim();
  filters.excluded_emails = filters.excluded_emails.filter((e) => e !== lower);
  saveFilters(filters);
  return filters;
}

function addExcludedKeyword(keyword) {
  const filters = loadFilters();
  const lower = keyword.toLowerCase().trim();
  if (!filters.excluded_keywords.includes(lower)) {
    filters.excluded_keywords.push(lower);
    saveFilters(filters);
  }
  return filters;
}

function removeExcludedKeyword(keyword) {
  const filters = loadFilters();
  const lower = keyword.toLowerCase().trim();
  filters.excluded_keywords = filters.excluded_keywords.filter((k) => k !== lower);
  saveFilters(filters);
  return filters;
}

// Build WHERE clause + params that excludes filtered emails and keyword matches
function excludeClause(emailCol) {
  const { excluded_emails, excluded_keywords } = loadFilters();
  const parts = [];
  const params = [];

  if (excluded_emails.length > 0) {
    parts.push(`${emailCol} NOT IN (${excluded_emails.map(() => "?").join(", ")})`);
    params.push(...excluded_emails);
  }

  for (const kw of excluded_keywords) {
    parts.push(`(${emailCol} IS NULL OR LOWER(${emailCol}) NOT LIKE ?)`);
    params.push(`%${kw}%`);
  }

  if (parts.length === 0) return { sql: "", params: [] };
  return { sql: " AND " + parts.join(" AND "), params };
}

// For refunds, join through charges to access customer_email
function refundExcludeJoin() {
  const { sql, params } = excludeClause("c.customer_email");
  if (params.length === 0) return { join: "", where: "", params: [] };
  return {
    join: " JOIN charges c ON refunds.charge_id = c.id",
    where: sql,
    params,
  };
}

// --- Query helpers ---

function getSalesByState() {
  const db = getDb();
  const ex = excludeClause("charges.customer_email");
  return db
    .prepare(
      `SELECT COALESCE(state, 'Unknown') as state,
              COUNT(*) as count,
              SUM(amount) as total_cents
       FROM charges
       WHERE status = 'succeeded'${ex.sql}
       GROUP BY state
       ORDER BY total_cents DESC`
    )
    .all(...ex.params);
}

function getMonthlySummary() {
  const db = getDb();
  const ex = excludeClause("charges.customer_email");
  return db
    .prepare(
      `SELECT strftime('%Y-%m', created, 'unixepoch') as month,
              COUNT(*) as sales_count,
              SUM(amount) as sales_cents
       FROM charges
       WHERE status = 'succeeded'${ex.sql}
       GROUP BY month
       ORDER BY month`
    )
    .all(...ex.params);
}

function getMonthlyRefunds() {
  const db = getDb();
  const re = refundExcludeJoin();
  return db
    .prepare(
      `SELECT strftime('%Y-%m', refunds.created, 'unixepoch') as month,
              COUNT(*) as refund_count,
              SUM(refunds.amount) as refund_cents
       FROM refunds${re.join}
       WHERE refunds.status = 'succeeded'${re.where}
       GROUP BY month
       ORDER BY month`
    )
    .all(...re.params);
}

function getTotals() {
  const db = getDb();
  const ex = excludeClause("charges.customer_email");
  const re = refundExcludeJoin();
  const sales = db
    .prepare(
      `SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total_cents
       FROM charges WHERE status = 'succeeded'${ex.sql}`
    )
    .get(...ex.params);
  const refunds = db
    .prepare(
      `SELECT COUNT(*) as count, COALESCE(SUM(refunds.amount), 0) as total_cents
       FROM refunds${re.join} WHERE refunds.status = 'succeeded'${re.where}`
    )
    .get(...re.params);
  return { sales, refunds };
}

module.exports = {
  getDb,
  getLastSync,
  setLastSync,
  bulkUpsertCharges,
  bulkUpsertRefunds,
  getSalesByState,
  getMonthlySummary,
  getMonthlyRefunds,
  getTotals,
  getExcludedEmails,
  addExcludedEmail,
  removeExcludedEmail,
  getExcludedKeywords,
  addExcludedKeyword,
  removeExcludedKeyword,
};
