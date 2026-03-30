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
      product_desc TEXT,
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
    CREATE INDEX IF NOT EXISTS idx_charges_email ON charges(customer_email);
    CREATE INDEX IF NOT EXISTS idx_refunds_created ON refunds(created);
  `);

  // Add product_desc column to existing DBs
  try { db.exec("ALTER TABLE charges ADD COLUMN product_desc TEXT"); } catch {}
  db.exec("CREATE INDEX IF NOT EXISTS idx_charges_product ON charges(product_desc)");
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
    INSERT INTO charges (id, amount, currency, status, state, country, customer_email, product_desc, created)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      amount = excluded.amount,
      status = excluded.status,
      state = excluded.state,
      country = excluded.country,
      customer_email = excluded.customer_email,
      product_desc = excluded.product_desc
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
      stmt.run(r.id, r.amount, r.currency, r.status, r.state, r.country, r.email, r.productDesc, r.created);
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
    parts.push(`LOWER(${emailCol}) NOT IN (${excluded_emails.map(() => "?").join(", ")})`);
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

// --- Date range helpers ---

function dateRangeClause(col, from, to) {
  const parts = [];
  const params = [];
  if (from) {
    // Start of the "from" day in local time: YYYY-MM-DDT00:00:00
    const [y, m, d] = from.split("-").map(Number);
    parts.push(`${col} >= ?`);
    params.push(Math.floor(new Date(y, m - 1, d, 0, 0, 0).getTime() / 1000));
  }
  if (to) {
    // End of the "to" day in local time: YYYY-MM-DDT23:59:59
    const [y, m, d] = to.split("-").map(Number);
    parts.push(`${col} <= ?`);
    params.push(Math.floor(new Date(y, m - 1, d, 23, 59, 59).getTime() / 1000));
  }
  if (parts.length === 0) return { sql: "", params: [] };
  return { sql: " AND " + parts.join(" AND "), params };
}

// --- Query helpers ---

function getSalesByState({ from, to } = {}) {
  const db = getDb();
  const ex = excludeClause("charges.customer_email");
  const dr = dateRangeClause("charges.created", from, to);
  return db
    .prepare(
      `SELECT COALESCE(state, 'Unknown') as state,
              COUNT(*) as count,
              SUM(amount) as total_cents
       FROM charges
       WHERE status = 'succeeded'${ex.sql}${dr.sql}
       GROUP BY state
       ORDER BY total_cents DESC`
    )
    .all(...ex.params, ...dr.params);
}

const GROUP_FORMATS = {
  day: '%Y-%m-%d',
  week: '%Y-W%W',
  month: '%Y-%m',
};

function getMonthlySummary({ from, to, group } = {}) {
  const db = getDb();
  const ex = excludeClause("charges.customer_email");
  const dr = dateRangeClause("charges.created", from, to);
  const groupFmt = GROUP_FORMATS[group] || GROUP_FORMATS.month;
  return db
    .prepare(
      `SELECT strftime('${groupFmt}', created, 'unixepoch') as period,
              COUNT(*) as sales_count,
              SUM(amount) as sales_cents
       FROM charges
       WHERE status = 'succeeded'${ex.sql}${dr.sql}
       GROUP BY period
       ORDER BY period`
    )
    .all(...ex.params, ...dr.params);
}

function getMonthlyRefunds({ from, to, group } = {}) {
  const db = getDb();
  const re = refundExcludeJoin();
  const dr = dateRangeClause("refunds.created", from, to);
  const groupFmt = GROUP_FORMATS[group] || GROUP_FORMATS.month;
  return db
    .prepare(
      `SELECT strftime('${groupFmt}', refunds.created, 'unixepoch') as period,
              COUNT(*) as refund_count,
              SUM(refunds.amount) as refund_cents
       FROM refunds${re.join}
       WHERE refunds.status = 'succeeded'${re.where}${dr.sql}
       GROUP BY period
       ORDER BY period`
    )
    .all(...re.params, ...dr.params);
}

function getTotals({ from, to } = {}) {
  const db = getDb();
  const ex = excludeClause("charges.customer_email");
  const re = refundExcludeJoin();
  const drCharges = dateRangeClause("charges.created", from, to);
  const drRefunds = dateRangeClause("refunds.created", from, to);
  const sales = db
    .prepare(
      `SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total_cents
       FROM charges WHERE status = 'succeeded'${ex.sql}${drCharges.sql}`
    )
    .get(...ex.params, ...drCharges.params);
  const refunds = db
    .prepare(
      `SELECT COUNT(*) as count, COALESCE(SUM(refunds.amount), 0) as total_cents
       FROM refunds${re.join} WHERE refunds.status = 'succeeded'${re.where}${drRefunds.sql}`
    )
    .get(...re.params, ...drRefunds.params);
  return { sales, refunds };
}

// --- Drill-down: customers for a given state ---

function getCustomersByState(state, { from, to } = {}) {
  const db = getDb();
  const ex = excludeClause("charges.customer_email");
  const dr = dateRangeClause("charges.created", from, to);
  const stateCondition = state === "Unknown" ? "state IS NULL" : "state = ?";
  const stateParams = state === "Unknown" ? [] : [state];
  return db
    .prepare(
      `SELECT customer_email as email,
              COUNT(*) as count,
              SUM(amount) as total_cents,
              MIN(created) as first_charge,
              MAX(created) as last_charge
       FROM charges
       WHERE status = 'succeeded' AND ${stateCondition}${ex.sql}${dr.sql}
       GROUP BY customer_email
       ORDER BY total_cents DESC`
    )
    .all(...stateParams, ...ex.params, ...dr.params);
}

// --- Drill-down: customers for a given month ---

function getCustomersByMonth(month, type, { from, to } = {}) {
  const db = getDb();
  if (type === "refunds") {
    const re = refundExcludeJoin();
    return db
      .prepare(
        `SELECT COALESCE(c.customer_email, '(unknown)') as email,
                COUNT(*) as count,
                SUM(refunds.amount) as total_cents
         FROM refunds
         JOIN charges c ON refunds.charge_id = c.id
         WHERE refunds.status = 'succeeded'
           AND strftime('%Y-%m', refunds.created, 'unixepoch') = ?${re.where}
         GROUP BY c.customer_email
         ORDER BY total_cents DESC`
      )
      .all(month, ...re.params);
  }
  const ex = excludeClause("charges.customer_email");
  return db
    .prepare(
      `SELECT COALESCE(customer_email, '(unknown)') as email,
              COUNT(*) as count,
              SUM(amount) as total_cents
       FROM charges
       WHERE status = 'succeeded'
         AND strftime('%Y-%m', created, 'unixepoch') = ?${ex.sql}
       GROUP BY customer_email
       ORDER BY total_cents DESC`
    )
    .all(month, ...ex.params);
}

// --- Drill-down: customers for a given day (YYYY-MM-DD) ---

function getCustomersByDay(day, type) {
  const db = getDb();
  if (type === "refunds") {
    const re = refundExcludeJoin();
    return db
      .prepare(
        `SELECT COALESCE(c.customer_email, '(unknown)') as email,
                COUNT(*) as count,
                SUM(refunds.amount) as total_cents
         FROM refunds
         JOIN charges c ON refunds.charge_id = c.id
         WHERE refunds.status = 'succeeded'
           AND strftime('%Y-%m-%d', refunds.created, 'unixepoch') = ?${re.where}
         GROUP BY c.customer_email
         ORDER BY total_cents DESC`
      )
      .all(day, ...re.params);
  }
  const ex = excludeClause("charges.customer_email");
  return db
    .prepare(
      `SELECT COALESCE(customer_email, '(unknown)') as email,
              COUNT(*) as count,
              SUM(amount) as total_cents
       FROM charges
       WHERE status = 'succeeded'
         AND strftime('%Y-%m-%d', created, 'unixepoch') = ?${ex.sql}
       GROUP BY customer_email
       ORDER BY total_cents DESC`
    )
    .all(day, ...ex.params);
}


// --- Internal revenue (after OL cut) ---

// Map product description to OL cost in cents using the Excel pricing data
// The OL costs from the Excel are in dollars; amounts in DB are cents
// Amount (cents) -> product key mapping based on current Regular pricing
const AMOUNT_TO_PRODUCT = {
  17900: "sema|inj|1",   // Sema Injection 1mo $179
  20900: "sema|oral|1",  // Sema Oral 1mo $209
  23900: "tirz|inj|1",   // Tirz Injection 1mo $239
  26900: "tirz|oral|1",  // Tirz Oral 1mo $269
  39900: "sema|inj|3",   // Sema Injection 3mo $399
  48900: "sema|oral|3",  // Sema Oral 3mo $489
  59700: "tirz|inj|3",   // Tirz Injection 3mo $597
  68700: "tirz|oral|3",  // Tirz Oral 3mo $687
  79800: "sema|inj|6",   // Sema Injection 6mo $798
  99600: "sema|inj|6",   // Sema Injection 6mo $996 (24-week supply variant)
  117600: "sema|oral|6", // Sema Oral 6mo $1176
  119400: "tirz|inj|6",  // Tirz Injection 6mo $1194
  137400: "tirz|oral|6", // Tirz Oral 6mo $1374
};

function classifyProduct(desc, amountCents) {
  // Priority 1: exact amount match — most reliable since prices are unique per product/plan
  if (amountCents && AMOUNT_TO_PRODUCT[amountCents]) {
    return AMOUNT_TO_PRODUCT[amountCents];
  }

  // Priority 2: description-based classification (for non-standard amounts like discounts)
  if (desc) {
    const d = desc.toLowerCase();
    const med = d.includes("tirzepatide") ? "tirz" : d.includes("semaglutide") ? "sema" : null;
    const method = d.includes("injection") ? "inj" : d.includes("oral") ? "oral" : null;

    if (med) {
      // Infer plan from amount since description can be misleading
      let plan = null;
      if (amountCents) {
        const amt = amountCents / 100;
        if (amt >= 700) plan = 6;
        else if (amt >= 350) plan = 3;
        else plan = 1;
      }

      if (method && plan) return `${med}|${method}|${plan}`;
      if (plan) return `${med}|inj|${plan}`;
    }
  }

  return null;
}

// OL costs in cents (from Excel current pricing — OL Cost column)
const OL_COSTS_CENTS = {
  "sema|oral|1": 14740,     // $147.40
  "sema|inj|1": 11460,      // $114.60
  "tirz|oral|1": 16040,     // $160.40
  "tirz|inj|1": 14740,      // $147.40
  "sema|oral|3": 44048,     // $440.48
  "sema|inj|3": 34668,      // $346.68
  "tirz|oral|3": 48740,     // $487.40
  "tirz|inj|3": 48860,      // $488.60
  "sema|oral|6": 88668,     // $886.68
  "sema|inj|6": 69616,      // $696.16
  "tirz|oral|6": 95760,     // $957.60
  "tirz|inj|6": 91520,      // $915.20
};

function getOlCostCents(productKey) {
  return OL_COSTS_CENTS[productKey] || 0;
}

// Get total refunds for a charge, keyed by charge_id
function getRefundsByCharge({ from, to } = {}) {
  const database = getDb();
  const dr = dateRangeClause("refunds.created", from, to);
  // Get all refunds (not filtered by date range on refunds — refunds apply to charges in range)
  const rows = database
    .prepare(
      `SELECT charge_id, SUM(amount) as refund_cents
       FROM refunds
       WHERE status = 'succeeded'
       GROUP BY charge_id`
    )
    .all();
  const map = {};
  for (const r of rows) map[r.charge_id] = r.refund_cents;
  return map;
}

function getInternalRevenueSummary({ from, to, group } = {}) {
  const database = getDb();
  const ex = excludeClause("charges.customer_email");
  const dr = dateRangeClause("charges.created", from, to);
  const groupFmt = GROUP_FORMATS[group] || GROUP_FORMATS.month;
  const rows = database
    .prepare(
      `SELECT id, amount, product_desc, strftime('${groupFmt}', created, 'unixepoch') as period
       FROM charges
       WHERE status = 'succeeded'${ex.sql}${dr.sql}
       ORDER BY period`
    )
    .all(...ex.params, ...dr.params);

  const refundMap = getRefundsByCharge();

  const periods = {};
  let totalGross = 0, totalOl = 0, totalNet = 0, totalRefunds = 0, totalUnmapped = 0, totalCount = 0;
  for (const row of rows) {
    const refund = refundMap[row.id] || 0;
    const fullyRefunded = refund >= row.amount;

    // Skip fully refunded charges — no revenue, no OL cost
    if (fullyRefunded) continue;

    const key = classifyProduct(row.product_desc, row.amount);
    const olCost = key ? getOlCostCents(key) : 0;
    const net = row.amount - olCost - refund;

    if (!periods[row.period]) periods[row.period] = { gross: 0, olCost: 0, refunds: 0, net: 0, count: 0, unmapped: 0 };
    periods[row.period].gross += row.amount;
    periods[row.period].olCost += olCost;
    periods[row.period].refunds += refund;
    periods[row.period].net += net;
    periods[row.period].count++;
    if (!key) periods[row.period].unmapped++;

    totalGross += row.amount;
    totalOl += olCost;
    totalRefunds += refund;
    totalNet += net;
    totalCount++;
    if (!key) totalUnmapped++;
  }

  // Query refunds separately by their own date (not charge date)
  const exRef = excludeClause("c.customer_email");
  const drRef = dateRangeClause("r.created", from, to);
  const refundRows = database
    .prepare(
      `SELECT r.amount, strftime('${groupFmt}', r.created, 'unixepoch') as period
       FROM refunds r
       JOIN charges c ON r.charge_id = c.id
       WHERE r.status = 'succeeded'${exRef.sql}${drRef.sql}`
    )
    .all(...exRef.params, ...drRef.params);

  let totalRefundsByDate = 0;
  const refundsByPeriod = {};
  for (const r of refundRows) {
    refundsByPeriod[r.period] = (refundsByPeriod[r.period] || 0) + r.amount;
    totalRefundsByDate += r.amount;
  }

  // Merge refund data into periods (ensure periods exist even if no sales)
  for (const [p, amt] of Object.entries(refundsByPeriod)) {
    if (!periods[p]) periods[p] = { gross: 0, olCost: 0, refunds: 0, net: 0, count: 0, unmapped: 0 };
    periods[p].refunds = amt;
  }

  // Calculate net with and without refunds for each period
  for (const p of Object.values(periods)) {
    p.netBeforeRefunds = p.gross - p.olCost;
    p.net = p.gross - p.olCost - p.refunds;
  }
  const totalNetBeforeRefunds = totalGross - totalOl;
  const totalNetWithRefunds = totalGross - totalOl - totalRefundsByDate;

  return {
    periods: Object.entries(periods).sort(([a],[b]) => a.localeCompare(b)).map(([p, d]) => ({ period: p, ...d })),
    totals: { gross: totalGross, olCost: totalOl, refunds: totalRefundsByDate, netBeforeRefunds: totalNetBeforeRefunds, net: totalNetWithRefunds, count: totalCount, unmapped: totalUnmapped },
  };
}

function getInternalByProduct({ from, to } = {}) {
  const database = getDb();
  const ex = excludeClause("charges.customer_email");
  const dr = dateRangeClause("charges.created", from, to);
  const rows = database
    .prepare(
      `SELECT id, amount, product_desc
       FROM charges
       WHERE status = 'succeeded'${ex.sql}${dr.sql}`
    )
    .all(...ex.params, ...dr.params);

  const refundMap = getRefundsByCharge();

  const products = {};
  for (const row of rows) {
    const refund = refundMap[row.id] || 0;
    // Skip fully refunded charges
    if (refund >= row.amount) continue;

    const key = classifyProduct(row.product_desc, row.amount) || "unmapped";
    const olCost = key !== "unmapped" ? getOlCostCents(key) : 0;
    if (!products[key]) products[key] = { gross: 0, olCost: 0, refunds: 0, net: 0, count: 0 };
    products[key].gross += row.amount;
    products[key].olCost += olCost;
    products[key].refunds += refund;
    products[key].net += row.amount - olCost - refund;
    products[key].count++;
  }

  const LABELS = {
    "sema|oral|1": "Sema Oral 1mo", "sema|inj|1": "Sema Inj 1mo",
    "tirz|oral|1": "Tirz Oral 1mo", "tirz|inj|1": "Tirz Inj 1mo",
    "sema|oral|3": "Sema Oral 3mo", "sema|inj|3": "Sema Inj 3mo",
    "tirz|oral|3": "Tirz Oral 3mo", "tirz|inj|3": "Tirz Inj 3mo",
    "sema|oral|6": "Sema Oral 6mo", "sema|inj|6": "Sema Inj 6mo",
    "tirz|oral|6": "Tirz Oral 6mo", "tirz|inj|6": "Tirz Inj 6mo",
    "unmapped": "Unmapped",
  };

  return Object.entries(products).map(([key, data]) => ({
    product: LABELS[key] || key,
    ...data,
  })).sort((a, b) => b.net - a.net);
}

// --- Internal drilldown: individual sales + refunds for a period ---

function getInternalDrilldown(period, group, { from, to } = {}) {
  const database = getDb();
  const ex = excludeClause("charges.customer_email");
  const dr = dateRangeClause("charges.created", from, to);
  const groupFmt = GROUP_FORMATS[group] || GROUP_FORMATS.month;

  // Get SALES: charges created in this period
  const charges = database
    .prepare(
      `SELECT id, amount, product_desc, customer_email, created
       FROM charges
       WHERE status = 'succeeded'
         AND strftime('${groupFmt}', created, 'unixepoch') = ?${ex.sql}${dr.sql}`
    )
    .all(period, ...ex.params, ...dr.params);

  const refundMap = getRefundsByCharge();
  const results = [];

  for (const c of charges) {
    const key = classifyProduct(c.product_desc, c.amount);
    const olCost = key ? getOlCostCents(key) : 0;
    const refund = refundMap[c.id] || 0;
    const fullyRefunded = refund >= c.amount;

    results.push({
      type: "sale",
      email: c.customer_email || "(unknown)",
      amount: c.amount,
      olCost: fullyRefunded ? 0 : olCost,
      refund: 0,
      net: fullyRefunded ? 0 : c.amount - olCost,
      fullyRefunded,
      date: c.created,
      product: c.product_desc || "Unknown",
    });
  }

  // Get REFUNDS that occurred in this period (separate transactions)
  const exRef = excludeClause("c.customer_email");
  const refunds = database
    .prepare(
      `SELECT r.id, r.amount, r.created, r.charge_id,
              c.amount as charge_amount, c.customer_email, c.product_desc
       FROM refunds r
       JOIN charges c ON r.charge_id = c.id
       WHERE r.status = 'succeeded'
         AND strftime('${groupFmt}', r.created, 'unixepoch') = ?${exRef.sql}`
    )
    .all(period, ...exRef.params);

  for (const r of refunds) {
    results.push({
      type: "refund",
      email: r.customer_email || "(unknown)",
      amount: r.amount,
      olCost: 0,
      refund: r.amount,
      net: 0,
      fullyRefunded: true,
      date: r.created,
      product: r.product_desc || "Unknown",
    });
  }

  // Sort: sales first, then refunds, by date desc
  results.sort((a, b) => {
    if (a.type !== b.type) return a.type === "sale" ? -1 : 1;
    return b.date - a.date;
  });
  return results;
}

// --- Live weighted break-even from Stripe data ---

function getLiveWeightedBreakEven({ from, to } = {}) {
  const database = getDb();
  const ex = excludeClause("charges.customer_email");
  const dr = dateRangeClause("charges.created", from, to);
  const rows = database
    .prepare(
      `SELECT id, amount, product_desc
       FROM charges
       WHERE status = 'succeeded'${ex.sql}${dr.sql}`
    )
    .all(...ex.params, ...dr.params);

  const refundMap = getRefundsByCharge();

  // Count by product key, skip fully refunded
  const counts = {};
  let total = 0;
  const validRows = [];
  for (const row of rows) {
    const refund = refundMap[row.id] || 0;
    if (refund >= row.amount) continue; // skip fully refunded
    const key = classifyProduct(row.product_desc, row.amount);
    if (!key) continue;
    counts[key] = (counts[key] || 0) + 1;
    total++;
    validRows.push(row);
  }

  if (total === 0) return { weights: [], weightedRevenuePerUnit: 0, totalCosts: 0, unitsPerMonth: 0, unitsPerDay: 0, totalSales: 0 };

  // Calculate weighted average revenue per unit (net after OL)
  let weightedRevenue = 0;
  const weights = [];
  for (const [key, count] of Object.entries(counts)) {
    const weight = count / total;
    const olCost = getOlCostCents(key);
    // Use amount-based average revenue for this product
    const matchingRows = validRows.filter(r => classifyProduct(r.product_desc, r.amount) === key);
    const avgAmount = matchingRows.reduce((s, r) => s + r.amount, 0) / matchingRows.length;
    const netPerUnit = avgAmount - olCost;
    weightedRevenue += weight * netPerUnit;

    const LABELS = {
      "sema|oral|1": "Sema Oral 1mo", "sema|inj|1": "Sema Inj 1mo",
      "tirz|oral|1": "Tirz Oral 1mo", "tirz|inj|1": "Tirz Inj 1mo",
      "sema|oral|3": "Sema Oral 3mo", "sema|inj|3": "Sema Inj 3mo",
      "tirz|oral|3": "Tirz Oral 3mo", "tirz|inj|3": "Tirz Inj 3mo",
      "sema|oral|6": "Sema Oral 6mo", "sema|inj|6": "Sema Inj 6mo",
      "tirz|oral|6": "Tirz Oral 6mo", "tirz|inj|6": "Tirz Inj 6mo",
    };
    weights.push({ product: LABELS[key] || key, count, weight, netPerUnit: netPerUnit / 100 });
  }

  // Read total monthly costs from Excel
  let totalCosts;
  try {
    const { parseExcel } = require("./excel");
    totalCosts = parseExcel().totalCosts * 100; // convert to cents
  } catch {
    totalCosts = 5675900; // fallback $56,759
  }

  const unitsPerMonth = weightedRevenue > 0 ? totalCosts / weightedRevenue : 0;
  const unitsPerDay = unitsPerMonth / 30;

  return {
    weights: weights.sort((a, b) => b.weight - a.weight),
    weightedRevenuePerUnit: weightedRevenue / 100,
    totalCosts: totalCosts / 100,
    unitsPerMonth,
    unitsPerDay,
    totalSales: total,
  };
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
  getCustomersByState,
  getCustomersByMonth,
  getExcludedEmails,
  addExcludedEmail,
  removeExcludedEmail,
  getCustomersByDay,
  getInternalRevenueSummary,
  getInternalByProduct,
  getInternalDrilldown,
  getLiveWeightedBreakEven,
  getExcludedKeywords,
  addExcludedKeyword,
  removeExcludedKeyword,
};
