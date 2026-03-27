require("dotenv").config();
const express = require("express");
const path = require("path");
const db = require("./db");
const { sync } = require("./sync");

const app = express();
const PORT = process.env.PORT || 3000;
const SYNC_INTERVAL = parseInt(process.env.SYNC_INTERVAL_MS, 10) || 300_000;

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// --- Helper to extract date range from query params ---
function dateRange(req) {
  return { from: req.query.from || null, to: req.query.to || null };
}

// --- API routes ---

app.get("/api/sales-by-state", (req, res) => {
  res.json(db.getSalesByState(dateRange(req)));
});

app.get("/api/monthly", (req, res) => {
  const range = dateRange(req);
  const sales = db.getMonthlySummary(range);
  const refunds = db.getMonthlyRefunds(range);
  res.json({ sales, refunds });
});

app.get("/api/totals", (req, res) => {
  res.json(db.getTotals(dateRange(req)));
});

app.get("/api/customers-by-state", (req, res) => {
  const { state } = req.query;
  if (!state) return res.status(400).json({ error: "state required" });
  res.json(db.getCustomersByState(state, dateRange(req)));
});

app.get("/api/customers-by-month", (req, res) => {
  const { month, type } = req.query;
  if (!month) return res.status(400).json({ error: "month required" });
  res.json(db.getCustomersByMonth(month, type || "sales", dateRange(req)));
});

app.get("/api/customers-by-day", (req, res) => {
  const { day, type } = req.query;
  if (!day) return res.status(400).json({ error: "day required" });
  res.json(db.getCustomersByDay(day, type || "sales"));
});

app.get("/api/sync-status", (_req, res) => {
  const lastCharges = db.getLastSync("charges");
  const lastRefunds = db.getLastSync("refunds");
  res.json({
    lastChargesSync: lastCharges ? new Date(lastCharges * 1000).toISOString() : null,
    lastRefundsSync: lastRefunds ? new Date(lastRefunds * 1000).toISOString() : null,
  });
});

app.post("/api/sync", async (_req, res) => {
  try {
    const result = await sync();
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Filter routes ---


app.get("/api/filters", (_req, res) => {
  res.json({
    excluded_emails: db.getExcludedEmails(),
    excluded_keywords: db.getExcludedKeywords(),
  });
});

app.post("/api/filters/email", (req, res) => {
  const { email } = req.body;
  if (!email) return res.status(400).json({ error: "email required" });
  res.json(db.addExcludedEmail(email));
});

app.delete("/api/filters/email", (req, res) => {
  const { email } = req.body;
  if (!email) return res.status(400).json({ error: "email required" });
  res.json(db.removeExcludedEmail(email));
});

app.post("/api/filters/keyword", (req, res) => {
  const { keyword } = req.body;
  if (!keyword) return res.status(400).json({ error: "keyword required" });
  res.json(db.addExcludedKeyword(keyword));
});

app.delete("/api/filters/keyword", (req, res) => {
  const { keyword } = req.body;
  if (!keyword) return res.status(400).json({ error: "keyword required" });
  res.json(db.removeExcludedKeyword(keyword));
});

// --- Startup ---

async function start() {
  console.log("Running initial data sync...");
  try {
    await sync();
  } catch (err) {
    console.error("Initial sync failed:", err.message);
    console.log("Starting server anyway — data may be stale or empty.");
  }

  app.listen(PORT, () => {
    console.log(`Dashboard running at http://localhost:${PORT}`);
  });

  setInterval(async () => {
    console.log(`[${new Date().toISOString()}] Running incremental sync...`);
    try {
      await sync();
    } catch (err) {
      console.error("Incremental sync failed:", err.message);
    }
  }, SYNC_INTERVAL);
}

start();
