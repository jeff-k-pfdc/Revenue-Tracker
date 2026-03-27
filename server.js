require("dotenv").config();
const express = require("express");
const path = require("path");
const db = require("./db");
const { sync } = require("./sync");

const app = express();
const PORT = process.env.PORT || 3000;
const SYNC_INTERVAL = parseInt(process.env.SYNC_INTERVAL_MS, 10) || 300_000; // 5 min default

app.use(express.json());

// Serve static dashboard
app.use(express.static(path.join(__dirname, "public")));

// --- API routes ---

app.get("/api/sales-by-state", (_req, res) => {
  res.json(db.getSalesByState());
});

app.get("/api/monthly", (_req, res) => {
  const sales = db.getMonthlySummary();
  const refunds = db.getMonthlyRefunds();
  res.json({ sales, refunds });
});

app.get("/api/totals", (_req, res) => {
  res.json(db.getTotals());
});

app.get("/api/sync-status", (_req, res) => {
  const lastCharges = db.getLastSync("charges");
  const lastRefunds = db.getLastSync("refunds");
  res.json({
    lastChargesSync: lastCharges ? new Date(lastCharges * 1000).toISOString() : null,
    lastRefundsSync: lastRefunds ? new Date(lastRefunds * 1000).toISOString() : null,
  });
});

app.get("/api/filters", (_req, res) => {
  res.json({
    excluded_emails: db.getExcludedEmails(),
    excluded_keywords: db.getExcludedKeywords(),
  });
});

app.post("/api/filters/email", (req, res) => {
  const { email } = req.body;
  if (!email) return res.status(400).json({ error: "email required" });
  const filters = db.addExcludedEmail(email);
  res.json(filters);
});

app.delete("/api/filters/email", (req, res) => {
  const { email } = req.body;
  if (!email) return res.status(400).json({ error: "email required" });
  const filters = db.removeExcludedEmail(email);
  res.json(filters);
});

app.post("/api/filters/keyword", (req, res) => {
  const { keyword } = req.body;
  if (!keyword) return res.status(400).json({ error: "keyword required" });
  const filters = db.addExcludedKeyword(keyword);
  res.json(filters);
});

app.delete("/api/filters/keyword", (req, res) => {
  const { keyword } = req.body;
  if (!keyword) return res.status(400).json({ error: "keyword required" });
  const filters = db.removeExcludedKeyword(keyword);
  res.json(filters);
});

app.post("/api/sync", async (_req, res) => {
  try {
    const result = await sync();
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Startup ---

async function start() {
  // Initial sync on startup
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

  // Schedule recurring incremental syncs
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
