require("dotenv").config();
const Stripe = require("stripe");
const db = require("./db");

const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);

const PAGE_SIZE = 100;

// --- State extraction from charge ---

function extractState(charge) {
  // Try billing details first, then payment method, then customer address
  const billing = charge.billing_details?.address;
  if (billing?.state) return { state: billing.state, country: billing.country || "US" };

  const shipping = charge.shipping?.address;
  if (shipping?.state) return { state: shipping.state, country: shipping.country || "US" };

  return { state: null, country: billing?.country || null };
}

// --- Fetch all charges with auto-pagination ---

async function fetchCharges(sinceTimestamp) {
  const params = { limit: PAGE_SIZE, expand: ["data.billing_details"] };
  if (sinceTimestamp) {
    params.created = { gt: sinceTimestamp };
  }

  let count = 0;
  let latestCreated = sinceTimestamp || 0;
  const batch = [];

  for await (const charge of stripe.charges.list(params)) {
    const { state, country } = extractState(charge);
    batch.push({
      id: charge.id,
      amount: charge.amount,
      currency: charge.currency,
      status: charge.status,
      state,
      country,
      email: charge.billing_details?.email || charge.receipt_email || null,
      created: charge.created,
    });

    if (charge.created > latestCreated) latestCreated = charge.created;

    // Flush in batches of 500
    if (batch.length >= 500) {
      db.bulkUpsertCharges(batch.splice(0));
      count += 500;
      process.stdout.write(`\r  Charges synced: ${count}`);
    }
  }

  // Flush remaining
  if (batch.length > 0) {
    count += batch.length;
    db.bulkUpsertCharges(batch);
  }

  if (latestCreated > 0) db.setLastSync("charges", latestCreated);
  return count;
}

// --- Fetch all refunds with auto-pagination ---

async function fetchRefunds(sinceTimestamp) {
  const params = { limit: PAGE_SIZE };
  if (sinceTimestamp) {
    params.created = { gt: sinceTimestamp };
  }

  let count = 0;
  let latestCreated = sinceTimestamp || 0;
  const batch = [];

  for await (const refund of stripe.refunds.list(params)) {
    batch.push({
      id: refund.id,
      charge_id: refund.charge,
      amount: refund.amount,
      currency: refund.currency,
      status: refund.status,
      created: refund.created,
    });

    if (refund.created > latestCreated) latestCreated = refund.created;

    if (batch.length >= 500) {
      db.bulkUpsertRefunds(batch.splice(0));
      count += 500;
      process.stdout.write(`\r  Refunds synced: ${count}`);
    }
  }

  if (batch.length > 0) {
    count += batch.length;
    db.bulkUpsertRefunds(batch);
  }

  if (latestCreated > 0) db.setLastSync("refunds", latestCreated);
  return count;
}

// --- Main sync function ---

async function sync() {
  const lastChargeSync = db.getLastSync("charges");
  const lastRefundSync = db.getLastSync("refunds");

  const isInitial = !lastChargeSync;
  console.log(isInitial ? "Initial sync — fetching all Stripe data..." : "Incremental sync...");

  const [chargeCount, refundCount] = await Promise.all([
    fetchCharges(lastChargeSync),
    fetchRefunds(lastRefundSync),
  ]);

  console.log(`\nSync complete: ${chargeCount} charges, ${refundCount} refunds`);
  return { chargeCount, refundCount, isInitial };
}

module.exports = { sync };

// Allow running directly: node sync.js
if (require.main === module) {
  sync().catch((err) => {
    console.error("Sync failed:", err.message);
    process.exit(1);
  });
}
