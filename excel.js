const XLSX = require("xlsx");
const path = require("path");

const EXCEL_PATH = path.join(__dirname, "resources", "Break-Even Calc.xlsx");

function parseExcel() {
  const wb = XLSX.readFile(EXCEL_PATH);
  const ws = wb.Sheets["Sheet1"];
  const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: "" });

  // --- Contract Info (rows 34-54) ---
  const contracts = {
    monthly: [
      { product: "Semaglutide Oral", pricing: n(rows[37][1]), olCost: n(rows[37][2]), netProfit: n(rows[37][3]) },
      { product: "Semaglutide Injection", pricing: n(rows[38][1]), olCost: n(rows[38][2]), netProfit: n(rows[38][3]) },
      { product: "Tirzepatide Oral", pricing: n(rows[39][1]), olCost: n(rows[39][2]), netProfit: n(rows[39][3]) },
      { product: "Tirzepatide Injection", pricing: n(rows[40][1]), olCost: n(rows[40][2]), netProfit: n(rows[40][3]) },
    ],
    threeMonth: [
      { product: "Semaglutide Oral", pricing: n(rows[44][1]), olCost: n(rows[44][2]), netProfit: n(rows[44][3]) },
      { product: "Semaglutide Injection", pricing: n(rows[45][1]), olCost: n(rows[45][2]), netProfit: n(rows[45][3]) },
      { product: "Tirzepatide Oral", pricing: n(rows[46][1]), olCost: n(rows[46][2]), netProfit: n(rows[46][3]) },
      { product: "Tirzepatide Injection", pricing: n(rows[47][1]), olCost: n(rows[47][2]), netProfit: n(rows[47][3]) },
    ],
    sixMonth: [
      { product: "Semaglutide Oral", pricing: n(rows[51][1]), olCost: n(rows[51][2]), netProfit: n(rows[51][3]) },
      { product: "Semaglutide Injection", pricing: n(rows[52][1]), olCost: n(rows[52][2]), netProfit: n(rows[52][3]) },
      { product: "Tirzepatide Oral", pricing: n(rows[53][1]), olCost: n(rows[53][2]), netProfit: n(rows[53][3]) },
      { product: "Tirzepatide Injection", pricing: n(rows[54][1]), olCost: n(rows[54][2]), netProfit: n(rows[54][3]) },
    ],
  };

  // --- Current Pricing & Revenue (rows 37-54 columns F-K) ---
  const currentPricing = {
    monthly: [
      { product: "Semaglutide Oral", fmdPrice: n(rows[37][6]), regularPrice: n(rows[37][7]), olCost: n(rows[37][8]), fmdRevenue: n(rows[37][9]), regularRevenue: n(rows[37][10]) },
      { product: "Semaglutide Injection", fmdPrice: n(rows[38][6]), regularPrice: n(rows[38][7]), olCost: n(rows[38][8]), fmdRevenue: n(rows[38][9]), regularRevenue: n(rows[38][10]) },
      { product: "Tirzepatide Oral", fmdPrice: n(rows[39][6]), regularPrice: n(rows[39][7]), olCost: n(rows[39][8]), fmdRevenue: n(rows[39][9]), regularRevenue: n(rows[39][10]) },
      { product: "Tirzepatide Injection", fmdPrice: n(rows[40][6]), regularPrice: n(rows[40][7]), olCost: n(rows[40][8]), fmdRevenue: n(rows[40][9]), regularRevenue: n(rows[40][10]) },
    ],
    threeMonth: [
      { product: "Semaglutide Oral", regularPrice: n(rows[44][6]), olCost: n(rows[44][7]), revenue: n(rows[44][8]) },
      { product: "Semaglutide Injection", regularPrice: n(rows[45][6]), olCost: n(rows[45][7]), revenue: n(rows[45][8]) },
      { product: "Tirzepatide Oral", regularPrice: n(rows[46][6]), olCost: n(rows[46][7]), revenue: n(rows[46][8]) },
      { product: "Tirzepatide Injection", regularPrice: n(rows[47][6]), olCost: n(rows[47][7]), revenue: n(rows[47][8]) },
    ],
    sixMonth: [
      { product: "Semaglutide Oral", regularPrice: n(rows[51][6]), olCost: n(rows[51][7]), revenue: n(rows[51][8]) },
      { product: "Semaglutide Injection", regularPrice: n(rows[52][6]), olCost: n(rows[52][7]), revenue: n(rows[52][8]) },
      { product: "Tirzepatide Oral", regularPrice: n(rows[53][6]), olCost: n(rows[53][7]), revenue: n(rows[53][8]) },
      { product: "Tirzepatide Injection", regularPrice: n(rows[54][6]), olCost: n(rows[54][7]), revenue: n(rows[54][8]) },
    ],
  };

  // --- Monthly Costs (rows 56-67) ---
  const costs = [
    { name: "Drip", amount: n(rows[57][6]) },
    { name: "OL Monthly", amount: n(rows[58][6]) },
    { name: "Meta Ad Spend", amount: n(rows[59][6]) },
    { name: "Tellescope", amount: n(rows[60][6]) },
    { name: "Zendesk", amount: n(rows[61][6]) },
    { name: "Salaries + Lease", amount: n(rows[62][6]) },
    { name: "Creatives", amount: n(rows[63][6]) },
    { name: "Customerio", amount: n(rows[64][6]) },
    { name: "Everflow", amount: n(rows[65][6]) },
    { name: "Embeddables", amount: n(rows[66][6]) },
  ];
  const totalCosts = n(rows[67][6]);

  // --- Pricing Tool data (rows 37-41 columns M-Q) ---
  const pricingTool = {
    example: {
      administration: str(rows[37][13]),
      plan: n(rows[38][13]),
      medication: str(rows[39][13]),
      pricing: n(rows[40][13]),
      generalPrice: n(rows[41][13]),
      generalFee: n(rows[42][13]),
      generalProfit: n(rows[43][13]),
      profit: n(rows[44][13]),
    },
    reverse: {
      method: str(rows[37][16]),
      month: n(rows[38][16]),
      medication: str(rows[39][16]),
      profit: n(rows[40][16]),
      generalPrice: n(rows[41][16]),
      generalFee: n(rows[42][16]),
      generalProfit: n(rows[43][16]),
      pricing: n(rows[44][16]),
    },
  };

  // --- Break-Even (rows 48-57) ---
  const breakEven = {
    withFMD: {
      monthly: { cheapest: n(rows[50][13]), expensive: n(rows[50][14]), cheapestDay: n(rows[50][15]), expensiveDay: n(rows[50][16]) },
      threeMonth: { cheapest: n(rows[51][13]), expensive: n(rows[51][14]), cheapestDay: n(rows[51][15]), expensiveDay: n(rows[51][16]) },
      sixMonth: { cheapest: n(rows[52][13]), expensive: n(rows[52][14]), cheapestDay: n(rows[52][15]), expensiveDay: n(rows[52][16]) },
    },
    noFMD: {
      monthly: { cheapest: n(rows[57][13]), expensive: n(rows[57][14]), cheapestDay: n(rows[57][15]), expensiveDay: n(rows[57][16]) },
    },
    specific: {
      monthly: { cheapest: n(rows[50][19]), cheapestDay: n(rows[50][20]) },
    },
  };

  // --- Weighted Break-Even (rows 60-84) ---
  const salesStats = {
    semaglutide: n(rows[61][13]),
    tirzepatide: n(rows[62][13]),
    injectionSales: n(rows[63][13]),
    oralSales: n(rows[64][13]),
    oneMonth: n(rows[65][13]),
    threeMonths: n(rows[66][13]),
    sixMonths: n(rows[67][13]),
    totalSales: n(rows[68][13]),
  };

  const productFrequency = [
    { label: "Sema | 1 | Inj.", weight: n(rows[71][13]) },
    { label: "Tirz | 1 | Inj.", weight: n(rows[71][15]) },
    { label: "Sema | 3 | Inj.", weight: n(rows[72][13]) },
    { label: "Tirz | 3 | Inj.", weight: n(rows[72][15]) },
    { label: "Sema | 6 | Inj.", weight: n(rows[73][13]) },
    { label: "Tirz | 6 | Inj.", weight: n(rows[73][15]) },
    { label: "Sema | 1 | Oral", weight: n(rows[74][13]) },
    { label: "Tirz | 1 | Oral", weight: n(rows[74][15]) },
    { label: "Sema | 3 | Oral", weight: n(rows[75][13]) },
    { label: "Tirz | 3 | Oral", weight: n(rows[75][15]) },
    { label: "Sema | 6 | Oral", weight: n(rows[76][13]) },
    { label: "Tirz | 6 | Oral", weight: n(rows[76][15]) },
  ];

  const weightedBreakEven = {
    fmd: { unitsPerMonth: n(rows[80][13]), unitsPerDay: n(rows[80][14]) },
    noFmd: { unitsPerMonth: n(rows[81][13]), unitsPerDay: n(rows[81][14]) },
  };

  return {
    contracts,
    currentPricing,
    costs,
    totalCosts,
    pricingTool,
    breakEven,
    salesStats,
    productFrequency,
    weightedBreakEven,
  };
}

function n(v) {
  const num = parseFloat(v);
  return isNaN(num) ? 0 : num;
}

function str(v) {
  return v ? String(v).trim() : "";
}

// Build a lookup for pricing tool calculations
function buildPricingLookup() {
  const data = parseExcel();
  const lookup = {};
  for (const [plan, products] of Object.entries(data.contracts)) {
    for (const p of products) {
      const med = p.product.includes("Semaglutide") ? "Semaglutide" : "Tirzepatide";
      const method = p.product.includes("Injection") ? "Injection" : "Oral";
      const months = plan === "monthly" ? 1 : plan === "threeMonth" ? 3 : 6;
      lookup[`${med}|${method}|${months}`] = { pricing: p.pricing, olCost: p.olCost, netProfit: p.netProfit };
    }
  }
  // Also add current pricing
  for (const [plan, products] of Object.entries(data.currentPricing)) {
    const months = plan === "monthly" ? 1 : plan === "threeMonth" ? 3 : 6;
    for (const p of products) {
      const med = p.product.includes("Semaglutide") ? "Semaglutide" : "Tirzepatide";
      const method = p.product.includes("Injection") ? "Injection" : "Oral";
      const key = `current|${med}|${method}|${months}`;
      if (months === 1) {
        lookup[key] = { fmdPrice: p.fmdPrice, regularPrice: p.regularPrice, olCost: p.olCost, fmdRevenue: p.fmdRevenue, regularRevenue: p.regularRevenue };
      } else {
        lookup[key] = { regularPrice: p.regularPrice, olCost: p.olCost, revenue: p.revenue };
      }
    }
  }
  return lookup;
}

module.exports = { parseExcel, buildPricingLookup };
