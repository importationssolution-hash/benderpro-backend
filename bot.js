const ccxt = require('ccxt');
const mongoose = require('mongoose');

// ── CONFIG ──
const COMMISSION_RATE    = 0.001;
const BENDER_WALLET      = process.env.BENDER_WALLET;
const MONGODB_URI        = process.env.MONGODB_URI;
const QUOTE_CURRENCIES   = ['USDC', 'USD']; // USDT bloque au Canada
const MAX_TRADE_AMOUNT   = 50;
const MIN_TRADE_AMOUNT   = 5;
const MAX_OPEN_POSITIONS = 20; // max positions ouvertes en meme temps par utilisateur

// ── MONGODB ──
const UserSchema = new mongoose.Schema({
  email: String, exchangeName: String,
  apiKey: String, apiSecret: String,
  tradeAmount: Number, active: Boolean
});
const TradeSchema = new mongoose.Schema({
  email: String, type: String, symbol: String,
  price: String, commissionUSD: Number,
  walletDestination: String, time: { type: Date, default: Date.now }
});
const User  = mongoose.model('User',  UserSchema);
const Trade = mongoose.model('Trade', TradeSchema);

// ── SECURITE MONTANT ──
function safeAmount(requested) {
  const n = Number(requested) || 0;
  if (n <= 0) return 0;
  if (n < MIN_TRADE_AMOUNT) return MIN_TRADE_AMOUNT;
  return Math.min(n, MAX_TRADE_AMOUNT);
}

// ── ALGORITHME RSI + EMA ──
function calcRSI(prices) {
  if (prices.length < 15) return 50;
  let g = 0, l = 0;
  for (let i = prices.length - 14; i < prices.length; i++) {
    const d = prices[i] - prices[i-1];
    if (d > 0) g += d; else l -= d;
  }
  return 100 - (100 / (1 + (g/14) / ((l/14)||0.001)));
}
function calcEMA(prices, n) {
  if (prices.length < n) return prices[prices.length-1];
  const k = 2/(n+1);
  let e = prices.slice(0,n).reduce((a,b)=>a+b)/n;
  for (let i = n; i < prices.length; i++) e = prices[i]*k+e*(1-k);
  return e;
}
function getSignal(prices) {
  const rsi = calcRSI(prices);
  const e20 = calcEMA(prices, 20);
  const e50 = calcEMA(prices, 50);
  const p   = prices[prices.length-1];
  let score = 0;
  if (rsi < 30) score += 3; else if (rsi < 40) score += 1;
  else if (rsi > 70) score -= 3; else if (rsi > 60) score -= 1;
  if (e20 > e50) score += 1; else score -= 1;
  if (p   > e50) score += 1; else score -= 1;
  const conf = Math.min(92, Math.abs(score)*10+55);
  if (score >= 3)  return { signal:'BUY',  confidence: Math.round(conf) };
  if (score <= -3) return { signal:'SELL', confidence: Math.round(conf) };
  return             { signal:'WAIT', confidence: Math.round(conf) };
}

// ── RECUPERE TOUTES LES PAIRES USDC/USD ACTIVES ──
async function fetchAllPairs(exchange) {
  const markets = await exchange.loadMarkets();
  return Object.keys(markets).filter(s => {
    const m = markets[s];
    const matchesQuote = QUOTE_CURRENCIES.some(q => s.endsWith('/' + q));
    return matchesQuote && m.active !== false && (m.spot === true || m.type === 'spot');
  });
}

// ── TRADING REEL SUR TOUTES LES PAIRES ──
async function tradeUser(user) {
  try {
    const ExClass = ccxt[user.exchangeName.toLowerCase()];
    if (!ExClass) { console.log('Exchange non supporte:', user.exchangeName); return; }

    const exchange = new ExClass({ apiKey: user.apiKey, secret: user.apiSecret, enableRateLimit: true });
    console.log('Trading pour:', user.email, 'sur', user.exchangeName);

    let balance;
    try { balance = await exchange.fetchBalance(); }
    catch (e) { console.log('Erreur fetchBalance:', e.message); return; }

    const usdc = balance.USDC?.free || 0;
    const usd  = balance.USD?.free  || 0;
    console.log(`Balance — USDC:${usdc} USD:${usd}`);

    if (usdc + usd <= 0) {
      console.log('Aucun fonds disponible — aucun trade possible');
      return;
    }

    const amount = safeAmount(user.tradeAmount);
    if (amount <= 0) { console.log('Montant invalide'); return; }

    // Recupere dynamiquement toutes les paires disponibles
    const symbols = await fetchAllPairs(exchange);
    console.log(`${symbols.length} paires USDC/USD disponibles sur Kraken`);

    let ordersPlaced = 0;

    for (const symbol of symbols) {
      // Limite de positions simultanées
      if (ordersPlaced >= MAX_OPEN_POSITIONS) {
        console.log(`Limite de ${MAX_OPEN_POSITIONS} positions atteinte — cycle suivant`);
        break;
      }

      try {
        const [base, quote] = symbol.split('/');
        const quoteBalance  = balance[quote]?.free || 0;

        const ohlcv  = await exchange.fetchOHLCV(symbol, '1h', undefined, 100);
        if (!ohlcv || ohlcv.length < 20) continue;
        const prices = ohlcv.map(c => c[4]);
        const sig    = getSignal(prices);
        const price  = prices[prices.length-1];

        if (sig.signal === 'WAIT' || sig.confidence <= 65) continue;

        console.log(`SIGNAL ${sig.signal} ${sig.confidence}% sur ${symbol} @ $${price}`);

        // ── BUY ──
        if (sig.signal === 'BUY' && quoteBalance >= amount) {
          const qty          = amount / price;
          const commissionUSD = amount * COMMISSION_RATE;
          console.log(`ORDRE REEL BUY: ${symbol} · ${qty.toFixed(6)} ${base} · $${amount}`);
          const order = await exchange.createMarketBuyOrder(symbol, qty);
          console.log('Ordre execute:', order.id);
          await new Trade({ email:user.email, type:'BUY', symbol, price:price.toFixed(6), commissionUSD, walletDestination:BENDER_WALLET }).save();
          ordersPlaced++;
        }

        // ── SELL ──
        if (sig.signal === 'SELL') {
          const baseBalance = balance[base]?.free || 0;
          if (baseBalance > 0.000001) {
            const tradeValue    = baseBalance * price;
            const commissionUSD = tradeValue * COMMISSION_RATE;
            console.log(`ORDRE REEL SELL: ${symbol} · ${baseBalance} ${base}`);
            const order = await exchange.createMarketSellOrder(symbol, baseBalance);
            console.log('Ordre execute:', order.id);
            await new Trade({ email:user.email, type:'SELL', symbol, price:price.toFixed(6), commissionUSD, walletDestination:BENDER_WALLET }).save();
            ordersPlaced++;
          }
        }

      } catch(e) {
        console.log(`Erreur ${symbol}:`, e.message);
      }
    }

    console.log(`Cycle termine — ${ordersPlaced} ordre(s) place(s)`);

  } catch(e) {
    console.log('Erreur utilisateur', user.email, ':', e.message);
  }
}

// ── CYCLE PRINCIPAL ──
async function runCycle() {
  console.log('=== Bender Pro Bot — Cycle trading reel ===');
  console.log('Heure:', new Date().toISOString());
  console.log('Limites: min', MIN_TRADE_AMOUNT, '$ · max', MAX_TRADE_AMOUNT, '$ · max positions:', MAX_OPEN_POSITIONS);

  const users = await User.find({ active: true, apiKey: { $exists: true } });
  console.log('Utilisateurs actifs:', users.length);
  for (const user of users) await tradeUser(user);
  console.log('=== Cycle termine ===\n');
}

// ── MAIN ──
async function main() {
  if (!MONGODB_URI) { console.log('MONGODB_URI manquant !'); process.exit(1); }
  await mongoose.connect(MONGODB_URI);
  console.log('MongoDB connecte !');

  if (process.argv.includes('--once')) {
    await runCycle();
    process.exit(0);
  } else {
    await runCycle();
    setInterval(() => runCycle().catch(e => console.error('Erreur cycle:', e.message)), 60000);
  }
}

main().catch(err => { console.error('Erreur fatale:', err.message); process.exit(1); });
