// Bender Pro — Module FUTURES Kraken (separe du Spot)
// Utilise le module https natif de Node.js (pas de node-fetch)
// et crypto natif pour l'authentification HMAC.
//
// IMPORTANT: Kraken Futures est un compte SEPARE de Kraken Spot.
// Il faut une cle API DIFFERENTE, generee sur futures.kraken.com (pas kraken.com).

const mongoose = require('mongoose');
const crypto   = require('crypto');
const https    = require('https');

// ── CONFIG ──
const BENDER_WALLET     = process.env.BENDER_WALLET;
const MONGODB_URI       = process.env.MONGODB_URI;
const FUTURES_BASE_URL  = 'futures.kraken.com';
const COMMISSION_RATE   = 0.001;
const FUTURES_SYMBOLS   = ['PF_XBTUSD', 'PF_ETHUSD', 'PF_SOLUSD'];
const MAX_TRADE_AMOUNT_USD = 50;
const MAX_LEVERAGE         = 10;

// ── MONGODB ──
const FuturesUserSchema = new mongoose.Schema({
  email:       { type: String, required: true, unique: true },
  apiKey:      String,
  apiSecret:   String,
  tradeAmount: { type: Number, default: 2 },
  leverage:    { type: Number, default: 1 },
  active:      { type: Boolean, default: true },
  createdAt:   { type: Date, default: Date.now }
});

const FuturesTradeSchema = new mongoose.Schema({
  email:             String,
  symbol:            String,
  side:              String,
  size:              Number,
  leverage:          Number,
  price:             String,
  commissionUSD:     Number,
  walletDestination: String,
  orderId:           String,
  time:              { type: Date, default: Date.now }
});

const FuturesUser  = mongoose.model('FuturesUser',  FuturesUserSchema);
const FuturesTrade = mongoose.model('FuturesTrade', FuturesTradeSchema);

// ── HTTP natif ──
function httpsGet(hostname, path) {
  return new Promise((resolve, reject) => {
    const req = https.request({ hostname, path, method: 'GET', headers: { 'User-Agent': 'BenderPro/8.0' } }, res => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch(e) { reject(new Error('JSON parse error: ' + data.slice(0, 100))); }
      });
    });
    req.on('error', reject);
    req.setTimeout(10000, () => { req.destroy(); reject(new Error('Timeout')); });
    req.end();
  });
}

function httpsPost(hostname, path, body, headers) {
  return new Promise((resolve, reject) => {
    const postData = body;
    const options = {
      hostname, path, method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(postData), ...headers }
    };
    const req = https.request(options, res => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch(e) { reject(new Error('JSON parse error: ' + data.slice(0, 100))); }
      });
    });
    req.on('error', reject);
    req.setTimeout(10000, () => { req.destroy(); reject(new Error('Timeout')); });
    req.write(postData);
    req.end();
  });
}

// ── AUTHENTIFICATION KRAKEN FUTURES ──
function signFuturesRequest(endpointPath, postData, nonce, apiSecret) {
  const message = postData + nonce + endpointPath;
  const hash = crypto.createHash('sha256').update(message).digest();
  const secretDecoded = Buffer.from(apiSecret, 'base64');
  const hmac = crypto.createHmac('sha512', secretDecoded).update(hash).digest('base64');
  return hmac;
}

async function futuresGet(path, apiKey, apiSecret) {
  const nonce = Date.now().toString();
  const signPath = path.replace('/derivatives', '');
  const authent = signFuturesRequest(signPath, '', nonce, apiSecret);
  return httpsGet(FUTURES_BASE_URL, path, { APIKey: apiKey, Nonce: nonce, Authent: authent });
}

async function futuresPost(path, apiKey, apiSecret, params) {
  const nonce = Date.now().toString();
  const signPath = path.replace('/derivatives', '');
  const postData = new URLSearchParams(params).toString();
  const authent = signFuturesRequest(signPath, postData, nonce, apiSecret);
  return httpsPost(FUTURES_BASE_URL, path, postData, { APIKey: apiKey, Nonce: nonce, Authent: authent });
}

// ── ALGORITHME RSI/EMA (identique au bot Spot) ──
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
  if (p > e50)   score += 1; else score -= 1;
  const conf = Math.min(92, Math.abs(score)*10+55);
  if (score >= 3)  return { signal:'BUY',  confidence: Math.round(conf) };
  if (score <= -3) return { signal:'SELL', confidence: Math.round(conf) };
  return { signal:'WAIT', confidence: Math.round(conf) };
}

// Recupere les bougies 1h via l'API publique Futures (sans auth)
async function fetchFuturesCandles(symbol) {
  try {
    const data = await httpsGet('futures.kraken.com', `/api/charts/v1/trade/${symbol}/1h?count=100`);
    if (!data.candles) return [];
    return data.candles.map(c => parseFloat(c.close));
  } catch (e) {
    console.log('Erreur fetchFuturesCandles', symbol, ':', e.message);
    return [];
  }
}

function getSafeTradeAmount(v) { return Math.min(Math.max(Number(v)||0, 0), MAX_TRADE_AMOUNT_USD); }
function getSafeLeverage(v)    { return Math.min(Math.max(Number(v)||1, 1), MAX_LEVERAGE); }

// ── TRADING REEL FUTURES ──
async function tradeUserFutures(user) {
  try {
    console.log('Futures trading pour:', user.email);
    const accountsRes = await futuresGet('/derivatives/api/v3/accounts', user.apiKey, user.apiSecret);
    if (accountsRes.result !== 'success') {
      console.log('Erreur compte futures:', accountsRes.error || JSON.stringify(accountsRes));
      return;
    }
    const availableMargin = accountsRes.accounts?.flex?.availableMargin || 0;
    console.log(`Marge disponible: ${availableMargin} USD`);
    if (availableMargin <= 0) { console.log('Aucune marge disponible'); return; }

    const safeAmount   = getSafeTradeAmount(user.tradeAmount);
    const safeLeverage = getSafeLeverage(user.leverage);

    for (const symbol of FUTURES_SYMBOLS) {
      try {
        const prices = await fetchFuturesCandles(symbol);
        if (prices.length < 20) continue;
        const sig   = getSignal(prices);
        const price = prices[prices.length - 1];
        console.log(symbol, '-', sig.signal, sig.confidence + '% · levier ' + safeLeverage + 'x');
        if (sig.signal === 'WAIT' || sig.confidence <= 65) continue;
        if (availableMargin < safeAmount) { console.log('Marge insuffisante pour', symbol); continue; }

        const side         = sig.signal === 'BUY' ? 'buy' : 'sell';
        const positionSize = (safeAmount * safeLeverage) / price;

        console.log(`ORDRE FUTURES ${side.toUpperCase()}: ${symbol} · taille ${positionSize.toFixed(4)} · ${safeLeverage}x`);
        const orderRes = await futuresPost('/derivatives/api/v3/sendorder', user.apiKey, user.apiSecret, {
          orderType: 'mkt', symbol, side, size: positionSize.toFixed(4)
        });

        if (orderRes.result === 'success' && orderRes.sendStatus?.status === 'placed') {
          const orderId = orderRes.sendStatus.order_id;
          console.log('Ordre futures execute:', orderId);
          await new FuturesTrade({
            email: user.email, symbol, side, size: positionSize,
            leverage: safeLeverage, price: price.toFixed(2),
            commissionUSD: safeAmount * COMMISSION_RATE,
            walletDestination: BENDER_WALLET, orderId
          }).save();
        } else {
          console.log('Echec ordre futures:', JSON.stringify(orderRes.sendStatus || orderRes.error));
        }
      } catch (e) { console.log('Erreur', symbol, ':', e.message); }
    }
  } catch (e) { console.log('Erreur utilisateur futures', user.email, ':', e.message); }
}

// ── CYCLE ──
async function runFuturesCycle() {
  console.log('=== Bender Pro Futures - Cycle ===');
  console.log('Heure:', new Date().toISOString());
  const users = await FuturesUser.find({ active: true, apiKey: { $exists: true } });
  console.log('Utilisateurs Futures actifs:', users.length);
  for (const user of users) { await tradeUserFutures(user); }
  console.log('=== Cycle Futures termine ===');
}

// ── MAIN ──
async function main() {
  if (!MONGODB_URI) { console.log('MONGODB_URI manquant !'); process.exit(1); }
  await mongoose.connect(MONGODB_URI);
  console.log('MongoDB connecte (Futures)!');
  const runOnce = process.argv.includes('--once');
  if (runOnce) {
    await runFuturesCycle();
    process.exit(0);
  } else {
    await runFuturesCycle();
    setInterval(() => runFuturesCycle().catch(e => console.error('Erreur cycle futures:', e.message)), 60000);
  }
}

if (require.main === module) {
  main().catch(err => { console.error('Erreur fatale futures:', err.message); process.exit(1); });
}

module.exports = { FuturesUser, FuturesTrade, runFuturesCycle, MAX_TRADE_AMOUNT_USD, MAX_LEVERAGE };
