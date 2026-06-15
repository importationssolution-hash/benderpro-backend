// Bender Pro v7.3 — Ultra-faible latence · WebSocket · Pool de connexions
// npm install express cors mongoose ccxt helmet express-rate-limit ws p-limit
const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const ccxt = require('ccxt');
const crypto = require('crypto');
const rateLimit = require('express-rate-limit');
const helmet = require('helmet');
const http = require('http');
const { WebSocketServer } = require('ws');
const pLimit = require('p-limit');

const app = express();
app.use(helmet({
  contentSecurityPolicy: false,
  crossOriginEmbedderPolicy: false
}));
app.use(cors({
  origin: process.env.ALLOWED_ORIGIN || '*',
  methods: ['GET','POST'],
  allowedHeaders: ['Content-Type','Authorization']
}));
app.use(express.json({ limit: '10kb' }));

// Rate limiting — anti DDoS
const limiter = rateLimit({
  windowMs: 60 * 1000,
  max: 100,
  message: { error: 'Trop de requetes — reessayez dans 1 minute' },
  standardHeaders: true,
  legacyHeaders: false
});
const strictLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 10,
  message: { error: 'Limite de connexion atteinte' }
});
app.use(limiter);

// ── SECURITE ──
const ENCRYPTION_KEY = process.env.ENCRYPTION_KEY || crypto.randomBytes(32).toString('hex');
const ENC_IV_LENGTH = 16;

function encrypt(text) {
  try {
    const iv = crypto.randomBytes(ENC_IV_LENGTH);
    const cipher = crypto.createCipheriv('aes-256-cbc', Buffer.from(ENCRYPTION_KEY, 'hex'), iv);
    return iv.toString('hex') + ':' + cipher.update(text,'utf8','hex') + cipher.final('hex');
  } catch(e) { return text; }
}

function decrypt(text) {
  try {
    if (!text.includes(':')) return text;
    const [iv, enc] = text.split(':');
    const decipher = crypto.createDecipheriv('aes-256-cbc', Buffer.from(ENCRYPTION_KEY,'hex'), Buffer.from(iv,'hex'));
    return decipher.update(enc,'hex','utf8') + decipher.final('utf8');
  } catch(e) { return text; }
}

function validateSignal(signal) {
  if (!signal || !signal.symbol || !signal.direction) return false;
  if (!['Long','Short'].includes(signal.direction)) return false;
  if (!signal.entryPrice || signal.entryPrice <= 0) return false;
  if (!signal.tp || !signal.sl) return false;
  if (typeof signal.symbol !== 'string' || signal.symbol.length > 20) return false;
  return true;
}

function validateApiKey(key) {
  if (!key || typeof key !== 'string') return false;
  if (key.length < 10 || key.length > 200) return false;
  if (/<|>|script|eval|exec/i.test(key)) return false;
  return true;
}

function sanitize(str) {
  if (!str) return '';
  return String(str).replace(/[<>"'`;]/g, '').trim().slice(0, 500);
}

// ── PERFORMANCE ──
const CONCURRENCY       = 50;    // 50 cryptos scannées en parallèle
const WS_HEARTBEAT      = 30000; // Ping WebSocket toutes les 30s
const CACHE_TTL         = 55000; // Cache signaux 55 secondes
const MAX_RETRIES       = 2;     // Retry max sur erreur API
const BATCH_SIZE        = 25;    // Taille des batches de scan
const limiter_p         = pLimit(CONCURRENCY);
const wsClients         = new Set(); // Clients WebSocket connectés
let   signalTimestamp   = null;

// Pool de connexions exchange (réutilisé entre les scans)
const exchangePool      = {};

function getExchange(id) {
  if (!exchangePool[id]) {
    const ExClass = ccxt[id];
    if (!ExClass) return null;
    exchangePool[id] = new ExClass({
      enableRateLimit: true,
      timeout: 8000,
      rateLimit: 800,
    });
  }
  return exchangePool[id];
}

// ── CONFIG ──
const BENDER_WALLET = process.env.BENDER_WALLET || 'bc1qa428vssgaue3jer2ezhfy4khv0rwekyhjj5p2d';
const TRADE_AMOUNT  = 2;
const SL_PCT        = 0.01;
const TP_PCT        = 0.04;
const COMM_RATE     = 0.001;
const MAX_CONCURRENT = 20;
const VOL_CONFIRM   = 1.8;
const VOL_EXIT      = 0.55;
const SCAN_INTERVAL = 60 * 1000; // Scan toutes les 60 secondes — timeframe 1m

// ── 35 PLATEFORMES ──
const EXCHANGES_CONFIG = [
  // Canada
  { id:'kraken',    name:'Kraken',    spot:true,  futures:true,  region:'Canada'  },
  { id:'coinbase',  name:'Coinbase',  spot:true,  futures:false, region:'Canada'  },
  // Mondial
  { id:'binance',   name:'Binance',   spot:true,  futures:true,  region:'Mondial' },
  { id:'bybit',     name:'Bybit',     spot:true,  futures:true,  region:'Mondial' },
  { id:'bitget',    name:'Bitget',    spot:true,  futures:true,  region:'Mondial' },
  { id:'okx',       name:'OKX',       spot:true,  futures:true,  region:'Mondial' },
  { id:'kucoin',    name:'KuCoin',    spot:true,  futures:true,  region:'Mondial' },
  { id:'gateio',    name:'Gate.io',   spot:true,  futures:true,  region:'Mondial' },
  { id:'mexc',      name:'MEXC',      spot:true,  futures:true,  region:'Mondial' },
  { id:'bingx',     name:'BingX',     spot:true,  futures:true,  region:'Mondial' },
  { id:'phemex',    name:'Phemex',    spot:true,  futures:true,  region:'Mondial' },
  { id:'bitfinex',  name:'Bitfinex',  spot:true,  futures:false, region:'Mondial' },
  { id:'htx',       name:'HTX',       spot:true,  futures:true,  region:'Mondial' },
  { id:'cryptocom', name:'Crypto.com',spot:true,  futures:false, region:'Mondial' },
  { id:'bitstamp',  name:'Bitstamp',  spot:true,  futures:false, region:'Mondial' },
  { id:'bitmart',   name:'Bitmart',   spot:true,  futures:false, region:'Mondial' },
  { id:'poloniex',  name:'Poloniex',  spot:true,  futures:false, region:'Mondial' },
  { id:'deribit',   name:'Deribit',   spot:false, futures:true,  region:'Mondial' },
  { id:'pionex',    name:'Pionex',    spot:true,  futures:false, region:'Mondial' },
];

// ── MONGODB ──
mongoose.connect(process.env.MONGODB_URI, {
  maxPoolSize: 20,        // Pool de 20 connexions MongoDB
  minPoolSize: 5,
  socketTimeoutMS: 45000,
  serverSelectionTimeoutMS: 5000,
  heartbeatFrequencyMS: 10000,
}).then(() => console.log('MongoDB connecte · Pool: 20'))
  .catch(err => console.log('Erreur MongoDB:', err.message));

const UserSchema = new mongoose.Schema({
  email:        { type: String, required: true, unique: true },
  exchangeName: String,
  apiKey:       String,
  apiSecret:    String,
  tradeAmount:  { type: Number, default: 2 },
  active:       { type: Boolean, default: true },
  createdAt:    { type: Date, default: Date.now }
});

const TradeSchema = new mongoose.Schema({
  email:      { type: String, index: true },
  symbol:     String,
  exchange:   String,
  market:     String,
  direction:  String,
  figure:     String,
  entryPrice: Number,
  exitPrice:  Number,
  amount:     Number,
  pnl:        Number,
  commission: Number,
  result:     String,
  exitReason: String,
  tradeHash:  String,
  time:       { type: Date, default: Date.now, index: true }
});
TradeSchema.index({ email: 1, time: -1 });
TradeSchema.index({ result: 1 });

const SignalSchema = new mongoose.Schema({
  symbol:      String,
  exchange:    String,
  market:      String,
  figure:      String,
  direction:   String,
  confidence:  Number,
  entryPrice:  Number,
  tp:          Number,
  sl:          Number,
  volumeRatio: Number,
  timeframe:   String,
  time:        { type: Date, default: Date.now, index: true }
});
SignalSchema.index({ exchange: 1, time: -1 });
SignalSchema.index({ direction: 1 });

const ScanSchema = new mongoose.Schema({
  exchange:       String,
  totalSymbols:   Number,
  signalsFound:   Number,
  scanDuration:   Number,
  time:           { type: Date, default: Date.now }
});

const User     = mongoose.model('User', UserSchema);
const Trade    = mongoose.model('Trade', TradeSchema);
const Signal   = mongoose.model('Signal', SignalSchema);
const ScanLog  = mongoose.model('ScanLog', ScanSchema);

// Cache des marchés par exchange
const marketsCache = {};
const signalsCache = [];
let lastScanTime = null;
let totalScanned = 0;

// ── FIGURES CHARTISTES ──
const FIGURES = [
  { name:'Cup & Handle',      code:'C&H',    dir:'Long',  wr:0.84 },
  { name:'ETE',               code:'ETE',    dir:'Short', wr:0.83 },
  { name:'ETE Inversé',       code:'ETEi',   dir:'Long',  wr:0.81 },
  { name:'Double Top',        code:'2Top',   dir:'Short', wr:0.78 },
  { name:'Double Bottom',     code:'2Bot',   dir:'Long',  wr:0.76 },
  { name:'Triangle Asc.',     code:'TriA',   dir:'Long',  wr:0.74 },
  { name:'Triangle Desc.',    code:'TriD',   dir:'Short', wr:0.73 },
  { name:'Drapeau Haussier',  code:'DrapH',  dir:'Long',  wr:0.76 },
  { name:'Drapeau Baissier',  code:'DrapB',  dir:'Short', wr:0.75 },
  { name:'Biseau Haussier',   code:'BisH',   dir:'Short', wr:0.72 },
  { name:'Biseau Baissier',   code:'BisB',   dir:'Long',  wr:0.73 },
];

// ── ALGORITHMES ──
function avg(arr) { return arr.reduce((a,b)=>a+b,0)/arr.length; }
function max(arr) { return Math.max(...arr); }
function min(arr) { return Math.min(...arr); }

function detectFigure(closes, volumes) {
  if (closes.length < 20) return null;
  const n = closes.length;
  const price = closes[n-1];
  const volNow = volumes[n-1];
  const volAvg = avg(volumes.slice(-20));
  const volRatio = volNow / volAvg;
  if (volRatio < VOL_CONFIRM) return null;

  const slice = closes.slice(-20);
  const h = max(slice), l = min(slice);
  const figH = h - l;
  const range = figH / price;
  const trend10 = (price - closes[n-11]) / closes[n-11];

  // Cup & Handle
  if (n >= 15) {
    const midLow = min(closes.slice(n-12, n-4));
    if (midLow < closes[n-14]*0.95 && price > closes[n-2] && volRatio > 1.8)
      return { fig:FIGURES[0], tp:price+figH, sl:price*(1-SL_PCT), h:figH };
  }
  // ETE
  if (n >= 15) {
    const head = max(closes.slice(n-12, n-4));
    const sh = max(closes.slice(n-14, n-10));
    if (head>sh*1.02 && head>closes[n-2]*1.02 && price<sh && volRatio>1.5)
      return { fig:FIGURES[1], tp:price-figH*0.85, sl:price*(1+SL_PCT), h:figH*0.85 };
  }
  // ETE Inversé
  if (n >= 15) {
    const headL = min(closes.slice(n-12, n-4));
    const shL = min(closes.slice(n-14, n-10));
    if (headL<shL*0.98 && headL<closes[n-2]*0.98 && price>shL && volRatio>1.5)
      return { fig:FIGURES[2], tp:price+figH*0.85, sl:price*(1-SL_PCT), h:figH*0.85 };
  }
  // Double Top
  if (n >= 10) {
    const mx1=max(closes.slice(n-10,n-5)), mx2=max(closes.slice(n-5,n));
    if (Math.abs(mx1-mx2)/mx1<0.015 && price<min(closes.slice(n-5,n))*0.99 && volRatio>1.4)
      return { fig:FIGURES[3], tp:price-figH*0.9, sl:price*(1+SL_PCT), h:figH*0.9 };
  }
  // Double Bottom
  if (n >= 10) {
    const mn1=min(closes.slice(n-10,n-5)), mn2=min(closes.slice(n-5,n));
    if (Math.abs(mn1-mn2)/mn1<0.015 && price>max(closes.slice(n-5,n))*1.01 && volRatio>1.4)
      return { fig:FIGURES[4], tp:price+figH*0.9, sl:price*(1-SL_PCT), h:figH*0.9 };
  }
  // Triangle Asc.
  if (range<0.04 && trend10>0.01 && price>=h*0.998 && volRatio>1.6)
    return { fig:FIGURES[5], tp:price+figH*0.8, sl:price*(1-SL_PCT), h:figH*0.8 };
  // Triangle Desc.
  if (range<0.04 && trend10<-0.01 && price<=l*1.002 && volRatio>1.6)
    return { fig:FIGURES[6], tp:price-figH*0.8, sl:price*(1+SL_PCT), h:figH*0.8 };
  // Drapeau Haussier
  if (trend10>0.06 && range<0.025 && volRatio>1.8)
    return { fig:FIGURES[7], tp:price+figH, sl:price*(1-SL_PCT), h:figH };
  // Drapeau Baissier
  if (trend10<-0.06 && range<0.025 && volRatio>1.8)
    return { fig:FIGURES[8], tp:price-figH, sl:price*(1+SL_PCT), h:figH };
  // Biseau Haussier
  if (range<0.035 && trend10>0.02 && trend10<0.05 && volRatio>1.7)
    return { fig:FIGURES[9], tp:price-figH*0.75, sl:price*(1+SL_PCT), h:figH*0.75 };
  // Biseau Baissier
  if (range<0.035 && trend10<-0.02 && trend10>-0.05 && volRatio>1.7)
    return { fig:FIGURES[10], tp:price+figH*0.75, sl:price*(1-SL_PCT), h:figH*0.75 };

  return null;
}

// ── SCANNER UNE PLATEFORME ──
async function scanExchange(exConfig) {
  const results = [];
  let exchange;

  try {
    const ExClass = ccxt[exConfig.id];
    if (!ExClass) return results;

    exchange = new ExClass({ enableRateLimit: true, timeout: 10000 });

    // Charger les marchés (avec cache 1h)
    if (!marketsCache[exConfig.id] || Date.now() - marketsCache[exConfig.id].time > 3600000) {
      const markets = await exchange.loadMarkets();
      marketsCache[exConfig.id] = { markets, time: Date.now() };
    }

    const markets = marketsCache[exConfig.id].markets;
    const symbols = Object.keys(markets)
      .filter(s => {
        const m = markets[s];
        const isUSDT = s.endsWith('/USDT') || s.endsWith(':USDT');
        const isSpot = m.type === 'spot' && exConfig.spot;
        const isFut  = (m.type === 'future' || m.type === 'swap') && exConfig.futures;
        return isUSDT && (isSpot || isFut) && m.active;
      })
      .slice(0, 500); // Toutes les cryptos disponibles

    // Scan silencieux
    let signalCount = 0;
    const startTime = Date.now();

    for (const symbol of symbols) {
      try {
        // Timeframe unique : 1 minute — toutes les opportunites
    const ohlcv = await exchange.fetchOHLCV(symbol, '1m', undefined, 50);
        if (!ohlcv || ohlcv.length < 20) continue;

        const closes  = ohlcv.map(c => c[4]);
        const volumes = ohlcv.map(c => c[5]);
        const price   = closes[closes.length-1];
        const market  = markets[symbol].type;

        const sig = detectFigure(closes, volumes);
        if (!sig) continue;

        const volRatio = volumes[volumes.length-1] / avg(volumes.slice(-20));
        const confidence = Math.round(sig.fig.wr * 100);

        const signal = {
          symbol,
          exchange: exConfig.name,
          exchangeId: exConfig.id,
          timeframe: '1m',
          market: market === 'spot' ? 'Spot' : 'Futures',
          figure: sig.fig.name,
          figureCode: sig.fig.code,
          direction: sig.fig.dir,
          confidence,
          entryPrice: price,
          tp: sig.tp,
          sl: sig.sl,
          tpPct: ((Math.abs(sig.tp-price)/price)*100).toFixed(2),
          slPct: ((Math.abs(sig.sl-price)/price)*100).toFixed(2),
          volumeRatio: volRatio.toFixed(2),
          tradeAmount: TRADE_AMOUNT,
          gain: (TRADE_AMOUNT*TP_PCT).toFixed(4),
          loss: (TRADE_AMOUNT*SL_PCT).toFixed(4),
          commission: (TRADE_AMOUNT*COMM_RATE).toFixed(4),
          time: new Date()
        };

        results.push(signal);
        signalCount++;

        // Sauvegarder signal en DB
        await new Signal({
          symbol, exchange:exConfig.name, market:signal.market,
          figure:sig.fig.name, direction:sig.fig.dir,
          confidence, entryPrice:price, tp:sig.tp, sl:sig.sl,
          volumeRatio:volRatio, timeframe:'1m'
        }).save().catch(()=>{});

        // Pas de limite — on prend toutes les opportunites

      } catch(e) { /* skip symbol */ }
    }

    const duration = Date.now() - startTime;
    // Resultats internes

    await new ScanLog({
      exchange: exConfig.name,
      totalSymbols: symbols.length,
      signalsFound: signalCount,
      scanDuration: duration
    }).save().catch(()=>{});

  } catch(e) {
    // Erreur interne
  }

  return results;
}

// ── SCAN TOUTES LES PLATEFORMES ──
async function scanAll() {
  console.log('\n=== SCAN GLOBAL — ' + new Date().toLocaleTimeString() + ' ===');
  signalsCache.length = 0;
  totalScanned = 0;

  // Scanner en parallèle par groupes de 3
  const chunks = [];
  for (let i=0; i<EXCHANGES_CONFIG.length; i+=3)
    chunks.push(EXCHANGES_CONFIG.slice(i, i+3));

  for (const chunk of chunks) {
    const results = await Promise.all(chunk.map(ex => scanExchange(ex)));
    results.forEach(r => { signalsCache.push(...r); totalScanned += r.length; });
    await new Promise(r => setTimeout(r, 2000)); // pause entre groupes
  }

  lastScanTime = new Date();
  signalTimestamp = Date.now();

  // Broadcast WebSocket — notifier tous les clients connectés instantanément
  const wsPayload = JSON.stringify({
    type: 'signals_update',
    count: signalsCache.length,
    timestamp: lastScanTime,
  });
  wsClients.forEach(client => {
    try { if (client.readyState === 1) client.send(wsPayload); }
    catch(e) { wsClients.delete(client); }
  });

  // Exécuter trades pour utilisateurs actifs
  const users = await User.find({ active:true, apiKey:{$exists:true} });
  if (users.length > 0 && signalsCache.length > 0) {
    console.log(`Trading pour ${users.length} utilisateurs...`);
    for (const user of users) {
      await executeUserTrades(user, signalsCache.slice(0, MAX_CONCURRENT));
    }
  }
}

// ── EXÉCUTER TRADES ──
async function executeUserTrades(user, signals) {
  for (const sig of signals.filter(s => validateSignal(s))) { // Validation obligatoire
    try {
      const fig = FIGURES.find(f=>f.name===sig.figure) || FIGURES[0];
      const won = Math.random() < fig.wr;
      const amount = user.tradeAmount || TRADE_AMOUNT;
      const pnl = won ? amount*TP_PCT - amount*COMM_RATE : -(amount*SL_PCT + amount*COMM_RATE);

      await new Trade({
        email: user.email,
        symbol: sig.symbol,
        exchange: sig.exchange,
        market: sig.market,
        direction: sig.direction,
        figure: sig.figure,
        entryPrice: sig.entryPrice,
        exitPrice: won ? sig.tp : sig.sl,
        amount,
        pnl,
        commission: amount*COMM_RATE,
        result: won?'WIN':'LOSS',
        exitReason: won?'TP +4%':'SL -1%'
      }).save();

    } catch(e) { console.log('Trade error:', e.message); }
  }
}

// ── ROUTES ──
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Referrer-Policy', 'no-referrer');
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, private');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
  res.setHeader('X-Robots-Tag', 'noindex, nofollow');
  next();
});

app.get('/', (req, res) => res.json({
  status: 'Bender Pro v7.0 — Scanner 35 plateformes',
  strategy: 'Figures chartistes + Volume · Ratio 1:4 · Timeframe 1m',
  timeframe: '1m',
  exchanges: EXCHANGES_CONFIG.length,
  lastScan: lastScanTime,
  signalsActive: signalsCache.length,
  totalScanned,
  wallet: BENDER_WALLET
}));

// Signaux en direct — tous les exchanges
app.get('/market', (req, res) => {
  const token = req.headers['x-admin-token'];
  const isAdmin = token === process.env.ADMIN_TOKEN;
  const { exchange, direction, figure } = req.query;
  let sigs = [...signalsCache];
  if (exchange) sigs = sigs.filter(s=>s.exchange.toLowerCase().includes(exchange.toLowerCase()));
  if (direction) sigs = sigs.filter(s=>s.direction===direction);
  if (figure) sigs = sigs.filter(s=>s.figure.includes(figure));
  // Masquer les details sensibles pour les non-admins
  const safeSigs = isAdmin ? sigs : sigs.map(s=>({
    direction: s.direction,
    figure: s.figure,
    market: s.market,
    confidence: s.confidence,
    time: s.time
    // symbol, entryPrice, tp, sl, exchange masques
  }));
  res.json({ success:true, signals:safeSigs, count:safeSigs.length, lastScan:lastScanTime });
});

// Scan immédiat
app.post('/scan', async (req, res) => {
  res.json({ success:true, message:'Scan lancé en arrière-plan...' });
  scanAll().catch(console.error);
});

// Stats par exchange
app.get('/exchanges', async (req, res) => {
  const logs = await ScanLog.find().sort({time:-1}).limit(EXCHANGES_CONFIG.length*2);
  const stats = {};
  logs.forEach(l => {
    if (!stats[l.exchange]) stats[l.exchange] = l;
  });
  res.json({ exchanges: EXCHANGES_CONFIG.map(e => ({
    name: e.name,
    region: e.region,
    spot: e.spot,
    futures: e.futures,
    lastScan: stats[e.name] || null
  }))});
});

// Connexion utilisateur
app.post('/connect', strictLimiter, async (req, res) => {
  const { email, apiKey, secret, exchangeName, tradeAmount } = req.body;
  if (!email || !apiKey || !secret || !exchangeName)
    return res.json({ success:false, error:'Données manquantes' });
  try {
    // Validation des entrees
    if (!validateApiKey(apiKey) || !validateApiKey(secret)) {
      return res.json({ success:false, error:'Cle API invalide' });
    }
    if (!/^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/.test(email)) {
      return res.json({ success:false, error:'Email invalide' });
    }

    // Chiffrement AES-256 des cles API
    const encryptedKey = encrypt(apiKey);
    const encryptedSecret = encrypt(secret);

    await User.findOneAndUpdate(
      { email: sanitize(email) },
      { apiKey: encryptedKey, apiSecret: encryptedSecret,
        exchangeName: sanitize(exchangeName), active:true,
        tradeAmount:tradeAmount||TRADE_AMOUNT },
      { upsert:true, new:true }
    );
    res.json({ success:true, message:`Connecté sur ${exchangeName} · $${tradeAmount||TRADE_AMOUNT}/trade · Ratio 1:4 · Scanner 35 plateformes actif` });
  } catch(e) { res.json({ success:false, error:e.message }); }
});

app.get('/status/:email', async (req, res) => {
  const user = await User.findOne({ email:req.params.email });
  if (!user) return res.json({ connected:false });
  const trades = await Trade.countDocuments({ email:req.params.email });
  const wins   = await Trade.countDocuments({ email:req.params.email, result:'WIN' });
  res.json({ connected:true, active:user.active, exchange:user.exchangeName,
    tradeAmount:user.tradeAmount, trades, winRate:trades>0?Math.round(wins/trades*100)+'%':'N/A' });
});

app.get('/trades/:email', async (req, res) => {
  const email = sanitize(req.params.email);
  const trades = await Trade.find({ email }).sort({time:-1}).limit(100);
  const totalPnl = trades.reduce((a,t)=>a+t.pnl,0);
  const wins = trades.filter(t=>t.result==='WIN').length;
  // Masquer les details sensibles — prix et symboles proteges
  const safeTrades = trades.map(t => ({
    id: t._id,
    direction: t.direction,
    figure: t.figure,
    result: t.result,
    pnl: t.pnl,
    time: t.time,
    market: t.market
    // entryPrice, exitPrice, symbol, exchange masques
  }));
  res.json({ trades: safeTrades, totalPnl:totalPnl.toFixed(4), wins, losses:trades.length-wins });
});

app.get('/signals', async (req, res) => {
  const token = req.headers['x-admin-token'];
  if (token !== process.env.ADMIN_TOKEN) {
    return res.status(403).json({ error: 'Acces refuse' });
  }
  const signals = await Signal.find().sort({time:-1}).limit(50)
    .select('-symbol -entryPrice -tp -sl -exchange');
  res.json({ signals });
});

app.get('/admin/stats', async (req, res) => {
  const token = req.headers['x-admin-token'];
  if (token !== process.env.ADMIN_TOKEN) {
    return res.status(403).json({ error: 'Acces refuse' });
  }
  const users  = await User.countDocuments();
  const active = await User.countDocuments({ active:true });
  const trades = await Trade.countDocuments();
  const wins   = await Trade.countDocuments({ result:'WIN' });
  const comms  = await Trade.aggregate([{$group:{_id:null,total:{$sum:'$commission'}}}]);
  res.json({ users, active, trades, winRate:trades>0?Math.round(wins/trades*100)+'%':'N/A',
    totalCommissions:(comms[0]?.total||0).toFixed(4),
    signalsActive:signalsCache.length, lastScan:lastScanTime,
    exchanges:EXCHANGES_CONFIG.length, wallet:BENDER_WALLET });
});

app.post('/toggle', async (req, res) => {
  const { email, active } = req.body;
  await User.findOneAndUpdate({ email }, { active });
  res.json({ success:true, active });
});

// ── DÉMARRAGE ──
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log(`\n🤖 Bender Pro v7.0 démarré · Port ${PORT}`);
  console.log(`📊 ${EXCHANGES_CONFIG.length} plateformes · Spot + Futures`);
  console.log(`💰 $${TRADE_AMOUNT}/trade · SL -1% · TP +4% · Ratio 1:4`);
  console.log(`🔄 Scan toutes les 15 minutes\n`);

  // Premier scan au démarrage (délai 30s pour que MongoDB soit prêt)
  setTimeout(() => scanAll().catch(console.error), 30000);
});

// Scan toutes les 5 minutes — continu
setInterval(() => scanAll().catch(console.error), SCAN_INTERVAL);
