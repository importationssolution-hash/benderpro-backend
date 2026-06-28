// Bender Pro v9.0 — 35 Plateformes + WebSocket Kraken
// npm install express cors mongoose ccxt helmet ws node-fetch
const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const ccxt = require('ccxt');
const helmet = require('helmet');
const WebSocket = require('ws');
const fetch = (...args) => import('node-fetch').then(({default: f}) => f(...args));

const app = express();
app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors());
app.use(express.json());

// CONFIG
const TRADE_AMOUNT      = 5;
const SL_PCT            = 0.02;
const TP_PCT            = 0.12;
const MAX_CONCURRENT    = 20;
const VOL_CONFIRM       = 1.8;
const SCAN_INTERVAL     = 60 * 1000;
const MAX_PAIRS         = 500;
const MAX_SIGNALS_CACHE = 200;

// MONGODB
mongoose.connect(process.env.MONGODB_URI)
  .then(() => console.log('MongoDB connecte!'))
  .catch(err => console.log('Erreur MongoDB:', err.message));

const UserSchema = new mongoose.Schema({
  email:        { type: String, required: true, unique: true },
  exchangeName: String,
  apiKey:       String,
  apiSecret:    String,
  tradeAmount:  { type: Number, default: 5 },
  currency:     { type: String, default: 'USD' },
  active:       { type: Boolean, default: true },
  xlmWallet:    String,
  createdAt:    { type: Date, default: Date.now }
});

const TradeSchema = new mongoose.Schema({
  email: String, symbol: String, exchange: String, market: String,
  direction: String, figure: String, entryPrice: Number, exitPrice: Number,
  amount: Number, pnl: Number, result: String, exitReason: String,
  timeframe: String, currency: String,
  time: { type: Date, default: Date.now }
});

const SignalSchema = new mongoose.Schema({
  symbol: String, exchange: String, market: String, figure: String,
  direction: String, confidence: Number, entryPrice: Number,
  tp: Number, sl: Number, volumeRatio: Number, timeframe: String,
  time: { type: Date, default: Date.now }
});

const OpenPositionSchema = new mongoose.Schema({
  email: String, symbol: String, exchange: String, exchangeId: String,
  figure: String, entryPrice: Number, tp: Number, sl: Number,
  tpPct: Number, figureTarget: Number, qty: Number, amount: Number,
  currency: String, timeframe: String,
  openedAt: { type: Date, default: Date.now }
});

const BillingSchema = new mongoose.Schema({
  email: String, periodStart: Date, periodEnd: Date,
  totalVolume: Number, totalPnl: Number, commission: Number,
  status: { type: String, default: 'PENDING' },
  paidAt: Date, txHash: String,
  createdAt: { type: Date, default: Date.now }
});

const User         = mongoose.model('User',         UserSchema);
const Trade        = mongoose.model('Trade',        TradeSchema);
const Signal       = mongoose.model('Signal',       SignalSchema);
const OpenPosition = mongoose.model('OpenPosition', OpenPositionSchema);
const Billing      = mongoose.model('Billing',      BillingSchema);

// ══════════════════════════════════════════════════════════════════════
// CONFIGURATION 35 PLATEFORMES
// currencies = devises quote supportées par cette plateforme
// quoteFilter = filtres sur les paires à scanner
// ══════════════════════════════════════════════════════════════════════
const EXCHANGES_CONFIG = [
  // ── CANADA ──
  { id:'kraken',      name:'Kraken',      geo:'BOTH',  currencies:['USD','CAD','EUR'], quoteFilter:['USD','CAD','EUR'], spot:true, futures:true,  ccxt:true  },
  { id:'coinbasepro', name:'Coinbase',    geo:'BOTH',  currencies:['USD','EUR','GBP'], quoteFilter:['USD','EUR'],       spot:true, futures:false, ccxt:true  },
  { id:'gemini',      name:'Gemini',      geo:'BOTH',  currencies:['USD','EUR'],       quoteFilter:['USD'],             spot:true, futures:false, ccxt:true  },
  { id:'bitbuy',      name:'Bitbuy',      geo:'CA',    currencies:['CAD'],             quoteFilter:['CAD'],             spot:true, futures:false, ccxt:true  },
  { id:'ndax',        name:'NDAX',        geo:'CA',    currencies:['CAD'],             quoteFilter:['CAD'],             spot:true, futures:false, ccxt:true  },
  { id:'newton',      name:'Newton',      geo:'CA',    currencies:['CAD'],             quoteFilter:['CAD'],             spot:true, futures:false, ccxt:true  },
  { id:'coinsquare',  name:'Coinsquare',  geo:'CA',    currencies:['CAD'],             quoteFilter:['CAD'],             spot:true, futures:false, ccxt:true  },
  { id:'shakepay',    name:'Shakepay',    geo:'CA',    currencies:['CAD'],             quoteFilter:['CAD'],             spot:true, futures:false, ccxt:false },
  { id:'coinberry',   name:'Coinberry',   geo:'CA',    currencies:['CAD'],             quoteFilter:['CAD'],             spot:true, futures:false, ccxt:false },
  // ── MONDIAL ──
  { id:'binance',     name:'Binance',     geo:'WORLD', currencies:['USDT','USDC'],     quoteFilter:['USDT','USDC'],     spot:true, futures:true,  ccxt:true  },
  { id:'bybit',       name:'Bybit',       geo:'WORLD', currencies:['USDT','USDC'],     quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'bitget',      name:'Bitget',      geo:'WORLD', currencies:['USDT','USDC'],     quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'okx',         name:'OKX',         geo:'WORLD', currencies:['USDT','USDC'],     quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'kucoin',      name:'KuCoin',      geo:'WORLD', currencies:['USDT','BTC'],      quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'gateio',      name:'Gate.io',     geo:'WORLD', currencies:['USDT','USDC'],     quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'mexc',        name:'MEXC',        geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'bingx',       name:'BingX',       geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'phemex',      name:'Phemex',      geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'bitfinex',    name:'Bitfinex',    geo:'WORLD', currencies:['USD','USDT'],      quoteFilter:['USD','USDT'],      spot:true, futures:true,  ccxt:true  },
  { id:'htx',         name:'HTX',         geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'cryptocom',   name:'Crypto.com',  geo:'WORLD', currencies:['USDT','USD'],      quoteFilter:['USDT'],            spot:true, futures:false, ccxt:true  },
  { id:'bitstamp',    name:'Bitstamp',    geo:'WORLD', currencies:['USD','EUR'],       quoteFilter:['USD','EUR'],       spot:true, futures:false, ccxt:true  },
  { id:'bitmart',     name:'Bitmart',     geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'poloniex',    name:'Poloniex',    geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'ascendex',    name:'AscendEX',    geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'xt',          name:'XT.com',      geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'lbank',       name:'LBank',       geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'deribit',     name:'Deribit',     geo:'WORLD', currencies:['USD','USDC'],      quoteFilter:['USD'],             spot:false,futures:true,  ccxt:true  },
  { id:'pionex',      name:'Pionex',      geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:false, ccxt:true  },
  { id:'woo',         name:'WOO X',       geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'bitrue',      name:'Bitrue',      geo:'WORLD', currencies:['USDT','XRP'],      quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  // Deepcoin, Toobit, CoinW — pas de ccxt officiel → signaux via proxy
  { id:'deepcoin',    name:'Deepcoin',    geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:false },
  { id:'toobit',      name:'Toobit',      geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:false },
  { id:'coinw',       name:'CoinW',       geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:false },
  { id:'huobi',       name:'Huobi',       geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
];

const signalsByExchange = {};
const signalsCache = [];
let lastScanTime = null;
const marketsCache = {};

function avg(arr) { return arr.reduce((a, b) => a + b, 0) / arr.length; }

// ══════════════════════════════════════════════════════════════════════
// DÉTECTION DE FIGURES
// SL = -2% fixe du prix d'entrée
// TP = dynamique selon hauteur de la figure
// ══════════════════════════════════════════════════════════════════════
const FIGURES = [
  { name:'Cup & Handle',     code:'C&H',   dir:'Long', wr:0.84 },
  { name:'ETE Inverse',      code:'ETEi',  dir:'Long', wr:0.81 },
  { name:'Double Bottom',    code:'2Bot',  dir:'Long', wr:0.76 },
  { name:'Triangle Asc.',    code:'TriA',  dir:'Long', wr:0.74 },
  { name:'Drapeau Haussier', code:'DrapH', dir:'Long', wr:0.76 },
  { name:'Biseau Baissier',  code:'BisB',  dir:'Long', wr:0.73 },
];

function detectFigure(closes, volumes, livePrice) {
  if (closes.length < 100) return null;
  const n = closes.length;
  const price = livePrice || closes[n - 1];
  const volNow = volumes[n - 1];
  const volAvg = avg(volumes.slice(-50));
  const volRatio = volNow / volAvg;
  if (volRatio < VOL_CONFIRM) return null;

  const slice = closes.slice(-150);
  const high = Math.max(...slice);
  const low  = Math.min(...slice);
  const range = (high - low) / price;
  const trend10 = closes[n-51] ? (price - closes[n-51]) / closes[n-51] : 0;

  const figureTarget = (high - low) / low;
  if (figureTarget < 0.50) return null;

  let tpPct;
  if      (figureTarget >= 0.80) tpPct = 0.20;
  else if (figureTarget >= 0.71) tpPct = 0.17;
  else if (figureTarget >= 0.61) tpPct = 0.15;
  else                           tpPct = 0.13;

  function buildLevels() {
    const tp = +(price * (1 + tpPct)).toFixed(8);
    const sl = +(price * (1 - SL_PCT)).toFixed(8);
    return { tp, sl };
  }

  if (n >= 100) {
    const cupLow     = Math.min(...closes.slice(n - 60, n - 20));
    const resistance = Math.max(...closes.slice(n - 30, n - 1));
    const handleLow  = Math.min(...closes.slice(n - 20, n - 1));
    if (cupLow < closes[n-70] * 0.95 && price > resistance && volRatio > 1.8)
      return { fig: FIGURES[0], ...buildLevels(), figureTarget, tpPct };
  }
  if (n >= 100) {
    const headLow     = Math.min(...closes.slice(n - 60, n - 20));
    const shoulderLow = Math.min(...closes.slice(n - 80, n - 60));
    const neckline    = Math.max(...closes.slice(n - 80, n - 2));
    if (headLow < shoulderLow * 0.97 && price > neckline && volRatio > 1.5)
      return { fig: FIGURES[1], ...buildLevels(), figureTarget, tpPct };
  }
  if (n >= 70) {
    const bot1   = Math.min(...closes.slice(n - 50, n - 25));
    const bot2   = Math.min(...closes.slice(n - 25, n - 1));
    const midTop = Math.max(...closes.slice(n - 40, n - 10));
    if (Math.abs(bot1 - bot2) / bot1 < 0.02 && price > midTop && volRatio > 1.4)
      return { fig: FIGURES[2], ...buildLevels(), figureTarget, tpPct };
  }
  if (range < 0.04 && trend10 > 0.01 && price > high * 0.999 && volRatio > 1.6)
    return { fig: FIGURES[3], ...buildLevels(), figureTarget, tpPct };
  if (trend10 > 0.06 && range < 0.025 && price > high * 0.999 && volRatio > 1.8)
    return { fig: FIGURES[4], ...buildLevels(), figureTarget, tpPct };
  if (range < 0.035 && trend10 < -0.02 && trend10 > -0.05 && price > high * 0.999 && volRatio > 1.7)
    return { fig: FIGURES[5], ...buildLevels(), figureTarget, tpPct };

  return null;
}

// ══════════════════════════════════════════════════════════════════════
// WEBSOCKET KRAKEN (temps réel)
// ══════════════════════════════════════════════════════════════════════
const krakenCandles    = {};
const krakenCandles4h  = {};
let krakenPairsList    = [];
let wsConnected        = false;
let ws = null, ws4h = null, wsTicker = null;

const livePrices       = {};
const lastScannedPrice = {};
const breakoutConfirm  = {};
const recentSignals    = new Map();

const QUOTE_CURRENCIES = ['USD'];

async function fetchKrakenUsdtPairs() {
  try {
    const exchange = new ccxt.kraken({ enableRateLimit: true, timeout: 15000 });
    const markets = await exchange.loadMarkets();
    const allSymbols = Object.keys(markets);
    console.log(`[Diagnostic] Kraken: ${allSymbols.length} marches`);
    const pairs = allSymbols.filter(s => {
      const m = markets[s];
      return QUOTE_CURRENCIES.some(q => s.endsWith('/' + q)) && m.active !== false && (m.spot === true || m.type === 'spot');
    });
    console.log(`[Diagnostic] ${pairs.length} paires /USD retenues`);
    return pairs.slice(0, MAX_PAIRS);
  } catch (e) { console.log('Erreur fetchKrakenUsdtPairs:', e.message); return []; }
}

function connectKrakenTicker(pairs) {
  if (wsTicker) { try { wsTicker.terminate(); } catch (e) {} }
  wsTicker = new WebSocket('wss://ws.kraken.com/v2');
  wsTicker.on('open', () => {
    console.log(`[Ticker] Connecté — ${pairs.length} paires`);
    const CHUNK = 50;
    for (let i = 0; i < pairs.length; i += CHUNK) {
      wsTicker.send(JSON.stringify({ method: 'subscribe', params: { channel: 'ticker', symbol: pairs.slice(i, i + CHUNK) } }));
    }
  });
  wsTicker.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.channel === 'ticker' && msg.data) {
        for (const t of msg.data) {
          if (t.symbol && t.last) {
            livePrices[t.symbol] = t.last;
            const alt = t.symbol.replace('XBT','BTC').replace('BTC','XBT');
            if (alt !== t.symbol) livePrices[alt] = t.last;
            // Confirmation 3 ticks
            const bc = breakoutConfirm[t.symbol];
            if (bc) {
              if (t.last > bc.resistance) {
                bc.count++;
                if (bc.count >= 3) {
                  console.log(`[Breakout] ${t.symbol} confirmé ${bc.count} ticks → ORDRE`);
                  delete breakoutConfirm[t.symbol];
                  executeTrade(bc.signal).catch(() => {});
                }
              } else {
                delete breakoutConfirm[t.symbol];
              }
            }
          }
        }
      }
    } catch (e) {}
  });
  wsTicker.on('close', () => { setTimeout(() => connectKrakenTicker(krakenPairsList), 5000); });
  wsTicker.on('error', (err) => { console.log('[Ticker] Erreur:', err.message); });
}

function connectKrakenWS(pairs) {
  if (ws) { try { ws.terminate(); } catch (e) {} }
  ws = new WebSocket('wss://ws.kraken.com/v2');
  ws.on('open', () => {
    wsConnected = true;
    console.log(`WebSocket Kraken connecté — ${pairs.length} paires`);
    const CHUNK = 50;
    for (let i = 0; i < pairs.length; i += CHUNK)
      ws.send(JSON.stringify({ method: 'subscribe', params: { channel: 'ohlc', symbol: pairs.slice(i, i + CHUNK), interval: 1440 } }));
  });
  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.channel === 'ohlc' && (msg.type === 'snapshot' || msg.type === 'update') && msg.data) {
        for (const c of msg.data) {
          const sym = c.symbol;
          const arr = krakenCandles[sym] || (krakenCandles[sym] = []);
          const lastC = arr[arr.length - 1];
          if (lastC && Math.abs(lastC.c - c.close) / (c.close||1) < 0.5) { lastC.c = c.close; lastC.v = c.volume; }
          else { arr.push({ c: c.close, v: c.volume }); if (arr.length > 150) arr.shift(); }
          const isNew = !lastC || arr[arr.length-1] !== lastC;
          if (isNew) setImmediate(() => scanSinglePair(sym, '1d', 'kraken'));
        }
      }
    } catch (e) {}
  });
  ws.on('close', () => { wsConnected = false; setTimeout(() => connectKrakenWS(krakenPairsList), 5000); });
  ws.on('error', (err) => { console.log('Erreur WS Kraken:', err.message); });
}

function connectKrakenWS4h(pairs) {
  if (ws4h) { try { ws4h.terminate(); } catch (e) {} }
  ws4h = new WebSocket('wss://ws.kraken.com/v2');
  ws4h.on('open', () => {
    console.log(`[WS-4h] Connecté — ${pairs.length} paires`);
    const CHUNK = 50;
    for (let i = 0; i < pairs.length; i += CHUNK)
      ws4h.send(JSON.stringify({ method: 'subscribe', params: { channel: 'ohlc', symbol: pairs.slice(i, i + CHUNK), interval: 240 } }));
  });
  ws4h.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.channel === 'ohlc' && (msg.type === 'snapshot' || msg.type === 'update') && msg.data) {
        for (const c of msg.data) {
          const sym = c.symbol;
          const arr = krakenCandles4h[sym] || (krakenCandles4h[sym] = []);
          const lastC = arr[arr.length - 1];
          if (lastC && Math.abs(lastC.c - c.close) / (c.close||1) < 0.5) { lastC.c = c.close; lastC.v = c.volume; }
          else { arr.push({ c: c.close, v: c.volume }); if (arr.length > 1000) arr.shift(); }
          const isNew = !lastC || arr[arr.length-1] !== lastC;
          if (isNew) setImmediate(() => scanSinglePair(sym, '4h', 'kraken'));
        }
      }
    } catch (e) {}
  });
  ws4h.on('close', () => { setTimeout(() => connectKrakenWS4h(krakenPairsList), 5000); });
  ws4h.on('error', (err) => { console.log('[WS-4h] Erreur:', err.message); });
}

async function preloadHistoricalCandles(pairs) {
  console.log(`Preloading ${pairs.length} paires (Daily + 4h)...`);
  const exchange = new ccxt.kraken({ enableRateLimit: true, timeout: 10000 });
  const BATCH = 50;
  for (let i = 0; i < pairs.length; i += BATCH) {
    const batch = pairs.slice(i, i + BATCH);
    await Promise.all(batch.map(async (symbol) => {
      try {
        const d = await exchange.fetchOHLCV(symbol, '1d', undefined, 160);
        if (d && d.length >= 10) krakenCandles[symbol] = d.map(c => ({ c: c[4], v: c[6]||c[5] }));
      } catch (e) {}
      try {
        const h4 = await exchange.fetchOHLCV(symbol, '4h', undefined, 1000);
        if (h4 && h4.length >= 10) krakenCandles4h[symbol] = h4.map(c => ({ c: c[4], v: c[6]||c[5] }));
      } catch (e) {}
    }));
    console.log(`Preloading... ${Math.min(i + BATCH, pairs.length)}/${pairs.length}`);
    if (i + BATCH < pairs.length) await new Promise(r => setTimeout(r, 500));
  }
  console.log('Preloading terminé!');
}

// ══════════════════════════════════════════════════════════════════════
// SCAN PAR PAIRE — Kraken WebSocket + REST autres exchanges
// ══════════════════════════════════════════════════════════════════════
function scanSinglePair(symbol, timeframe = '1d', exchangeId = 'kraken') {
  try {
    const candleStore = timeframe === '4h' ? krakenCandles4h : krakenCandles;
    const candles = candleStore[symbol];
    if (!candles || candles.length < 100) return;
    const closes  = candles.map(c => c.c);
    const volumes = candles.map(c => c.v);
    const livePrice = livePrices[symbol];
    if (!livePrice) return;
    const sig = detectFigure(closes, volumes, livePrice);
    if (!sig) return;

    const sigKey = symbol + '|' + sig.fig.name + '|' + timeframe;
    const lastSigKey = recentSignals.get(sigKey);
    if (lastSigKey && Date.now() - lastSigKey < 60 * 60 * 1000) return;
    recentSignals.set(sigKey, Date.now());

    const volRatio = volumes[volumes.length - 1] / avg(volumes.slice(-50));
    const exConfig = EXCHANGES_CONFIG.find(e => e.id === exchangeId) || EXCHANGES_CONFIG[0];
    const signal = {
      symbol, exchange: exConfig.name, exchangeId,
      timeframe, market: 'Spot',
      figure: sig.fig.name, figureCode: sig.fig.code, direction: sig.fig.dir,
      confidence: Math.round(sig.fig.wr * 100), reliable: sig.fig.wr >= 0.65,
      entryPrice: livePrice, tp: sig.tp, sl: sig.sl,
      tpPct: +(sig.tpPct * 100).toFixed(1),
      slPct: +(SL_PCT * 100).toFixed(1),
      figureTarget: +(sig.figureTarget * 100).toFixed(1),
      volumeRatio: volRatio.toFixed(2),
      tradeAmount: TRADE_AMOUNT,
      gain: (TRADE_AMOUNT * sig.tpPct).toFixed(4),
      loss: (TRADE_AMOUNT * SL_PCT).toFixed(4),
      time: new Date()
    };

    const idx = signalsCache.findIndex(s => s.symbol === symbol && s.exchangeId === exchangeId);
    if (idx >= 0) signalsCache[idx] = signal;
    else if (signalsCache.length < MAX_SIGNALS_CACHE) signalsCache.push(signal);

    new Signal({ symbol, exchange: exConfig.name, market: 'Spot',
      figure: sig.fig.name, direction: sig.fig.dir,
      confidence: signal.confidence, entryPrice: livePrice,
      tp: sig.tp, sl: sig.sl, volumeRatio: volRatio, timeframe }).save().catch(() => {});

    const resistance = Math.max(...closes.slice(-30));
    if (!breakoutConfirm[symbol]) {
      breakoutConfirm[symbol] = { count: 1, resistance, figure: sig.fig.name, signal, startedAt: Date.now() };
      console.log(`[Breakout] ${symbol} · ${sig.fig.name} · ${exConfig.name} · résistance ${resistance} · 1/3`);
    }
    setTimeout(() => {
      if (breakoutConfirm[symbol] && breakoutConfirm[symbol].figure === sig.fig.name) {
        console.log(`[Breakout] ${symbol} — timeout 30s — annulé`);
        delete breakoutConfirm[symbol];
      }
    }, 30000);
  } catch(e) {}
}

// ══════════════════════════════════════════════════════════════════════
// SCAN REST — Pour les 34 autres exchanges (non-Kraken)
// ══════════════════════════════════════════════════════════════════════
async function scanExchangeRest(exConfig) {
  const results = [];
  if (!exConfig.ccxt) return results; // plateforme sans support ccxt
  try {
    const ExClass = ccxt[exConfig.id];
    if (!ExClass) { console.log(`[${exConfig.name}] ccxt id '${exConfig.id}' non trouvé`); return results; }
    const exchange = new ExClass({ enableRateLimit: true, timeout: 15000 });

    if (!marketsCache[exConfig.id] || Date.now() - marketsCache[exConfig.id].time > 3600000) {
      const markets = await exchange.loadMarkets();
      marketsCache[exConfig.id] = { markets, time: Date.now() };
    }

    const markets = marketsCache[exConfig.id].markets;
    const symbols = Object.keys(markets)
      .filter(s => {
        const m = markets[s];
        const matchesQuote = exConfig.quoteFilter.some(q => s.endsWith('/' + q));
        const isSpot = (m.type === 'spot' || m.spot === true) && exConfig.spot;
        return matchesQuote && isSpot && m.active !== false;
      })
      .slice(0, 200); // max 200 paires par exchange

    console.log(`[${exConfig.name}] ${symbols.length} paires à scanner`);

    const BATCH = 15;
    for (let i = 0; i < symbols.length; i += BATCH) {
      const batch = symbols.slice(i, i + BATCH);
      const batchResults = await Promise.all(batch.map(async (symbol) => {
        try {
          // Daily
          const ohlcv = await exchange.fetchOHLCV(symbol, '1d', undefined, 160);
          if (!ohlcv || ohlcv.length < 20) return null;
          const closes  = ohlcv.map(c => c[4]);
          const volumes = ohlcv.map(c => c[5]);
          const price   = closes[closes.length - 1];
          const sig = detectFigure(closes, volumes, price);
          if (!sig) return null;
          const volRatio = volumes[volumes.length - 1] / avg(volumes.slice(-50));
          return {
            symbol, exchange: exConfig.name, exchangeId: exConfig.id,
            timeframe: '1d', market: 'Spot',
            figure: sig.fig.name, figureCode: sig.fig.code, direction: sig.fig.dir,
            confidence: Math.round(sig.fig.wr * 100), reliable: sig.fig.wr >= 0.65,
            entryPrice: price, tp: sig.tp, sl: sig.sl,
            tpPct: +(sig.tpPct * 100).toFixed(1),
            slPct: +(SL_PCT * 100).toFixed(1),
            figureTarget: +(sig.figureTarget * 100).toFixed(1),
            volumeRatio: volRatio.toFixed(2),
            tradeAmount: TRADE_AMOUNT,
            gain: (TRADE_AMOUNT * sig.tpPct).toFixed(4),
            loss: (TRADE_AMOUNT * SL_PCT).toFixed(4),
            time: new Date()
          };
        } catch (e) { return null; }
      }));
      batchResults.forEach(r => { if (r) results.push(r); });
    }
    console.log(`[${exConfig.name}] ${results.length} signal(s) trouvé(s)`);
  } catch (e) { console.log(`[${exConfig.name}] Erreur: ${e.message}`); }
  return results;
}

// ══════════════════════════════════════════════════════════════════════
// EXÉCUTION TRADES — Supporte les 35 exchanges
// ══════════════════════════════════════════════════════════════════════
async function executeTrade(signal) {
  try {
    // Trouver tous les utilisateurs connectés sur cet exchange
    const exchangeId = signal.exchangeId || 'kraken';
    const exConfig = EXCHANGES_CONFIG.find(e => e.id === exchangeId);
    const users = await User.find({
      active: true,
      apiKey: { $exists: true },
      exchangeName: new RegExp(exchangeId, 'i')
    });

    for (const user of users) {
      try {
        // Règle 1: position déjà ouverte
        const existingPos = await OpenPosition.findOne({ email: user.email, symbol: signal.symbol, exchangeId });
        if (existingPos) continue;

        // Règle 2: même figure déjà tradée
        const lastTrade = await Trade.findOne(
          { email: user.email, symbol: signal.symbol, figure: signal.figure, exchange: signal.exchange },
          null, { sort: { time: -1 } }
        );
        if (lastTrade) {
          const priceDiff = Math.abs(signal.entryPrice - lastTrade.entryPrice) / lastTrade.entryPrice;
          if (priceDiff < 0.20) { console.log(`[Bot] ${signal.symbol} — même formation — skip`); continue; }
        }

        // Règle 3: trade récent (<4h) sur cette paire
        const recentTrade = await Trade.findOne({
          email: user.email, symbol: signal.symbol, exchange: signal.exchange,
          time: { $gte: new Date(Date.now() - 4 * 60 * 60 * 1000) }
        });
        if (recentTrade) { console.log(`[Bot] ${signal.symbol} — trade récent — skip`); continue; }

        // Connexion à l'exchange
        if (!exConfig || !exConfig.ccxt) {
          console.log(`[Bot] ${signal.exchange} sans support ccxt — signal ignoré`);
          continue;
        }
        const ExClass = ccxt[exchangeId];
        if (!ExClass) continue;

        const exchange = new ExClass({ apiKey: user.apiKey, secret: user.apiSecret, enableRateLimit: true });
        const balance = await exchange.fetchBalance();

        // Détecter la devise disponible selon l'exchange
        const currency = user.currency || exConfig.currencies[0];
        const available = balance[currency]?.free || 0;
        const amount = Math.max(user.tradeAmount || TRADE_AMOUNT, 5);

        if (available < amount) {
          console.log(`[Bot] ⚠ ${signal.symbol} — ${available.toFixed(2)} ${currency} dispo / ${amount} requis — skip`);
          continue;
        }

        const qty     = amount / signal.entryPrice;
        const tpPrice = signal.tp;
        const slPrice = signal.sl;

        // Paramètres spécifiques par exchange
        const orderParams = {};
        if (exchangeId === 'kraken') orderParams.oflags = 'fciq';

        console.log(`[Bot] BUY ${signal.exchange}: ${signal.symbol} · ${signal.figure} · ${amount}${currency} · TP:${tpPrice} · SL:${slPrice}`);
        const order = await exchange.createOrder(signal.symbol, 'market', 'buy', qty, undefined, orderParams);
        console.log(`[Bot] Ordre exécuté: ${order.id}`);

        await new OpenPosition({
          email: user.email, symbol: signal.symbol,
          exchange: signal.exchange, exchangeId,
          figure: signal.figure, entryPrice: signal.entryPrice,
          tp: tpPrice, sl: slPrice,
          tpPct: signal.tpPct, figureTarget: signal.figureTarget,
          qty, amount, currency, timeframe: signal.timeframe
        }).save();

        await new Trade({
          email: user.email, symbol: signal.symbol,
          exchange: signal.exchange, market: 'Spot',
          direction: signal.direction, figure: signal.figure,
          entryPrice: signal.entryPrice, exitPrice: null,
          amount, pnl: 0, currency,
          result: 'OPEN', exitReason: 'Position ouverte — en attente TP/SL',
          timeframe: signal.timeframe
        }).save();

      } catch(e) { console.log(`[Bot] Erreur trade ${signal.symbol} ${signal.exchange}:`, e.message); }
    }
  } catch(e) { console.log('[executeTrade] Erreur:', e.message); }
}

// ══════════════════════════════════════════════════════════════════════
// SUIVI TP/SL — Toutes les 2 secondes
// ══════════════════════════════════════════════════════════════════════
const positionsInProgress = new Set();

async function checkTPSLInstant() {
  try {
    const positions = await OpenPosition.find({});
    if (positions.length === 0) return;

    for (const pos of positions) {
      const posId = pos._id.toString();
      if (positionsInProgress.has(posId)) continue;

      // Prix live — Kraken pour les paires USD, sinon via fetchTicker
      const currentPrice = livePrices[pos.symbol];
      if (!currentPrice) {
        // Pour les exchanges non-Kraken, on va chercher le prix via REST
        // mais on limite la fréquence pour ne pas spammer les APIs
        continue; // géré par checkTPSLRest() toutes les 30s
      }

      if (pos.tp <= pos.entryPrice || pos.sl >= pos.entryPrice) {
        const correctedTP = +(pos.entryPrice * 1.15).toFixed(8); // fallback 15% si TP incohérent
        const correctedSL = +(pos.entryPrice * (1 - SL_PCT)).toFixed(8);
        await OpenPosition.updateOne({ _id: pos._id }, { tp: correctedTP, sl: correctedSL });
        continue;
      }

      const hitTP = currentPrice >= pos.tp;
      const hitSL = currentPrice <= pos.sl;
      if (!hitTP && !hitSL) continue;

      const reason = hitTP ? 'TP' : 'SL';
      console.log(`[INSTANT ${reason}] ${pos.symbol} ${pos.exchange} prix:${currentPrice} TP:${pos.tp} SL:${pos.sl}`);

      positionsInProgress.add(posId);
      try {
        const user = await User.findOne({ email: pos.email });
        if (!user) { await OpenPosition.deleteOne({ _id: pos._id }); positionsInProgress.delete(posId); continue; }

        const exchangeId = pos.exchangeId || user.exchangeName.toLowerCase();
        const ExClass = ccxt[exchangeId];
        if (!ExClass) { await OpenPosition.deleteOne({ _id: pos._id }); positionsInProgress.delete(posId); continue; }

        const exchange = new ExClass({ apiKey: user.apiKey, secret: user.apiSecret, enableRateLimit: true });
        const balance = await exchange.fetchBalance();
        const [base] = pos.symbol.split('/');
        const baseBalance = balance[base]?.free || 0;

        if (baseBalance < 0.000001) {
          await Trade.findOneAndUpdate(
            { email: pos.email, symbol: pos.symbol, result: 'OPEN' },
            { exitPrice: currentPrice, pnl: 0, result: 'CLOSED_MANUAL', exitReason: 'Vendu manuellement ou solde vide' },
            { sort: { time: -1 } }
          );
          await OpenPosition.deleteOne({ _id: pos._id });
          positionsInProgress.delete(posId); continue;
        }

        const orderParams = {};
        if (exchangeId === 'kraken') orderParams.oflags = 'fciq';
        const order = await exchange.createOrder(pos.symbol, 'market', 'sell', baseBalance, undefined, orderParams);
        console.log(`[INSTANT ${reason}] Ordre SELL: ${order.id}`);

        const posTpPct = pos.tpPct ? pos.tpPct / 100 : TP_PCT;
        const pnl = hitTP ? pos.amount * posTpPct : -(pos.amount * SL_PCT);

        await Trade.findOneAndUpdate(
          { email: pos.email, symbol: pos.symbol, result: 'OPEN' },
          { exitPrice: currentPrice, pnl, result: hitTP ? 'WIN' : 'LOSS',
            exitReason: hitTP ? `TP +${pos.tpPct||TP_PCT*100}% atteint` : 'SL -2% touché' },
          { sort: { time: -1 } }
        );
        await OpenPosition.deleteOne({ _id: pos._id });
        console.log(`[INSTANT ${reason}] PnL: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(4)}`);

      } catch(e) {
        console.log(`[INSTANT TP/SL] Erreur ${pos.symbol}:`, e.message);
        if (e.message && e.message.includes('Insufficient funds'))
          await OpenPosition.deleteOne({ _id: pos._id }).catch(() => {});
      } finally { positionsInProgress.delete(posId); }
    }
  } catch(e) { console.log('[INSTANT TP/SL] Erreur globale:', e.message); }
}

// TP/SL via REST pour les exchanges non-Kraken (toutes les 30s)
// Seulement si des positions non-Kraken existent
async function checkTPSLRest() {
  try {
    const positions = await OpenPosition.find({ exchangeId: { $ne: 'kraken' } });
    if (positions.length === 0) return; // rien à faire

    for (const pos of positions) {
      const posId = pos._id.toString();
      if (positionsInProgress.has(posId)) continue;

      try {
        const user = await User.findOne({ email: pos.email });
        if (!user) continue;
        const exchangeId = pos.exchangeId || user.exchangeName.toLowerCase();
        const ExClass = ccxt[exchangeId];
        if (!ExClass) continue;

        const exchange = new ExClass({ apiKey: user.apiKey, secret: user.apiSecret, enableRateLimit: true });
        const ticker = await exchange.fetchTicker(pos.symbol);
        const currentPrice = ticker.last;
        if (!currentPrice) continue;

        // Mettre à jour livePrices pour ce symbole
        livePrices[pos.symbol] = currentPrice;

        const hitTP = currentPrice >= pos.tp;
        const hitSL = currentPrice <= pos.sl;
        if (!hitTP && !hitSL) continue;

        const reason = hitTP ? 'TP' : 'SL';
        console.log(`[REST ${reason}] ${pos.symbol} ${pos.exchange} prix:${currentPrice}`);
        positionsInProgress.add(posId);

        const balance = await exchange.fetchBalance();
        const [base] = pos.symbol.split('/');
        const baseBalance = balance[base]?.free || 0;

        if (baseBalance > 0.000001) {
          const order = await exchange.createOrder(pos.symbol, 'market', 'sell', baseBalance);
          const posTpPct = pos.tpPct ? pos.tpPct / 100 : TP_PCT;
          const pnl = hitTP ? pos.amount * posTpPct : -(pos.amount * SL_PCT);
          await Trade.findOneAndUpdate(
            { email: pos.email, symbol: pos.symbol, result: 'OPEN' },
            { exitPrice: currentPrice, pnl, result: hitTP ? 'WIN' : 'LOSS',
              exitReason: hitTP ? `TP +${pos.tpPct||TP_PCT*100}% atteint` : 'SL -2% touché' },
            { sort: { time: -1 } }
          );
          await OpenPosition.deleteOne({ _id: pos._id });
          console.log(`[REST ${reason}] PnL: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(4)}`);
        }
      } catch(e) { console.log(`[REST TP/SL] Erreur ${pos.symbol}:`, e.message); }
      finally { positionsInProgress.delete(posId); }
    }
  } catch(e) { console.log('[REST TP/SL] Erreur:', e.message); }
}

// ══════════════════════════════════════════════════════════════════════
// SCAN ALL — Rattrapage toutes les 60s pour les 34 exchanges REST
// ══════════════════════════════════════════════════════════════════════
function scanKrakenFromMemory() {
  const results = [];
  for (const symbol of krakenPairsList) {
    const candles = krakenCandles[symbol];
    if (!candles || candles.length < 20) continue;
    const closes = candles.filter(c => c.c > 0).map(c => c.c);
    const volumes = candles.filter(c => c.v > 0).map(c => c.v);
    const price = closes[closes.length - 1];
    const sig = detectFigure(closes, volumes, price);
    if (!sig) continue;
    const volRatio = volumes[volumes.length - 1] / avg(volumes.slice(-50));
    results.push({
      symbol, exchange: 'Kraken', exchangeId: 'kraken', timeframe: '1d', market: 'Spot',
      figure: sig.fig.name, figureCode: sig.fig.code, direction: sig.fig.dir,
      confidence: Math.round(sig.fig.wr * 100), reliable: sig.fig.wr >= 0.65,
      entryPrice: price, tp: sig.tp, sl: sig.sl,
      tpPct: +(sig.tpPct * 100).toFixed(1), slPct: +(SL_PCT * 100).toFixed(1),
      figureTarget: +(sig.figureTarget * 100).toFixed(1),
      volumeRatio: volRatio.toFixed(2),
      tradeAmount: TRADE_AMOUNT,
      gain: (TRADE_AMOUNT * sig.tpPct).toFixed(4),
      loss: (TRADE_AMOUNT * SL_PCT).toFixed(4),
      time: new Date()
    });
  }
  return results;
}

let scanRunning = false;
async function scanAll() {
  if (scanRunning) return;
  scanRunning = true;
  const startTime = Date.now();
  console.log(`\n=== SCAN — ${new Date().toLocaleTimeString()} ===`);
  signalsCache.length = 0;
  Object.keys(signalsByExchange).forEach(k => delete signalsByExchange[k]);
  if (typeof global.gc === 'function') global.gc();

  try {
    const users = await User.find({ active: true, apiKey: { $exists: true } });

    // Scan Kraken depuis mémoire (déjà en temps réel via WS)
    const krakenResults = scanKrakenFromMemory();
    signalsCache.push(...krakenResults);
    signalsByExchange['kraken'] = krakenResults;
    lastScanTime = new Date();
    console.log(`[Kraken] ${krakenResults.length} signal(s) · ${Date.now()-startTime}ms`);

    if (users.length === 0) {
      console.log('[Scan] Aucun utilisateur connecté — scan REST ignoré');
      return;
    }

    // Scan REST uniquement pour les exchanges où des utilisateurs sont connectés
    const uniqueExchanges = [...new Set(users.map(u => u.exchangeName.toLowerCase()))];
    const nonKraken = uniqueExchanges.filter(id => id !== 'kraken');

    if (nonKraken.length === 0) {
      console.log('[Scan] Aucun utilisateur non-Kraken connecté — scan REST ignoré');
    } else {
      console.log(`[Scan] ${nonKraken.length} exchange(s) à scanner: ${nonKraken.join(', ')}`);
      for (const exchangeId of nonKraken) {
        const exConfig = EXCHANGES_CONFIG.find(e => e.id === exchangeId || e.name.toLowerCase() === exchangeId);
        if (!exConfig || !exConfig.ccxt) {
          console.log(`[Scan] ${exchangeId} — pas de support ccxt — skip`);
          continue;
        }
        try {
          const results = await scanExchangeRest(exConfig);
          signalsCache.push(...results);
          signalsByExchange[exConfig.id] = results;
        } catch(e) { console.log(`[${exchangeId}] Erreur scan:`, e.message); }
      }
    }

    lastScanTime = new Date();
    console.log(`=== FIN · ${signalsCache.length} signaux · ${Date.now()-startTime}ms ===\n`);

    // Exécuter les trades pour tous les utilisateurs
    for (const user of users) {
      const userExchangeId = user.exchangeName.toLowerCase();
      const exConfig = EXCHANGES_CONFIG.find(e => e.id === userExchangeId || e.name.toLowerCase() === userExchangeId);
      if (!exConfig || !exConfig.ccxt) continue;

      const userSignals = signalsCache.filter(s => s.exchangeId === userExchangeId);
      if (userSignals.length === 0) continue;

      try {
        const ExClass = ccxt[userExchangeId];
        if (!ExClass) continue;
        const exchange = new ExClass({ apiKey: user.apiKey, secret: user.apiSecret, enableRateLimit: true });
        const balance = await exchange.fetchBalance();
        const currency = user.currency || exConfig.currencies[0];
        const available = balance[currency]?.free || 0;
        const amount = Math.max(user.tradeAmount || TRADE_AMOUNT, 5);
        if (available < amount) continue;

        let ordersPlaced = 0;
        for (const sig of userSignals.slice(0, MAX_CONCURRENT)) {
          if (ordersPlaced >= MAX_CONCURRENT) break;
          try {
            const existingPos = await OpenPosition.findOne({ email: user.email, symbol: sig.symbol, exchangeId: userExchangeId });
            if (existingPos) continue;

            const lastTrade = await Trade.findOne(
              { email: user.email, symbol: sig.symbol, figure: sig.figure, exchange: sig.exchange },
              null, { sort: { time: -1 } }
            );
            if (lastTrade) {
              const diff = Math.abs(sig.entryPrice - lastTrade.entryPrice) / lastTrade.entryPrice;
              if (diff < 0.20) continue;
            }

            const recentTrade = await Trade.findOne({
              email: user.email, symbol: sig.symbol, exchange: sig.exchange,
              time: { $gte: new Date(Date.now() - 4 * 60 * 60 * 1000) }
            });
            if (recentTrade) continue;

            const qty = amount / sig.entryPrice;
            const orderParams = {};
            if (userExchangeId === 'kraken') orderParams.oflags = 'fciq';
            console.log(`[Bot] BUY ${sig.exchange}: ${sig.symbol} · ${amount}${currency}`);
            const order = await exchange.createOrder(sig.symbol, 'market', 'buy', qty, undefined, orderParams);

            await new OpenPosition({
              email: user.email, symbol: sig.symbol,
              exchange: sig.exchange, exchangeId: userExchangeId,
              figure: sig.figure, entryPrice: sig.entryPrice,
              tp: sig.tp, sl: sig.sl, tpPct: sig.tpPct,
              figureTarget: sig.figureTarget, qty, amount, currency
            }).save();

            await new Trade({
              email: user.email, symbol: sig.symbol,
              exchange: sig.exchange, market: 'Spot',
              direction: sig.direction, figure: sig.figure,
              entryPrice: sig.entryPrice, exitPrice: null,
              amount, pnl: 0, currency,
              result: 'OPEN', exitReason: 'Position ouverte — en attente TP/SL'
            }).save();
            ordersPlaced++;
          } catch(e) { console.log(`[Bot] Erreur ${sig.symbol}:`, e.message); }
        }
        if (ordersPlaced > 0) console.log(`[Bot] ${ordersPlaced} ordre(s) pour ${user.email} sur ${exConfig.name}`);
      } catch(e) { console.log(`[Bot] Erreur ${user.email}:`, e.message); }
    }
  } finally { scanRunning = false; }
}

// ══════════════════════════════════════════════════════════════════════
// VÉRIFICATION PAIEMENTS XLM
// ══════════════════════════════════════════════════════════════════════
async function checkXlmPayments() {
  try {
    const BENDER_XLM = 'GDIZP4VPNBZLV7CCDUG4BORFYERX3NQVBE3N6W2FAFAIT3OTTRUIUCBR';
    const users = await User.find({ xlmWallet: { $exists: true, $ne: '' } });
    if (users.length === 0) return;
    const resp = await fetch(`https://horizon.stellar.org/accounts/${BENDER_XLM}/payments?order=desc&limit=50`);
    const data = await resp.json();
    const payments = data._embedded?.records || [];
    for (const user of users) {
      if (!user.xlmWallet) continue;
      const payment = payments.find(p => p.from === user.xlmWallet && p.asset_type === 'native' && p.to === BENDER_XLM);
      if (!payment) continue;
      const billing = await Billing.findOne({ email: user.email, status: 'PENDING' });
      if (!billing) continue;
      await Billing.findOneAndUpdate({ _id: billing._id }, { status: 'PAID', paidAt: new Date(), txHash: payment.transaction_hash });
      console.log(`[XLM] Paiement détecté pour ${user.email} — PAID`);
    }
  } catch(e) { console.log('[XLM] Erreur:', e.message); }
}

// ══════════════════════════════════════════════════════════════════════
// ROUTES
// ══════════════════════════════════════════════════════════════════════
app.get('/', (req, res) => res.json({
  status: 'Bender Pro v9.0 — 35 Plateformes',
  exchanges: EXCHANGES_CONFIG.length,
  krakenWsConnected: wsConnected,
  krakenPairsTracked: krakenPairsList.length,
  lastScan: lastScanTime,
  signalsActive: signalsCache.length,
  slPct: SL_PCT * 100 + '%',
}));

app.get('/market', (req, res) => {
  let sigs = [...signalsCache];
  if (req.query.exchange) sigs = sigs.filter(s => s.exchange.toLowerCase().includes(req.query.exchange.toLowerCase()));
  if (req.query.direction) sigs = sigs.filter(s => s.direction === req.query.direction);
  res.json({ success: true, signals: sigs, count: sigs.length, lastScan: lastScanTime });
});

app.get('/scan', async (req, res) => {
  res.json({ success: true, message: 'Scan lancé...' });
  scanAll().catch(console.error);
});

app.post('/register-email', async (req, res) => {
  const { email } = req.body;
  if (!email || !email.includes('@')) return res.json({ success: false, error: 'Email invalide' });
  try {
    await User.findOneAndUpdate({ email }, { email, active: true }, { upsert: true, new: true });
    res.json({ success: true, message: 'Email enregistré' });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

app.post('/connect', async (req, res) => {
  const { email, apiKey, secret, exchangeName, tradeAmount, currency } = req.body;
  if (!email || !apiKey || !secret || !exchangeName)
    return res.json({ success: false, error: 'Données manquantes' });
  try {
    const exConfig = EXCHANGES_CONFIG.find(e => e.id === exchangeName.toLowerCase() || e.name.toLowerCase() === exchangeName.toLowerCase());
    const selectedCurrency = currency || (exConfig ? exConfig.currencies[0] : 'USD');
    await User.findOneAndUpdate(
      { email },
      { apiKey, apiSecret: secret, exchangeName: exConfig ? exConfig.id : exchangeName.toLowerCase(),
        active: true, tradeAmount: tradeAmount || TRADE_AMOUNT, currency: selectedCurrency },
      { upsert: true, new: true }
    );
    res.json({ success: true, message: `Connecté sur ${exConfig?.name || exchangeName} · ${selectedCurrency} · TP dynamique · SL -2%` });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

app.get('/status/:email', async (req, res) => {
  try {
    const user = await User.findOne({ email: req.params.email });
    if (!user) return res.json({ connected: false });
    const trades = await Trade.countDocuments({ email: req.params.email });
    const wins   = await Trade.countDocuments({ email: req.params.email, result: 'WIN' });
    res.json({ connected: true, active: user.active, exchange: user.exchangeName, tradeAmount: user.tradeAmount, trades,
      winRate: trades > 0 ? Math.round(wins/trades*100) + '%' : 'N/A' });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

app.get('/positions/:email', async (req, res) => {
  try {
    const positions = await OpenPosition.find({ email: req.params.email });
    const enriched = positions.map(pos => ({
      ...pos.toObject(),
      currentPrice: livePrices[pos.symbol] || pos.entryPrice,
      pnlPct: livePrices[pos.symbol] ? ((livePrices[pos.symbol] - pos.entryPrice) / pos.entryPrice * 100) : 0,
      pnlUsd: livePrices[pos.symbol] ? ((livePrices[pos.symbol] - pos.entryPrice) / pos.entryPrice) * pos.amount : 0,
    }));
    res.json({ success: true, positions: enriched, count: enriched.length });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

app.get('/trades/:email', async (req, res) => {
  try {
    const email = req.params.email;
    const allTrades = await Trade.find({ email });
    const totalPnl  = allTrades.reduce((a, t) => a + t.pnl, 0);
    const totalWins = allTrades.filter(t => t.result === 'WIN').length;
    const trades    = await Trade.find({ email }).sort({ time: -1 }).limit(100);
    res.json({ trades, totalTradesCount: allTrades.length, totalPnl: totalPnl.toFixed(4),
      wins: totalWins, losses: allTrades.length - totalWins, displayedCount: trades.length });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

const COMMISSION_RATE = 0.0025;
const BILLING_WALLET  = process.env.BILLING_WALLET || 'GDIZP4VPNBZLV7CCDUG4BORFYERX3NQVBE3N6W2FAFAIT3OTTRUIUCBR';
const BILLING_DAYS    = 30;

app.get('/billing/:email', async (req, res) => {
  try {
    const email = req.params.email;
    const user  = await User.findOne({ email });
    if (!user) return res.json({ success: false, error: 'Utilisateur non trouvé' });
    const periodEnd   = new Date();
    const periodStart = new Date(periodEnd - BILLING_DAYS * 24 * 3600 * 1000);
    const trades = await Trade.find({ email, time: { $gte: periodStart, $lte: periodEnd }, result: { $in: ['WIN','LOSS'] } });
    const totalVolume = trades.reduce((a, t) => a + t.amount, 0);
    const totalPnl    = trades.reduce((a, t) => a + t.pnl, 0);
    const wins        = trades.filter(t => t.result === 'WIN').length;
    const commission  = +(totalVolume * COMMISSION_RATE).toFixed(4);
    let billing = await Billing.findOne({ email, periodStart: { $gte: new Date(periodStart.getTime() - 3600000) } });
    if (!billing) billing = await new Billing({ email, periodStart, periodEnd, totalPnl: +totalPnl.toFixed(4), totalVolume: +totalVolume.toFixed(4), commission, status: 'PENDING' }).save();
    res.json({ success: true, billing: { id: billing._id, email, periodStart: periodStart.toLocaleDateString('fr-CA'), periodEnd: periodEnd.toLocaleDateString('fr-CA'),
      trades: trades.length, wins, losses: trades.length - wins,
      winRate: trades.length > 0 ? Math.round(wins/trades.length*100) + '%' : 'N/A',
      totalVolume: +totalVolume.toFixed(4), commission, status: billing.status,
      paidAt: billing.paidAt ? new Date(billing.paidAt).toLocaleDateString('fr-CA') : null,
      wallet: BILLING_WALLET, message: `Commission: $${commission} USD (0.25% de $${totalVolume.toFixed(4)})` } });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

app.post('/billing/paid/:email', async (req, res) => {
  try {
    const { txHash } = req.body;
    const periodStart = new Date(Date.now() - BILLING_DAYS * 24 * 3600 * 1000);
    const billing = await Billing.findOneAndUpdate(
      { email: req.params.email, periodStart: { $gte: new Date(periodStart.getTime() - 3600000) } },
      { status: 'PAID', paidAt: new Date(), txHash: txHash || '' },
      { sort: { createdAt: -1 }, new: true }
    );
    if (!billing) return res.json({ success: false, error: 'Facture non trouvée' });
    res.json({ success: true, billing });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

app.get('/signals', async (req, res) => {
  try { res.json({ signals: await Signal.find().sort({ time: -1 }).limit(100) }); }
  catch(e) { res.json({ success: false, error: e.message }); }
});

app.get('/exchanges', (req, res) => res.json({ exchanges: EXCHANGES_CONFIG }));

let pricesCache = {}, pricesCacheTime = 0;
function refreshPricesFromMemory() {
  const out = {};
  const watch = ['BTC/USD','ETH/USD','SOL/USD','XRP/USD','ADA/USD','AVAX/USD','DOGE/USD',
    'DOT/USD','LINK/USD','LTC/USD','ATOM/USD','UNI/USD','NEAR/USD','XLM/USD','ARB/USD','OP/USD'];
  for (const sym of watch) {
    const candles = krakenCandles[sym];
    if (candles && candles.length >= 2) {
      const last = candles[candles.length-1], prev = candles[0];
      out[sym.split('/')[0]] = { price: last.c, changePct: prev.c ? ((last.c-prev.c)/prev.c)*100 : null };
    }
  }
  pricesCache = out; pricesCacheTime = Date.now();
}
app.get('/prices', (req, res) => { refreshPricesFromMemory(); res.json({ success: true, prices: pricesCache, time: pricesCacheTime }); });

app.get('/platform-signals/:email', async (req, res) => {
  try {
    const user = await User.findOne({ email: req.params.email });
    if (!user) return res.json({ success: false, error: 'Utilisateur non trouvé' });
    const exchangeId = user.exchangeName.toLowerCase();
    const exConfig   = EXCHANGES_CONFIG.find(e => e.id === exchangeId);
    const sigs       = signalsByExchange[exchangeId] || [];
    const amount     = user.tradeAmount || TRADE_AMOUNT;
    res.json({ success: true, exchange: exConfig?.name || user.exchangeName, tradeAmount: amount,
      lastScan: lastScanTime, count: sigs.length,
      signals: sigs.map(s => ({ ...s, potentialGainUSD: +(amount * (s.tpPct||TP_PCT*100) / 100).toFixed(4), potentialLossUSD: +(amount * SL_PCT).toFixed(4) })) });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

app.post('/save-xlm', async (req, res) => {
  try {
    const { email, xlmWallet } = req.body;
    if (!email || !xlmWallet || !xlmWallet.startsWith('G') || xlmWallet.length < 40)
      return res.json({ success: false, error: 'Adresse XLM invalide' });
    await User.findOneAndUpdate({ email }, { xlmWallet }, { upsert: true });
    res.json({ success: true, message: 'Wallet XLM sauvegardé' });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

app.post('/disconnect', async (req, res) => {
  try {
    const { email } = req.body;
    if (!email) return res.json({ success: false, error: 'Email manquant' });
    await User.findOneAndUpdate({ email }, { active: false });
    res.json({ success: true, message: 'Déconnecté — toutes les sessions fermées' });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

app.get('/admin/stats', async (req, res) => {
  try {
    const users  = await User.countDocuments();
    const active = await User.countDocuments({ active: true });
    const trades = await Trade.countDocuments();
    const wins   = await Trade.countDocuments({ result: 'WIN' });
    // Stats par exchange
    const byExchange = {};
    for (const ex of EXCHANGES_CONFIG) {
      const count = await User.countDocuments({ active: true, exchangeName: ex.id });
      if (count > 0) byExchange[ex.name] = count;
    }
    res.json({ users, active, trades, winRate: trades > 0 ? Math.round(wins/trades*100) + '%' : 'N/A',
      signalsActive: signalsCache.length, lastScan: lastScanTime,
      exchanges: EXCHANGES_CONFIG.length, krakenWsConnected: wsConnected,
      krakenPairsTracked: krakenPairsList.length, usersByExchange: byExchange });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

app.post('/toggle', async (req, res) => {
  try {
    const { email, active } = req.body;
    await User.findOneAndUpdate({ email }, { active });
    res.json({ success: true, active });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

app.get('/clear-users', async (req, res) => {
  try {
    const result = await User.deleteMany({});
    res.json({ success: true, deleted: result.deletedCount });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

app.get('/admin/billing', async (req, res) => {
  try {
    const pending  = await Billing.find({ status: 'PENDING', commission: { $gt: 0 } }).sort({ createdAt: -1 });
    const totalDue = pending.reduce((a, b) => a + b.commission, 0);
    res.json({ success: true, pending: pending.length, totalDue: +totalDue.toFixed(4), wallet: BILLING_WALLET, billings: pending });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

// ══════════════════════════════════════════════════════════════════════
// DÉMARRAGE
// ══════════════════════════════════════════════════════════════════════
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`\n🤖 Bender Pro v9.0 · Port ${PORT}`);
  console.log(` 35 Plateformes · Figures chartistes · SL -2% · TP dynamique`);
  console.log(` Scan Kraken via WebSocket · REST pour autres exchanges\n`);

  setImmediate(async () => {
    // ÉTAPE 1: TP/SL instantané dès le démarrage
    setTimeout(() => checkTPSLInstant().catch(console.error), 100);
    setInterval(() => checkTPSLInstant().catch(console.error), 2000);
    // TP/SL REST pour exchanges non-Kraken (toutes les 30s)
    setInterval(() => checkTPSLRest().catch(console.error), 30000);
    console.log(' TP/SL instantané actif (2s Kraken · 30s autres exchanges)');

    // XLM payments toutes les 24h
    setTimeout(() => checkXlmPayments().catch(console.error), 5000);
    setInterval(() => checkXlmPayments().catch(console.error), 24 * 60 * 60 * 1000);

    // ÉTAPE 2: Kraken WebSocket
    krakenPairsList = await fetchKrakenUsdtPairs().catch(() => []);
    if (krakenPairsList.length === 0) {
      setTimeout(() => { fetchKrakenUsdtPairs().then(pairs => { krakenPairsList = pairs; connectKrakenTicker(pairs); connectKrakenWS(pairs); connectKrakenWS4h(pairs); }); }, 15000);
    } else {
      connectKrakenTicker(krakenPairsList);
      connectKrakenWS(krakenPairsList);
      connectKrakenWS4h(krakenPairsList);
      console.log(` ${krakenPairsList.length} paires Kraken · WebSocket actif`);

      preloadHistoricalCandles(krakenPairsList).then(() => {
        console.log(' Preloading terminé');
        scanAll().catch(console.error);
      }).catch(console.error);
    }

    // ÉTAPE 3: Scan toutes les 60s
    setTimeout(() => setInterval(() => scanAll().catch(console.error), SCAN_INTERVAL), 65000);
  });
});
