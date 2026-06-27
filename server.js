// Bender Pro v8.0 â€” Scan via WebSocket Kraken (quasi instantanÃ©)
// npm install express cors mongoose ccxt helmet ws
const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const ccxt = require('ccxt');
const helmet = require('helmet');
const WebSocket = require('ws');

const app = express();
app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors());
app.use(express.json());

// CONFIG
const TRADE_AMOUNT   = 5;    // Minimum 5 USD par trade
const SL_PCT         = 0.03; // -3%
const TP_PCT         = 0.12; // +12%
const MAX_CONCURRENT = 20;
const VOL_CONFIRM    = 1.8;
const SCAN_INTERVAL  = 60 * 1000; // scan toutes les 60s (bougies 1D)
const MAX_PAIRS      = 500;
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
  tradeAmount:  { type: Number, default: 2 },
  active:       { type: Boolean, default: true },
  createdAt:    { type: Date, default: Date.now }
});

const TradeSchema = new mongoose.Schema({
  email:      String,
  symbol:     String,
  exchange:   String,
  market:     String,
  direction:  String,
  figure:     String,
  entryPrice: Number,
  exitPrice:  Number,
  amount:     Number,
  pnl:        Number,
  result:     String,
  exitReason: String,
  time:       { type: Date, default: Date.now }
});

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
  time:        { type: Date, default: Date.now }
});

const OpenPositionSchema = new mongoose.Schema({
  email:        String,
  symbol:       String,
  exchange:     String,
  figure:       String,
  entryPrice:   Number,
  tp:           Number,
  sl:           Number,
  tpPct:        Number, // TP% dynamique selon hauteur figure
  figureTarget: Number, // hauteur mesurÃ©e de la figure en %
  qty:          Number,
  amount:       Number,
  openedAt:     { type: Date, default: Date.now }
});

const BillingSchema = new mongoose.Schema({
  email:       String,
  periodStart: Date,
  periodEnd:   Date,
  totalVolume: Number,
  totalPnl:    Number,
  commission:  Number,
  status:      { type: String, default: 'PENDING' },
  paidAt:      Date,
  txHash:      String,
  createdAt:   { type: Date, default: Date.now }
});

const User         = mongoose.model('User',         UserSchema);
const Trade        = mongoose.model('Trade',        TradeSchema);
const Signal       = mongoose.model('Signal',       SignalSchema);
const OpenPosition = mongoose.model('OpenPosition', OpenPositionSchema);
const Billing      = mongoose.model('Billing',      BillingSchema);

// FIGURES CHARTISTES
const FIGURES = [
  { name:'Cup & Handle',     code:'C&H',   dir:'Long', wr:0.84 },
  { name:'ETE Inverse',      code:'ETEi',  dir:'Long', wr:0.81 },
  { name:'Double Bottom',    code:'2Bot',  dir:'Long', wr:0.76 },
  { name:'Triangle Asc.',    code:'TriA',  dir:'Long', wr:0.74 },
  { name:'Drapeau Haussier', code:'DrapH', dir:'Long', wr:0.76 },
  { name:'Biseau Baissier',  code:'BisB',  dir:'Long', wr:0.73 },
];

// EXCHANGES CONFIG
const EXCHANGES_CONFIG = [
  { id:'kraken',      name:'Kraken',   spot:true, futures:false },
  { id:'binance',     name:'Binance',  spot:true, futures:false },
  { id:'bybit',       name:'Bybit',    spot:true, futures:false },
  { id:'bitget',      name:'Bitget',   spot:true, futures:false },
  { id:'okx',         name:'OKX',      spot:true, futures:false },
  { id:'kucoin',      name:'KuCoin',   spot:true, futures:false },
  { id:'gateio',      name:'Gate.io',  spot:true, futures:false },
  { id:'mexc',        name:'MEXC',     spot:true, futures:false },
  { id:'bingx',       name:'BingX',    spot:true, futures:false },
  { id:'phemex',      name:'Phemex',   spot:true, futures:false },
  { id:'coinbasepro', name:'Coinbase', spot:true, futures:false },
  { id:'bitfinex',    name:'Bitfinex', spot:true, futures:false },
  { id:'bitstamp',    name:'Bitstamp', spot:true, futures:false },
];

const signalsByExchange = {};
const signalsCache = [];
let lastScanTime = null;
const marketsCache = {};

function avg(arr) { return arr.reduce((a, b) => a + b, 0) / arr.length; }

// DÃ‰TECTION DE FIGURES â€” TP +12% / SL -3%
function detectFigure(closes, volumes, livePrice) {
  if (closes.length < 100) return null;
  const n = closes.length;
  const price = livePrice || closes[n - 1];
  const volNow = volumes[n - 1];
  const volAvg = avg(volumes.slice(-50));
  const volRatio = volNow / volAvg;
  if (volRatio < VOL_CONFIRM) return null;

  const slice = closes.slice(-150);
  const h = Math.max(...slice), l = Math.min(...slice);
  const figH = h - l;
  const range = figH / price;
  const trend10 = (price - closes[n - 51]) / closes[n - 51];

  // Calcul de la hauteur de la figure (haut - bas) / bas
  // â†’ DÃ©termine le TP dynamique selon la puissance du signal
  const figureTarget = (h - l) / l; // hauteur relative de la figure

  // Filtre: figure trop petite (<40%) â†’ ignorÃ©e
  if (figureTarget < 0.40) return null;

  // TP dynamique selon la hauteur mesurÃ©e de la figure
  // SL toujours -3%
  let tpPct;
  if      (figureTarget >= 0.80) tpPct = 0.20; // 80-100% â†’ TP +20%
  else if (figureTarget >= 0.71) tpPct = 0.17; // 71-80%  â†’ TP +17%
  else if (figureTarget >= 0.61) tpPct = 0.15; // 61-70%  â†’ TP +15%
  else if (figureTarget >= 0.50) tpPct = 0.13; // 50-60%  â†’ TP +13%
  else                           tpPct = 0.08; // 40-49%  â†’ TP +8%

  const tp = +(price * (1 + tpPct)).toFixed(8);
  const sl = +(price * (1 - SL_PCT)).toFixed(8); // SL toujours -3%

  // Cup & Handle
  if (n >= 100) {
    const midLow = Math.min(...closes.slice(n - 60, n - 20));
    const resistance = Math.max(...closes.slice(n - 30, n - 1));
    if (midLow < closes[n - 70] * 0.95 && price > resistance && volRatio > 1.8)
      return { fig: FIGURES[0], tp, sl, figureTarget };
  }

  // ETE Inverse
  if (n >= 100) {
    const headL = Math.min(...closes.slice(n - 60, n - 20));
    const shL = Math.min(...closes.slice(n - 70, n - 50));
    const necklineL = Math.max(...closes.slice(n - 60, n - 2));
    if (headL < shL * 0.98 && headL < closes[n - 2] * 0.98 && price > necklineL && volRatio > 1.5)
      return { fig: FIGURES[1], tp, sl, figureTarget };
  }

  // Double Bottom
  if (n >= 70) {
    const mn1 = Math.min(...closes.slice(n - 50, n - 25));
    const mn2 = Math.min(...closes.slice(n - 25, n));
    const sommet = Math.max(...closes.slice(n - 40, n - 2));
    if (Math.abs(mn1 - mn2) / mn1 < 0.015 && price > sommet && volRatio > 1.4)
      return { fig: FIGURES[2], tp, sl, figureTarget };
  }

  // Triangle Ascendant
  if (range < 0.04 && trend10 > 0.01 && price > h * 0.999 && volRatio > 1.6)
    return { fig: FIGURES[3], tp, sl, figureTarget };

  // Drapeau Haussier
  if (trend10 > 0.06 && range < 0.025 && price > h * 0.999 && volRatio > 1.8)
    return { fig: FIGURES[4], tp, sl, figureTarget };

  // Biseau Baissier (bullish) â€” cassure haut du biseau
  if (range < 0.035 && trend10 < -0.02 && trend10 > -0.05 && price > h * 0.999 && volRatio > 1.7)
    return { fig: FIGURES[5], tp, sl, figureTarget };

  return null;
}

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// WEBSOCKET KRAKEN
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
const krakenCandles = {};    // bougies Daily (1440 min)
const krakenCandles4h = {};  // bougies 4h (240 min)
let krakenPairsList = [];
let wsConnected = false;
let ws = null;
let ws4h = null; // WebSocket sÃ©parÃ© pour 4h

const QUOTE_CURRENCIES = ['USD'];

async function fetchKrakenUsdtPairs() {
  try {
    const exchange = new ccxt.kraken({ enableRateLimit: true, timeout: 15000 });
    const markets = await exchange.loadMarkets();
    const allSymbols = Object.keys(markets);
    console.log(`[Diagnostic] Kraken: ${allSymbols.length} marches au total via ccxt`);

    const pairs = allSymbols.filter(s => {
      const m = markets[s];
      const matchesQuote = QUOTE_CURRENCIES.some(q => s.endsWith('/' + q));
      const isActive = m.active !== false;
      const isSpot = m.spot === true || m.type === 'spot';
      return matchesQuote && isActive && isSpot;
    });

    QUOTE_CURRENCIES.forEach(q => {
      const count = pairs.filter(s => s.endsWith('/' + q)).length;
      console.log(`[Diagnostic] ${count} paires /${q} retenues`);
    });
    console.log(`[Diagnostic] ${pairs.length} paires au total (USD uniquement)`);
    return pairs.slice(0, MAX_PAIRS);
  } catch (e) {
    console.log('Erreur fetchKrakenUsdtPairs:', e.message);
    return [];
  }
}

// Prix live par paire â€” mis Ã  jour par le ticker WebSocket
const livePrices = {};
let wsTicker = null;

function connectKrakenTicker(pairs) {
  if (wsTicker) { try { wsTicker.terminate(); } catch (e) {} }
  wsTicker = new WebSocket('wss://ws.kraken.com/v2');
  wsTicker.on('open', () => {
    console.log(`[Ticker] WebSocket prix live connecte â€” ${pairs.length} paires`);
    const CHUNK = 50;
    for (let i = 0; i < pairs.length; i += CHUNK) {
      const chunk = pairs.slice(i, i + CHUNK);
      wsTicker.send(JSON.stringify({ method: 'subscribe', params: { channel: 'ticker', symbol: chunk } }));
    }
  });
  wsTicker.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.channel === 'ticker' && msg.data) {
        for (const t of msg.data) {
          if (t.symbol && t.last) {
            livePrices[t.symbol] = t.last;
            // Aussi stocker sous format alternatif (BTCâ†”XBT)
            const alt = t.symbol.replace('XBT','BTC').replace('BTC','XBT');
            if (alt !== t.symbol) livePrices[alt] = t.last;
          }
        }
      }
    } catch (e) {}
  });
  wsTicker.on('close', () => {
    console.log('[Ticker] Deconnecte â€” reconnexion dans 5s');
    setTimeout(() => connectKrakenTicker(krakenPairsList), 5000);
  });
  wsTicker.on('error', (err) => { console.log('[Ticker] Erreur:', err.message); });
}

function connectKrakenWS(pairs) {
  if (ws) { try { ws.terminate(); } catch (e) {} }
  ws = new WebSocket('wss://ws.kraken.com/v2');

  ws.on('open', () => {
    wsConnected = true;
    console.log(`WebSocket Kraken connecte â€” abonnement a ${pairs.length} paires`);
    const CHUNK = 50;
    for (let i = 0; i < pairs.length; i += CHUNK) {
      const chunk = pairs.slice(i, i + CHUNK);
      ws.send(JSON.stringify({
        method: 'subscribe',
        params: { channel: 'ohlc', symbol: chunk, interval: 1440 }
      }));
    }
  });

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.channel === 'ohlc' && (msg.type === 'snapshot' || msg.type === 'update') && msg.data) {
        for (const c of msg.data) {
          const sym = c.symbol;
          if (!krakenCandles[sym]) krakenCandles[sym] = [];
          // Stocker seulement close + volume â†’ 3x moins de RAM
          const arr = krakenCandles[sym] || (krakenCandles[sym] = []);
          const lastC = arr[arr.length - 1];
          if (lastC && Math.abs(lastC.c - c.close) / (c.close||1) < 0.5) {
            lastC.c = c.close; lastC.v = c.volume;
          } else {
            arr.push({ c: c.close, v: c.volume });
            if (arr.length > 150) arr.shift();
          }
          // Scanner UNIQUEMENT cette paire immÃ©diatement â€” pas les 500
          // â†’ dÃ©tection instantanÃ©e sans OOM
          setImmediate(() => scanSinglePair(sym));
        }
      }
    } catch (e) {}
  });

  ws.on('close', () => {
    wsConnected = false;
    console.log('WebSocket Kraken deconnecte â€” reconnexion dans 5s');
    setTimeout(() => connectKrakenWS(krakenPairsList), 5000);
  });

  ws.on('error', (err) => { console.log('Erreur WebSocket Kraken:', err.message); });
}

// WebSocket 4h â€” en parallÃ¨le du Daily
function connectKrakenWS4h(pairs) {
  if (ws4h) { try { ws4h.terminate(); } catch (e) {} }
  ws4h = new WebSocket('wss://ws.kraken.com/v2');

  ws4h.on('open', () => {
    console.log(`[WS-4h] ConnectÃ© â€” abonnement ${pairs.length} paires`);
    const CHUNK = 50;
    for (let i = 0; i < pairs.length; i += CHUNK) {
      const chunk = pairs.slice(i, i + CHUNK);
      ws4h.send(JSON.stringify({
        method: 'subscribe',
        params: { channel: 'ohlc', symbol: chunk, interval: 240 } // 240 min = 4h
      }));
    }
  });

  ws4h.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.channel === 'ohlc' && (msg.type === 'snapshot' || msg.type === 'update') && msg.data) {
        for (const c of msg.data) {
          const sym = c.symbol;
          const arr = krakenCandles4h[sym] || (krakenCandles4h[sym] = []);
          const lastC = arr[arr.length - 1];
          if (lastC && Math.abs(lastC.c - c.close) / (c.close || 1) < 0.5) {
            lastC.c = c.close; lastC.v = c.volume;
          } else {
            arr.push({ c: c.close, v: c.volume });
            if (arr.length > 1000) arr.shift(); // 1000 bougies 4h = ~166 jours
          }
          // Scanner cette paire en 4h immÃ©diatement
          setImmediate(() => scanSinglePair(sym, '4h'));
        }
      }
    } catch (e) {}
  });

  ws4h.on('close', () => {
    console.log('[WS-4h] DÃ©connectÃ© â€” reconnexion dans 5s');
    setTimeout(() => connectKrakenWS4h(krakenPairsList), 5000);
  });

  ws4h.on('error', (err) => { console.log('[WS-4h] Erreur:', err.message); });
}

async function preloadHistoricalCandles(pairs) {
  console.log(`Preloading ${pairs.length} paires via REST (160 bougies Daily historiques)...`);
  const exchange = new ccxt.kraken({ enableRateLimit: true, timeout: 10000 });
  const BATCH = 50;
  let loaded = 0;
  for (let i = 0; i < pairs.length; i += BATCH) {
    const batch = pairs.slice(i, i + BATCH);
    await Promise.all(batch.map(async (symbol) => {
      try {
        const ohlcv = await exchange.fetchOHLCV(symbol, '1d', undefined, 160);
        if (!ohlcv || ohlcv.length < 10) return;
        krakenCandles[symbol] = ohlcv.map(c => ({
          c: c[4], v: c[6] || c[5]  // seulement close + volume en mÃ©moire
        }));
        loaded++;
      } catch (e) {}
    }));
    console.log(`Preloading... ${Math.min(i + BATCH, pairs.length)}/${pairs.length} paires`);
    if (i + BATCH < pairs.length) await new Promise(r => setTimeout(r, 500));
  }
  console.log(`Preloading termine â€” ${loaded}/${pairs.length} paires chargees avec historique`);

  // Preload 4h en parallÃ¨le
  console.log('Preloading 4h...');
  let loaded4h = 0;
  for (let i = 0; i < pairs.length; i += BATCH) {
    const batch = pairs.slice(i, i + BATCH);
    await Promise.all(batch.map(async (symbol) => {
      try {
        const ohlcv = await exchange.fetchOHLCV(symbol, '4h', undefined, 1000);
        if (!ohlcv || ohlcv.length < 10) return;
        krakenCandles4h[symbol] = ohlcv.map(c => ({ c: c[4], v: c[6] || c[5] }));
        loaded4h++;
      } catch (e) {}
    }));
    if (i + BATCH < pairs.length) await new Promise(r => setTimeout(r, 300));
  }
  console.log(`Preloading 4h terminÃ© â€” ${loaded4h}/${pairs.length} paires`);
}

async function initKrakenWS() {
  krakenPairsList = await fetchKrakenUsdtPairs();
  if (krakenPairsList.length === 0) {
    console.log('Aucune paire Kraken trouvee â€” retry dans 15s');
    setTimeout(initKrakenWS, 15000);
    return;
  }
  console.log(`${krakenPairsList.length} paires /USD Kraken trouvees pour le flux WebSocket`);
  await preloadHistoricalCandles(krakenPairsList);
  connectKrakenWS(krakenPairsList);
  connectKrakenTicker(krakenPairsList);
}

// Anti-spam: Ã©viter de remettre le mÃªme signal en cache si dÃ©jÃ  prÃ©sent
const recentSignals = new Map(); // symbol â†’ timestamp dernier signal

// Scan d'UNE seule paire â€” appelÃ© instantanÃ©ment sur chaque update WebSocket
function scanSinglePair(symbol, timeframe = '1d') {
  try {
    const candleStore = timeframe === '4h' ? krakenCandles4h : krakenCandles;
    const candles = candleStore[symbol];
    if (!candles || candles.length < 100) return;
    const closes  = candles.map(c => c.c);
    const volumes = candles.map(c => c.v);
    const livePrice = livePrices[symbol] || closes[closes.length - 1];
    const sig = detectFigure(closes, volumes, livePrice);
    if (!sig) return;

    // Anti-spam mÃ©moire: Ã©vite de spammer executeTrade sur chaque tick WebSocket
    // La vraie logique "une figure = un trade" est dans executeTrade() via la DB
    const lastSig = recentSignals.get(symbol);
    const sigKey = symbol + '|' + sig.fig.name; // clÃ© unique par paire + figure
    const lastSigKey = recentSignals.get(sigKey);
    if (lastSigKey && Date.now() - lastSigKey < 60 * 60 * 1000) return; // 1h anti-spam mÃ©moire
    recentSignals.set(sigKey, Date.now());

    const volRatio = volumes[volumes.length - 1] / avg(volumes.slice(-50));
    const tpPctSignal = sig.tp / livePrice - 1; // tpPct rÃ©el calculÃ©
    const signal = {
      symbol,
      exchange:    'Kraken',
      exchangeId:  'kraken',
      timeframe,
      market:      'Spot',
      figure:      sig.fig.name,
      figureCode:  sig.fig.code,
      direction:   sig.fig.dir,
      confidence:  Math.round(sig.fig.wr * 100),
      reliable:    sig.fig.wr >= 0.65,
      entryPrice:  livePrice,
      tp:          sig.tp,
      sl:          sig.sl,
      tpPct:       +(tpPctSignal * 100).toFixed(1),   // ex: 17.0
      slPct:       +(SL_PCT * 100).toFixed(1),         // 3.0
      figureTarget:+(sig.figureTarget * 100).toFixed(1), // hauteur figure %
      volumeRatio: volRatio.toFixed(2),
      tradeAmount: TRADE_AMOUNT,
      gain:        (TRADE_AMOUNT * tpPctSignal).toFixed(4),
      loss:        (TRADE_AMOUNT * SL_PCT).toFixed(4),
      time:        new Date()
    };

    // Ajouter au cache (remplacer si dÃ©jÃ  prÃ©sent)
    const idx = signalsCache.findIndex(s => s.symbol === symbol);
    if (idx >= 0) signalsCache[idx] = signal;
    else if (signalsCache.length < MAX_SIGNALS_CACHE) signalsCache.push(signal);

    // Sauvegarder en DB
    new Signal({
      symbol, exchange: 'Kraken', market: 'Spot',
      figure: sig.fig.name, direction: sig.fig.dir,
      confidence: signal.confidence, entryPrice: livePrice,
      tp: sig.tp, sl: sig.sl, volumeRatio: volRatio, timeframe: '1d'
    }).save().catch(() => {});

    // ExÃ©cuter le trade immÃ©diatement si utilisateurs connectÃ©s
    executeTrade(signal).catch(() => {});

    console.log(`[Signal] ${symbol} Â· ${sig.fig.name} Â· ${livePrice}`);
  } catch(e) {}
}

// ExÃ©cute un trade pour tous les utilisateurs Kraken connectÃ©s
async function executeTrade(signal) {
  try {
    const users = await User.find({ active: true, apiKey: { $exists: true }, exchangeName: /kraken/i });
    for (const user of users) {
      try {
        // RÃ¨gle 1: Pas de position ouverte sur cette paire
        const existingPos = await OpenPosition.findOne({ email: user.email, symbol: signal.symbol });
        if (existingPos) {
          console.log(`[Bot] Position dÃ©jÃ  ouverte sur ${signal.symbol} â€” skip`);
          continue;
        }

        // RÃ¨gle 2: mÃªme figure dÃ©jÃ  tradÃ©e â†’ skip jusqu'Ã  nouvelle formation
        const lastTrade = await Trade.findOne(
          { email: user.email, symbol: signal.symbol },
          null,
          { sort: { time: -1 } }
        );
        if (lastTrade) {
          const sameFigure = lastTrade.figure === signal.figure;
          const priceDiff = Math.abs(signal.entryPrice - lastTrade.entryPrice) / lastTrade.entryPrice;
          const newFormation = priceDiff > 0.20;
          if (sameFigure && !newFormation) {
            console.log(`[Bot] ${signal.symbol} â€” mÃªme figure ${signal.figure} dÃ©jÃ  tradÃ©e, attente nouvelle formation (besoin >20% de diff de prix)`);
            continue;
          }
        }
        const ExClass = ccxt['kraken'];
        if (!ExClass) continue;
        const exchange = new ExClass({ apiKey: user.apiKey, secret: user.apiSecret, enableRateLimit: true });
        const balance = await exchange.fetchBalance();
        const usd = balance.USD?.free || 0;
        const amount = Math.max(user.tradeAmount || TRADE_AMOUNT, 5); // montant libre, min 5
        if (usd < amount) {
          console.log(`[Bot] âš  Fonds insuffisants pour ${signal.symbol} (${usd.toFixed(2)} USD dispo, besoin ${amount} USD) â€” signal ignorÃ©, trop tard pour entrer`);
          continue; // skip immÃ©diat â€” pas de retry
        }
        const qty     = amount / signal.entryPrice;
        const tpPrice = signal.tp;
        const slPrice = signal.sl;
        console.log(`[Bot] ORDRE BUY: ${signal.symbol} Â· ${signal.figure} Â· $${amount} Â· TP:${tpPrice} Â· SL:${slPrice}`);
        const order = await exchange.createOrder(signal.symbol, 'market', 'buy', qty, undefined, { oflags: 'fciq' });
        console.log(`[Bot] Ordre exÃ©cutÃ©: ${order.id}`);
        await new OpenPosition({ email: user.email, symbol: signal.symbol, exchange: 'Kraken',
          figure: signal.figure, entryPrice: signal.entryPrice,
          tp: tpPrice, sl: slPrice,
          tpPct: signal.tpPct, figureTarget: signal.figureTarget,
          qty, amount }).save();
        await new Trade({ email: user.email, symbol: signal.symbol, exchange: 'Kraken',
          market: 'Spot', direction: signal.direction, figure: signal.figure,
          entryPrice: signal.entryPrice, exitPrice: null, amount, pnl: 0,
          result: 'OPEN', exitReason: 'Position ouverte â€” en attente TP/SL' }).save();
      } catch(e) { console.log(`[Bot] Erreur trade ${signal.symbol}:`, e.message); }
    }
  } catch(e) { console.log('[executeTrade] Erreur:', e.message); }
}

function scanKrakenFromMemory() {
  const results = [];
  for (const symbol of krakenPairsList) {
    const candles = krakenCandles[symbol];
    if (!candles || candles.length < 20) continue;
    const closes = candles.filter(c => c.c > 0).map(c => c.c);
    const volumes = candles.filter(c => c.v > 0).map(c => c.v);
    const livePrice = closes[closes.length - 1];
    const price = livePrice;
    const sig = detectFigure(closes, volumes, livePrice);
    if (!sig) continue;

    const volRatio = volumes[volumes.length - 1] / avg(volumes.slice(-50));
    const signal = {
      symbol,
      exchange:    'Kraken',
      exchangeId:  'kraken',
      timeframe:   '1d',
      market:      'Spot',
      figure:      sig.fig.name,
      figureCode:  sig.fig.code,
      direction:   sig.fig.dir,
      confidence:  Math.round(sig.fig.wr * 100),
      reliable:    sig.fig.wr >= 0.65,
      entryPrice:  price,
      tp:          sig.tp,
      sl:          sig.sl,
      volumeRatio: volRatio.toFixed(2),
      tradeAmount: TRADE_AMOUNT,
      gain:        (TRADE_AMOUNT * TP_PCT).toFixed(4),
      loss:        (TRADE_AMOUNT * SL_PCT).toFixed(4),
      time:        new Date()
    };
    results.push(signal);

    new Signal({
      symbol, exchange: 'Kraken', market: 'Spot',
      figure: sig.fig.name, direction: sig.fig.dir,
      confidence: signal.confidence, entryPrice: price,
      tp: sig.tp, sl: sig.sl, volumeRatio: volRatio, timeframe: '1d'
    }).save().catch(() => {});
  }
  return results;
}

async function scanExchangeRest(exConfig) {
  const results = [];
  try {
    const ExClass = ccxt[exConfig.id];
    if (!ExClass) return results;
    const exchange = new ExClass({ enableRateLimit: true, timeout: 10000 });

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
        return isUSDT && isSpot && m.active;
      })
      .slice(0, 100);

    const BATCH = 15;
    for (let i = 0; i < symbols.length; i += BATCH) {
      const batch = symbols.slice(i, i + BATCH);
      const batchResults = await Promise.all(batch.map(async (symbol) => {
        try {
          const ohlcv = await exchange.fetchOHLCV(symbol, '1d', undefined, 160);
          if (!ohlcv || ohlcv.length < 20) return null;
          const closes  = ohlcv.map(c => c[4]);
          const volumes = ohlcv.map(c => c[5]);
          const price   = closes[closes.length - 1];
          const market  = markets[symbol].type;
          const sig = detectFigure(closes, volumes, price);
          if (!sig) return null;
          const volRatio = volumes[volumes.length - 1] / avg(volumes.slice(-50));
          return {
            symbol, exchange: exConfig.name, exchangeId: exConfig.id, timeframe: '1d',
            market: market === 'spot' ? 'Spot' : 'Futures',
            figure: sig.fig.name, figureCode: sig.fig.code, direction: sig.fig.dir,
            confidence: Math.round(sig.fig.wr * 100),
            reliable: sig.fig.wr >= 0.65, entryPrice: price,
            tp: sig.tp, sl: sig.sl, volumeRatio: volRatio.toFixed(2),
            tradeAmount: TRADE_AMOUNT, gain: (TRADE_AMOUNT * TP_PCT).toFixed(4),
            time: new Date()
          };
        } catch (e) { return null; }
      }));
      batchResults.forEach(r => { if (r) results.push(r); });
    }
  } catch (e) {
    console.log(`[${exConfig.name}] Erreur: ${e.message}`);
  }
  return results;
}

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// SUIVI TP/SL INSTANTANÃ‰ â€” toutes les 2 secondes via prix WebSocket
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// Verrou anti-boucle: positions en cours de traitement
const positionsInProgress = new Set();

async function checkTPSLInstant() {
  try {
    const positions = await OpenPosition.find({});
    if (positions.length === 0) return;

    for (const pos of positions) {
      const posId = pos._id.toString();

      // Skip si deja en cours de traitement (evite les doublons toutes les 2s)
      if (positionsInProgress.has(posId)) continue;

      const currentPrice = livePrices[pos.symbol];
      if (!currentPrice) {
        // Log diagnostic si prix manquant â€” aide Ã  dÃ©bugger
        const keys = Object.keys(livePrices).slice(0, 3).join(', ');
        console.log(`[TP/SL] âš  Prix manquant pour ${pos.symbol} (livePrices a ${Object.keys(livePrices).length} entrÃ©es, ex: ${keys})`);
        continue;
      }

      // Validation TP/SL coherents (TP doit etre > entryPrice, SL < entryPrice)
      if (pos.tp <= pos.entryPrice || pos.sl >= pos.entryPrice) {
        console.log(`[INSTANT TP/SL] âš  TP/SL incohÃ©rents pour ${pos.symbol} â€” correction auto`);
        const correctedTP = +(pos.entryPrice * (1 + TP_PCT)).toFixed(8);
        const correctedSL = +(pos.entryPrice * (1 - SL_PCT)).toFixed(8);
        await OpenPosition.updateOne({ _id: pos._id }, { tp: correctedTP, sl: correctedSL });
        console.log(`[INSTANT TP/SL] CorrigÃ©: TP=${correctedTP} SL=${correctedSL}`);
        continue;
      }

      const hitTP = currentPrice >= pos.tp;
      const hitSL = currentPrice <= pos.sl;
      if (!hitTP && !hitSL) continue;

      const reason = hitTP ? 'TP' : 'SL';
      console.log(`[INSTANT ${reason}] ${pos.symbol} prix:${currentPrice} TP:${pos.tp} SL:${pos.sl}`);

      positionsInProgress.add(posId);
      try {
        const user = await User.findOne({ email: pos.email });
        if (!user) {
          await OpenPosition.deleteOne({ _id: pos._id });
          positionsInProgress.delete(posId);
          continue;
        }

        const exchangeName = user.exchangeName.toLowerCase();
        const ExClass = ccxt[exchangeName];
        if (!ExClass) {
          console.log(`[INSTANT TP/SL] Exchange ${exchangeName} non supportÃ© â€” position supprimÃ©e`);
          await OpenPosition.deleteOne({ _id: pos._id });
          positionsInProgress.delete(posId);
          continue;
        }

        const exchange = new ExClass({ apiKey: user.apiKey, secret: user.apiSecret, enableRateLimit: true });
        const balance = await exchange.fetchBalance();
        const [base] = pos.symbol.split('/');
        const baseBalance = balance[base]?.free || 0;

        if (baseBalance < 0.000001) {
          // Fonds insuffisants â€” la position a probablement deja ete vendue manuellement
          console.log(`[INSTANT ${reason}] ${pos.symbol} â€” solde ${base} insuffisant (${baseBalance}) â€” position supprimÃ©e de la DB`);
          await Trade.findOneAndUpdate(
            { email: pos.email, symbol: pos.symbol, result: 'OPEN' },
            { exitPrice: currentPrice, pnl: 0, result: 'CLOSED_MANUAL', exitReason: 'Vendu manuellement ou solde vide' },
            { sort: { time: -1 } }
          );
          await OpenPosition.deleteOne({ _id: pos._id });
          positionsInProgress.delete(posId);
          continue;
        }

        const order = await exchange.createOrder(pos.symbol, 'market', 'sell', baseBalance, undefined, { oflags: 'fciq' });
        console.log(`[INSTANT ${reason}] Ordre SELL execute: ${order.id}`);
        // Utiliser le tpPct rÃ©el de la position si disponible, sinon fallback TP_PCT
        const posTpPct = pos.tpPct ? pos.tpPct / 100 : TP_PCT;
        const pnl = hitTP ? pos.amount * posTpPct : -(pos.amount * SL_PCT);
        await Trade.findOneAndUpdate(
          { email: pos.email, symbol: pos.symbol, result: 'OPEN' },
          { exitPrice: currentPrice, pnl, result: hitTP ? 'WIN' : 'LOSS', exitReason: hitTP ? `TP +${TP_PCT*100}% atteint` : `SL -${SL_PCT*100}% touche` },
          { sort: { time: -1 } }
        );
        await OpenPosition.deleteOne({ _id: pos._id });
        console.log(`[INSTANT ${reason}] Position fermee PnL: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(4)}`);

      } catch (e) {
        console.log(`[INSTANT TP/SL] Erreur ${pos.symbol}:`, e.message);
        // Sur erreur nonce ou reseau: on retire du verrou pour reessayer au prochain cycle
        // Sur erreur "Insufficient funds": on supprime la position
        if (e.message && e.message.includes('Insufficient funds')) {
          console.log(`[INSTANT TP/SL] Fonds insuffisants confirmÃ©s â€” suppression position ${pos.symbol}`);
          await OpenPosition.deleteOne({ _id: pos._id }).catch(() => {});
        }
      } finally {
        positionsInProgress.delete(posId);
      }
    }
  } catch (e) { console.log('[INSTANT TP/SL] Erreur globale:', e.message); }
}

let scanRunning = false;
async function scanAll() {
  if (scanRunning) {
    console.log('Scan precedent encore en cours â€” on attend le prochain cycle');
    return;
  }
  scanRunning = true;
  const startTime = Date.now();
  console.log(`\n=== SCAN 1D â€” ${new Date().toLocaleTimeString()} ===`);
  signalsCache.length = 0;
  Object.keys(signalsByExchange).forEach(k => delete signalsByExchange[k]);
  // Forcer GC si disponible (node --expose-gc)
  if (typeof global.gc === 'function') global.gc();

  try {
    const users = await User.find({ active: true, apiKey: { $exists: true } });

    if (users.length === 0) {
      console.log('Aucun utilisateur â€” scan Kraken par defaut (mode test, via WebSocket)');
      const results = scanKrakenFromMemory();
      signalsCache.push(...results);
      signalsByExchange['kraken'] = results;
      lastScanTime = new Date();
      console.log(`[Kraken-WS] ${krakenPairsList.length} paires en memoire Â· ${results.length} signal(s)`);
      console.log(`=== FIN test Â· ${signalsCache.length} signaux Â· ${Date.now() - startTime}ms ===\n`);
      return;
    }

    const uniqueExchanges = [...new Set(users.map(u => u.exchangeName.toLowerCase()))];
    console.log(`Utilisateurs: ${users.length} Â· Plateformes: ${uniqueExchanges.join(', ')}`);

    for (const exchangeId of uniqueExchanges) {
      const exConfig = EXCHANGES_CONFIG.find(e => e.id === exchangeId || e.name.toLowerCase() === exchangeId);
      if (!exConfig) continue;

      let results;
      if (exConfig.id === 'kraken') {
        results = scanKrakenFromMemory();
        console.log(`[Kraken-WS] ${krakenPairsList.length} paires en memoire Â· ${results.length} signal(s)`);
      } else {
        results = await scanExchangeRest(exConfig);
        console.log(`[${exConfig.name}-REST] ${results.length} signal(s)`);
      }
      signalsCache.push(...results);
      signalsByExchange[exConfig.id] = results;
    }

    lastScanTime = new Date();
    console.log(`=== FIN Â· ${signalsCache.length} signaux Â· ${Date.now() - startTime}ms ===\n`);

    // Trades exÃ©cutÃ©s instantanÃ©ment par executeTrade() sur chaque signal WebSocket
    // scanAll() est seulement un scan de rattrapage toutes les 60s
    for (const user of users) {
      const userExchangeName = user.exchangeName.toLowerCase();
      const userSignals = signalsCache.filter(s =>
        s.exchange.toLowerCase() === userExchangeName ||
        s.exchangeId === userExchangeName
      );
      if (userSignals.length === 0) continue;

      try {
        const ExClass = ccxt[userExchangeName];
        if (!ExClass) continue;
        const exchange = new ExClass({ apiKey: user.apiKey, secret: user.apiSecret, enableRateLimit: true });

        let balance;
        try { balance = await exchange.fetchBalance(); }
        catch (e) { console.log(`[Bot] Erreur balance ${user.email}:`, e.message); continue; }

        const usd = balance.USD?.free || 0;
        const rawAmount = user.tradeAmount || TRADE_AMOUNT;
        const amount    = Math.max(rawAmount, 5);
        if (usd < amount) {
          console.log(`[Bot] âš  Fonds insuffisants (${usd.toFixed(2)} USD) â€” tous les signaux ignorÃ©s pour ${user.email}`);
          continue; // skip immÃ©diat
        }
        let ordersPlaced = 0;

        for (const sig of userSignals.slice(0, MAX_CONCURRENT)) {
          if (ordersPlaced >= MAX_CONCURRENT) break;
          try {
            const [base, quote] = sig.symbol.split('/');
            const quoteBalance = balance[quote]?.free || balance['USD']?.free || 0;
            const price = sig.entryPrice;

            if (sig.direction === 'Long' && quoteBalance >= amount) {
              // RÃ¨gle 1: pas de position ouverte
              const existingPos = await OpenPosition.findOne({ email: user.email, symbol: sig.symbol });
              if (existingPos) { console.log(`[Bot] Position ouverte sur ${sig.symbol} â€” skip`); continue; }

              // RÃ¨gle 2: mÃªme figure dÃ©jÃ  tradÃ©e â†’ attendre nouvelle formation
              const lastTrade = await Trade.findOne(
                { email: user.email, symbol: sig.symbol },
                null, { sort: { time: -1 } }
              );
              if (lastTrade) {
                const sameFigure = lastTrade.figure === sig.figure;
                const priceDiff = Math.abs(sig.entryPrice - lastTrade.entryPrice) / lastTrade.entryPrice;
                const newFormation = priceDiff > 0.20;
                if (sameFigure && !newFormation) {
                  console.log(`[Bot] ${sig.symbol} â€” mÃªme figure ${sig.figure} dÃ©jÃ  tradÃ©e, attente nouvelle formation`);
                  continue;
                }
              }
              const qty     = amount / price;
              const tpPrice = +(price * (1 + TP_PCT)).toFixed(8);
              const slPrice = +(price * (1 - SL_PCT)).toFixed(8);
              console.log(`[Bot] ORDRE BUY MARKET: ${sig.symbol} Â· ${sig.figure} Â· $${amount} Â· TP:${tpPrice} Â· SL:${slPrice}`);
              const order = await exchange.createOrder(sig.symbol, 'market', 'buy', qty, undefined, { oflags: 'fciq' });
              console.log(`[Bot] Ordre execute: ${order.id}`);
              await new OpenPosition({
                email: user.email, symbol: sig.symbol, exchange: sig.exchange,
                figure: sig.figure, entryPrice: price,
                tp: tpPrice, sl: slPrice, qty, amount
              }).save();
              await new Trade({
                email: user.email, symbol: sig.symbol, exchange: sig.exchange,
                market: sig.market, direction: sig.direction, figure: sig.figure,
                entryPrice: price, exitPrice: null,
                amount, pnl: 0,
                result: 'OPEN', exitReason: 'Position ouverte â€” en attente TP/SL'
              }).save();
              ordersPlaced++;
            }
          } catch (e) { console.log(`[Bot] Erreur ordre ${sig.symbol}:`, e.message); }
        }
        if (ordersPlaced > 0) console.log(`[Bot] ${ordersPlaced} ordre(s) place(s) pour ${user.email}`);
      } catch (e) { console.log(`[Bot] Erreur utilisateur ${user.email}:`, e.message); }
    }
  } finally {
    scanRunning = false;
  }
}

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// ROUTES
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
app.get('/', (req, res) => res.json({
  status:             'Bender Pro v8.0 actif',
  strategy:           'Figures chartistes + Volume Â· TP+12% SL-3% Â· Timeframe 1D',
  scanMethod:         'WebSocket Kraken (temps reel) + REST en repli',
  tradeAmount:        TRADE_AMOUNT,
  slPct:              SL_PCT * 100 + '%',
  tpPct:              TP_PCT * 100 + '%',
  exchanges:          EXCHANGES_CONFIG.length,
  krakenWsConnected:  wsConnected,
  krakenPairsTracked: krakenPairsList.length,
  lastScan:           lastScanTime,
  signalsActive:      signalsCache.length,
}));

app.get('/market', (req, res) => {
  let sigs = [...signalsCache];
  if (req.query.exchange) sigs = sigs.filter(s => s.exchange.toLowerCase().includes(req.query.exchange.toLowerCase()));
  if (req.query.direction) sigs = sigs.filter(s => s.direction === req.query.direction);
  res.json({ success: true, signals: sigs, count: sigs.length, lastScan: lastScanTime });
});

app.get('/scan', async (req, res) => {
  res.json({ success: true, message: 'Scan lance...' });
  scanAll().catch(console.error);
});

app.post('/connect', async (req, res) => {
  const { email, apiKey, secret, exchangeName, tradeAmount } = req.body;
  if (!email || !apiKey || !secret || !exchangeName)
    return res.json({ success: false, error: 'Donnees manquantes' });
  try {
    await User.findOneAndUpdate(
      { email },
      { apiKey, apiSecret: secret, exchangeName, active: true, tradeAmount: tradeAmount || TRADE_AMOUNT },
      { upsert: true, new: true }
    );
    res.json({ success: true, message: `Connecte sur ${exchangeName} Â· $${tradeAmount || TRADE_AMOUNT}/trade Â· TP+${TP_PCT*100}% SL-${SL_PCT*100}% Â· Daily 150 bougies` });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

app.get('/status/:email', async (req, res) => {
  try {
    const user = await User.findOne({ email: req.params.email });
    if (!user) return res.json({ connected: false });
    const trades = await Trade.countDocuments({ email: req.params.email });
    const wins   = await Trade.countDocuments({ email: req.params.email, result: 'WIN' });
    res.json({
      connected: true, active: user.active, exchange: user.exchangeName,
      tradeAmount: user.tradeAmount, trades,
      winRate: trades > 0 ? Math.round(wins / trades * 100) + '%' : 'N/A'
    });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

app.get('/trades/:email', async (req, res) => {
  try {
    const email = req.params.email;
    const totalCount  = await Trade.countDocuments({ email });
    const allTrades   = await Trade.find({ email });
    const totalPnl    = allTrades.reduce((a, t) => a + t.pnl, 0);
    const totalWins   = allTrades.filter(t => t.result === 'WIN').length;
    const totalLosses = totalCount - totalWins;
    const trades      = await Trade.find({ email }).sort({ time: -1 }).limit(100);
    res.json({
      trades,
      totalTradesCount: totalCount,
      totalPnl:         totalPnl.toFixed(4),
      wins:             totalWins,
      losses:           totalLosses,
      displayedCount:   trades.length
    });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// FACTURATION â€” Commission 0.25% du volume / 2 semaines
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
const COMMISSION_RATE = 0.0025;
const BILLING_WALLET  = process.env.BILLING_WALLET || 'VOTRE_WALLET_CRYPTO_ICI';
const BILLING_DAYS    = 14;

app.get('/billing/:email', async (req, res) => {
  try {
    const email = req.params.email;
    const user  = await User.findOne({ email });
    if (!user) return res.json({ success: false, error: 'Utilisateur non trouve' });

    const periodEnd   = new Date();
    const periodStart = new Date(periodEnd - BILLING_DAYS * 24 * 3600 * 1000);

    const trades = await Trade.find({
      email,
      time:   { $gte: periodStart, $lte: periodEnd },
      result: { $in: ['WIN', 'LOSS'] }
    });

    let totalVolume = trades.reduce((a, t) => a + t.amount, 0);
    const totalPnl  = trades.reduce((a, t) => a + t.pnl, 0);
    const wins      = trades.filter(t => t.result === 'WIN').length;
    const losses    = trades.filter(t => t.result === 'LOSS').length;
    const commission = +(totalVolume * COMMISSION_RATE).toFixed(4);

    let billing = await Billing.findOne({
      email,
      periodStart: { $gte: new Date(periodStart.getTime() - 3600000) }
    });

    if (!billing) {
      billing = await new Billing({
        email, periodStart, periodEnd,
        totalPnl:    +totalPnl.toFixed(4),
        totalVolume: +totalVolume.toFixed(4),
        commission,
        status: 'PENDING'
      }).save();
    }

    res.json({
      success: true,
      billing: {
        id:          billing._id,
        email,
        periodStart: periodStart.toLocaleDateString('fr-CA'),
        periodEnd:   periodEnd.toLocaleDateString('fr-CA'),
        trades:      trades.length,
        wins,
        losses,
        winRate:     trades.length > 0 ? Math.round(wins / trades.length * 100) + '%' : 'N/A',
        totalVolume: +totalVolume.toFixed(4),
        commission,
        status:      billing.status,
        paidAt:      billing.paidAt ? new Date(billing.paidAt).toLocaleDateString('fr-CA') : null,
        wallet:      BILLING_WALLET,
        message:     `Commission due: $${commission} USD (0.25% des $${totalVolume.toFixed(4)} trades)`,
      }
    });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

app.post('/billing/paid/:email', async (req, res) => {
  try {
    const { txHash } = req.body;
    const email       = req.params.email;
    const periodStart = new Date(Date.now() - BILLING_DAYS * 24 * 3600 * 1000);
    const billing = await Billing.findOneAndUpdate(
      { email, periodStart: { $gte: new Date(periodStart.getTime() - 3600000) } },
      { status: 'PAID', paidAt: new Date(), txHash: txHash || '' },
      { sort: { createdAt: -1 }, new: true }
    );
    if (!billing) return res.json({ success: false, error: 'Facture non trouvee' });
    res.json({ success: true, message: 'Paiement confirme', billing });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

app.get('/admin/billing', async (req, res) => {
  try {
    const pending  = await Billing.find({ status: 'PENDING', commission: { $gt: 0 } }).sort({ createdAt: -1 });
    const totalDue = pending.reduce((a, b) => a + b.commission, 0);
    res.json({
      success:  true,
      pending:  pending.length,
      totalDue: +totalDue.toFixed(4),
      wallet:   BILLING_WALLET,
      billings: pending
    });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

app.get('/signals', async (req, res) => {
  try {
    const signals = await Signal.find().sort({ time: -1 }).limit(100);
    res.json({ signals });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

app.get('/exchanges', (req, res) => {
  res.json({ exchanges: EXCHANGES_CONFIG });
});

let pricesCache = {};
let pricesCacheTime = 0;
function refreshPricesFromMemory() {
  const out = {};
  const watch = ['BTC/USD','ETH/USD','SOL/USD','XRP/USD','ADA/USD',
    'AVAX/USD','DOGE/USD','DOT/USD','LINK/USD','LTC/USD',
    'ATOM/USD','UNI/USD','NEAR/USD','ARB/USD','OP/USD','APT/USD','SUI/USD','INJ/USD'];
  for (const sym of watch) {
    const candles = krakenCandles[sym];
    if (candles && candles.length >= 2) {
      const last = candles[candles.length - 1];
      const prev = candles[0];
      out[sym.split('/')[0]] = {
        price:     last.c,
        changePct: prev.c ? ((last.c - prev.c) / prev.c) * 100 : null
      };
    }
  }
  pricesCache     = out;
  pricesCacheTime = Date.now();
}

app.get('/prices', (req, res) => {
  refreshPricesFromMemory();
  res.json({ success: true, prices: pricesCache, time: pricesCacheTime });
});

app.get('/platform-signals/:email', async (req, res) => {
  try {
    const user = await User.findOne({ email: req.params.email });
    if (!user) return res.json({ success: false, error: 'Utilisateur non trouve' });
    const exchangeId = user.exchangeName.toLowerCase();
    const exConfig   = EXCHANGES_CONFIG.find(e => e.id === exchangeId || e.name.toLowerCase() === exchangeId);
    const platformSignals = signalsByExchange[exConfig ? exConfig.id : exchangeId] || [];
    const amount  = user.tradeAmount || TRADE_AMOUNT;
    const enriched = platformSignals.map(s => ({
      ...s,
      potentialGainUSD: +(amount * TP_PCT).toFixed(4),
      potentialLossUSD: +(amount * SL_PCT).toFixed(4)
    }));
    res.json({
      success:     true,
      exchange:    exConfig ? exConfig.name : user.exchangeName,
      tradeAmount: amount,
      lastScan:    lastScanTime,
      count:       enriched.length,
      signals:     enriched
    });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

app.get('/clear-users', async (req, res) => {
  try {
    const result = await User.deleteMany({});
    console.log(`[clear-users] ${result.deletedCount} utilisateur(s) supprimes`);
    res.json({ success: true, deleted: result.deletedCount, message: 'Tous les utilisateurs ont ete supprimes.' });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

app.get('/admin/stats', async (req, res) => {
  try {
    const users  = await User.countDocuments();
    const active = await User.countDocuments({ active: true });
    const trades = await Trade.countDocuments();
    const wins   = await Trade.countDocuments({ result: 'WIN' });
    res.json({
      users, active, trades,
      winRate:            trades > 0 ? Math.round(wins / trades * 100) + '%' : 'N/A',
      signalsActive:      signalsCache.length,
      lastScan:           lastScanTime,
      exchanges:          EXCHANGES_CONFIG.length,
      krakenWsConnected:  wsConnected,
      krakenPairsTracked: krakenPairsList.length
    });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

app.post('/toggle', async (req, res) => {
  try {
    const { email, active } = req.body;
    await User.findOneAndUpdate({ email }, { active });
    res.json({ success: true, active });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// DÃ‰MARRAGE
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// Augmenter la limite mÃ©moire Node.js si pas dÃ©jÃ  fait
// â†’ Ajouter dans Render: Start Command = node --max-old-space-size=460 server.js
// (460MB = safe pour instance 512MB)

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`\n Bender Pro v8.0 Â· Port ${PORT}`);
  console.log(` Figures chartistes + Volume Â· TP+${TP_PCT*100}% SL-${SL_PCT*100}% Â· Daily 150 bougies`);
  console.log(` Scan Kraken via WebSocket (quasi instantane)`);
  console.log(` $${TRADE_AMOUNT}/trade Â· SL -${SL_PCT*100}% Â· TP +${TP_PCT*100}%`);
  console.log(` Scan toutes les 60 secondes (bougies 1D)\n`);
  setImmediate(async () => {

    // â”€â”€ Ã‰TAPE 1: DÃ©marrer TP/SL IMMÃ‰DIATEMENT (avant tout le reste)
    setInterval(() => checkTPSLInstant().catch(console.error), 2000);
    console.log(' Suivi TP/SL instantanÃ© actif (toutes les 2 secondes)');

    // â”€â”€ Ã‰TAPE 2: RÃ©cupÃ©rer les paires Kraken
    krakenPairsList = await fetchKrakenUsdtPairs().catch(() => []);
    if (krakenPairsList.length === 0) {
      console.log('Aucune paire trouvÃ©e â€” retry dans 15s');
      setTimeout(() => initKrakenWS(), 15000);
    } else {
      console.log(`${krakenPairsList.length} paires /USD Kraken trouvÃ©es`);

      // â”€â”€ Ã‰TAPE 3: Connecter le Ticker IMMÃ‰DIATEMENT (prix live pour TP/SL)
      connectKrakenTicker(krakenPairsList);
      console.log(' Ticker WebSocket connectÃ© â€” prix live actifs');

      // â”€â”€ Ã‰TAPE 4: Connecter les WebSocket OHLC IMMÃ‰DIATEMENT (Daily + 4h)
      connectKrakenWS(krakenPairsList);
      connectKrakenWS4h(krakenPairsList); // 4h en parallÃ¨le

      // â”€â”€ Ã‰TAPE 5: Preloading en arriÃ¨re-plan (sans bloquer)
      preloadHistoricalCandles(krakenPairsList).then(() => {
        console.log(' Preloading terminÃ© â€” signaux historiques disponibles');
        // Premier scan complet aprÃ¨s preloading
        scanAll().catch(console.error);
      }).catch(console.error);
    }

    // â”€â”€ Ã‰TAPE 6: Scan toutes les 60s (indÃ©pendant du preloading)
    setTimeout(() => {
      setInterval(() => scanAll().catch(console.error), SCAN_INTERVAL);
    }, 65000); // Attendre 65s que le preloading avance avant le premier scan
  });
});
