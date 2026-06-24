// Bender Pro v8.0 — Scan via WebSocket Kraken (quasi instantane)
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
const TRADE_AMOUNT   = 5; // Minimum 5 USD par trade
const SL_PCT         = 0.02;   // -2%
const TP_PCT         = 0.17;   // +17%
const MAX_CONCURRENT = 20;
const VOL_CONFIRM    = 1.8;
const SCAN_INTERVAL  = 60 * 1000; // scan toutes les 60s (bougies 1D)
const MAX_PAIRS      = 500;

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

// Positions ouvertes — suivi TP/SL automatique
const OpenPositionSchema = new mongoose.Schema({
  email:       String,
  symbol:      String,
  exchange:    String,
  figure:      String,
  entryPrice:  Number,
  tp:          Number,   // prix cible +4%
  sl:          Number,   // prix stop -1%
  qty:         Number,   // quantite achetee
  amount:      Number,   // montant en USD
  openedAt:    { type: Date, default: Date.now }
});

const User          = mongoose.model('User',          UserSchema);
const Trade         = mongoose.model('Trade',         TradeSchema);
const Signal        = mongoose.model('Signal',        SignalSchema);
const OpenPosition  = mongoose.model('OpenPosition',  OpenPositionSchema);

// FIGURES CHARTISTES
const FIGURES = [
  { name:'Cup & Handle',     code:'C&H',   dir:'Long',  wr:0.84 },
  // ETE retire — Short uniquement
  { name:'ETE Inverse',      code:'ETEi',  dir:'Long',  wr:0.81 },
  // Double Top retire — Short uniquement
  { name:'Double Bottom',    code:'2Bot',  dir:'Long',  wr:0.76 },
  { name:'Triangle Asc.',    code:'TriA',  dir:'Long',  wr:0.74 },
  // Triangle Desc. retire — Short uniquement
  { name:'Drapeau Haussier', code:'DrapH', dir:'Long',  wr:0.76 },
  // Drapeau Baissier retire — Short uniquement
  // Biseau Haussier retire — Short uniquement
  { name:'Biseau Baissier',  code:'BisB',  dir:'Long',  wr:0.73 },
];

// EXCHANGES (affichage seulement — le scan WebSocket rapide ne couvre que Kraken pour l'instant)
const EXCHANGES_FULL = [
  { id:'kraken',      name:'Kraken',   spot:true,  futures:false },
  { id:'binance',     name:'Binance',  spot:true,  futures:false },
  { id:'bybit',       name:'Bybit',    spot:true,  futures:false },
  { id:'bitget',      name:'Bitget',   spot:true,  futures:false },
  { id:'okx',         name:'OKX',      spot:true,  futures:false },
  { id:'kucoin',      name:'KuCoin',   spot:true,  futures:false },
  { id:'gateio',      name:'Gate.io',  spot:true,  futures:false },
  { id:'mexc',        name:'MEXC',     spot:true,  futures:false },
  { id:'bingx',       name:'BingX',    spot:true,  futures:false },
  { id:'phemex',      name:'Phemex',   spot:true,  futures:false },
  { id:'coinbasepro', name:'Coinbase', spot:true,  futures:false },
  { id:'bitfinex',    name:'Bitfinex', spot:true,  futures:false },
  { id:'bitstamp',    name:'Bitstamp', spot:true,  futures:false },
];

const signalsByExchange = {};
const signalsCache = [];
let lastScanTime = null;
const marketsCache = {};

function avg(arr) { return arr.reduce((a,b)=>a+b,0)/arr.length; }

// detectFigure — TP/SL FIXES · Entree a la cassure de resistance
// TP = +5.2% fixe / SL = -1% fixe / Filtre = 20% hauteur minimum
function detectFigure(closes, volumes, livePrice) {
  if (closes.length < 100) return null; // minimum 100 bougies Daily
  const n = closes.length;
  const price = livePrice || closes[n-1];
  const volNow = volumes[n-1];
  const volAvg = avg(volumes.slice(-50)); // moyenne volume sur 50 jours
  const volRatio = volNow / volAvg;
  if (volRatio < VOL_CONFIRM) return null;

  const slice = closes.slice(-150); // fenetre 150 bougies Daily (5 mois)
  const h = Math.max(...slice), l = Math.min(...slice);
  const figH = h - l;
  const range = figH / price;
  const trend10 = (price - closes[n-51]) / closes[n-51];

  // Filtre: figure d'au moins 50% de hauteur avec 65%+ de reussite historique
  if (range < 0.5 || sig.fig.wr < 0.65) return null;

  const tp = +(price * (1 + TP_PCT)).toFixed(8); // TP 17%
  const sl = +(price * (1 - 0.02)).toFixed(8);    // SL 2%

  // Cup & Handle — entree des que resistance franchie
  if (n >= 100) {
    const midLow = Math.min(...closes.slice(n-60, n-20));
    const resistance = Math.max(...closes.slice(n-30, n-1));
    if (midLow < closes[n-70]*0.95 && price > resistance && volRatio > 1.8)
      return { fig:FIGURES[0], tp, sl };
  }
  // ETE retire (Short — Spot Long seulement)
  // ETE Inverse — entree des que neckline cassee a la hausse
  if (n >= 100) {
    const headL = Math.min(...closes.slice(n-60, n-20));
    const shL = Math.min(...closes.slice(n-70, n-50));
    const necklineL = Math.max(...closes.slice(n-60, n-2));
    if (headL<shL*0.98 && headL<closes[n-2]*0.98 && price > necklineL && volRatio>1.5)
      return { fig:FIGURES[2], tp, sl };
  }
  // Double Top retire (Short — Spot Long seulement)
  // Double Bottom — entree des que resistance cassee
  if (n >= 70) {
    const mn1=Math.min(...closes.slice(n-50,n-25)), mn2=Math.min(...closes.slice(n-25,n));
    const sommet = Math.max(...closes.slice(n-40, n-2));
    if (Math.abs(mn1-mn2)/mn1<0.015 && price > sommet && volRatio>1.4)
      return { fig:FIGURES[4], tp, sl };
  }
  // Triangle Ascendant — cassure resistance haute
  if (range<0.04 && trend10>0.01 && price > h*0.999 && volRatio>1.6)
    return { fig:FIGURES[5], tp, sl };
  // Triangle Descendant retire (Short — Spot Long seulement)
  // Drapeau Haussier — cassure haut du canal
  if (trend10>0.06 && range<0.025 && price > h*0.999 && volRatio>1.8)
    return { fig:FIGURES[7], tp, sl };
  // Drapeau Baissier retire (Short — Spot Long seulement)
  // Biseau Haussier retire (Short — Spot Long seulement)
  // Biseau Baissier (bullish) — cassure haut du biseau
  if (range<0.035 && trend10<-0.02 && trend10>-0.05 && price > h*0.999 && volRatio>1.7)
    return { fig:FIGURES[10], tp, sl };

  return null;
}

// ═══════════════════════════════════════════════════════════════════
// WEBSOCKET KRAKEN — donnees publiques en continu (sans cle API)
// On garde en memoire les 50 dernieres bougies 1m de chaque paire,
// mises a jour en temps reel par le flux WebSocket.
// Le "scan" devient alors instantane: on lit juste la memoire.
// ═══════════════════════════════════════════════════════════════════
const krakenCandles = {}; // { 'BTC/USDT': [{o,h,l,c,v}, ...] }
let krakenPairsList = [];
let wsConnected = false;
let ws = null;

const QUOTE_CURRENCIES = ['USD']; // Seulement USD (USDT bloque Canada, USDC retire sur demande)

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

    // Compte par devise pour le diagnostic
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

// Prix live par paire — mis a jour par le ticker WebSocket (tick par tick)
// Utilise pour le suivi TP/SL instantane — plus precis que le close de bougie 30m
const livePrices = {}; // { 'BTC/USD': 105234.50 }
let wsTicker = null;

function connectKrakenTicker(pairs) {
  if (wsTicker) { try { wsTicker.terminate();   } catch(e) {} }
  wsTicker = new WebSocket('wss://ws.kraken.com/v2');
  wsTicker.on('open', () => {
    console.log(`[Ticker] WebSocket prix live connecte — ${pairs.length} paires`);
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
          if (t.symbol && t.last) livePrices[t.symbol] = t.last;
        }
      }
  } catch(e) {}
  });
  wsTicker.on('close', () => {
    console.log('[Ticker] Deconnecte — reconnexion dans 5s');
    setTimeout(() => connectKrakenTicker(krakenPairsList), 5000);
  });
  wsTicker.on('error', (err) => { console.log('[Ticker] Erreur:', err.message); });
}

function connectKrakenWS(pairs) {
  if (ws) {
    try { ws.terminate();   } catch(e) {}
  }
  ws = new WebSocket('wss://ws.kraken.com/v2');

  ws.on('open', () => {
    wsConnected = true;
    console.log(`WebSocket Kraken connecte — abonnement a ${pairs.length} paires`);
    // Kraken limite le nombre de symboles par message d'abonnement;
    // on envoie par lots de 50 pour rester safe
    const CHUNK = 50;
    for (let i = 0; i < pairs.length; i += CHUNK) {
      const chunk = pairs.slice(i, i + CHUNK);
      ws.send(JSON.stringify({
        method: 'subscribe',
        params: {
          channel: 'ohlc',
          symbol: chunk,
          interval: 1440
        }
      }));
    }
  });

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.channel === 'ohlc' && (msg.type === 'snapshot' || msg.type === 'update') && msg.data) {
        for (const c of msg.data) {
          const sym = c.symbol; // ex: "BTC/USDT"
          if (!krakenCandles[sym]) krakenCandles[sym] = [];
          const candle = {
            o: c.open, h: c.high, l: c.low, c: c.close, v: c.volume,
            t: c.interval_begin
          };
          const arr = krakenCandles[sym];
          // Si meme intervalle de temps, on remplace la derniere bougie (mise a jour live)
          // sinon on en ajoute une nouvelle
          if (arr.length > 0 && arr[arr.length - 1].t === candle.t) {
            arr[arr.length - 1] = candle;
          } else {
            arr.push(candle);
            if (arr.length > 150) arr.shift();
          }
          // Scan immediat sur nouvelles donnees
          setImmediate(() => {
            const results = scanKrakenFromMemory();
            results.forEach(r => signalsCache.push(r));
          });
        }
      }
    } catch (e) {}
  });

  ws.on('close', () => {
    wsConnected = false;
    console.log('WebSocket Kraken deconnecte — reconnexion dans 5s');
    setTimeout(() => connectKrakenWS(krakenPairsList), 5000);
  });

  ws.on('error', (err) => {
    console.log('Erreur WebSocket Kraken:', err.message);
  });
}

// Precharge les 50 dernieres bougies historiques via REST au demarrage
// pour ne pas attendre 50 minutes que le WebSocket les accumule.
async function preloadHistoricalCandles(pairs) {
  console.log(`Preloading ${pairs.length} paires via REST (160 bougies Daily historiques)...`);
  const exchange = new ccxt.kraken({ enableRateLimit: true, timeout: 10000 });
  const BATCH = 50; // Batches plus grands pour aller plus vite
  let loaded = 0;
  for (let i = 0; i < pairs.length; i += BATCH) {
    const batch = pairs.slice(i, i + BATCH);
    await Promise.all(batch.map(async (symbol) => {
      try {
        const ohlcv = await exchange.fetchOHLCV(symbol, '1d', undefined, 160);
        if (!ohlcv || ohlcv.length < 10) return;
        krakenCandles[symbol] = ohlcv.map(c => ({
          t: String(c[0]),
          o: c[1], h: c[2], l: c[3], c: c[4], v: c[6] || c[5]
        }));
        loaded++;
    } catch(e) {}
    }));
    console.log(`Preloading... ${Math.min(i + BATCH, pairs.length)}/${pairs.length} paires`);
    // Pause reduite entre les batches
    if (i + BATCH < pairs.length) {
      await new Promise(r => setTimeout(r, 500));
    }
  }
  console.log(`Preloading termine — ${loaded}/${pairs.length} paires chargees avec historique`);
}

async function initKrakenWS() {
  krakenPairsList = await fetchKrakenUsdtPairs();
  if (krakenPairsList.length === 0) {
    console.log('Aucune paire Kraken trouvee — retry dans 15s');
    setTimeout(initKrakenWS, 15000);
    return;
  }
  console.log(`${krakenPairsList.length} paires /USD Kraken trouvees pour le flux WebSocket`);
  // Precharge les bougies historiques avant de connecter le WebSocket
  await preloadHistoricalCandles(krakenPairsList);
  connectKrakenWS(krakenPairsList);
  connectKrakenTicker(krakenPairsList); // ticker prix live pour TP/SL instantane
}

// Scan instantane: lit les bougies deja en memoire (mises a jour par le WebSocket)
// au lieu de faire des requetes HTTP une par une.
function scanKrakenFromMemory() {
  const results = [];
  for (const symbol of krakenPairsList) {
    const candles = krakenCandles[symbol];
    if (!candles || candles.length < 20) continue;
    // Scan des que nouvelles donnees reçues (meme bougie pas finie)
    const closes = candles.filter(c => c.c > 0).map(c => c.c);
    const volumes = candles.filter(c => c.v > 0).map(c => c.v);
    const livePrice = closes[closes.length - 1];
    const price   = livePrice;
    const sig = detectFigure(closes, volumes, livePrice);
    if (!sig) continue;

    const volRatio = volumes[volumes.length-1] / avg(volumes.slice(-50));
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
      gain:        (TRADE_AMOUNT*TP_PCT).toFixed(4),
      loss:        (TRADE_AMOUNT*SL_PCT).toFixed(4),
      time:        new Date()
    };
    results.push(signal);

    new Signal({
      symbol, exchange:'Kraken', market:'Spot',
      figure:sig.fig.name, direction:sig.fig.dir,
      confidence:signal.confidence, entryPrice:price,
      tp:sig.tp, sl:sig.sl, volumeRatio:volRatio, timeframe:'1d'
    }).save().catch(()=>{});
  }
  return results;
}

// Pour les plateformes autres que Kraken (pas encore branchees en WebSocket),
// on garde l'ancienne methode REST ccxt en repli, plus lente mais fonctionnelle.
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
      .slice(0, 100); // limite plus basse en REST pour rester sous 60s

    const BATCH = 15;
    for (let i = 0; i < symbols.length; i += BATCH) {
      const batch = symbols.slice(i, i + BATCH);
      const batchResults = await Promise.all(batch.map(async (symbol) => {
        try {
          const ohlcv = await exchange.fetchOHLCV(symbol, '1d', undefined, 160);
          if (!ohlcv || ohlcv.length < 20) return null;
          const closes  = ohlcv.map(c => c[4]);
          const volumes = ohlcv.map(c => c[5]);
          const price   = closes[closes.length-1];
          const market  = markets[symbol].type;
          const sig = detectFigure(closes, volumes, price);
          if (!sig) return null;
          const volRatio = volumes[volumes.length-1] / avg(volumes.slice(-50));
          return {
            symbol, exchange: exConfig.name, exchangeId: exConfig.id, timeframe: '1d',
            market: market === 'spot' ? 'Spot' : 'Futures',
            figure: sig.fig.name, figureCode: sig.fig.code, direction: sig.fig.dir,
            confidence: Math.round(sig.fig.wr * 100),
            reliable: sig.fig.wr >= 0.65, entryPrice: price,
            tp: sig.tp, sl: sig.sl, volumeRatio: volRatio.toFixed(2),
            tradeAmount: TRADE_AMOUNT, gain: (TRADE_AMOUNT*TP_PCT).toFixed(4),
            time: new Date()
          };
          } catch(e) { return null; }
      }));
      batchResults.forEach(r => { if (r) results.push(r); });
    }
  } catch(e) {
    console.log(`[${exConfig.name}] Erreur: ${e.message}`);
  }
  return results;
}

// ── SUIVI AUTOMATIQUE TP/SL DES POSITIONS OUVERTES ──
async function checkOpenPositions(user, exchange, balance) {
  const positions = await OpenPosition.find({ email: user.email });
  if (positions.length === 0) return;
  console.log(`[TP/SL] Verification de ${positions.length} position(s) ouverte(s) pour ${user.email}`);

  for (const pos of positions) {
    try {
      const ticker = await exchange.fetchTicker(pos.symbol);
      const currentPrice = ticker.last;
      const [base] = pos.symbol.split('/');
      

      if (currentPrice >= pos.tp) {
        // TP ATTEINT — vendre
        const baseBalance = balance[base]?.free || pos.qty;
        if (baseBalance > 0.000001) {
          console.log(`[TP/SL] TP ATTEINT sur ${pos.symbol} — prix:${currentPrice} >= TP:${pos.tp} — VENTE`);
          const order = await exchange.createOrder(pos.symbol, 'market', 'sell', baseBalance, undefined, {
            oflags: 'fciq' // force market, ignore Post Only
          });
          console.log(`[TP/SL] Ordre SELL execute: ${order.id}`);
          const pnl = pos.amount * TP_PCT;
          await Trade.findOneAndUpdate(
            { email: user.email, symbol: pos.symbol, result: 'OPEN' },
            { exitPrice: currentPrice, pnl, result: 'WIN', exitReason: 'TP +4% atteint' },
            { sort: { time: -1 } }
          );
          await OpenPosition.deleteOne({ _id: pos._id });
          console.log(`[TP/SL] Position fermee — PnL: +$${pnl.toFixed(4)}`);
        }
      } else if (currentPrice <= pos.sl) {
        // SL TOUCHE — vendre
        const baseBalance = balance[base]?.free || pos.qty;
        if (baseBalance > 0.000001) {
          console.log(`[TP/SL] SL TOUCHE sur ${pos.symbol} — prix:${currentPrice} <= SL:${pos.sl} — VENTE`);
          const order = await exchange.createOrder(pos.symbol, 'market', 'sell', baseBalance, undefined, {
            oflags: 'fciq' // force market, ignore Post Only
          });
          console.log(`[TP/SL] Ordre SELL execute: ${order.id}`);
          const pnl = -(pos.amount * SL_PCT);
          await Trade.findOneAndUpdate(
            { email: user.email, symbol: pos.symbol, result: 'OPEN' },
            { exitPrice: currentPrice, pnl, result: 'LOSS', exitReason: 'SL -1% touche' },
            { sort: { time: -1 } }
          );
          await OpenPosition.deleteOne({ _id: pos._id });
          console.log(`[TP/SL] Position fermee — PnL: $${pnl.toFixed(4)}`);
        }
      } else {
        const pct = ((currentPrice - pos.entryPrice) / pos.entryPrice * 100).toFixed(2);
        console.log(`[TP/SL] ${pos.symbol}: ${currentPrice} · ${pct}% (TP:${pos.tp} SL:${pos.sl})`);
      }
    } catch(e) {
      console.log(`[TP/SL] Erreur ${pos.symbol}:`, e.message);
    }
  }
}

// ═══════════════════════════════════════════════════════════════════
// SUIVI TP/SL INSTANTANE — toutes les 2 secondes via prix WebSocket
// Pas dappel API pour le prix — on lit directement la memoire
// ═══════════════════════════════════════════════════════════════════
async function checkTPSLInstant() {
  try {
    const positions = await OpenPosition.find({});
    if (positions.length === 0) return;
    for (const pos of positions) {
      // Prix tick par tick depuis le ticker WebSocket — pas le close de bougie 30m
      const currentPrice = livePrices[pos.symbol];
      if (!currentPrice) continue; // ticker pas encore recu pour cette paire
      if (!currentPrice) continue;
      const hitTP = currentPrice >= pos.tp;
      const hitSL = currentPrice <= pos.sl;
      if (!hitTP && !hitSL) continue;
      const reason = hitTP ? 'TP' : 'SL';
      console.log(`[INSTANT ${reason}] ${pos.symbol} prix:${currentPrice} TP:${pos.tp} SL:${pos.sl}`);
      try {
        const user = await User.findOne({ email: pos.email });
        if (!user) { await OpenPosition.deleteOne({ _id: pos._id }); continue; }
        const ExClass = ccxt[user.exchangeName.toLowerCase()];
        if (!ExClass) continue;
        const exchange = new ExClass({ apiKey: user.apiKey, secret: user.apiSecret, enableRateLimit: true });
        const balance = await exchange.fetchBalance();
        const [base] = pos.symbol.split('/');
        const baseBalance = balance[base]?.free || pos.qty;
        
        if (baseBalance > 0.000001) {
          const order = await exchange.createOrder(pos.symbol, 'market', 'sell', baseBalance, undefined, { oflags: 'fciq' });
          console.log(`[INSTANT ${reason}] Ordre SELL execute: ${order.id}`);
          const pnl = hitTP ? pos.amount * TP_PCT : -(pos.amount * SL_PCT);
          await Trade.findOneAndUpdate(
            { email: pos.email, symbol: pos.symbol, result: 'OPEN' },
            { exitPrice: currentPrice, pnl, result: hitTP ? 'WIN' : 'LOSS', exitReason: hitTP ? 'TP +4% atteint' : 'SL -1% touche' },
            { sort: { time: -1 } }
          );
          await OpenPosition.deleteOne({ _id: pos._id });
          console.log(`[INSTANT ${reason}] Position fermee PnL: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(4)}`);
        }
      } catch(e) { console.log(`[INSTANT TP/SL] Erreur ${pos.symbol}:`, e.message); }
    }
  } catch(e) { console.log('[INSTANT TP/SL] Erreur globale:', e.message); }
}

let scanRunning = false;
async function scanAll() {
  if (scanRunning) {
    console.log('Scan precedent encore en cours — on attend le prochain cycle');
    return;
  }
  scanRunning = true;
  const startTime = Date.now();
  console.log(`\n=== SCAN 1D — ${new Date().toLocaleTimeString()} ===`);
  signalsCache.length = 0;
  Object.keys(signalsByExchange).forEach(k => delete signalsByExchange[k]);

  try {
    const users = await User.find({ active:true, apiKey:{$exists:true} });

    if (users.length === 0) {
      console.log('Aucun utilisateur — scan Kraken par defaut (mode test, via WebSocket)');
      const results = scanKrakenFromMemory();
      signalsCache.push(...results);
      signalsByExchange['kraken'] = results;
      lastScanTime = new Date();
      console.log(`[Kraken-WS] ${krakenPairsList.length} paires en memoire · ${results.length} signal(s)`);
      console.log(`=== FIN test · ${signalsCache.length} signaux · ${Date.now()-startTime}ms ===\n`);
      return;
    }

    const uniqueExchanges = [...new Set(users.map(u => u.exchangeName.toLowerCase()))];
    console.log(`Utilisateurs: ${users.length} · Plateformes: ${uniqueExchanges.join(', ')}`);

    for (const exchangeId of uniqueExchanges) {
      const exConfig = EXCHANGES_CONFIG.find(e => e.id === exchangeId || e.name.toLowerCase() === exchangeId);
      if (!exConfig) continue;

      let results;
      if (exConfig.id === 'kraken') {
        // Scan quasi instantane via WebSocket (donnees deja en memoire)
        results = scanKrakenFromMemory();
        console.log(`[Kraken-WS] ${krakenPairsList.length} paires en memoire · ${results.length} signal(s)`);
      } else {
        // Repli REST plus lent pour les autres plateformes
        results = await scanExchangeRest(exConfig);
        console.log(`[${exConfig.name}-REST] ${results.length} signal(s)`);
      }
      signalsCache.push(...results);
      signalsByExchange[exConfig.id] = results;
    }

    lastScanTime = new Date();
    console.log(`=== FIN · ${signalsCache.length} signaux · ${Date.now()-startTime}ms ===\n`);

    // ── EXECUTION REELLE DES TRADES (plus de simulation Math.random) ──
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
        const exchange = new ExClass({
          apiKey: user.apiKey, secret: user.apiSecret, enableRateLimit: true
        });

        // Verifie le solde disponible
        let balance;
        try { balance = await exchange.fetchBalance(); }
        catch(e) { console.log(`[Bot] Erreur balance ${user.email}:`, e.message); continue; }

        const usd = balance.USD?.free || 0;
        if (usd <= 0) {
          console.log(`[Bot] Aucun fonds USD pour ${user.email}`);
          continue;
        }

        const rawAmount  = user.tradeAmount || TRADE_AMOUNT;
        const amount     = Math.min(Math.max(rawAmount, 5), 50); // min 5$, max 50$
        let ordersPlaced = 0;

        for (const sig of userSignals.slice(0, MAX_CONCURRENT)) {
          if (ordersPlaced >= MAX_CONCURRENT) break;
          try {
            const [base, quote] = sig.symbol.split('/');
            const quoteBalance = balance[quote]?.free || balance['USD']?.free || 0;
            const price         = sig.entryPrice;

            // ── LONG → BUY (seulement si pas deja une position ouverte sur cette paire) ──
            if (sig.direction === 'Long' && quoteBalance >= amount) {
              const existingPos = await OpenPosition.findOne({ email: user.email, symbol: sig.symbol });
              if (existingPos) {
                console.log(`[Bot] Position deja ouverte sur ${sig.symbol} — on attend TP/SL`);
                continue;
              }
              const qty           = amount / price;
              
              const tpPrice       = +(price * (1 + TP_PCT)).toFixed(8);
              const slPrice       = +(price * (1 - SL_PCT)).toFixed(8);
              console.log(`[Bot] ORDRE BUY MARKET: ${sig.symbol} · ${sig.figure} · $${amount} · TP:${tpPrice} · SL:${slPrice}`);
              const order = await exchange.createOrder(sig.symbol, 'market', 'buy', qty, undefined, {
                oflags: 'fciq' // force market, ignore Post Only
              });
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
                result: 'OPEN', exitReason: 'Position ouverte — en attente TP/SL'
              }).save();
              ordersPlaced++;
            }

          } catch(e) {
            console.log(`[Bot] Erreur ordre ${sig.symbol}:`, e.message);
          }
        }

        if (ordersPlaced > 0) console.log(`[Bot] ${ordersPlaced} ordre(s) place(s) pour ${user.email}`);
        // Suivi TP/SL gere par checkTPSLInstant (toutes les 2s) — plus besoin ici

      } catch(e) {
        console.log(`[Bot] Erreur utilisateur ${user.email}:`, e.message);
      }
    }
  } finally {
    scanRunning = false;
  }
}

// ROUTES
app.get('/', (req, res) => res.json({
  status:        'Bender Pro v8.0 actif',
  strategy:      'Figures chartistes + Volume · Ratio 1:4 · Timeframe 1m',
  scanMethod:    'WebSocket Kraken (temps reel) + REST en repli pour autres plateformes',
  tradeAmount:   TRADE_AMOUNT,
  slPct:         SL_PCT*100+'%',
  tpPct:         TP_PCT*100+'%',
  exchanges:     EXCHANGES_CONFIG.length,
  krakenWsConnected: wsConnected,
  krakenPairsTracked: krakenPairsList.length,
  lastScan:      lastScanTime,
  signalsActive: signalsCache.length,
}));

app.get('/market', (req, res) => {
  let sigs = [...signalsCache];
  if (req.query.exchange) sigs = sigs.filter(s=>s.exchange.toLowerCase().includes(req.query.exchange.toLowerCase()));
  if (req.query.direction) sigs = sigs.filter(s=>s.direction===req.query.direction);
  res.json({ success:true, signals:sigs, count:sigs.length, lastScan:lastScanTime });
});

app.get('/scan', async (req, res) => {
  res.json({ success:true, message:'Scan lance...' });
  scanAll().catch(console.error);
});

app.post('/connect', async (req, res) => {
  const { email, apiKey, secret, exchangeName, tradeAmount } = req.body;
  if (!email || !apiKey || !secret || !exchangeName)
    return res.json({ success:false, error:'Donnees manquantes' });
  try {
    await User.findOneAndUpdate(
      { email },
      { apiKey, apiSecret:secret, exchangeName, active:true, tradeAmount:tradeAmount||TRADE_AMOUNT },
      { upsert:true, new:true }
    );
    res.json({ success:true, message:`Connecte sur ${exchangeName} · $${tradeAmount||TRADE_AMOUNT}/trade · TP+5.2% SL-1% · Daily 150 bougies` });
  } catch(e) { res.json({ success:false, error:e.message });
app.get(`/status/:email`, async (req, res) => {
  const user = await User.findOne({ email:req.params.email });
  if (!user) return res.json({ connected:false });
  const trades = await Trade.countDocuments({ email:req.params.email });
  const wins   = await Trade.countDocuments({ email:req.params.email, result:'WIN' });
  res.json({
    connected:true, active:user.active, exchange:user.exchangeName,
    tradeAmount:user.tradeAmount, trades,
    winRate:trades>0?Math.round(wins/trades*100)+'%':'N/A'
  });
});

app.get('/trades/:email', async (req, res) => {
  const email = req.params.email;
  const totalCount = await Trade.countDocuments({ email });
  const allTrades = await Trade.find({ email });
  const totalPnl = allTrades.reduce((a,t)=>a+t.pnl,0);
  const totalWins = allTrades.filter(t=>t.result==='WIN').length;
  const totalLosses = totalCount - totalWins;
  const trades = await Trade.find({ email }).sort({time:-1}).limit(100);
  res.json({
    trades,
    totalTradesCount: totalCount,
    totalPnl: totalPnl.toFixed(4),
    wins: totalWins,
    losses: totalLosses,
    displayedCount: trades.length
  });
});


// ═══════════════════════════════════════════════════════════════════
// SYSTEME DE FACTURATION — Commission 0.5% des profits / 2 semaines
// ═══════════════════════════════════════════════════════════════════
const COMMISSION_RATE = 0.0025; // 0.25% du volume total traded (0.25% maker + 0.25% taker)
const BILLING_WALLET  = process.env.BILLING_WALLET || 'VOTRE_WALLET_CRYPTO_ICI';
const BILLING_DAYS    = 14; // periode de facturation en jours

  try {
    const email = req.params.email;
app.get(`/billing/:email`, async (req, res) => {
    if (!user) return res.json({ success:false, error:'Utilisateur non trouve' });

    // Periode actuelle — 14 derniers jours
    const periodEnd   = new Date();
    const periodStart = new Date(periodEnd - BILLING_DAYS * 24 * 3600 * 1000);

    // Trades de la periode
    const trades = await Trade.find({
      email,
      time: { $gte: periodStart, $lte: periodEnd },
      result: { $in: ['WIN', 'LOSS'] }
    });

    const totalVolume = trades.reduce((a,t) => a + t.amount, 0);
    const wins        = trades.filter(t => t.result === 'WIN').length;
    const losses      = trades.filter(t => t.result === 'LOSS').length;
    // Commission seulement si profit positif
    const totalVolume = trades.reduce((a,t) => a + t.amount, 0); // somme des volumes
    const commission  = +(totalVolume * COMMISSION_RATE).toFixed(4); // 0.5% du volume

    // Verifie si facture deja existante pour cette periode
    let billing = await Billing.findOne({
      email,
      periodStart: { $gte: new Date(periodStart.getTime() - 3600000) }
    });

    if (!billing) {
      billing = await new Billing({
        email, periodStart, periodEnd,
        totalPnl: +totalPnl.toFixed(4),
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
        winRate:     trades.length > 0 ? Math.round(wins/trades.length*100)+'%' : 'N/A',
        totalVolume: +totalVolume.toFixed(4),
        commission,
        status:      billing.status,
        paidAt:      billing.paidAt ? new Date(billing.paidAt).toLocaleDateString('fr-CA') : null,
        wallet:      BILLING_WALLET,
        message:     totalPnl <= 0
          ? 'Commission due: $${commission} USD (0.25% des $${totalVolume.toFixed(4)} trades)'
          : `Commission due: $${commission} USD (0.5% du volume total de $${totalVolume.toFixed(4)})`,
      }
    });
  } catch(e) { res.json({ success:false, error:e.message });
});

// Marque une facture comme payee (avec hash de transaction optionnel)
app.post('/billing/paid/:email', async (req, res) => {
  try {
    const { txHash } = req.body;
    const email = req.params.email;
    const periodStart = new Date(Date.now() - BILLING_DAYS * 24 * 3600 * 1000);

    const billing = await Billing.findOneAndUpdate(
      { email, periodStart: { $gte: new Date(periodStart.getTime() - 3600000) } },
      { status: 'PAID', paidAt: new Date(), txHash: txHash || '' },
      { sort: { createdAt: -1 }, new: true }
    );
    if (!billing) return res.json({ success:false, error:'Facture non trouvee' });
    res.json({ success:true, message:'Paiement confirme', billing });
  } catch(e) { res.json({ success:false, error:e.message });
});

// Admin — toutes les factures en attente
app.get('/admin/billing', async (req, res) => {
  try {
    const pending = await Billing.find({ status:'PENDING', commission:{ $gt:0 } }).sort({ createdAt:-1 });
    const totalDue = pending.reduce((a,b) => a + b.commission, 0);
    res.json({
      success: true,
      pending: pending.length,
      totalDue: +totalDue.toFixed(4),
      wallet: BILLING_WALLET,
      billings: pending
    });
  } catch(e) { res.json({ success:false, error:e.message });
});

app.get('/signals', async (req, res) => {
  const signals = await Signal.find().sort({time:-1}).limit(100);
  res.json({ signals });
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
        price: last.c,
        changePct: prev.c ? ((last.c - prev.c) / prev.c) * 100 : null
      };
    }
  }
  pricesCache = out;
  pricesCacheTime = Date.now();
}
app.get('/prices', (req, res) => {
  refreshPricesFromMemory();
  res.json({ success: true, prices: pricesCache, time: pricesCacheTime });
});

app.get('/platform-signals/:email', async (req, res) => {
  const user = await User.findOne({ email: req.params.email });
  if (!user) return res.json({ success:false, error:'Utilisateur non trouve' });

  const exConfig = EXCHANGES.find(e => e.id === user.exchangeName);
if (!exConfig) continue;
const exchangeId = exConfig.id;
  const exConfig = EXCHANGES_CONFIG.find(e => e.id === exchangeId || e.name.toLowerCase() === exchangeId);
  const platformSignals = signalsByExchange[exConfig ? exConfig.id : exchangeId] || [];

  const amount = user.tradeAmount || TRADE_AMOUNT;
  const enriched = platformSignals.map(s => ({
    ...s,
    potentialGainUSD: +(amount * TP_PCT).toFixed(4),
    potentialLossUSD: +(amount * SL_PCT).toFixed(4)
  }));

  res.json({
    success: true,
    exchange: exConfig ? exConfig.name : user.exchangeName,
    tradeAmount: amount,
    lastScan: lastScanTime,
    count: enriched.length,
    signals: enriched
  });
});

// Route temporaire pour vider tous les utilisateurs — a supprimer apres usage
app.get('/clear-users', async (req, res) => {
  try {
    const result = await User.deleteMany({});
    console.log(`[clear-users] ${result.deletedCount} utilisateur(s) supprimes`);
    res.json({ success: true, deleted: result.deletedCount, message: 'Tous les utilisateurs ont ete supprimes. Reconnectez-vous sur le site.' });
  } catch(e) {
    res.json({ success: false, error: e.message });
  }
});

app.get('/admin/stats', async (req, res) => {
  const users  = await User.countDocuments();
  const active = await User.countDocuments({ active:true });
  const trades = await Trade.countDocuments();
  const wins   = await Trade.countDocuments({ result:'WIN' });
  res.json({
    users, active, trades,
    winRate:      trades>0?Math.round(wins/trades*100)+'%':'N/A',
    signalsActive:signalsCache.length,
    lastScan:     lastScanTime,
    exchanges:    EXCHANGES_CONFIG.length,
    krakenWsConnected: wsConnected,
    krakenPairsTracked: krakenPairsList.length
  });
});

app.post('/toggle', async (req, res) => {
  const { email, active } = req.body;
  await User.findOneAndUpdate({ email }, { active });
  res.json({ success:true, active });
});

// DEMARRAGE
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`\n Bender Pro v8.0 · Port ${PORT}`);
  console.log(` Figures chartistes + Volume · TP+5.2% SL-1% · Daily 150 bougies`);
  console.log(` Scan Kraken via WebSocket (quasi instantane)`);
  console.log(` Helmet actif · Securite HTTP headers`);
  console.log(` $${TRADE_AMOUNT}/trade · SL -1% · TP +4% · Ratio 4:1`);
  console.log(` Scan toutes les 60 secondes (bougies 1D)\n`);
  setImmediate(() => {
    initKrakenWS().then(() => {
      setTimeout(() => scanAll().catch(console.error), 5000);
      setInterval(() => scanAll().catch(console.error), SCAN_INTERVAL);
      // Suivi TP/SL instantane — toutes les 2 secondes via prix WebSocket
      setInterval(() => checkTPSLInstant().catch(console.error), 2000);
      console.log(" Suivi TP/SL instantane actif (toutes les 2 secondes)");
    }).catch(console.error);
  });
});
app.post('/toggle', async (req, res) => {
  const { email, active } = req.body;
  await User.findOneAndUpdate({ email }, { active });
  res.json({ success:true, active });
});

app.post('/toggle', async (req, res) => {
  const { email, active } = req.body;
  await User.findOneAndUpdate({ email }, { active });
  res.json({ success:true, active });
});
// Route facturation

const BillingSchema = new mongoose.Schema({
  email:        String, 
  periodStart:  Date,
  periodEnd:    Date,
  totalVolume:  Number, 
  totalPnl:     Number,
  commission:   Number,  // 0.5% du volume total trade
  status:       { type: String, default: 'PENDING' },
  paidAt:       Date, 
  txHash:       String,
  createdAt:    { type: Date, default: Date.now }  
});


const BillingSchema = new mongoose.Schema({
  email:        String, 
  periodStart:  Date,
  periodEnd:    Date,
  totalVolume:  Number, 
  totalPnl:     Number,
  commission:   Number,  // 0.5% du volume total trade
  status:       { type: String, default: 'PENDING' },
  paidAt:       Date, 
  txHash:       String,
  createdAt:    { type: Date, default: Date.now }  
});

const Billing = mongoose.model('Billing', BillingSchema);

// Route facturation

const { EXCHANGES } = require('./exchanges'); // les 35 exchanges

app.get('/exchanges', (req, res) => {
  const userCountry = req.userCountry || 'INT';
  const exchanges = EXCHANGES.filter(e => 
    e.countries.includes(userCountry) ||
    e.countries.includes(userCountry + '/Mondial') ||  
    e.countries.includes('INT')
  );
  res.json({ exchanges });
});

