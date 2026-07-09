// Bender Pro v10.0 â€” Bot Classique + Mode IA Autonome
// npm install express cors mongoose ccxt helmet ws node-fetch @tensorflow/tfjs
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

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// CONFIG GLOBALE
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
const TRADE_AMOUNT      = 5;
const SL_PCT = 0.02; // -2%
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

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// SCHEMAS
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
const UserSchema = new mongoose.Schema({
  email:        { type: String, required: true, unique: true },
  exchangeName: String,
  apiKey:       String,
  apiSecret:    String,
  tradeAmount:  { type: Number, default: 5 },
  currency:     { type: String, default: 'USD' },
  active:       { type: Boolean, default: true },
  botMode:      { type: String, default: 'classic', enum: ['classic', 'ai'] }, // MODE BOT
  // ContrÃ´le capital Mode IA
  aiTradeAmount:    { type: Number, default: 5  },   // $ par trade IA (max 5$ jamais nÃ©gociable)
  aiMaxTrades:      { type: Number, default: 1  },   // trades simultanÃ©s max IA
  aiDailyCapital:   { type: Number, default: 10 },   // capital max utilisÃ© par jour IA
  xlmWallet:    String,
  createdAt:    { type: Date, default: Date.now }
});

const TradeSchema = new mongoose.Schema({
  email: String, symbol: String, exchange: String, market: String,
  direction: String, figure: String, entryPrice: Number, exitPrice: Number,
  amount: Number, pnl: Number, result: String, exitReason: String,
  timeframe: String, currency: String, botMode: String,
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
  currency: String, timeframe: String, botMode: String,
  openedAt: { type: Date, default: Date.now }
});

const BillingSchema = new mongoose.Schema({
  email: String, periodStart: Date, periodEnd: Date,
  totalVolume: Number, totalPnl: Number, commission: Number,
  status: { type: String, default: 'PENDING' },
  paidAt: Date, txHash: String,
  createdAt: { type: Date, default: Date.now }
});

// Schema IA â€” historique des versions et performances

const User         = mongoose.model('User',         UserSchema);
const Trade        = mongoose.model('Trade',        TradeSchema);
const Signal       = mongoose.model('Signal',       SignalSchema);
const OpenPosition = mongoose.model('OpenPosition', OpenPositionSchema);
const Billing      = mongoose.model('Billing',      BillingSchema);


// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// CONFIGURATION 35 PLATEFORMES
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
const EXCHANGES_CONFIG = [
  { id:'kraken',      name:'Kraken',      geo:'BOTH',  currencies:['USD','CAD','EUR'], quoteFilter:['USD','CAD','EUR'], spot:true, futures:true,  ccxt:true  },
  { id:'coinbasepro', name:'Coinbase',    geo:'BOTH',  currencies:['USD','EUR','GBP'], quoteFilter:['USD','EUR'],       spot:true, futures:false, ccxt:true  },
  { id:'gemini',      name:'Gemini',      geo:'BOTH',  currencies:['USD','EUR'],       quoteFilter:['USD'],             spot:true, futures:false, ccxt:true  },
  { id:'bitbuy',      name:'Bitbuy',      geo:'CA',    currencies:['CAD'],             quoteFilter:['CAD'],             spot:true, futures:false, ccxt:true  },
  { id:'ndax',        name:'NDAX',        geo:'CA',    currencies:['CAD'],             quoteFilter:['CAD'],             spot:true, futures:false, ccxt:true  },
  { id:'newton',      name:'Newton',      geo:'CA',    currencies:['CAD'],             quoteFilter:['CAD'],             spot:true, futures:false, ccxt:true  },
  { id:'coinsquare',  name:'Coinsquare',  geo:'CA',    currencies:['CAD'],             quoteFilter:['CAD'],             spot:true, futures:false, ccxt:true  },
  { id:'shakepay',    name:'Shakepay',    geo:'CA',    currencies:['CAD'],             quoteFilter:['CAD'],             spot:true, futures:false, ccxt:false },
  { id:'coinberry',   name:'Coinberry',   geo:'CA',    currencies:['CAD'],             quoteFilter:['CAD'],             spot:true, futures:false, ccxt:false },
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

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// FIGURES CHARTISTES (Bot Classique)
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
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
    return { tp: +(price * (1 + tpPct)).toFixed(8), sl: +(price * (1 - SL_PCT)).toFixed(8) };
  }
  if (n >= 100) {
    const cupLow = Math.min(...closes.slice(n-60,n-20));
    const resistance = Math.max(...closes.slice(n-30,n-1));
    if (cupLow < closes[n-70]*0.95 && price > resistance && volRatio > 1.8)
      return { fig:FIGURES[0], ...buildLevels(), figureTarget, tpPct };
  }
  if (n >= 100) {
    const headLow = Math.min(...closes.slice(n-60,n-20));
    const shoulderLow = Math.min(...closes.slice(n-80,n-60));
    const neckline = Math.max(...closes.slice(n-80,n-2));
    if (headLow < shoulderLow*0.97 && price > neckline && volRatio > 1.5)
      return { fig:FIGURES[1], ...buildLevels(), figureTarget, tpPct };
  }
  if (n >= 70) {
    const bot1 = Math.min(...closes.slice(n-50,n-25));
    const bot2 = Math.min(...closes.slice(n-25,n-1));
    const midTop = Math.max(...closes.slice(n-40,n-10));
    if (Math.abs(bot1-bot2)/bot1 < 0.02 && price > midTop && volRatio > 1.4)
      return { fig:FIGURES[2], ...buildLevels(), figureTarget, tpPct };
  }
  if (range < 0.04 && trend10 > 0.01 && price > high*0.999 && volRatio > 1.6)
    return { fig:FIGURES[3], ...buildLevels(), figureTarget, tpPct };
  if (trend10 > 0.06 && range < 0.025 && price > high*0.999 && volRatio > 1.8)
    return { fig:FIGURES[4], ...buildLevels(), figureTarget, tpPct };
  if (range < 0.035 && trend10 < -0.02 && trend10 > -0.05 && price > high*0.999 && volRatio > 1.7)
    return { fig:FIGURES[5], ...buildLevels(), figureTarget, tpPct };
  return null;
}

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// MODE IA AUTONOME â€” Cerveau de l'IA
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

// MÃ©moire IA par utilisateur

// GÃ©nÃ©rer une nouvelle stratÃ©gie IA alÃ©atoire mais encadrÃ©e









// Score IA â€” combine tous les indicateurs selon la stratÃ©gie



// VÃ©rifier si l'IA doit Ãªtre remplacÃ©e (fin de journÃ©e dans le rouge)



// Initialiser ou rÃ©cupÃ©rer la mÃ©moire IA d'un utilisateur



// ExÃ©cution d'un trade IA



// Scan IA â€” analyse toutes les paires disponibles



// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// WEBSOCKET KRAKEN (identique v9.0)
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
const krakenCandles    = {};  // Daily 1D
const krakenCandles1h  = {};  // 1H
const krakenCandles4h  = {};  // 4H
let krakenPairsList    = [];
let wsConnected        = false;
let ws = null, ws1h = null, ws4h = null, wsTicker = null;
const livePrices       = {};
const breakoutConfirm  = {};
const recentSignals    = new Map();
const QUOTE_CURRENCIES = ['USD'];

async function fetchKrakenUsdtPairs() {
  try {
    const exchange = new ccxt.kraken({ enableRateLimit: true, timeout: 15000 });
    const markets = await exchange.loadMarkets();
    const pairs = Object.keys(markets).filter(s => {
      const m = markets[s];
      return QUOTE_CURRENCIES.some(q => s.endsWith('/'+q)) && m.active !== false && (m.spot===true||m.type==='spot');
    });
    console.log(`[Diagnostic] ${pairs.length} paires /USD`);
    return pairs.slice(0, MAX_PAIRS);
  } catch(e) { console.log('Erreur fetchKrakenUsdtPairs:', e.message); return []; }
}

function connectKrakenTicker(pairs) {
  if (wsTicker) { try { wsTicker.terminate(); } catch(e) {} }
  wsTicker = new WebSocket('wss://ws.kraken.com/v2');
  wsTicker.on('open', () => {
    console.log(`[Ticker] ConnectÃ© â€” ${pairs.length} paires`);
    for (let i=0;i<pairs.length;i+=50)
      wsTicker.send(JSON.stringify({method:'subscribe',params:{channel:'ticker',symbol:pairs.slice(i,i+50)}}));
    // Ping géré par le ping global (setInterval 20s au boot)
  });
  wsTicker.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.channel==='ticker' && msg.data) {
        for (const t of msg.data) {
          if (t.symbol && t.last) {
            livePrices[t.symbol] = t.last;
            const alt = t.symbol.replace('XBT','BTC').replace('BTC','XBT');
            if (alt !== t.symbol) livePrices[alt] = t.last;
            // Prix live uniquement — cassure gérée par clôture de bougie (handleOhlcMessage)            }
          }
        }
      }
    } catch(e) {}
  });
  wsTicker.on('close',()=>{if(wsTicker._hb)clearInterval(wsTicker._hb);setTimeout(()=>connectKrakenTicker(krakenPairsList),15000);});
  wsTicker.on('error', (err) => { console.log('[Ticker] Erreur:', err.message); });
}

function connectKrakenWS(pairs) {
  if (ws) { try { ws.terminate(); } catch(e) {} }
  ws = new WebSocket('wss://ws.kraken.com/v2');
  ws.on('open', () => {
    wsConnected = true;
    console.log(`WebSocket Kraken â€” ${pairs.length} paires`);
    for (let i=0;i<pairs.length;i+=50)
      ws.send(JSON.stringify({method:'subscribe',params:{channel:'ohlc',symbol:pairs.slice(i,i+50),interval:1440}}));
    // Ping géré par le ping global (setInterval 20s au boot)
  });
  ws.on('message', (raw) => handleOhlcMessage(raw, krakenCandles, '1d'));
  ws.on('close',()=>{wsConnected=false;if(ws._hb)clearInterval(ws._hb);setTimeout(()=>connectKrakenWS(krakenPairsList),15000);});
  ws.on('error', (err) => { console.log('Erreur WS:', err.message); });
}




// ── Traitement générique d'une mise à jour OHLC (toutes timeframes)
function handleOhlcMessage(raw, store, tf) {
  try {
    const msg = JSON.parse(raw);
    if (msg.channel==='ohlc' && (msg.type==='snapshot'||msg.type==='update') && msg.data) {
      for (const c of msg.data) {
        const sym  = c.symbol;
        const arr  = store[sym] || (store[sym] = []);
        const last = arr[arr.length - 1];

        // Nouvelle bougie si le timestamp change
        const isNewCandle = !last || (c.timestamp && last.ts && c.timestamp !== last.ts);

        if (!isNewCandle) {
          if (last) { last.c = c.close; last.v = c.volume; }
        } else {
          // Bougie précédente CLÔTURÉE
          const closedClose = last ? last.c : null;
          arr.push({ c: c.close, v: c.volume, ts: c.timestamp });
          if (arr.length > 500) arr.shift();

          // Vérifier cassure sur clôture
          const key = sym + '|' + tf;
          const bc  = breakoutConfirm[key] || breakoutConfirm[sym];
          if (bc && closedClose !== null && closedClose > bc.resistance) {
            console.log(`[CASSURE ${tf}] ${sym} · ${bc.figure} · Clôture ${closedClose.toFixed(4)} > ${bc.resistance.toFixed(4)} → ORDRE`);
            const sig = bc.signal;
            delete breakoutConfirm[key];
            delete breakoutConfirm[sym];
            executeTrade(sig).catch(() => {});
          } else {
            setImmediate(() => scanSinglePair(sym, tf, 'kraken', store));
          }
        }
      }
    }
  } catch(e) {}
}

// ── WebSocket 1H
function connectKrakenWS1h(pairs) {
  if (ws1h) { try { ws1h.terminate(); } catch(e) {} }
  ws1h = new WebSocket('wss://ws.kraken.com/v2');
  ws1h.on('open', () => {
    console.log(`[WS-1H] ${pairs.length} paires`);
    let i = 0;
    const send = () => {
      if (i >= pairs.length || ws1h.readyState !== 1) return;
      ws1h.send(JSON.stringify({ method:'subscribe', params:{ channel:'ohlc', symbol:pairs.slice(i,i+25), interval:60 }}));
      i += 25;
      if (i < pairs.length) setTimeout(send, 200);
    };
    send();
  });
  ws1h.on('message', (raw) => handleOhlcMessage(raw, krakenCandles1h, '1h'));
  ws1h.on('close', () => { setTimeout(() => connectKrakenWS1h(krakenPairsList), 15000); });
  ws1h.on('error', (e) => console.log('[WS-1H]', e.message));
}

// ── WebSocket 4H
function connectKrakenWS4h(pairs) {
  if (ws4h) { try { ws4h.terminate(); } catch(e) {} }
  ws4h = new WebSocket('wss://ws.kraken.com/v2');
  ws4h.on('open', () => {
    console.log(`[WS-4H] ${pairs.length} paires`);
    let i = 0;
    const send = () => {
      if (i >= pairs.length || ws4h.readyState !== 1) return;
      ws4h.send(JSON.stringify({ method:'subscribe', params:{ channel:'ohlc', symbol:pairs.slice(i,i+25), interval:240 }}));
      i += 25;
      if (i < pairs.length) setTimeout(send, 200);
    };
    send();
  });
  ws4h.on('message', (raw) => handleOhlcMessage(raw, krakenCandles4h, '4h'));
  ws4h.on('close', () => { setTimeout(() => connectKrakenWS4h(krakenPairsList), 15000); });
  ws4h.on('error', (e) => console.log('[WS-4H]', e.message));
}

// Charger l'historique max en paginant en arrière
// Pages × 720 bougies par appel
async function fetchMaxHistory(exchange, symbol, tf, maxPages) {
  const limit = 720;
  let allCandles = [];
  try {
    // Premier appel — bougies récentes
    const first = await exchange.fetchOHLCV(symbol, tf, undefined, limit);
    if (!first || first.length < 2) return [];
    allCandles = [...first];
    let oldestTs = first[0][0];

    // Paginer en arrière
    for (let page = 1; page < maxPages; page++) {
      try {
        // since = très ancien, until = juste avant la plus ancienne bougie connue
        const sinceMs = oldestTs - limit * tfToMs(tf);
        const prev = await exchange.fetchOHLCV(symbol, tf, sinceMs, limit);
        if (!prev || prev.length < 2) break;
        const newOldest = prev[0][0];
        if (newOldest >= oldestTs) break; // pas de progression
        allCandles = [...prev, ...allCandles];
        oldestTs = newOldest;
        await new Promise(r => setTimeout(r, 250)); // rate limit
      } catch(e) { break; }
    }
  } catch(e) { return []; }

  // Dédupliquer et trier
  const seen = new Set();
  return allCandles
    .filter(c => { if (seen.has(c[0])) return false; seen.add(c[0]); return true; })
    .sort((a, b) => a[0] - b[0])
    .map(c => ({ c: c[4], v: c[6]||c[5], ts: c[0] }));
}

// Convertir timeframe en millisecondes
function tfToMs(tf) {
  const map = { '1h': 3600000, '4h': 14400000, '1d': 86400000 };
  return map[tf] || 86400000;
}

async function preloadHistoricalCandles(pairs) {
  console.log(`Preloading historique max — ${pairs.length} paires...`);
  const exchange = new ccxt.kraken({ enableRateLimit: true, timeout: 20000 });
  // Batch de 20 paires — compromis vitesse/stabilité
  const BATCH = 20;

  for (let i = 0; i < pairs.length; i += BATCH) {
    const batch = pairs.slice(i, i + BATCH);

    await Promise.all(batch.map(async (symbol) => {
      // 1D — 5 pages × 720 = ~3600 bougies = ~10 ans d'historique
      try {
        const d = await fetchMaxHistory(exchange, symbol, '1d', 5);
        if (d.length >= 10) {
          krakenCandles[symbol] = d;
        }
      } catch(e) {}

      // 4H — 3 pages × 720 = ~2160 bougies = ~360 jours
      try {
        const h4 = await fetchMaxHistory(exchange, symbol, '4h', 3);
        if (h4.length >= 10) krakenCandles4h[symbol] = h4;
      } catch(e) {}

      // 1H — 2 pages × 720 = ~1440 bougies = ~60 jours
      try {
        const h1 = await fetchMaxHistory(exchange, symbol, '1h', 2);
        if (h1.length >= 10) krakenCandles1h[symbol] = h1;
      } catch(e) {}
    }));

    const done = Math.min(i + BATCH, pairs.length);
    const sample = krakenCandles[batch[0]];
    const oldest = sample ? new Date(sample[0].ts).toLocaleDateString('fr-CA') : '?';
    console.log(`Preloading... ${done}/${pairs.length} (1D depuis ${oldest})`);
    if (done < pairs.length) await new Promise(r => setTimeout(r, 800));
  }
  console.log('✅ Preloading terminé!');
}
