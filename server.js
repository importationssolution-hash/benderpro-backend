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

const AIMemorySchema = new mongoose.Schema({
  email:       String,
  generation:  { type: Number, default: 1 },
  params: {
    rsiMin:         { type: Number, default: 30 },
    rsiMax:         { type: Number, default: 75 },
    volMultiplier:  { type: Number, default: 1.2 },
  },
  totalTrades:   { type: Number, default: 0 },
  totalWins:     { type: Number, default: 0 },
  winRate:       { type: Number, default: 0 },
  avoidFigures:  [String],
  bestFigures:   [String],
  avoidHours:    [Number],
  bestTimeframes:[String],
  lessons:       [String],
  lastLearning:  { type: Date, default: Date.now },
});
const AIMemory = mongoose.model('AIMemory', AIMemorySchema);

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
const aiLearningCache = {};

async function loadAIMemory(email) {
  if (aiLearningCache[email]) return aiLearningCache[email];
  let mem = await AIMemory.findOne({ email });
  if (!mem) mem = await new AIMemory({ email }).save();
  aiLearningCache[email] = mem.toObject();
  return aiLearningCache[email];
}

async function learnFromTrade(email, trade) {
  try {
    const mem    = await loadAIMemory(email);
    let params        = { ...mem.params };
    let avoidFigures  = [...(mem.avoidFigures  || [])];
    let bestFigures   = [...(mem.bestFigures   || [])];
    let avoidHours    = [...(mem.avoidHours    || [])];
    let bestTimeframes= [...(mem.bestTimeframes|| [])];
    const lessons = [];
    const isWin   = trade.result === 'WIN';
    const isLoss  = trade.result === 'LOSS';
    const figure  = trade.figure || '';
    const tf      = trade.timeframe || '1d';
    const hour    = new Date(trade.time || Date.now()).getHours();
    const pnlPct  = trade.entryPrice ? (trade.exitPrice-trade.entryPrice)/trade.entryPrice*100 : 0;

    if (isLoss) {
      // Resserrer RSI si perte lourde
      if (pnlPct < -3) {
        params.rsiMin = Math.min((params.rsiMin||30)+2, 45);
        params.rsiMax = Math.max((params.rsiMax||75)-2, 60);
        lessons.push(`RSI resserré (${params.rsiMin}-${params.rsiMax}) après perte ${pnlPct.toFixed(1)}%`);
      }
      // Exiger plus de volume
      params.volMultiplier = Math.min((params.volMultiplier||1.2)+0.1, 2.5);
      // Éviter la figure temporairement
      if (figure && !avoidFigures.includes(figure)) {
        avoidFigures.push(figure);
        if (avoidFigures.length > 3) avoidFigures.shift();
        lessons.push(`Figure "${figure}" mise en veille`);
      }
      if (!avoidHours.includes(hour)) {
        avoidHours.push(hour); if (avoidHours.length>6) avoidHours.shift();
      }
    }

    if (isWin) {
      // Récompenser la figure
      if (figure && !bestFigures.includes(figure) && pnlPct > 5) {
        bestFigures.push(figure); if (bestFigures.length>5) bestFigures.shift();
        lessons.push(`Figure "${figure}" ajoutée aux favoris (+${pnlPct.toFixed(1)}%)`);
      }
      // Réhabiliter la figure si elle gagne
      const aIdx = avoidFigures.indexOf(figure);
      if (aIdx >= 0) avoidFigures.splice(aIdx, 1);
      // Relâcher légèrement les filtres
      params.volMultiplier = Math.max((params.volMultiplier||1.2)-0.05, 1.1);
      if (!bestTimeframes.includes(tf)) {
        bestTimeframes.push(tf); if (bestTimeframes.length>3) bestTimeframes.shift();
      }
      const hIdx = avoidHours.indexOf(hour);
      if (hIdx >= 0) avoidHours.splice(hIdx, 1);
    }

    const totalTrades = (mem.totalTrades||0)+1;
    const totalWins   = (mem.totalWins  ||0)+(isWin?1:0);
    const winRate     = +(totalWins/totalTrades*100).toFixed(1);
    let generation    = mem.generation||1;

    // Évolution génétique — si winRate < 40% après 10+ trades → nouvelle génération
    if (totalTrades >= 10 && totalTrades%10===0 && winRate < 40) {
      generation++;
      params.rsiMin        = Math.max((params.rsiMin||30)-3, 25);
      params.rsiMax        = Math.min((params.rsiMax||75)+3, 80);
      params.volMultiplier = Math.max((params.volMultiplier||1.2)-0.1, 1.1);
      avoidFigures = [];
      lessons.push(`⚡ Génération ${generation} — paramètres réinitialisés (WR: ${winRate}%)`);
      console.log(`[IA Learning] ${email} → Génération ${generation} · WR: ${winRate}%`);
    }

    const updated = await AIMemory.findOneAndUpdate(
      { email },
      { params, avoidFigures, bestFigures, avoidHours, bestTimeframes,
        totalTrades, totalWins, winRate, generation, lastLearning: new Date(),
        $push: { lessons: { $each: lessons, $slice: -20 } } },
      { upsert: true, new: true }
    );
    aiLearningCache[email] = updated.toObject();

    if (lessons.length > 0)
      console.log(`[IA Learning] ${email} G${generation} · WR:${winRate}% · ${lessons.join(' | ')}`);
  } catch(e) { console.log('[IA Learning] Erreur:', e.message); }
}

async function isFigureAllowed(email, figure, hour) {
  try {
    const mem = await loadAIMemory(email);
    if ((mem.avoidFigures||[]).includes(figure)) {
      console.log(`[IA Learning] ${email} — "${figure}" évitée`);
      return false;
    }
    return true;
  } catch(e) { return true; }
}
const signalsCache = [];
let lastScanTime = null;
const marketsCache = {};
function avg(arr) { return arr.reduce((a, b) => a + b, 0) / arr.length; }

function calcRSI(prices, period=14) {
  if (prices.length < period+1) return 50;
  let gains=0, losses=0;
  for (let i=prices.length-period; i<prices.length; i++) {
    const d=prices[i]-prices[i-1];
    if(d>0) gains+=d; else losses-=d;
  }
  const avgG=gains/period, avgL=losses/period;
  if(avgL===0) return 100;
  return 100-(100/(1+avgG/avgL));
}

function calcEMA(prices, period) {
  if(prices.length<period) return prices[prices.length-1];
  const k=2/(period+1);
  let ema=prices.slice(0,period).reduce((a,b)=>a+b)/period;
  for(let i=period; i<prices.length; i++) ema=prices[i]*k+ema*(1-k);
  return ema;
}

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

function detectFigure(closes, volumes, livePrice, aiParams) {
  if (closes.length < 50) return null;
  // Paramètres IA adaptatifs (ou valeurs par défaut)
  const RSI_MIN = (aiParams && aiParams.rsiMin) || 30;
  const RSI_MAX = (aiParams && aiParams.rsiMax) || 75;
  const VOL_MULT = (aiParams && aiParams.volMultiplier) || 1.2;
  const n = closes.length;
  const price = livePrice || closes[n - 1];
  const volNow = volumes[n - 1];
  const volAvg = avg(volumes.slice(-50));
  const volRatio = volNow / volAvg;
  if (volRatio < Math.max(VOL_CONFIRM, VOL_MULT)) return null;
  const rsi = calcRSI(closes);
  const ema20 = calcEMA(closes.slice(-60), Math.min(20, closes.length-1));
  const ema50 = calcEMA(closes.slice(-100), Math.min(50, closes.length-1));
  const trendBull = ema20 > ema50;
  const rsiOk = rsi >= RSI_MIN && rsi <= RSI_MAX;
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
            // Prix live uniquement — cassure sur clôture de bougie            }
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
function tfToMs(tf) {
  return { '1h': 3600000, '4h': 14400000, '1d': 86400000 }[tf] || 86400000;
}

async function fetchMaxHistory(exchange, symbol, tf, maxPages) {
  const limit = 720;
  let all = [];
  try {
    const first = await exchange.fetchOHLCV(symbol, tf, undefined, limit);
    if (!first || first.length < 2) return [];
    all = [...first];
    let oldestTs = first[0][0];
    for (let page = 1; page < maxPages; page++) {
      try {
        const sinceMs = oldestTs - limit * tfToMs(tf);
        const prev = await exchange.fetchOHLCV(symbol, tf, sinceMs, limit);
        if (!prev || prev.length < 2) break;
        const newOldest = prev[0][0];
        if (newOldest >= oldestTs) break;
        all = [...prev, ...all];
        oldestTs = newOldest;
        await new Promise(r => setTimeout(r, 250));
      } catch(e) { break; }
    }
  } catch(e) { return []; }
  const seen = new Set();
  return all
    .filter(c => { if (seen.has(c[0])) return false; seen.add(c[0]); return true; })
    .sort((a, b) => a[0] - b[0])
    .map(c => ({ c: c[4], v: c[6]||c[5], ts: c[0] }));
}

async function preloadHistoricalCandles(pairs) {
  console.log(`Preloading historique max — ${pairs.length} paires...`);
  const exchange = new ccxt.kraken({ enableRateLimit: true, timeout: 20000 });
  const BATCH = 20;
  for (let i = 0; i < pairs.length; i += BATCH) {
    const batch = pairs.slice(i, i + BATCH);
    await Promise.all(batch.map(async (symbol) => {
      try {
        const d = await fetchMaxHistory(exchange, symbol, '1d', 5);
        if (d.length >= 10) krakenCandles[symbol] = d;
      } catch(e) {}
      try {
        const h4 = await fetchMaxHistory(exchange, symbol, '4h', 3);
        if (h4.length >= 10) krakenCandles4h[symbol] = h4;
      } catch(e) {}
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
  console.log('✅ Preloading MAX terminé!');
}

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// SCAN CLASSIQUE (Bot Classique uniquement)
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
function scanSinglePair(symbol, timeframe='1d', exchangeId='kraken', candleStoreOverride=null) {
  try {
    const candleStore = candleStoreOverride || krakenCandles;
    const candles = candleStore[symbol];
    if (!candles || candles.length<100) return;
    const closes  = candles.map(c=>c.c);
    const volumes = candles.map(c=>c.v);
    const livePrice = livePrices[symbol];
    if (!livePrice) return;
    // Charger les params IA de l'utilisateur pour adapter la détection
    const sig = detectFigure(closes, volumes, livePrice);
    if (!sig) return;
    const sigKey = symbol+'|'+sig.fig.name+'|'+timeframe;
    const lastSigKey = recentSignals.get(sigKey);
    if (lastSigKey && Date.now()-lastSigKey < 60*60*1000) return;
    recentSignals.set(sigKey, Date.now());
    const volRatio = volumes[volumes.length-1]/avg(volumes.slice(-50));
    const exConfig = EXCHANGES_CONFIG.find(e=>e.id===exchangeId) || EXCHANGES_CONFIG[0];
    const signal = {
      symbol, exchange:exConfig.name, exchangeId, timeframe, market:'Spot',
      figure:sig.fig.name, figureCode:sig.fig.code, direction:sig.fig.dir,
      confidence:Math.round(sig.fig.wr*100), reliable:sig.fig.wr>=0.65,
      entryPrice:livePrice, tp:sig.tp, sl:sig.sl,
      tpPct:+(sig.tpPct*100).toFixed(1), slPct:+(SL_PCT*100).toFixed(1),
      figureTarget:+(sig.figureTarget*100).toFixed(1),
      volumeRatio:volRatio.toFixed(2), tradeAmount:TRADE_AMOUNT,
      gain:(TRADE_AMOUNT*sig.tpPct).toFixed(4), loss:(TRADE_AMOUNT*SL_PCT).toFixed(4),
      time:new Date()
    };
    const idx = signalsCache.findIndex(s=>s.symbol===symbol&&s.exchangeId===exchangeId);
    if (idx>=0) signalsCache[idx]=signal;
    else if (signalsCache.length<MAX_SIGNALS_CACHE) signalsCache.push(signal);
    new Signal({ symbol, exchange:exConfig.name, market:'Spot', figure:sig.fig.name,
      direction:sig.fig.dir, confidence:signal.confidence, entryPrice:livePrice,
      tp:sig.tp, sl:sig.sl, volumeRatio:volRatio, timeframe }).save().catch(()=>{});
    const resistance = Math.max(...closes.slice(-30));
    const pctAbove = (livePrice - resistance) / resistance;
    if (pctAbove > 0.04) return; // trop tard

    const bcKey = symbol + '|' + timeframe;
    if (!breakoutConfirm[bcKey] || breakoutConfirm[bcKey].figure !== sig.fig.name) {
      breakoutConfirm[bcKey] = { resistance, figure: sig.fig.name, signal };
      console.log(`[Figure ${timeframe}] ${symbol} · ${sig.fig.name} · Résistance ${resistance.toFixed(4)} (${(pctAbove*100).toFixed(1)}%)`);
    }
    setTimeout(() => {
      if (breakoutConfirm[bcKey]?.figure === sig.fig.name) delete breakoutConfirm[bcKey];
    }, 7*24*60*60*1000);
  } catch(e) {}
}

// ExÃ©cution Bot Classique (mode='classic' uniquement)
async function executeTrade(signal) {
  try {
    const exchangeId = signal.exchangeId||'kraken';
    const exConfig = EXCHANGES_CONFIG.find(e=>e.id===exchangeId);
    // Seulement les utilisateurs en mode classique
    const users = await User.find({
      active:true, apiKey:{$exists:true},
      exchangeName:new RegExp(exchangeId,'i'),
      botMode:'classic' // â† IMPORTANT: seulement le bot classique
    });
    for (const user of users) {
      try {
        const existingPos = await OpenPosition.findOne({email:user.email,symbol:signal.symbol,exchangeId});
        if (existingPos) continue;
        const lastTrade = await Trade.findOne({email:user.email,symbol:signal.symbol,figure:signal.figure,exchange:signal.exchange},null,{sort:{time:-1}});
        if (lastTrade) {
          const diff = Math.abs(signal.entryPrice-lastTrade.entryPrice)/lastTrade.entryPrice;
          if (diff<0.20) continue;
        }
        const recentTrade = await Trade.findOne({email:user.email,symbol:signal.symbol,exchange:signal.exchange,time:{$gte:new Date(Date.now()-4*60*60*1000)}});
        if (recentTrade) continue;
        if (!exConfig||!exConfig.ccxt) continue;
        const ExClass = ccxt[exchangeId];
        if (!ExClass) continue;
        const exchange = new ExClass({apiKey:user.apiKey,secret:user.apiSecret,enableRateLimit:true});
        const balance = await exchange.fetchBalance();
        const currency = user.currency||exConfig.currencies[0];
        const available = balance[currency]?.free||0;
        const amount = Math.max(user.tradeAmount||TRADE_AMOUNT,5);
        if (available<amount) continue;
        const qty = amount/signal.entryPrice;
        const orderParams = {};
        if (exchangeId==='kraken') orderParams.oflags='fciq';
        console.log(`[Classic] BUY ${signal.symbol} Â· ${signal.figure} Â· ${amount}${currency}`);
        const order = await exchange.createOrder(signal.symbol,'market','buy',qty,undefined,orderParams);
        await new OpenPosition({email:user.email,symbol:signal.symbol,exchange:signal.exchange,exchangeId,
          figure:signal.figure,entryPrice:signal.entryPrice,tp:signal.tp,sl:signal.sl,
          tpPct:signal.tpPct,figureTarget:signal.figureTarget,qty,amount,currency,
          timeframe:signal.timeframe}).save();
        await new Trade({email:user.email,symbol:signal.symbol,exchange:signal.exchange,market:'Spot',
          direction:signal.direction,figure:signal.figure,entryPrice:signal.entryPrice,
          exitPrice:null,amount,pnl:0,currency,result:'OPEN',
          exitReason:'Position ouverte â€” en attente TP/SL',botMode:'classic'}).save();
      } catch(e) { console.log(`[Classic] Erreur ${signal.symbol}:`,e.message); }
    }
  } catch(e) { console.log('[executeTrade] Erreur:',e.message); }
}

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// TP/SL â€” Fonctionne pour les deux modes
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
const positionsInProgress = new Set();

async function checkTPSLInstant() {
  try {
    const positions = await OpenPosition.find({});
    if (positions.length===0) return;
    for (const pos of positions) {
      const posId = pos._id.toString();
      if (positionsInProgress.has(posId)) continue;
      const currentPrice = livePrices[pos.symbol];
      if (!currentPrice) continue;
      if (pos.tp<=pos.entryPrice||pos.sl>=pos.entryPrice) {
        const correctedTP = +(pos.entryPrice*1.15).toFixed(8);
        const correctedSL = +(pos.entryPrice*(1-SL_PCT)).toFixed(8);
        await OpenPosition.updateOne({_id:pos._id},{tp:correctedTP,sl:correctedSL});
        continue;
      }
      const hitTP = currentPrice>=pos.tp;
      const hitSL = currentPrice<=pos.sl;
      if (!hitTP&&!hitSL) continue;
      const reason = hitTP?'TP':'SL';
      console.log(`[Classic] ${pos.symbol} prix:${currentPrice}`);
      positionsInProgress.add(posId);
      try {
        const user = await User.findOne({email:pos.email});
        if (!user) { await OpenPosition.deleteOne({_id:pos._id}); positionsInProgress.delete(posId); continue; }
        const exchangeId = pos.exchangeId||user.exchangeName.toLowerCase();
        const ExClass = ccxt[exchangeId];
        if (!ExClass) { await OpenPosition.deleteOne({_id:pos._id}); positionsInProgress.delete(posId); continue; }
        const exchange = new ExClass({apiKey:user.apiKey,secret:user.apiSecret,enableRateLimit:true});
        const balance = await exchange.fetchBalance();
        const [base] = pos.symbol.split('/');
        const baseBalance = balance[base]?.free||0;
        if (baseBalance<0.000001) {
          await Trade.findOneAndUpdate({email:pos.email,symbol:pos.symbol,result:'OPEN'},
            {exitPrice:currentPrice,pnl:0,result:'CLOSED_MANUAL',exitReason:'Vendu manuellement ou solde vide'},{sort:{time:-1}});
          await OpenPosition.deleteOne({_id:pos._id});
          positionsInProgress.delete(posId); continue;
        }
        const orderParams = {};
        if (exchangeId==='kraken') orderParams.oflags='fciq';
        const order = await exchange.createOrder(pos.symbol,'market','sell',baseBalance,undefined,orderParams);
        const posTpPct = pos.tpPct ? pos.tpPct/100 : TP_PCT;
        const pnl = hitTP ? pos.amount*posTpPct : -(pos.amount*SL_PCT);
        await Trade.findOneAndUpdate({email:pos.email,symbol:pos.symbol,result:'OPEN'},
          {exitPrice:currentPrice,pnl,result:hitTP?'WIN':'LOSS',
           exitReason:hitTP?`TP +${pos.tpPct}% atteint`:'SL -2% touchÃ©'},{sort:{time:-1}});
        await OpenPosition.deleteOne({_id:pos._id});
        console.log(`[${reason}] PnL: ${pnl>=0?'+':''}$${pnl.toFixed(4)}`);
        // Apprentissage IA après chaque trade
        learnFromTrade(pos.email, {
          result: hitTP?'WIN':'LOSS', figure:pos.figure, timeframe:pos.timeframe||'1d',
          entryPrice:pos.entryPrice, exitPrice:currentPrice, pnl, time:new Date()
        }).catch(()=>{});
        // Mettre Ã  jour la mÃ©moire IA si mode IA
      } catch(e) {
        console.log(`[TP/SL] Erreur ${pos.symbol}:`,e.message);
        if (e.message&&e.message.includes('Insufficient funds'))
          await OpenPosition.deleteOne({_id:pos._id}).catch(()=>{});
      } finally { positionsInProgress.delete(posId); }
    }
  } catch(e) { console.log('[TP/SL] Erreur:',e.message); }
}

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// SCAN ALL (classique + REST)
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
function scanKrakenFromMemory() {
  const results = [];
  for (const symbol of krakenPairsList) {
    const candles = krakenCandles[symbol];
    if (!candles||candles.length<20) continue;
    const closes = candles.filter(c=>c.c>0).map(c=>c.c);
    const volumes = candles.filter(c=>c.v>0).map(c=>c.v);
    const price = closes[closes.length-1];
    const sig = detectFigure(closes,volumes,price);
    if (!sig) continue;
    const volRatio = volumes[volumes.length-1]/avg(volumes.slice(-50));
    results.push({
      symbol,exchange:'Kraken',exchangeId:'kraken',timeframe:'1d',market:'Spot',
      figure:sig.fig.name,figureCode:sig.fig.code,direction:sig.fig.dir,
      confidence:Math.round(sig.fig.wr*100),reliable:sig.fig.wr>=0.65,
      entryPrice:price,tp:sig.tp,sl:sig.sl,
      tpPct:+(sig.tpPct*100).toFixed(1),slPct:+(SL_PCT*100).toFixed(1),
      figureTarget:+(sig.figureTarget*100).toFixed(1),
      volumeRatio:volRatio.toFixed(2),tradeAmount:TRADE_AMOUNT,
      gain:(TRADE_AMOUNT*sig.tpPct).toFixed(4),loss:(TRADE_AMOUNT*SL_PCT).toFixed(4),time:new Date()
    });
  }
  return results;
}

async function scanExchangeRest(exConfig) {
  const results = [];
  if (!exConfig.ccxt) return results;
  try {
    const ExClass = ccxt[exConfig.id];
    if (!ExClass) return results;
    const exchange = new ExClass({enableRateLimit:true,timeout:15000});
    if (!marketsCache[exConfig.id]||Date.now()-marketsCache[exConfig.id].time>3600000) {
      marketsCache[exConfig.id] = {markets:await exchange.loadMarkets(),time:Date.now()};
    }
    const markets = marketsCache[exConfig.id].markets;
    const symbols = Object.keys(markets).filter(s=>{
      const m=markets[s];
      return exConfig.quoteFilter.some(q=>s.endsWith('/'+q))&&(m.type==='spot'||m.spot===true)&&exConfig.spot&&m.active!==false;
    }).slice(0,200);
    const BATCH=15;
    for (let i=0;i<symbols.length;i+=BATCH) {
      const batch=symbols.slice(i,i+BATCH);
      const batchResults=await Promise.all(batch.map(async(symbol)=>{
        try {
          const ohlcv=await exchange.fetchOHLCV(symbol,'1d',undefined,160);
          if (!ohlcv||ohlcv.length<20) return null;
          const closes=ohlcv.map(c=>c[4]);const volumes=ohlcv.map(c=>c[5]);
          const price=closes[closes.length-1];
          const sig=detectFigure(closes,volumes,price);
          if (!sig) return null;
          const volRatio=volumes[volumes.length-1]/avg(volumes.slice(-50));
          return {symbol,exchange:exConfig.name,exchangeId:exConfig.id,timeframe:'1d',market:'Spot',
            figure:sig.fig.name,figureCode:sig.fig.code,direction:sig.fig.dir,
            confidence:Math.round(sig.fig.wr*100),reliable:sig.fig.wr>=0.65,
            entryPrice:price,tp:sig.tp,sl:sig.sl,
            tpPct:+(sig.tpPct*100).toFixed(1),slPct:+(SL_PCT*100).toFixed(1),
            figureTarget:+(sig.figureTarget*100).toFixed(1),volumeRatio:volRatio.toFixed(2),
            tradeAmount:TRADE_AMOUNT,gain:(TRADE_AMOUNT*sig.tpPct).toFixed(4),time:new Date()};
        } catch(e){return null;}
      }));
      batchResults.forEach(r=>{if(r)results.push(r);});
    }
  } catch(e){console.log(`[${exConfig.name}] Erreur:`,e.message);}
  return results;
}

let scanRunning=false;
async function scanAll() {
  if (scanRunning) return;
  scanRunning=true;
  const startTime=Date.now();
  console.log(`\n=== SCAN â€” ${new Date().toLocaleTimeString()} ===`);
  signalsCache.length=0;
  Object.keys(signalsByExchange).forEach(k=>delete signalsByExchange[k]);
  if (typeof global.gc==='function') global.gc();
  try {
    const users=await User.find({active:true,apiKey:{$exists:true}});
    const krakenResults=scanKrakenFromMemory();
    signalsCache.push(...krakenResults);
    signalsByExchange['kraken']=krakenResults;
    lastScanTime=new Date();
    console.log(`[Kraken] ${krakenResults.length} signal(s) Â· ${Date.now()-startTime}ms`);
    if (users.length===0){console.log('[Scan] Aucun utilisateur');return;}
    const uniqueExchanges=[...new Set(users.map(u=>u.exchangeName.toLowerCase()))];
    const nonKraken=uniqueExchanges.filter(id=>id!=='kraken');
    for (const exchangeId of nonKraken) {
      const exConfig=EXCHANGES_CONFIG.find(e=>e.id===exchangeId||e.name.toLowerCase()===exchangeId);
      if (!exConfig||!exConfig.ccxt) continue;
      try {
        const results=await scanExchangeRest(exConfig);
        signalsCache.push(...results);signalsByExchange[exConfig.id]=results;
      } catch(e){console.log(`[${exchangeId}] Erreur:`,e.message);}
    }
    lastScanTime=new Date();
    console.log(`=== FIN Â· ${signalsCache.length} signaux Â· ${Date.now()-startTime}ms ===\n`);
    // Trades classiques uniquement
    for (const user of users.filter(u=>u.botMode==='classic'||!u.botMode)) {
      const userExchangeId=user.exchangeName.toLowerCase();
      const exConfig=EXCHANGES_CONFIG.find(e=>e.id===userExchangeId||e.name.toLowerCase()===userExchangeId);
      if (!exConfig||!exConfig.ccxt) continue;
      const userSignals=signalsCache.filter(s=>s.exchangeId===userExchangeId);
      if (userSignals.length===0) continue;
      try {
        const ExClass=ccxt[userExchangeId];if(!ExClass)continue;
        const exchange=new ExClass({apiKey:user.apiKey,secret:user.apiSecret,enableRateLimit:true});
        const balance=await exchange.fetchBalance();
        const currency=user.currency||exConfig.currencies[0];
        const available=balance[currency]?.free||0;
        const amount=Math.max(user.tradeAmount||TRADE_AMOUNT,5);
        if (available<amount) continue;
        let ordersPlaced=0;
        for (const sig of userSignals.slice(0,MAX_CONCURRENT)) {
          if (ordersPlaced>=MAX_CONCURRENT) break;
          try {
            const existingPos=await OpenPosition.findOne({email:user.email,symbol:sig.symbol,exchangeId:userExchangeId});
            if (existingPos) continue;
            const lastTrade=await Trade.findOne({email:user.email,symbol:sig.symbol,figure:sig.figure,exchange:sig.exchange},null,{sort:{time:-1}});
            if (lastTrade){const diff=Math.abs(sig.entryPrice-lastTrade.entryPrice)/lastTrade.entryPrice;if(diff<0.20)continue;}
            const recentTrade=await Trade.findOne({email:user.email,symbol:sig.symbol,exchange:sig.exchange,time:{$gte:new Date(Date.now()-4*60*60*1000)}});
            if (recentTrade) continue;
            const qty=amount/sig.entryPrice;
            const orderParams={};if(userExchangeId==='kraken')orderParams.oflags='fciq';
            const order=await exchange.createOrder(sig.symbol,'market','buy',qty,undefined,orderParams);
            await new OpenPosition({email:user.email,symbol:sig.symbol,exchange:sig.exchange,exchangeId:userExchangeId,
              figure:sig.figure,entryPrice:sig.entryPrice,tp:sig.tp,sl:sig.sl,tpPct:sig.tpPct,
              figureTarget:sig.figureTarget,qty,amount,currency,botMode:'classic'}).save();
            await new Trade({email:user.email,symbol:sig.symbol,exchange:sig.exchange,market:'Spot',
              direction:sig.direction,figure:sig.figure,entryPrice:sig.entryPrice,exitPrice:null,
              amount,pnl:0,currency,result:'OPEN',exitReason:'Position ouverte â€” en attente TP/SL',botMode:'classic'}).save();
            ordersPlaced++;
          } catch(e){console.log(`[Classic] Erreur ${sig.symbol}:`,e.message);}
        }
        if (ordersPlaced>0) console.log(`[Classic] ${ordersPlaced} ordre(s) pour ${user.email}`);
      } catch(e){console.log(`[Classic] Erreur ${user.email}:`,e.message);}
    }
  } finally {scanRunning=false;}
}

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// XLM PAYMENTS
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
async function checkXlmPayments() {
  try {
    const BENDER_XLM='GDIZP4VPNBZLV7CCDUG4BORFYERX3NQVBE3N6W2FAFAIT3OTTRUIUCBR';
    const users=await User.find({xlmWallet:{$exists:true,$ne:''}});
    if (users.length===0) return;
    const resp=await fetch(`https://horizon.stellar.org/accounts/${BENDER_XLM}/payments?order=desc&limit=50`);
    const data=await resp.json();
    const payments=data._embedded?.records||[];
    for (const user of users) {
      if (!user.xlmWallet) continue;
      const payment=payments.find(p=>p.from===user.xlmWallet&&p.asset_type==='native'&&p.to===BENDER_XLM);
      if (!payment) continue;
      const billing=await Billing.findOne({email:user.email,status:'PENDING'});
      if (!billing) continue;
      await Billing.findOneAndUpdate({_id:billing._id},{status:'PAID',paidAt:new Date(),txHash:payment.transaction_hash});
      console.log(`[XLM] Paiement â€” ${user.email} PAID`);
    }
  } catch(e){console.log('[XLM] Erreur:',e.message);}
}

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// ROUTES
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
app.get('/', (req,res) => res.json({
  status:'Bender Pro v10.0 â€” Bot Classique + IA',
  modes:['classic','ai'],
  exchanges:EXCHANGES_CONFIG.length,
  krakenWsConnected:wsConnected,
  krakenPairsTracked:krakenPairsList.length,
  lastScan:lastScanTime,
  signalsActive:signalsCache.length,
}));

app.get('/market',(req,res)=>{
  let sigs=[...signalsCache];
  if (req.query.exchange) sigs=sigs.filter(s=>s.exchange.toLowerCase().includes(req.query.exchange.toLowerCase()));
  res.json({success:true,signals:sigs,count:sigs.length,lastScan:lastScanTime});
});

app.get('/scan',async(req,res)=>{
  res.json({success:true,message:'Scan lancÃ©...'});
  scanAll().catch(console.error);
});

app.post('/register-email',async(req,res)=>{
  const {email}=req.body;
  if (!email||!email.includes('@')) return res.json({success:false,error:'Email invalide'});
  try {
    await User.findOneAndUpdate({email},{email,active:true},{upsert:true,new:true});
    res.json({success:true,message:'Email enregistrÃ©'});
  } catch(e){res.json({success:false,error:e.message});}
});

// Route connect â€” supporte le choix de mode
app.post('/connect',async(req,res)=>{
  const {email,apiKey,secret,exchangeName,tradeAmount,currency,botMode}=req.body;
  if (!email||!apiKey||!secret||!exchangeName)
    return res.json({success:false,error:'DonnÃ©es manquantes'});
  try {
    const exConfig=EXCHANGES_CONFIG.find(e=>e.id===exchangeName.toLowerCase()||e.name.toLowerCase()===exchangeName.toLowerCase());
    const selectedCurrency=currency||(exConfig?exConfig.currencies[0]:'USD');
    await User.findOneAndUpdate({email},
      {apiKey,apiSecret:secret,exchangeName:exConfig?exConfig.id:exchangeName.toLowerCase(),
       active:true,tradeAmount:tradeAmount||TRADE_AMOUNT,currency:selectedCurrency,botMode:'classic'},
      {upsert:true,new:true});
    res.json({success:true,message:`🤖 Bot Classique · ${exConfig?.name||exchangeName} · ${selectedCurrency} · SL -2%`});
  } catch(e){res.json({success:false,error:e.message});}
});

// Route pour changer de mode sans dÃ©connecter
app.post('/switch-mode',async(req,res)=>{
  try {
    const {email,botMode}=req.body;
    if (!email||!botMode) return res.json({success:false,error:'DonnÃ©es manquantes'});
    const mode=botMode==='ai'?'ai':'classic';
    await User.findOneAndUpdate({email},{botMode:mode});
    res.json({success:true,mode,message:`Mode basculÃ© vers: ${mode==='ai'?'IA Autonome':'Classique'}`});
  } catch(e){res.json({success:false,error:e.message});}
});

app.get('/status/:email',async(req,res)=>{
  try {
    const user=await User.findOne({email:req.params.email});
    if (!user) return res.json({connected:false});
    const trades=await Trade.countDocuments({email:req.params.email});
    const wins=await Trade.countDocuments({email:req.params.email,result:'WIN'});
    res.json({connected:true,active:user.active,exchange:user.exchangeName,
      tradeAmount:user.tradeAmount,trades,botMode:user.botMode||'classic',
      winRate:trades>0?Math.round(wins/trades*100)+'%':'N/A'});
  } catch(e){res.json({success:false,error:e.message});}
});


app.get('/positions/:email',async(req,res)=>{
  try {
    const positions=await OpenPosition.find({email:req.params.email});
    const enriched=positions.map(pos=>({
      ...pos.toObject(),
      currentPrice:livePrices[pos.symbol]||pos.entryPrice,
      pnlPct:livePrices[pos.symbol]?((livePrices[pos.symbol]-pos.entryPrice)/pos.entryPrice*100):0,
      pnlUsd:livePrices[pos.symbol]?((livePrices[pos.symbol]-pos.entryPrice)/pos.entryPrice)*pos.amount:0,
    }));
    res.json({success:true,positions:enriched,count:enriched.length});
  } catch(e){res.json({success:false,error:e.message});}
});

app.get('/trades/:email',async(req,res)=>{
  try {
    const email=req.params.email;
    const allTrades=await Trade.find({email});
    const totalPnl=allTrades.reduce((a,t)=>a+t.pnl,0);
    const totalWins=allTrades.filter(t=>t.result==='WIN').length;
    const trades=await Trade.find({email}).sort({time:-1}).limit(100);
    res.json({trades,totalTradesCount:allTrades.length,totalPnl:totalPnl.toFixed(4),
      wins:totalWins,losses:allTrades.length-totalWins,displayedCount:trades.length});
  } catch(e){res.json({success:false,error:e.message});}
});

const COMMISSION_RATE=0.0025;
const BILLING_WALLET=process.env.BILLING_WALLET||'GDIZP4VPNBZLV7CCDUG4BORFYERX3NQVBE3N6W2FAFAIT3OTTRUIUCBR';
const BILLING_DAYS=30;

app.get('/billing/:email',async(req,res)=>{
  try {
    const email=req.params.email;
    const user=await User.findOne({email});
    if (!user) return res.json({success:false,error:'Utilisateur non trouvÃ©'});
    const periodEnd=new Date();
    const periodStart=new Date(periodEnd-BILLING_DAYS*24*3600*1000);
    const trades=await Trade.find({email,time:{$gte:periodStart,$lte:periodEnd},result:{$in:['WIN','LOSS']}});
    const totalVolume=trades.reduce((a,t)=>a+t.amount,0);
    const totalPnl=trades.reduce((a,t)=>a+t.pnl,0);
    const wins=trades.filter(t=>t.result==='WIN').length;
    const commission=+(totalVolume*COMMISSION_RATE).toFixed(4);
    let billing=await Billing.findOne({email,periodStart:{$gte:new Date(periodStart.getTime()-3600000)}});
    if (!billing) billing=await new Billing({email,periodStart,periodEnd,totalPnl:+totalPnl.toFixed(4),totalVolume:+totalVolume.toFixed(4),commission,status:'PENDING'}).save();
    res.json({success:true,billing:{id:billing._id,email,periodStart:periodStart.toLocaleDateString('fr-CA'),
      periodEnd:periodEnd.toLocaleDateString('fr-CA'),trades:trades.length,wins,losses:trades.length-wins,
      winRate:trades.length>0?Math.round(wins/trades.length*100)+'%':'N/A',
      totalVolume:+totalVolume.toFixed(4),commission,status:billing.status,
      paidAt:billing.paidAt?new Date(billing.paidAt).toLocaleDateString('fr-CA'):null,
      wallet:BILLING_WALLET,message:`Commission: $${commission} USD (0.25% de $${totalVolume.toFixed(4)})`}});
  } catch(e){res.json({success:false,error:e.message});}
});


app.post('/save-xlm',async(req,res)=>{
  try {
    const {email,xlmWallet}=req.body;
    if (!email||!xlmWallet||!xlmWallet.startsWith('G')||xlmWallet.length<40)
      return res.json({success:false,error:'Adresse XLM invalide'});
    await User.findOneAndUpdate({email},{xlmWallet},{upsert:true});
    res.json({success:true,message:'Wallet XLM sauvegardÃ©'});
  } catch(e){res.json({success:false,error:e.message});}
});

app.post('/disconnect',async(req,res)=>{
  try {
    const {email}=req.body;
    if (!email) return res.json({success:false,error:'Email manquant'});
    await User.findOneAndUpdate({email},{active:false});
    res.json({success:true,message:'DÃ©connectÃ©'});
  } catch(e){res.json({success:false,error:e.message});}
});

app.get('/admin/stats',async(req,res)=>{
  try {
    const users=await User.countDocuments();
    const active=await User.countDocuments({active:true});
    const classicUsers=await User.countDocuments({active:true,botMode:'classic'});
    const aiUsers=await User.countDocuments({active:true,botMode:'ai'});
    const trades=await Trade.countDocuments();
    const wins=await Trade.countDocuments({result:'WIN'});
    res.json({users,active,classicUsers,aiUsers,trades,
      winRate:trades>0?Math.round(wins/trades*100)+'%':'N/A',
      signalsActive:signalsCache.length,lastScan:lastScanTime,
      exchanges:EXCHANGES_CONFIG.length,krakenWsConnected:wsConnected,
      krakenPairsTracked:krakenPairsList.length});
  } catch(e){res.json({success:false,error:e.message});}
});

app.post('/admin/clear-alerts', (req, res) => {
  watchdogAlerts.length = 0;
  res.json({ success: true, message: 'Alertes effacées' });
});

app.post('/toggle',async(req,res)=>{
  try{const{email,active}=req.body;await User.findOneAndUpdate({email},{active});res.json({success:true,active});}
  catch(e){res.json({success:false,error:e.message});}
});

app.get('/clear-users',async(req,res)=>{
  try{const r=await User.deleteMany({});res.json({success:true,deleted:r.deletedCount});}
  catch(e){res.json({success:false,error:e.message});}
});

app.get('/admin/billing',async(req,res)=>{
  try {
    const pending=await Billing.find({status:'PENDING',commission:{$gt:0}}).sort({createdAt:-1});
    const totalDue=pending.reduce((a,b)=>a+b.commission,0);
    res.json({success:true,pending:pending.length,totalDue:+totalDue.toFixed(4),wallet:BILLING_WALLET,billings:pending});
  } catch(e){res.json({success:false,error:e.message});}
});

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// ðŸ›¡ï¸ BOT WATCHDOG â€” Surveillance & Protection
// Tourne toutes les 30 secondes â€” surveille tout
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

const watchdogAlerts = []; // historique des alertes
const tradeAttempts  = new Map(); // { key: [timestamps] } anti-boucle
const suspiciousIPs  = new Map(); // { ip: compteur }

// â”€â”€ 1. VÃ©rifier que le WebSocket Kraken est vivant
function watchWebSocket() {
  const wsOk   = wsTicker && wsTicker.readyState===1;
  const wsOk1d = ws   && ws.readyState===1;
  const wsOk1h = ws1h && ws1h.readyState===1;
  const wsOk4h = ws4h && ws4h.readyState===1;
  const nPrices = Object.keys(livePrices).length;
  if(!wsOk)  {watchAlert('WEBSOCKET','Ticker KO');  connectKrakenTicker(krakenPairsList);}
  if(!wsOk1d){watchAlert('WEBSOCKET','Daily KO');   connectKrakenWS(krakenPairsList);}
  if(!wsOk1h){watchAlert('WEBSOCKET','1H KO');      connectKrakenWS1h(krakenPairsList);}
  if(!wsOk4h){watchAlert('WEBSOCKET','4H KO');      connectKrakenWS4h(krakenPairsList);}
  if(nPrices===0&&krakenPairsList.length>0){watchAlert('PRIX','Aucun prix live');connectKrakenTicker(krakenPairsList);}
}
// â”€â”€ 2. VÃ©rifier les positions ouvertes (protocole SL/TP)
async function watchPositions() {
  try {
    const positions = await OpenPosition.find({});
    for (const pos of positions) {
      // VÃ©rifier cohÃ©rence TP/SL
      if (pos.tp <= pos.entryPrice) {
        watchAlert('PROTOCOLE', `${pos.symbol} TP incohÃ©rent (${pos.tp} <= entrÃ©e ${pos.entryPrice}) â€” correction`);
        const correctedTP = +(pos.entryPrice * 1.15).toFixed(8);
        await OpenPosition.updateOne({ _id: pos._id }, { tp: correctedTP });
      }
      if (pos.sl >= pos.entryPrice) {
        watchAlert('PROTOCOLE', `${pos.symbol} SL incohÃ©rent (${pos.sl} >= entrÃ©e ${pos.entryPrice}) â€” correction`);
        const correctedSL = +(pos.entryPrice * (1 - SL_PCT)).toFixed(8);
        await OpenPosition.updateOne({ _id: pos._id }, { sl: correctedSL });
      }

      // VÃ©rifier position orpheline (utilisateur inactif)
      const user = await User.findOne({ email: pos.email });
      if (!user || !user.active) {
        watchAlert('ORPHELINE', `Position orpheline ${pos.symbol} pour ${pos.email} â€” utilisateur inactif`);
        // Ne pas supprimer automatiquement â€” juste alerter
      }

      // VÃ©rifier position trop ancienne (>7 jours sans TP/SL)
      const ageHours = (Date.now() - new Date(pos.openedAt).getTime()) / (1000 * 60 * 60);
      if (ageHours > 240) { // >10j = fantôme → fermer
        const curP = livePrices[pos.symbol];
        if (curP) {
          const pnl = (curP-pos.entryPrice)/pos.entryPrice*pos.amount;
          await Trade.findOneAndUpdate({email:pos.email,symbol:pos.symbol,result:"OPEN"},{exitPrice:curP,pnl:+pnl.toFixed(4),result:pnl>=0?"WIN":"LOSS",exitReason:"Fantôme >10j fermé auto"},{sort:{time:-1}}).catch(()=>{});
          await OpenPosition.deleteOne({_id:pos._id}).catch(()=>{});
          learnFromTrade(pos.email,{result:pnl>=0?"WIN":"LOSS",figure:pos.figure,timeframe:pos.timeframe||"1d",entryPrice:pos.entryPrice,exitPrice:curP,pnl:+pnl.toFixed(4),time:new Date()}).catch(()=>{});
          console.log(`[Watchdog] Fantôme fermé: ${pos.symbol} PnL:${pnl>=0?"+":" "}$${pnl.toFixed(4)}`);
        }
      } else if (ageHours > 168) {
        watchAlert("POSITION", `${pos.symbol} ouverte depuis ${Math.floor(ageHours)}h`);
      }

    }
  } catch(e) { console.log('[Watchdog] Erreur watchPositions:', e.message); }
}

// â”€â”€ 3. DÃ©tecter les boucles de trade (mÃªme signal tentÃ© trop souvent)
function watchTradeBoucle(symbol, exchangeId) {
  const key = symbol + '|' + exchangeId;
  const now = Date.now();
  if (!tradeAttempts.has(key)) tradeAttempts.set(key, []);
  const attempts = tradeAttempts.get(key);
  // Garder seulement les 60 derniÃ¨res secondes
  const recent = attempts.filter(t => now - t < 60000);
  recent.push(now);
  tradeAttempts.set(key, recent);
  if (recent.length >= 5) {
    watchAlert('BOUCLE', `${symbol} tentÃ© ${recent.length}x en 60s â€” possible boucle infinie`);
    return true; // bloquer
  }
  return false;
}

// â”€â”€ 4. DÃ©tecter manipulation de prix (spike anormal)
function watchPriceManipulation(symbol, newPrice) {
  const candles = krakenCandles[symbol];
  if (!candles || candles.length < 10) return;
  const avgPrice = candles.slice(-10).reduce((a, c) => a + c.c, 0) / 10;
  const deviation = Math.abs(newPrice - avgPrice) / avgPrice;
  if (deviation > 0.15) { // +/-15% en un tick = suspect
    watchAlert('MANIPULATION', `${symbol} spike de prix dÃ©tectÃ©: ${(deviation*100).toFixed(1)}% d'Ã©cart vs moyenne`);
    // Invalider le prix suspect
    delete livePrices[symbol];
    return true;
  }
  return false;
}

// â”€â”€ 5. Surveiller le capital IA journalier

// â”€â”€ 6. Surveiller MongoDB (connexion active)
async function watchDatabase() {
  try {
    const state = mongoose.connection.readyState;
    // 0=dÃ©connectÃ©, 1=connectÃ©, 2=connexion, 3=dÃ©connexion
    if (state !== 1) {
      watchAlert('DATABASE', `MongoDB Ã©tat anormal: ${state} â€” tentative de reconnexion`);
      await mongoose.connect(process.env.MONGODB_URI).catch(() => {});
    }
    // VÃ©rifier accumulation de signaux (fuite mÃ©moire potentielle)
    if (signalsCache.length > MAX_SIGNALS_CACHE * 1.5) {
      watchAlert('MEMOIRE', `Cache signaux trop grand: ${signalsCache.length} â€” nettoyage`);
      signalsCache.length = MAX_SIGNALS_CACHE;
    }
    // VÃ©rifier recentSignals (fuite mÃ©moire Map)
    if (recentSignals.size > 10000) {
      watchAlert('MEMOIRE', `recentSignals trop grand (${recentSignals.size}) â€” nettoyage`);
      // Supprimer les entrÃ©es de plus de 6h
      const cutoff = Date.now() - 6 * 60 * 60 * 1000;
      for (const [key, time] of recentSignals) {
        if (time < cutoff) recentSignals.delete(key);
      }
    }
  } catch(e) { console.log('[Watchdog] Erreur watchDatabase:', e.message); }
}

// â”€â”€ 7. VÃ©rifier la santÃ© globale du bot
async function watchBotHealth() {
  try {
    const activeUsers = await User.countDocuments({ active: true, apiKey: { $exists: true } });
    const openPos     = await OpenPosition.countDocuments({});
    const nPrices     = Object.keys(livePrices).length;
    const wsOk        = wsTicker && wsTicker.readyState === 1;

    // Log santÃ© toutes les 5 minutes
    console.log(`[Watchdog] âœ… SantÃ©: ${activeUsers} users Â· ${openPos} positions Â· ${nPrices} prix live Â· WS:${wsOk?'OK':'KO'} Â· Alertes: ${watchdogAlerts.length}`);

    // VÃ©rifier que les positions ont bien des prix live
    if (openPos > 0 && nPrices === 0) {
      watchAlert('CRITIQUE', `${openPos} positions ouvertes mais AUCUN prix live â€” TP/SL aveugle!`);
      // Forcer reconnexion ticker
      connectKrakenTicker(krakenPairsList);
    }

    // VÃ©rifier positionsInProgress bloquÃ©es
    if (positionsInProgress.size > 10) {
      watchAlert('VERROU', `${positionsInProgress.size} positions en cours de traitement â€” possible blocage`);
      // Reset si bloquÃ© depuis trop longtemps (normalement vide en quelques secondes)
    }
  } catch(e) { console.log('[Watchdog] Erreur watchBotHealth:', e.message); }
}

// â”€â”€ Enregistrer une alerte Watchdog
function watchAlert(type, message) {
  const oneHourAgo = Date.now() - 60*60*1000;
  const isDup = watchdogAlerts.some(a => a.type===type && a.message===message && new Date(a.time).getTime() > oneHourAgo);
  if (isDup) return;
  const alert = { type, message, time: new Date() };
  watchdogAlerts.unshift(alert);
  if (watchdogAlerts.length > 100) watchdogAlerts.pop();
  console.log(`[Watchdog ⚠ ${type}] ${message}`);
}

// â”€â”€ Cycle principal du Watchdog (toutes les 30 secondes)
async function runWatchdog() {
  try {
    watchWebSocket();                  // WebSocket vivant ?
    await watchPositions();            // Positions cohÃ©rentes ?
    await watchDatabase();             // MongoDB et mÃ©moire OK ?
    await watchBotHealth();            // SantÃ© globale
  } catch(e) { console.log('[Watchdog] Erreur cycle:', e.message); }
}

// Route admin pour voir les alertes Watchdog
app.get('/ai-memory/:email', async(req,res) => {
  try {
    const mem = await AIMemory.findOne({ email: req.params.email });
    if (!mem) return res.json({ success:false, message:'Pas encore de mémoire' });
    res.json({ success:true,
      generation: mem.generation, winRate: mem.winRate,
      totalTrades: mem.totalTrades, totalWins: mem.totalWins,
      params: mem.params, bestFigures: mem.bestFigures,
      avoidFigures: mem.avoidFigures, bestTimeframes: mem.bestTimeframes,
      lessons: (mem.lessons||[]).slice(-10), lastLearning: mem.lastLearning
    });
  } catch(e) { res.json({ success:false, error:e.message }); }
});

// Fermer manuellement une position fantôme
app.post('/admin/close-position', async(req, res) => {
  try {
    const { positionId, email } = req.body;
    const pos = await OpenPosition.findById(positionId);
    if (!pos) return res.json({ success: false, error: 'Position non trouvée' });
    const currentPrice = livePrices[pos.symbol] || pos.entryPrice;
    const pnl = (currentPrice - pos.entryPrice) / pos.entryPrice * pos.amount;
    await Trade.findOneAndUpdate(
      { email: pos.email, symbol: pos.symbol, result: 'OPEN' },
      { exitPrice: currentPrice, pnl: +pnl.toFixed(4),
        result: pnl >= 0 ? 'WIN' : 'LOSS',
        exitReason: 'Fermée manuellement via admin' },
      { sort: { time: -1 } }
    ).catch(() => {});
    await OpenPosition.deleteOne({ _id: pos._id });
    res.json({ success: true, message: `Position ${pos.symbol} fermée · PnL: $${pnl.toFixed(4)}` });
  } catch(e) { res.json({ success: false, error: e.message }); }
});

app.get('/admin/watchdog', (req, res) => {
  res.json({
    success:     true,
    alerts:      watchdogAlerts.slice(0, 50),
    count:       watchdogAlerts.length,
    wsStatus: {
      ticker: wsTicker?.readyState === 1 ? 'CONNECTED' : 'DISCONNECTED',
      ohlc1d: ws?.readyState       === 1 ? 'CONNECTED' : 'DISCONNECTED',
    },
    livePricesCount:    Object.keys(livePrices).length,
    recentSignalsCount: recentSignals.size,
    signalsCacheCount:  signalsCache.length,
    positionsInProgress: positionsInProgress.size,
    krakenPairs:         krakenPairsList.length,
  });
});

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// DÃ‰MARRAGE
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`\nðŸ¤– Bender Pro v10.0 Â· Port ${PORT}`);
  console.log(` Mode Classique + Mode IA Autonome`);
  console.log(` 35 Plateformes Â· SL -2% Â· TP dynamique\n`);

  setImmediate(async () => {
    // TP/SL instantanÃ© (les deux modes)
    setTimeout(() => checkTPSLInstant().catch(console.error), 100);
    setInterval(() => checkTPSLInstant().catch(console.error), 2000);
    console.log(' TP/SL actif (2s)');

    // XLM toutes les 24h
    // Watchdog actif toutes les 30s
    setTimeout(() => runWatchdog().catch(console.error), 10000);
    setInterval(() => runWatchdog().catch(console.error), 30000);
    // ── PING GLOBAL — toutes les 20s sur toutes les connexions
    setInterval(() => {
      const ping = JSON.stringify({ method: 'ping' });
      if (wsTicker && wsTicker.readyState === 1) wsTicker.send(ping);
      if (ws       && ws.readyState       === 1) ws.send(ping);
      if (ws1h     && ws1h.readyState     === 1) ws1h.send(ping);
      if (ws4h     && ws4h.readyState     === 1) ws4h.send(ping);

    }, 20000);

    // ── KEEPALIVE — reconnexion proactive toutes les 30s
    setInterval(()=>{
      if(!krakenPairsList.length) return;
      if(!wsTicker||wsTicker.readyState!==1) connectKrakenTicker(krakenPairsList);
      if(!ws||ws.readyState!==1)             connectKrakenWS(krakenPairsList);
      if(!ws1h||ws1h.readyState!==1)         connectKrakenWS1h(krakenPairsList);
      if(!ws4h||ws4h.readyState!==1)         connectKrakenWS4h(krakenPairsList);
    }, 30000);
    console.log(' Watchdog de surveillance actif');

    setTimeout(() => checkXlmPayments().catch(console.error), 5000);
    setInterval(() => checkXlmPayments().catch(console.error), 24*60*60*1000);

    // VÃ©rification fin de journÃ©e IA (toutes les 5 minutes)

    // Scan IA toutes les 15 minutes

    // Kraken WebSocket
    krakenPairsList = await fetchKrakenUsdtPairs().catch(() => []);
    if (krakenPairsList.length > 0) {
      connectKrakenTicker(krakenPairsList);
      connectKrakenWS(krakenPairsList);
      // WS 4h dÃ©sactivÃ© â€” bot trade uniquement en Daily
      // connectKrakenWS4h(krakenPairsList); // dÃ©sactivÃ©
      console.log(` ${krakenPairsList.length} paires Kraken Â· WebSocket actif`);
      preloadHistoricalCandles(krakenPairsList).then(() => {
        console.log(' Preloading terminÃ©');
        scanAll().catch(console.error);
                      }).catch(console.error);
    }

    // Scan classique toutes les 60s
    setTimeout(() => setInterval(() => scanAll().catch(console.error), SCAN_INTERVAL), 65000);
  });
});
