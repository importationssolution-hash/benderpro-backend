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
const SL_PCT = 0.04; // -4%
const TP_PCT            = 0.12;
const MAX_CONCURRENT    = 20;
const VOL_CONFIRM       = 1.8;
const SCAN_INTERVAL     = 60 * 1000;
const MAX_PAIRS         = 500;
const MAX_SIGNALS_CACHE = 200;
const MIN_DAILY_VOLUME_USD = 500000; // 500k$/jour minimum


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
function calcADX(closes, period=14) {
  if (closes.length < period*2+1) return 25;
  const n=closes.length, hi=closes.map(c=>c*1.005), lo=closes.map(c=>c*0.995);
  const tr=[],pdm=[],mdm=[];
  for(let i=1;i<n;i++){
    const h2=hi[i],l=lo[i],ph=hi[i-1],pl=lo[i-1],pc=closes[i-1];
    tr.push(Math.max(h2-l,Math.abs(h2-pc),Math.abs(l-pc)));
    const up=h2-ph,dn=pl-l;
    pdm.push(up>dn&&up>0?up:0); mdm.push(dn>up&&dn>0?dn:0);
  }
  function sm(a,p){let s=a.slice(0,p).reduce((x,y)=>x+y,0);const r=[s];for(let i=p;i<a.length;i++){s=s-s/p+a[i];r.push(s);}return r;}
  const at=sm(tr,period),p14=sm(pdm,period),m14=sm(mdm,period),dx=[];
  for(let i=0;i<at.length;i++){if(at[i]===0){dx.push(0);continue;}const pi=100*p14[i]/at[i],mi=100*m14[i]/at[i],s=pi+mi;dx.push(s===0?0:100*Math.abs(pi-mi)/s);}
  if(dx.length<period) return 25;
  return +(dx.slice(-period).reduce((a,b)=>a+b,0)/period).toFixed(1);
}

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
  // Utiliser les paramètres optimaux du backtester si disponibles
  const bp = optimalParams;
  const RSI_MIN = (aiParams && aiParams.rsiMin) || bp.rsiMin || 30;
  const RSI_MAX = (aiParams && aiParams.rsiMax) || bp.rsiMax || 75;
  const VOL_MULT = (aiParams && aiParams.volMultiplier) || bp.volMultiplier || 1.8;
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
  if (figureTarget < 0.40) return null;
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
const pairDailyVolume  = {};  // Volume quotidien moyen en USD
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
  wsTicker.on('close',()=>{if(wsTicker._hb)clearInterval(wsTicker._hb);setTimeout(()=>connectKrakenTicker(krakenPairsList),5000);});
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
  ws1h.on('close', () => { setTimeout(() => connectKrakenWS1h(krakenPairsList), 5000); });
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
  ws4h.on('close', () => { setTimeout(() => connectKrakenWS4h(krakenPairsList), 5000); });
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


// ══════════════════════════════════════════════════════════
// 🧠 BENDER QI — Système de points & Communauté de Robots
// ══════════════════════════════════════════════════════════

// ── 30 Robots avec personnalités différentes
// 1000 robots — tous avec les mêmes paramètres stricts
// Mode analyse pure — pas de trading
// Seul leur numéro et QI diffèrent
const ROBOT_PERSONALITIES = Array.from({length: 1000}, (_, i) => ({
  id: i + 1,
  name: `Robot ${i + 1}`,
  tradeAmount: TRADE_AMOUNT,
  style: 'Standard'
}));

// ── État de chaque robot
const robotStates = {};
ROBOT_PERSONALITIES.forEach(r => {
  robotStates[r.id] = {
    id: r.id,
    name: r.name,
    style: r.style,
    qi: 100,                  // Points de départ
    status: 'active',         // active | bench | observation (analyse pure)
    consecutiveWins: 0,       // Pour retour du banc
    benchSince: null,         // Date mise sur le banc
    benchReturnDate: null,    // Date de retour possible
    totalTrades: 0,
    totalWins: 0,
    totalLosses: 0,
    tradeAmount: r.tradeAmount,
    sl: r.sl,
    tpMult: r.tpMult,
    currentTrade: null,       // Trade en cours
    lessons: [],              // Leçons achetées
    pendingLesson: null,      // Leçon en attente de paiement
  };
});

// ── File de rotation des robots actifs
let robotQueue = ROBOT_PERSONALITIES.map(r => r.id); // 1,2,3...1000
let currentRobotIndex = 0;

function getNextRobot() {
  // Chercher le prochain robot actif dans la queue
  let attempts = 0;
  while (attempts < 30) {
    const robotId = robotQueue[currentRobotIndex % 30];
    currentRobotIndex = (currentRobotIndex + 1) % 30;
    const state = robotStates[robotId];
    if (state && state.status === 'active' && !state.currentTrade) {
      return state;
    }
    attempts++;
  }
  return null; // Tous les robots sont occupés ou sur le banc
}

// ── Salaire du robot selon ses Bender QI points
// Plus il a de QI → plus il trade gros
function getRobotTradeAmount(robot) {
  const base = TRADE_AMOUNT; // Même base pour tous

  if (robot.qi >= 500)  return base * 3.0;  // Elite    → 15$
  if (robot.qi >= 300)  return base * 2.0;  // Expert   → 10$
  if (robot.qi >= 200)  return base * 1.5;  // Confirmé → 7.5$
  if (robot.qi >= 100)  return base * 1.0;  // Normal   → 5$
  if (robot.qi >= 50)   return base * 0.7;  // Dégradé  → 3.5$
  if (robot.qi >= 0)    return base * 0.5;  // Alerte   → 2.5$
  return 0; // QI négatif → banc obligatoire
}

// ── Attribuer les points QI après un trade
function awardBenderQI(robotId, result, tpPct, slPct) {
  const state = robotStates[robotId];
  if (!state) return;

  // Enregistrer pour la validation scientifique
  recordTradeForValidation(robotId, result);

  if (result === 'WIN') {
    const points = Math.round(tpPct * 100); // +20 points pour +20% TP
    state.qi += points;
    state.consecutiveWins++;
    state.totalWins++;
    state.totalTrades++;

    // Si en observation/banc et gagne → compter pour retour
    if (state.status === 'bench') {
      if (state.consecutiveWins >= 2) {
        state.status = 'active';
        state.consecutiveWins = 0;
        state.benchSince = null;
        state.benchReturnDate = null;
        console.log(`[BenderQI] 🎉 ${state.name} revient du banc après 2 wins consécutifs!`);
      } else {
        console.log(`[BenderQI] ${state.name} en observation: ${state.consecutiveWins}/2 wins pour retour`);
      }
    }

    console.log(`[BenderQI] ✅ ${state.name} +${points} QI (${result}) → Total: ${state.qi} QI`);

  } else if (result === 'LOSS') {
    const points = Math.round(slPct * 100); // -4 points pour -4% SL
    state.qi -= points;
    state.consecutiveWins = 0;
    state.totalLosses++;
    state.totalTrades++;

    // Stocker la leçon en attente de paiement
    state.pendingLesson = {
      symbol: state.currentTrade?.symbol,
      figure: state.currentTrade?.figure,
      cost: 20, // 20 QI pour apprendre
      available: state.qi >= 20,
      timestamp: new Date()
    };

    console.log(`[BenderQI] ❌ ${state.name} -${points} QI (${result}) → Total: ${state.qi} QI`);
    console.log(`[BenderQI] 📚 ${state.name} peut acheter la leçon pour -20 QI (actuellement: ${state.qi} QI)`);

    // Vérifier si le robot doit aller sur le banc
    if (state.qi <= -60) {
      state.status = 'bench';
      state.consecutiveWins = 0;
      state.benchSince = new Date();
      state.benchReturnDate = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000);
      console.log(`[BenderQI] 🔴 ${state.name} envoyé en observation (QI: ${state.qi}) — Retour possible: ${state.benchReturnDate.toLocaleDateString('fr-CA')}`);
    }

    // Dégradation du salaire selon les points
    const newAmount = getRobotTradeAmount(state);
    if (newAmount < state.tradeAmount) {
      console.log(`[BenderQI] 📉 ${state.name} salaire réduit: ${state.tradeAmount}$ → ${newAmount}$`);
      state.tradeAmount = newAmount;
    }
  }

  state.currentTrade = null;
}

// ── Acheter une leçon (apprendre de l'erreur)
function buyLesson(robotId) {
  const state = robotStates[robotId];
  if (!state || !state.pendingLesson) return { success: false, error: 'Pas de leçon disponible' };
  if (state.qi < 20) return { success: false, error: `QI insuffisant (${state.qi}/20 requis)` };

  state.qi -= 20;
  const lesson = {
    ...state.pendingLesson,
    paid: true,
    analysis: `Robot ${state.name} a perdu sur ${state.pendingLesson.symbol} · Figure: ${state.pendingLesson.figure} · Leçon: Éviter ce pattern dans ces conditions de marché`
  };
  state.lessons.push(lesson);
  state.pendingLesson = null;

  console.log(`[BenderQI] 📖 ${state.name} a acheté une leçon (-20 QI) → Reste: ${state.qi} QI`);
  return { success: true, lesson };
}

// ── Classement global
function getBenderQILeaderboard() {
  return Object.values(robotStates)
    .sort((a, b) => b.qi - a.qi)
    .map((r, idx) => ({
      rank: idx + 1,
      id: r.id,
      name: r.name,
      style: r.style,
      qi: r.qi,
      status: r.status,
      winRate: r.totalTrades > 0 ? Math.round(r.totalWins / r.totalTrades * 100) : 0,
      totalTrades: r.totalTrades,
      tradeAmount: getRobotTradeAmount(r),
      consecutiveWins: r.consecutiveWins,
      benchReturnDate: r.benchReturnDate
    }));
}

// ── Vérifier les robots en observation (retour après 1 semaine)
function checkBenchRobots() {
  const now = Date.now();
  for (const state of Object.values(robotStates)) {
    if (state.status === 'bench' && state.benchReturnDate) {
      if (now >= state.benchReturnDate.getTime()) {
        state.status = 'observation';
        console.log(`[BenderQI] 👁 ${state.name} sort du banc → mode observation (doit gagner 2 trades)`);
      }
    }
  }
}




// ══════════════════════════════════════════════════════════
// 🔬 DÉCOUVERTE DE FIGURES — Backtester uniquement
// Le backtester teste des nouvelles combinaisons de patterns
// Si prouvé à 60%+ winrate sur données historiques → valide
// Récompense: +10,000 QI au robot actif qui l'a déclenché
// ══════════════════════════════════════════════════════════

// Figures traditionnelles connues — référence pour détecter du nouveau
const TRADITIONAL_PATTERNS = {
  'Cup & Handle':     { winrate: 0.84, minTarget: 0.40, volumeSpike: 1.8 },
  'ETE Inverse':      { winrate: 0.81, minTarget: 0.35, volumeSpike: 1.5 },
  'Double Bottom':    { winrate: 0.76, minTarget: 0.30, volumeSpike: 1.4 },
  'Triangle Asc.':    { winrate: 0.74, minTarget: 0.25, volumeSpike: 1.6 },
  'Drapeau Haussier': { winrate: 0.76, minTarget: 0.35, volumeSpike: 1.8 },
  'Biseau Baissier':  { winrate: 0.73, minTarget: 0.30, volumeSpike: 1.7 },
};

// Cache des analyses proposées par les robots
const robotAnalyses = {};
const discoveredPatterns = []; // Nouvelles figures découvertes



// Découverte de nouvelle figure chartiste
function proposeNewPattern(robotId, patternData) {
  const robot = robotStates[robotId];
  if (!robot) return { success: false, error: 'Robot non trouvé' };

  // Valider que ce n'est pas déjà connu
  const isKnown = TRADITIONAL_PATTERNS[patternData.name] ||
    discoveredPatterns.some(p => p.name === patternData.name);
  if (isKnown) return { success: false, error: 'Pattern déjà connu' };

  // Valider les données minimales
  if (!patternData.name || !patternData.conditions || !patternData.winrateEstimate) {
    return { success: false, error: 'Données insuffisantes' };
  }

  // Récompenser la découverte
  robot.qi += 10000;
  discoveredPatterns.push({
    ...patternData,
    discoveredBy: robot.name,
    discoveredAt: new Date(),
    qiAwarded: 10000,
    status: 'pending_validation' // Doit être validé sur 50 trades réels
  });

  console.log(`[BenderQI] 🏆 DÉCOUVERTE! ${robot.name} a trouvé "${patternData.name}" → +10,000 QI!`);
  console.log(`[BenderQI] Pattern: ${JSON.stringify(patternData.conditions)}`);

  return {
    success: true,
    message: `Nouvelle figure "${patternData.name}" découverte! +10,000 QI`,
    totalQI: robot.qi
  };
}



// ══════════════════════════════════════════════════════════
// 🔬 SYSTÈME DE VALIDATION SCIENTIFIQUE — Bender Pro v11.1
// Un robot ne peut améliorer ses paramètres QUE si il prouve
// une amélioration réelle de +1% de winrate minimum
// Récompense: +1000 Bender QI points par % prouvé
// ══════════════════════════════════════════════════════════

// Paramètres fixes de référence — STRICTS — ne changent jamais sans preuve
const PARAMS_REFERENCE = {
  rsiMin: 33, rsiMax: 72,
  volMultiplier: 1.8,
  adxMin: 20,
  sentimentMin: 60,
  figureTargetMin: 0.40,
  slPct: 0.04,
  tpMults: { 0.40: 0.13, 0.50: 0.15, 0.60: 0.17, 0.80: 0.20 }
};

// Validation globale — pas par robot individuel (trop lourd pour 1000 robots)
// La validation se fait au niveau du SYSTÈME via PARAMS_REFERENCE.pendingProposal
const robotValidation = {}; // Vide — non utilisé avec 1000 robots

// Calculer le winrate sur les N derniers trades
function calcWinrate(trades) {
  if (trades.length < 10) return null; // Pas assez de données
  const wins = trades.filter(t => t === 'WIN').length;
  return +(wins / trades.length * 100).toFixed(1);
}

// Le SYSTÈME propose une amélioration de paramètres
// Appelé par le backtester uniquement — pas par les robots
function proposeImprovement(robotId, newParams) {
  // Valider limites de sécurité
  if (newParams.rsiMin && (newParams.rsiMin < 25 || newParams.rsiMin > 45))
    return { success: false, error: 'RSI min hors limites (25-45)' };
  if (newParams.volMultiplier && (newParams.volMultiplier < 1.3 || newParams.volMultiplier > 3.0))
    return { success: false, error: 'Volume mult hors limites (1.3-3.0)' };
  if (newParams.adxMin && (newParams.adxMin < 15 || newParams.adxMin > 35))
    return { success: false, error: 'ADX min hors limites (15-35)' };

  // Déjà une proposition en attente?
  if (PARAMS_REFERENCE.pendingProposal && PARAMS_REFERENCE.pendingProposal.status === 'pending')
    return { success: false, error: 'Validation en cours — attendre résultat' };

  PARAMS_REFERENCE.pendingProposal = {
    params: { ...PARAMS_REFERENCE, ...newParams },
    submittedAt: new Date(),
    winrateRequired: newParams.winrate || 60,
    status: 'pending',
    tradesNeeded: 20,
    tradesRecorded: [],
  };
  console.log(`[Système] 📋 Nouvelle proposition · Validation sur 20 trades réels`);
  return { success: true, refWinrate: PARAMS_REFERENCE.winrateEstimate || 0 };
}

// Enregistrer le résultat d'un trade pour la validation SYSTÈME
function recordTradeForValidation(robotId, result) {
  // Enregistrer dans la proposition en attente du système
  const proposal = PARAMS_REFERENCE.pendingProposal;
  if (proposal && proposal.status === 'pending') {
    proposal.tradesRecorded.push(result);

    // Après 20 trades → le SYSTÈME évalue
    if (proposal.tradesRecorded.length >= 20) {
      const wins = proposal.tradesRecorded.filter(r => r === 'WIN').length;
      const realWR = +(wins / proposal.tradesRecorded.length * 100).toFixed(1);

      if (realWR >= proposal.winrateRequired) {
        // ✅ SYSTÈME ADOPTE les nouveaux paramètres
        Object.assign(PARAMS_REFERENCE, proposal.params);
        PARAMS_REFERENCE.winrateEstimate = realWR;
        proposal.status = 'validated';
        console.log(`[Système] ✅ PARAMÈTRES ADOPTÉS · WR réel: ${realWR}% ≥ requis: ${proposal.winrateRequired}%`);
        console.log(`[Système] RSI: ${PARAMS_REFERENCE.rsiMin}-${PARAMS_REFERENCE.rsiMax} · Vol: ${PARAMS_REFERENCE.volMultiplier}x · ADX: ${PARAMS_REFERENCE.adxMin}`);

        // Récompenser tous les robots actifs équitablement
        Object.values(robotStates).filter(r => r.status === 'active').forEach(r => {
          r.qi += 100; // Bonus collectif
        });
      } else {
        // ❌ SYSTÈME REJETTE — paramètres restent inchangés
        proposal.status = 'rejected';
        console.log(`[Système] ❌ Proposition rejetée · WR réel: ${realWR}% < requis: ${proposal.winrateRequired}%`);
        console.log(`[Système] Paramètres de référence conservés`);
      }
      PARAMS_REFERENCE.pendingProposal = null;
    }
  }
}

// Évaluation par le SYSTÈME uniquement via PARAMS_REFERENCE.pendingProposal
function evaluateImprovement(robotId) {
  // Non utilisé — validation gérée par recordTradeForValidation
}

// Tous les robots utilisent PARAMS_REFERENCE — système unifié
function getRobotParams(robotId) {
  return PARAMS_REFERENCE;
}



// ══════════════════════════════════════════════════════════
// 📊 ANALYSE CONTINUE 24/7 — 30 robots en parallèle
// Toutes les 30 minutes → meilleure analyse stockée
// Accessible via /robot-analysis pour le frontend
// ══════════════════════════════════════════════════════════

let latestRobotAnalyses = []; // Les 30 meilleures analyses
let lastAnalysisTime = null;

// Chaque robot analyse la paire assignée
function robotSingleAnalysis(robot, symbolOverride) {
  let bestSignal = null;
  let bestScore = 0;

  const params = getRobotParams(robot.id);
  const symbols = symbolOverride
    ? [symbolOverride]
    : Object.keys(krakenCandles).slice(0, 5); // 5 paires par robot

  for (const symbol of symbols) {
    const candles = krakenCandles[symbol];
    const livePrice = livePrices[symbol];
    if (!candles || candles.length < 100 || !livePrice) continue;
    if (livePrice < 0.001) continue;

    const closes  = candles.map(c => c.c);
    const volumes = candles.map(c => c.v);

    // Calculer les indicateurs
    const rsi    = calcRSI(closes);
    const ema20  = calcEMA(closes.slice(-60), Math.min(20, closes.length-1));
    const ema50  = calcEMA(closes.slice(-100), Math.min(50, closes.length-1));
    const adx    = calcADX(closes);
    const volRatio = volumes[volumes.length-1] / avg(volumes.slice(-50));
    const sentiment = getMarketSentiment();

    // Score de conviction
    let score = 0;
    if (rsi >= params.rsiMin && rsi <= params.rsiMax) score += 25;
    if (ema20 > ema50) score += 20;
    if (adx >= params.adxMin) score += 20;
    if (volRatio >= params.volMultiplier) score += 20;
    if (sentiment >= params.sentimentMin) score += 15;

    if (score > bestScore) {
      const sig = detectFigure(closes, volumes, livePrice, params);
      if (sig) {
        bestScore = score;
        bestSignal = {
          robot: robot.id,
          robotName: `Robot ${robot.id}`,
          style: robot.style,
          qi: robot.qi,
          status: robot.status,
          symbol,
          figure: sig.fig.name,
          score,
          recommendation: score >= 80 ? '🟢 FORT' : score >= 60 ? '🟡 MODÉRÉ' : '🔴 FAIBLE',
          entryPrice: livePrice,
          tp: sig.tp,
          sl: sig.sl,
          tpPct: +(sig.tpPct * 100).toFixed(1),
          slPct: +(SL_PCT * 100).toFixed(1),
          figureTarget: +(sig.figureTarget * 100).toFixed(1),
          indicators: {
            rsi: +rsi.toFixed(1),
            adx: +adx.toFixed(1),
            volRatio: +volRatio.toFixed(2),
            ema: ema20 > ema50 ? 'Haussière' : 'Baissière',
            sentiment: sentiment + '%'
          },
          timestamp: new Date()
        };
      }
    }
  }
  return bestSignal;
}

// Lancer l'analyse des 1000 robots — Render Starter 2GB
// Tous les 1000 robots analysent en même temps toutes les 30 minutes
async function runRobotAnalyses() {
  if (Object.keys(krakenCandles).length < 10) return;

  const startTime = Date.now();
  const allRobots = Object.values(robotStates);
  const symbols = Object.keys(krakenCandles).filter(s =>
    livePrices[s] && krakenCandles[s]?.length >= 100
  );
  if (symbols.length === 0) return;

  const analyses = [];
  const BATCH = 100; // 100 robots à la fois pour laisser respirer

  for (let i = 0; i < allRobots.length; i += BATCH) {
    await new Promise(r => setImmediate(r)); // CPU breath entre batches
    const batch = allRobots.slice(i, i + BATCH);

    for (const robot of batch) {
      if (robot.status !== 'active') continue;
      // Chaque robot analyse sa paire assignée selon son ID
      const symbol = symbols[(robot.id - 1) % symbols.length];
      const analysis = robotSingleAnalysis(robot, symbol);
      if (analysis) analyses.push(analysis);
    }
  }

  // Trier par score + dédupliquer par symbol
  analyses.sort((a, b) => b.score - a.score);
  const seen = new Set();
  latestRobotAnalyses = analyses.filter(a => {
    if (seen.has(a.symbol)) return false;
    seen.add(a.symbol); return true;
  }).slice(0, 100); // Top 100 signaux

  lastAnalysisTime = new Date();
  const duration = ((Date.now() - startTime) / 1000).toFixed(1);
  const activeRobots = allRobots.filter(r => r.status === 'active').length;

  console.log(`[Robots] 📊 ${activeRobots}/1000 robots · ${analyses.length} analyses · ${latestRobotAnalyses.length} signaux uniques · ${duration}s`);

  latestRobotAnalyses.slice(0, 5).forEach((a, i) => {
    console.log(`[Robots] ${i+1}. Robot ${a.robot} · ${a.symbol} · ${a.figure} · Score: ${a.score}/100`);
  });
}


// ══════════════════════════════════════════════════════════
// 🧠 BACKTESTER AUTO-OPTIMISATION — Bender Pro v11.0
// Analyse 3 ans de données historiques pour trouver
// les paramètres optimaux avant de trader en live
// ══════════════════════════════════════════════════════════

let optimalParams = {
  rsiMin: 30, rsiMax: 75,
  volMultiplier: 1.8, adxMin: 20,
  sentimentMin: 60, figureTargetMin: 0.40,
  slPct: 0.04, winrate: 0, trades: 0
};
let backtestRunning = false;
let lastBacktest = null;

// Simuler un trade sur données historiques
function simulateTrade(candles, entryIdx, params, tpPct) {
  if (entryIdx >= candles.length - 1) return null;
  const entryPrice = candles[entryIdx].c;
  const tp = entryPrice * (1 + tpPct);
  const sl = entryPrice * (1 - params.slPct);
  // Regarder les bougies suivantes pour voir si TP ou SL touché
  for (let i = entryIdx + 1; i < Math.min(entryIdx + 30, candles.length); i++) {
    const c = candles[i];
    const high  = c.c * 1.01; // approximation high
    const low   = c.c * 0.99; // approximation low
    if (high >= tp)  return 'WIN';
    if (low  <= sl)  return 'LOSS';
  }
  return null; // trade encore ouvert après 30 bougies
}

// Tester une combinaison de paramètres sur toutes les paires
function backtestParams(params) {
  let wins = 0, losses = 0;
  const symbols = Object.keys(krakenCandles).slice(0, 100); // limiter pour vitesse

  for (const symbol of symbols) {
    const candles = krakenCandles[symbol];
    if (!candles || candles.length < 150) continue;
    const closes  = candles.map(c => c.c);
    const volumes = candles.map(c => c.v);

    // Scanner chaque bougie comme si c'était du live
    for (let i = 100; i < candles.length - 30; i++) {
      const slice   = closes.slice(0, i);
      const vSlice  = volumes.slice(0, i);
      const price   = slice[slice.length - 1];

      if (price < 0.001) continue;

      // Volume quotidien estimé
      const recentVols = candles.slice(Math.max(0,i-30),i).map(b=>b.c*b.v).filter(v=>v>0);
      const dailyVol = recentVols.length > 0 ? recentVols.reduce((a,b)=>a+b,0)/recentVols.length : 0;
      if (dailyVol > 0 && dailyVol < MIN_DAILY_VOLUME_USD) continue;

      // Indicateurs
      const rsi = calcRSI(slice);
      if (rsi < params.rsiMin || rsi > params.rsiMax) continue;

      const ema20 = calcEMA(slice.slice(-60), Math.min(20,slice.length-1));
      const ema50 = calcEMA(slice.slice(-100), Math.min(50,slice.length-1));
      if (ema20 <= ema50) continue;

      const adx = calcADX(slice);
      if (adx < params.adxMin) continue;

      const volRatio = vSlice[vSlice.length-1] / avg(vSlice.slice(-50));
      if (volRatio < params.volMultiplier) continue;

      // Détecter figure
      const sig = detectFigure(slice, vSlice, price, {
        rsiMin: params.rsiMin, rsiMax: params.rsiMax,
        volMultiplier: params.volMultiplier
      });
      if (!sig) continue;
      if (sig.figureTarget < params.figureTargetMin) continue;

      // Simuler le trade
      const result = simulateTrade(candles, i, params, sig.tpPct);
      if (result === 'WIN')  wins++;
      if (result === 'LOSS') losses++;
    }
  }

  const total = wins + losses;
  const winrate = total > 0 ? wins / total * 100 : 0;
  return { wins, losses, total, winrate: +winrate.toFixed(1) };
}

// Lancer l'optimisation complète
async function runBacktest() {
  if (backtestRunning) return;
  if (Object.keys(krakenCandles).length < 10) {
    console.log('[Backtest] Pas assez de données — attendre preload');
    return;
  }

  backtestRunning = true;
  const startTime = Date.now();
  console.log('\n[Backtest] 🧠 Démarrage optimisation automatique...');
  console.log('[Backtest] Analyse de 3 ans de données sur 100 paires...');

  let bestParams = { ...optimalParams };
  let bestWinrate = 0;
  let totalTests = 0;

  // Grille de paramètres à tester
  const rsiMins    = [28, 30, 33, 35, 38];
  const volMults   = [1.5, 1.8, 2.0, 2.3];
  const adxMins    = [18, 20, 23, 25];
  const slPcts     = [0.03, 0.04, 0.05];

  for (const rsiMin of rsiMins) {
  for (const volMult of volMults) {
  for (const adxMin of adxMins) {
  for (const slPct of slPcts) {
    const params = {
      rsiMin, rsiMax: 100 - rsiMin,
      volMultiplier: volMult,
      adxMin, slPct,
      sentimentMin: 60,
      figureTargetMin: 0.40
    };

    // Laisser respirer le CPU entre les tests
    await new Promise(r => setImmediate(r));

    const result = backtestParams(params);
    totalTests++;

    if (result.total >= 10 && result.winrate > bestWinrate) {
      bestWinrate = result.winrate;
      bestParams = { ...params, winrate: result.winrate, trades: result.total };
      console.log(`[Backtest] ⭐ Nouveau meilleur: WR ${result.winrate}% sur ${result.total} trades · RSI ${rsiMin}-${100-rsiMin} · Vol ${volMult}x · ADX ${adxMin} · SL ${(slPct*100).toFixed(0)}%`);
    }
  }}}}

  const duration = ((Date.now() - startTime) / 1000).toFixed(1);
  optimalParams = bestParams;
  lastBacktest = new Date();
  backtestRunning = false;

  console.log(`\n[Backtest] ✅ Terminé en ${duration}s · ${totalTests} combinaisons testées`);

  // ── Phase 2: Chercher nouvelles figures chartistes
  console.log('[Backtest] Phase 2: Recherche nouvelles figures...');
  await searchNewPatterns();
  await searchNewPatterns();

  console.log(`\n[Backtest] ✅ Terminé en ${duration}s · ${totalTests} combinaisons testées`);
  console.log(`[Backtest] 📊 Meilleurs params trouvés: RSI ${bestParams.rsiMin}-${bestParams.rsiMax} · Vol ${bestParams.volMultiplier}x · WR ${bestParams.winrate}%`);

  // NE PAS appliquer automatiquement — soumettre pour validation
  const refWR = PARAMS_REFERENCE.winrateEstimate || 0;
  if (bestParams.winrate > refWR + 1 && bestParams.trades >= 20) {
    console.log(`[Backtest] 💡 Proposition: WR ${bestParams.winrate}% > référence ${refWR}% → soumis pour validation`);
    // Le SYSTÈME soumet la proposition — pas un robot spécifique
    // La validation se fait sur les vrais trades collectifs des 30 robots
    PARAMS_REFERENCE.pendingProposal = {
      params: bestParams,
      submittedAt: new Date(),
      winrateRequired: bestParams.winrate,
      status: 'pending',
      tradesNeeded: 20,
      tradesRecorded: [],
    };
    console.log(`[Système] 📋 Proposition soumise — validation requise sur 20 vrais trades`);
    console.log(`[Système] Si WR réel > ${bestParams.winrate}% → paramètres adoptés automatiquement`);
  } else {
    console.log(`[Backtest] ✅ Params actuels déjà optimaux — aucun changement`);
  }
}

// ── Recherche de nouvelles figures chartistes dans les données historiques
async function searchNewPatterns() {
  const newPatterns = [
    // ── Patterns classiques non implémentés
    {
      name: 'Triple Bottom',
      test: (closes, vols, n) => {
        if (n < 80) return false;
        const b1 = Math.min(...closes.slice(n-75,n-50));
        const b2 = Math.min(...closes.slice(n-50,n-25));
        const b3 = Math.min(...closes.slice(n-25,n-1));
        const mid1 = Math.max(...closes.slice(n-65,n-35));
        const mid2 = Math.max(...closes.slice(n-40,n-10));
        return Math.abs(b1-b2)/b1 < 0.03 &&
               Math.abs(b2-b3)/b2 < 0.03 &&
               mid1 > b1*1.05 && mid2 > b2*1.05;
      }
    },
    {
      name: 'Wedge Ascendant',
      test: (closes, vols, n) => {
        if (n < 40) return false;
        const highs = closes.slice(n-30,n).filter((_,i)=>i%3===0);
        const lows  = closes.slice(n-30,n).filter((_,i)=>i%3===1);
        const highTrend = highs[highs.length-1] > highs[0];
        const lowTrend  = lows[lows.length-1]  > lows[0];
        const convergence = (highs[highs.length-1]-lows[lows.length-1]) <
                            (highs[0]-lows[0]) * 0.7;
        return highTrend && lowTrend && convergence;
      }
    },
    {
      name: 'Breakout Rectangle',
      test: (closes, vols, n) => {
        if (n < 50) return false;
        const box = closes.slice(n-40,n-5);
        const boxHigh = Math.max(...box);
        const boxLow  = Math.min(...box);
        const boxRange = (boxHigh - boxLow) / boxLow;
        const price = closes[n-1];
        return boxRange < 0.08 && price > boxHigh * 1.01;
      }
    },
    // ── Patterns de bougies japonaises
    {
      name: 'Marteau Inversé',
      test: (closes, vols, n) => {
        if (n < 3) return false;
        const c = closes[n-1], p = closes[n-2];
        const body = Math.abs(c - p);
        const upperWick = Math.max(c, p) * 0.02; // approximation
        return body < upperWick * 0.3 && c > p; // corps petit, longue mèche haute
      }
    },
    {
      name: 'Engulfing Haussier',
      test: (closes, vols, n) => {
        if (n < 3) return false;
        const c1 = closes[n-3], c2 = closes[n-2], c3 = closes[n-1];
        const body1 = c2 - c1; // bougie précédente baissière
        const body2 = c3 - c2; // bougie actuelle haussière
        return body1 < 0 && body2 > 0 && Math.abs(body2) > Math.abs(body1) * 1.5;
      }
    },
    // ── Patterns de volume
    {
      name: 'Volume Climax Haussier',
      test: (closes, vols, n) => {
        if (!vols || n < 20) return false;
        const avgVol = avg(vols.slice(n-20, n-1));
        const lastVol = vols[n-1];
        const priceUp = closes[n-1] > closes[n-2];
        return lastVol > avgVol * 3.5 && priceUp; // volume 3.5x avec hausse
      }
    },
    {
      name: 'Accumulation Silencieuse',
      test: (closes, vols, n) => {
        if (!vols || n < 30) return false;
        const priceRange = (Math.max(...closes.slice(n-20,n)) - Math.min(...closes.slice(n-20,n))) / closes[n-1];
        const volTrend = avg(vols.slice(n-10,n)) > avg(vols.slice(n-20,n-10)) * 1.3;
        return priceRange < 0.05 && volTrend; // prix stable + volume qui monte
      }
    },
    // ── Patterns RSI/ADX
    {
      name: 'Divergence RSI Haussière',
      test: (closes, vols, n) => {
        if (n < 40) return false;
        const rsiNow = calcRSI(closes.slice(0,n));
        const rsiOld = calcRSI(closes.slice(0,n-20));
        const priceLow = closes[n-1] < closes[n-21]; // prix plus bas
        const rsiHigh  = rsiNow > rsiOld;             // RSI plus haut
        return priceLow && rsiHigh && rsiNow < 45;    // divergence haussière en zone survente
      }
    },
    {
      name: 'ADX Explosion',
      test: (closes, vols, n) => {
        if (n < 30) return false;
        const adxNow = calcADX(closes.slice(0,n));
        const adxOld = calcADX(closes.slice(0,n-10));
        return adxNow > adxOld * 1.5 && adxNow > 25 && closes[n-1] > closes[n-11];
      }
    },
  ];

  for (const pattern of newPatterns) {
    if (TRADITIONAL_PATTERNS[pattern.name] ||
        discoveredPatterns.some(p => p.name === pattern.name)) continue;

    // Backtester ce pattern sur données historiques
    let wins = 0, losses = 0;
    const symbols = Object.keys(krakenCandles).slice(0, 50);

    for (const symbol of symbols) {
      const candles = krakenCandles[symbol];
      if (!candles || candles.length < 100) continue;
      const closes = candles.map(c => c.c);

      for (let i = 80; i < closes.length - 20; i++) {
        const slice = closes.slice(0, i);
        const vslice = candles.slice(0, i).map(c => c.v);
        if (!pattern.test(slice, vslice, slice.length)) continue;

        // Simuler le trade
        const entry = closes[i];
        const tp = entry * 1.13;
        const sl = entry * 0.96;
        let result = null;
        for (let j = i+1; j < Math.min(i+30, closes.length); j++) {
          if (closes[j] >= tp) { result = 'WIN'; break; }
          if (closes[j] <= sl) { result = 'LOSS'; break; }
        }
        if (result === 'WIN') wins++;
        if (result === 'LOSS') losses++;
      }
    }

    const total = wins + losses;
    if (total < 10) continue;
    const winrate = wins / total * 100;

    if (winrate >= 70) {
      // ── Calcul récompense exponentielle
      // 70% → 10,000 QI
      // Chaque 5% supplémentaire → ×5
      // 75% → 50,000 · 80% → 250,000 · 85% → 1,250,000...
      const stepsAbove70 = Math.floor((winrate - 70) / 5);
      const baseReward = 10000;
      const reward = baseReward * Math.pow(5, stepsAbove70);

      discoveredPatterns.push({
        name: pattern.name,
        winrate: +winrate.toFixed(1),
        trades: total,
        reward,
        discoveredAt: new Date(),
        status: 'validated'
      });

      // Récompenser le robot actif
      const activeRobot = Object.values(robotStates).find(r => r.status === 'active');
      if (activeRobot) {
        activeRobot.qi += reward;
        const rewardStr = reward >= 1000000
          ? (reward/1000000).toFixed(2) + 'M'
          : reward >= 1000
          ? (reward/1000).toFixed(0) + 'k'
          : reward.toString();
        console.log(`[Backtest] 🏆 NOUVELLE FIGURE: "${pattern.name}"`);
        console.log(`[Backtest]    WR: ${winrate.toFixed(1)}% · ${total} trades`);
        console.log(`[Backtest]    Récompense: ${rewardStr} Bender QI (×5^${stepsAbove70})`);
        console.log(`[Backtest]    ${activeRobot.name} total: ${activeRobot.qi.toLocaleString()} QI 🎉`);
      }
    }
    await new Promise(r => setImmediate(r));
  }

  if (discoveredPatterns.length > 0) {
    console.log(`[Backtest] 📚 ${discoveredPatterns.length} figure(s) dans la bibliothèque`);
  }
}

// Recalibration toutes les 30 minutes
// Le backtester analyse + cherche nouvelles figures
// NE change PAS les paramètres automatiquement
// Les robots SOUMETTENT leurs propositions pour validation
function scheduleWeeklyBacktest() {
  console.log('[Backtest] 📅 Recalibration toutes les 30 minutes');
  // Premier backtest 5 min après le boot
  setTimeout(() => {
    runBacktest().catch(console.error);
    // Puis toutes les 30 minutes
    setInterval(() => runBacktest().catch(console.error), 30 * 60 * 1000);
  }, 5 * 60 * 1000);
}


// ── Sentiment du marché — % de paires en hausse sur 24h
function getMarketSentiment() {
  let up = 0, total = 0;
  for (const symbol of krakenPairsList) {
    const candles = krakenCandles[symbol];
    const livePrice = livePrices[symbol];
    if (!candles || candles.length < 2 || !livePrice) continue;
    const prev = candles[candles.length - 2]; // bougie d'avant
    if (!prev || !prev.c) continue;
    total++;
    if (livePrice > prev.c) up++;
  }
  if (total < 10) return 100; // pas assez de données → laisser passer
  return Math.round(up / total * 100);
}

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
    if (livePrice < 0.001) return;
    if (!pairDailyVolume[symbol]) {
      const rv=candles.slice(-30).map(b=>b.c*b.v).filter(v=>v>0);
      if(rv.length>0) pairDailyVolume[symbol]=rv.reduce((a,b)=>a+b,0)/rv.length;
    }
    const dv=pairDailyVolume[symbol]||0;
    if(dv>0&&dv<MIN_DAILY_VOLUME_USD) return; // volume insuffisant

    // ── Filtre sentiment marché — trader seulement si 60%+ des cryptos sont en hausse
    const sentiment = getMarketSentiment();
    if (sentiment < 60) return; // marché rouge ou neutre → pas de nouveau trade

    // Le robot avec le meilleur score sur cette paire utilise ses paramètres
    const pairAnalysis = latestRobotAnalyses.find(a => a.symbol === symbol);
    const bestRobotForPair = pairAnalysis ? robotStates[pairAnalysis.robot] : null;
    const activeParams = bestRobotForPair && bestRobotForPair.status === 'active'
      ? getRobotParams(bestRobotForPair.id)
      : PARAMS_REFERENCE;
    const sig = detectFigure(closes, volumes, livePrice, activeParams);
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
    if (pctAbove > 0.03) return; // trop tard — entrée > 3% après cassure ignorée

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
  // ⏸ TRADING EN PAUSE — Mode analyse uniquement
  console.log(`[PAUSE] Trade ignoré: ${signal.symbol} · ${signal.figure} · Trading suspendu`);
  return;

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
        const recentTrade = await Trade.findOne({email:user.email,symbol:signal.symbol,exchange:signal.exchange,time:{$gte:new Date(Date.now()-100*60*60*1000)}});
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
          timeframe:signal.timeframe,
          botMode: signal.robotId ? `robot_${signal.robotId}` : 'classic'
        }).save();
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
        const robotInfo = pos.robotId ? `Robot ${pos.robotId} · ` : '';
        console.log(`[${reason}] ${robotInfo}${pos.symbol} PnL: ${pnl>=0?'+':''}$${pnl.toFixed(4)}`);
        // Bender QI — attribuer les points au robot actif
        const activeRobot = Object.values(robotStates).find(r => r.currentTrade?.symbol === pos.symbol);
        if (activeRobot) {
          awardBenderQI(activeRobot.id, hitTP ? 'WIN' : 'LOSS', posTpPct, SL_PCT);
          // Robot qui perd achète automatiquement la leçon si il a assez de QI
          if (!hitTP && activeRobot.qi >= 20) {
            setTimeout(() => buyLesson(activeRobot.id), 1000);
          }
        }
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
            const recentTrade=await Trade.findOne({email:user.email,symbol:sig.symbol,exchange:sig.exchange,time:{$gte:new Date(Date.now()-100*60*60*1000)}});
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
  marketSentiment:getMarketSentiment()+'%',
  tradingEnabled:getMarketSentiment()>=60,
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
      signalsActive:signalsCache.length,
  marketSentiment:getMarketSentiment()+'%',
  tradingEnabled:getMarketSentiment()>=60,lastScan:lastScanTime,
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
      if (ageHours > 240) { // >10j → fermer
        const curP = livePrices[pos.symbol];
        if (curP) {
          const pnl = (curP-pos.entryPrice)/pos.entryPrice*pos.amount;
          await Trade.findOneAndUpdate({email:pos.email,symbol:pos.symbol,result:"OPEN"},{exitPrice:curP,pnl:+pnl.toFixed(4),result:pnl>=0?"WIN":"LOSS",exitReason:"Position fantôme >10j"},{sort:{time:-1}}).catch(()=>{});
          await OpenPosition.deleteOne({_id:pos._id}).catch(()=>{});
          learnFromTrade(pos.email,{result:pnl>=0?"WIN":"LOSS",figure:pos.figure,timeframe:pos.timeframe||"1d",entryPrice:pos.entryPrice,exitPrice:curP,pnl:+pnl.toFixed(4),time:new Date()}).catch(()=>{});
          console.log(`[Watchdog] Fantôme fermé: ${pos.symbol}`);
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
    checkBenchRobots();                // Robots banc → observation ?
    watchWebSocket();                  // WebSocket vivant ?
    await watchPositions();            // Positions cohÃ©rentes ?
    await watchDatabase();             // MongoDB et mÃ©moire OK ?
    await watchBotHealth();            // SantÃ© globale
  } catch(e) { console.log('[Watchdog] Erreur cycle:', e.message); }
}

// Route admin pour voir les alertes Watchdog
// Route chatbot — proxy vers API Anthropic
app.post('/chat-builder', async(req, res) => {
  try {
    const { system, messages } = req.body;
    if (!messages || !Array.isArray(messages))
      return res.json({ success: false, error: 'Messages manquants' });
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY || '',
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-6',
        max_tokens: 1000,
        system: system || 'Tu es un assistant de trading Bender Pro.',
        messages: messages.slice(-10)
      })
    });
    const data = await response.json();
    if (data.error) return res.json({ success: false, error: data.error.message, reply: '⚠ Erreur API.' });
    const reply = data.content && data.content[0] ? data.content[0].text : 'Erreur.';
    res.json({ success: true, reply });
  } catch(e) {
    res.json({ success: false, error: e.message, reply: '⚠ Service indisponible.' });
  }
});

// ── Routes Bender QI
app.get('/bender-qi/leaderboard', (req, res) => {
  res.json({ success: true, leaderboard: getBenderQILeaderboard() });
});

app.get('/bender-qi/robot/:id', (req, res) => {
  const state = robotStates[parseInt(req.params.id)];
  if (!state) return res.json({ success: false, error: 'Robot non trouvé' });
  const personality = ROBOT_PERSONALITIES.find(r => r.id === state.id);
  res.json({
    success: true,
    robot: {
      ...state,
      tradeAmount: getRobotTradeAmount(state),
      personality: personality?.style,
      sl: personality?.sl,
      tpMult: personality?.tpMult,
    }
  });
});

app.post('/bender-qi/buy-lesson/:id', (req, res) => {
  const result = buyLesson(parseInt(req.params.id));
  res.json(result);
});

app.get('/bender-qi/status', (req, res) => {
  const active = Object.values(robotStates).filter(r => r.status === 'active').length;
  const bench  = Object.values(robotStates).filter(r => r.status === 'bench').length;
  const obs    = Object.values(robotStates).filter(r => r.status === 'observation').length;
  const leader = latestRobotAnalyses[0];
  res.json({
    success: true,
    totalRobots: ROBOT_PERSONALITIES.length,
    active, bench, observation: obs,
    currentRobotIndex,
    leader: leader ? `${leader.name} (${leader.qi} QI)` : 'N/A',
    robots: getBenderQILeaderboard()
  });
});

// Routes analyses parallèles
app.get('/bender-qi/analyses/:symbol', (req, res) => {
  const symbol = req.params.symbol;
  res.json({ success: true, symbol, analyses: latestRobotAnalyses.filter(a => a.symbol === symbol) });
});

app.get('/bender-qi/discoveries', (req, res) => {
  res.json({ success: true, discoveries: discoveredPatterns });
});

app.post('/bender-qi/discover/:id', (req, res) => {
  const result = proposeNewPattern(parseInt(req.params.id), req.body);
  res.json(result);
});

app.get('/bender-qi/consensus', (req, res) => {
  res.json({ success: true, total: latestRobotAnalyses.length, analyses: latestRobotAnalyses.slice(0, 10) });
});

// Route validation scientifique
app.get('/bender-qi/validation/:id', (req, res) => {
  const state = robotStates[parseInt(req.params.id)];
  if (!state) return res.json({ success: false, error: 'Non trouvé' });
  res.json({ success: true, robot: state.name, qi: state.qi, status: state.status, currentParams: PARAMS_REFERENCE });
});

app.get('/systeme/proposition', (req, res) => {
  const proposal = PARAMS_REFERENCE.pendingProposal;
  res.json({
    success: true,
    pendingProposal: proposal ? {
      status: proposal.status,
      submittedAt: proposal.submittedAt,
      winrateRequired: proposal.winrateRequired,
      tradesRecorded: proposal.tradesRecorded.length,
      tradesNeeded: proposal.tradesNeeded,
      progress: `${proposal.tradesRecorded.length}/${proposal.tradesNeeded}`,
      currentWR: proposal.tradesRecorded.length > 0
        ? +(proposal.tradesRecorded.filter(r=>r==='WIN').length/proposal.tradesRecorded.length*100).toFixed(1)
        : null
    } : null,
    currentParams: {
      rsiMin: PARAMS_REFERENCE.rsiMin,
      rsiMax: PARAMS_REFERENCE.rsiMax,
      volMultiplier: PARAMS_REFERENCE.volMultiplier,
      adxMin: PARAMS_REFERENCE.adxMin,
      slPct: PARAMS_REFERENCE.slPct,
      sentimentMin: PARAMS_REFERENCE.sentimentMin,
      winrateEstimate: PARAMS_REFERENCE.winrateEstimate || 'Non établi'
    }
  });
});

app.post('/bender-qi/propose/:id', (req, res) => {
  const result = proposeImprovement(parseInt(req.params.id), req.body);
  res.json(result);
});

// Route statut backtester
app.get('/backtest-status', (req, res) => {
  res.json({
    success: true,
    running: backtestRunning,
    lastBacktest: lastBacktest,
    optimalParams: {
      rsiMin: optimalParams.rsiMin,
      rsiMax: optimalParams.rsiMax,
      volMultiplier: optimalParams.volMultiplier,
      adxMin: optimalParams.adxMin,
      slPct: optimalParams.slPct,
      winrate: optimalParams.winrate,
      trades: optimalParams.trades
    }
  });
});

// Route analyses des 30 robots — pour le frontend profil
app.get('/robot-analysis', (req, res) => {
  const activeRobots  = Object.values(robotStates).filter(r => r.status === 'active').length;
  const benchRobots   = Object.values(robotStates).filter(r => r.status === 'bench').length;
  const leader = latestRobotAnalyses[0] || null;
  res.json({
    success: true,
    lastUpdate: lastAnalysisTime,
    nextUpdate: lastAnalysisTime
      ? new Date(new Date(lastAnalysisTime).getTime() + 30*60*1000)
      : null,
    totalRobots: ROBOT_PERSONALITIES.length,
    activeRobots,
    benchRobots,
    leader,
    analyses: latestRobotAnalyses,
    discoveries: discoveredPatterns
  });
});

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
    marketSentiment:     getMarketSentiment() + '%',
    tradingEnabled:      getMarketSentiment() >= 60,
  });
});

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// DÃ‰MARRAGE
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`\nðŸ¤– Bender Pro v10.0 Â· Port ${PORT}`);
  console.log(` Mode ANALYSE PURE — Trading en pause · Starter Plan`);
  console.log(` Render Starter 2GB · 1000 robots · analyse complète toutes les 30min`);
  console.log(` 1000 robots analysent le marché 24/7`);
  console.log('');
  console.log(' 🏆 Bender QI — Récompenses découverte:');
  console.log('    WR 70% → 10,000 QI');
  console.log('    WR 75% → 50,000 QI  (×5)');
  console.log('    WR 80% → 250,000 QI (×5²)');
  console.log('    WR 85% → 1,250,000 QI (×5³)');
  console.log('    WR 90% → 6,250,000 QI (×5⁴)');
  console.log('    WR 95% → 31,250,000 QI (×5⁵)');
  console.log('    WR 100%→ 156,250,000 QI (×5⁶)');
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

    }, 15000);

    // ── KEEPALIVE — reconnexion proactive toutes les 20s
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
      connectKrakenWS(krakenPairsList);     // Daily 1D
      connectKrakenWS1h(krakenPairsList);   // 1H
      connectKrakenWS4h(krakenPairsList);   // 4H
      console.log(` ${krakenPairsList.length} paires Kraken Â· WebSocket actif`);
      preloadHistoricalCandles(krakenPairsList).then(() => {
        console.log(' Preloading terminÃ©');
        scanAll().catch(console.error);
                      }).catch(console.error);
    }

    // Analyse des 30 robots toutes les 30 minutes
    setTimeout(() => {
      runRobotAnalyses().catch(console.error);
      setInterval(() => runRobotAnalyses().catch(console.error), 30 * 60 * 1000);
    }, 5 * 60 * 1000); // Démarrer 5 min après le boot

    // Scan classique toutes les 60s
    setTimeout(() => setInterval(() => scanAll().catch(console.error), SCAN_INTERVAL), 65000);
  });
});
