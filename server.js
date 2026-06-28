// Bender Pro v10.0 — Bot Classique + Mode IA Autonome
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

// ══════════════════════════════════════════════════════════════════════
// CONFIG GLOBALE
// ══════════════════════════════════════════════════════════════════════
const TRADE_AMOUNT      = 5;
const SL_PCT = 0.02; // -2%
const TP_PCT            = 0.12;
const MAX_CONCURRENT    = 20;
const VOL_CONFIRM       = 1.8;
const SCAN_INTERVAL     = 60 * 1000;
const MAX_PAIRS         = 500;
const MAX_SIGNALS_CACHE = 200;

// CONFIG MODE IA
const AI_MAX_TRADE      = 5;    // Max 5$ par trade (jamais négociable)
const AI_MAX_LOSS_DAY   = 0.10; // Max -10% du capital en une journée
const AI_MIN_GREEN      = 0;    // Doit finir dans le vert (P&L > 0)
const AI_VERSION_KEY    = 'aiVersion'; // clé en DB pour la version IA active

// MONGODB
mongoose.connect(process.env.MONGODB_URI)
  .then(() => console.log('MongoDB connecte!'))
  .catch(err => console.log('Erreur MongoDB:', err.message));

// ══════════════════════════════════════════════════════════════════════
// SCHEMAS
// ══════════════════════════════════════════════════════════════════════
const UserSchema = new mongoose.Schema({
  email:        { type: String, required: true, unique: true },
  exchangeName: String,
  apiKey:       String,
  apiSecret:    String,
  tradeAmount:  { type: Number, default: 5 },
  currency:     { type: String, default: 'USD' },
  active:       { type: Boolean, default: true },
  botMode:      { type: String, default: 'classic', enum: ['classic', 'ai'] }, // MODE BOT
  // Contrôle capital Mode IA
  aiTradeAmount:    { type: Number, default: 5  },   // $ par trade IA (max 5$ jamais négociable)
  aiMaxTrades:      { type: Number, default: 1  },   // trades simultanés max IA
  aiDailyCapital:   { type: Number, default: 10 },   // capital max utilisé par jour IA
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

// Schema IA — historique des versions et performances
const AIVersionSchema = new mongoose.Schema({
  email:      String,
  version:    Number,          // numéro de version IA
  date:       Date,            // date de début
  pnlDay:     Number,          // P&L du jour
  trades:     Number,          // nombre de trades aujourd'hui
  status:     String,          // 'active', 'retired', 'replaced'
  strategy:   Object,          // paramètres de la stratégie IA
  retiredAt:  Date,
  reason:     String,          // raison du remplacement
});

const User         = mongoose.model('User',         UserSchema);
const Trade        = mongoose.model('Trade',        TradeSchema);
const Signal       = mongoose.model('Signal',       SignalSchema);
const OpenPosition = mongoose.model('OpenPosition', OpenPositionSchema);
const Billing      = mongoose.model('Billing',      BillingSchema);
const AIVersion    = mongoose.model('AIVersion',    AIVersionSchema);

// ══════════════════════════════════════════════════════════════════════
// CONFIGURATION 35 PLATEFORMES
// ══════════════════════════════════════════════════════════════════════
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

// ══════════════════════════════════════════════════════════════════════
// FIGURES CHARTISTES (Bot Classique)
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

// ══════════════════════════════════════════════════════════════════════
// MODE IA AUTONOME — Cerveau de l'IA
// ══════════════════════════════════════════════════════════════════════

// Mémoire IA par utilisateur
const aiMemory = {}; // { email: { version, dayPnl, dayTrades, strategy, lastReset } }

// Générer une nouvelle stratégie IA aléatoire mais encadrée
function generateAIStrategy(version) {
  return {
    version,
    // Indicateurs que l'IA peut combiner librement
    rsiOversold:    Math.floor(20 + Math.random() * 20),     // 20-40
    rsiOverbought:  Math.floor(60 + Math.random() * 20),     // 60-80
    volumeThresh:   1.2 + Math.random() * 1.5,              // 1.2x à 2.7x la moyenne
    trendPeriod:    Math.floor(10 + Math.random() * 40),     // 10-50 bougies
    tpMultiplier:   0.05 + Math.random() * 0.15,            // TP 5% à 20%
    minConfidence:  50 + Math.floor(Math.random() * 30),     // 50% à 80%
    timeframes:     Math.random() > 0.5 ? ['1d', '4h'] : ['1d'],
    preferredHour:  Math.floor(Math.random() * 24),          // heure préférée pour trader
    momentumWeight: Math.random(),                            // poids du momentum
    volumeWeight:   Math.random(),                            // poids du volume
    trendWeight:    Math.random(),                            // poids de la tendance
    maxPositions:   Math.floor(2 + Math.random() * 8),       // 2 à 10 positions max
    cooldown:       Math.floor(1 + Math.random() * 6),       // heures de cooldown entre trades
  };
}

// Calcul RSI
function calcRSI(prices, period = 14) {
  if (prices.length < period + 1) return 50;
  let gains = 0, losses = 0;
  for (let i = prices.length - period; i < prices.length; i++) {
    const diff = prices[i] - prices[i-1];
    if (diff > 0) gains += diff;
    else losses -= diff;
  }
  const avgGain = gains / period;
  const avgLoss = losses / period;
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - (100 / (1 + rs));
}

// Calcul EMA
function calcEMA(prices, period) {
  if (prices.length < period) return prices[prices.length-1];
  const k = 2 / (period + 1);
  let ema = prices.slice(0, period).reduce((a,b) => a+b) / period;
  for (let i = period; i < prices.length; i++) ema = prices[i]*k + ema*(1-k);
  return ema;
}

// Score IA — combine tous les indicateurs selon la stratégie
function aiScore(closes, volumes, strategy, livePrice) {
  if (closes.length < 100) return { score: 0, shouldBuy: false };
  const price = livePrice || closes[closes.length-1];
  const volNow = volumes[volumes.length-1];
  const volAvg = avg(volumes.slice(-50));
  const volRatio = volNow / volAvg;

  // RSI
  const rsi = calcRSI(closes);
  const rsiScore = rsi < strategy.rsiOversold ? 3 :
                   rsi < 45 ? 1 :
                   rsi > strategy.rsiOverbought ? -3 : 0;

  // EMA Trend
  const ema20 = calcEMA(closes, 20);
  const ema50 = calcEMA(closes, 50);
  const trendScore = (ema20 > ema50 && price > ema20) ? 2 :
                     (ema20 < ema50) ? -2 : 0;

  // Volume
  const volScore = volRatio > strategy.volumeThresh ? 2 :
                   volRatio > 1.2 ? 1 : 0;

  // Momentum (variation sur N bougies)
  const n = Math.min(strategy.trendPeriod, closes.length - 1);
  const momentum = (price - closes[closes.length - 1 - n]) / closes[closes.length - 1 - n];
  const momScore = momentum > 0.05 ? 2 :
                   momentum > 0.02 ? 1 :
                   momentum < -0.05 ? -2 : 0;

  // Score pondéré selon la stratégie
  const totalScore = (rsiScore * strategy.volumeWeight) +
                     (trendScore * strategy.trendWeight) +
                     (volScore * strategy.volumeWeight) +
                     (momScore * strategy.momentumWeight);

  // Seuil dynamique selon la confiance minimale de la stratégie
  const threshold = strategy.minConfidence / 100 * 4;
  const shouldBuy = totalScore >= threshold;

  return {
    score: totalScore,
    shouldBuy,
    rsi, rsiScore, trendScore, volScore, momScore,
    tpPct: strategy.tpMultiplier,
    confidence: Math.min(99, Math.max(0, Math.round(50 + totalScore * 10)))
  };
}

// Vérifier si l'IA doit être remplacée (fin de journée dans le rouge)
async function checkAIDayResult() {
  try {
    const now = new Date();
    // Vérifier à minuit UTC
    const isEndOfDay = now.getHours() === 0 && now.getMinutes() < 5;
    if (!isEndOfDay) return;

    const aiUsers = await User.find({ active: true, botMode:'ai', apiKey: { $exists: true } });
    for (const user of aiUsers) {
      const mem = aiMemory[user.email];
      if (!mem) continue;

      // P&L du jour
      const dayStart = new Date(); dayStart.setHours(0,0,0,0);
      const dayTrades = await Trade.find({
        email: user.email, botMode:'ai',
        time: { $gte: dayStart }, result: { $in: ['WIN','LOSS'] }
      });
      const dayPnl = dayTrades.reduce((a,t) => a + (t.pnl||0), 0);

      if (dayPnl < AI_MIN_GREEN) {
        // IA dans le rouge → la remplacer
        const oldVersion = mem.version || 1;
        const newVersion = oldVersion + 1;

        console.log(`[IA] ${user.email} — Version IA_${oldVersion} dans le rouge (P&L: ${dayPnl.toFixed(4)}$) → Remplacement par IA_${newVersion}`);

        // Archiver l'ancienne version
        await AIVersion.findOneAndUpdate(
          { email: user.email, version: oldVersion },
          { status: 'replaced', retiredAt: new Date(), pnlDay: dayPnl, reason: `P&L journalier négatif: ${dayPnl.toFixed(4)}$` }
        );

        // Créer la nouvelle stratégie
        const newStrategy = generateAIStrategy(newVersion);
        aiMemory[user.email] = {
          version: newVersion,
          dayPnl: 0,
          dayTrades: 0,
          strategy: newStrategy,
          lastReset: new Date()
        };

        // Sauvegarder en DB
        await new AIVersion({
          email: user.email, version: newVersion,
          date: new Date(), status: 'active',
          strategy: newStrategy, pnlDay: 0, trades: 0
        }).save();

        console.log(`[IA] ${user.email} — IA_${newVersion} activée avec nouvelle stratégie`);
      } else {
        console.log(`[IA] ${user.email} — IA_${mem.version} dans le vert (P&L: +${dayPnl.toFixed(4)}$) → Continue demain`);
        // Reset le compteur du jour
        if (mem) { mem.dayPnl = 0; mem.dayTrades = 0; }
      }
    }
  } catch(e) { console.log('[IA] Erreur checkAIDayResult:', e.message); }
}

// Initialiser ou récupérer la mémoire IA d'un utilisateur
async function initAIMemory(email) {
  if (aiMemory[email]) return aiMemory[email];

  // Chercher la version active en DB
  const activeVersion = await AIVersion.findOne({ email, status: 'active' }).sort({ version: -1 });

  if (activeVersion) {
    aiMemory[email] = {
      version: activeVersion.version,
      dayPnl: 0,
      dayTrades: 0,
      strategy: activeVersion.strategy,
      lastReset: new Date()
    };
  } else {
    // Première fois — créer IA v1
    const strategy = generateAIStrategy(1);
    aiMemory[email] = { version: 1, dayPnl: 0, dayTrades: 0, strategy, lastReset: new Date() };
    await new AIVersion({ email, version: 1, date: new Date(), status: 'active', strategy, pnlDay: 0, trades: 0 }).save();
    console.log(`[IA] ${email} — IA_1 créée avec stratégie initiale`);
  }
  return aiMemory[email];
}

// Exécution d'un trade IA
async function executeAITrade(user, symbol, analysis, exchange, balance) {
  try {
    const mem = await initAIMemory(user.email);
    const strategy = mem.strategy;

    // ── GARDE-FOUS ABSOLUS (jamais négociables) ──
    // 1. Max AI_MAX_TRADE$ par trade
    const amount = Math.min(AI_MAX_TRADE, user.tradeAmount || AI_MAX_TRADE);

    // 2. Circuit breaker journalier
    const dayStart = new Date(); dayStart.setHours(0,0,0,0);
    const dayTrades = await Trade.find({ email: user.email, botMode:'ai', time: { $gte: dayStart } });
    const dayPnl = dayTrades.reduce((a,t) => a + (t.pnl||0), 0);
    const totalCapital = Object.values(balance).reduce((a,b) => a + (b?.total||0), 0);

    if (dayPnl < -(totalCapital * AI_MAX_LOSS_DAY)) {
      console.log(`[IA] ${user.email} — Circuit breaker activé (pertes journalières: ${dayPnl.toFixed(2)}$)`);
      return;
    }

    // 3. Max positions simultanées selon la stratégie
    const openPositions = await OpenPosition.countDocuments({ email: user.email, botMode:'ai' });
    if (openPositions >= strategy.maxPositions) {
      console.log(`[IA] ${user.email} — Max positions atteint (${openPositions}/${strategy.maxPositions})`);
      return;
    }

    // 4. Cooldown entre trades
    const lastTrade = await Trade.findOne({ email: user.email, botMode:'ai' }, null, { sort: { time: -1 } });
    if (lastTrade) {
      const hoursSince = (Date.now() - new Date(lastTrade.time).getTime()) / (1000 * 60 * 60);
      if (hoursSince < strategy.cooldown) {
        console.log(`[IA] ${user.email} — Cooldown actif (${hoursSince.toFixed(1)}h / ${strategy.cooldown}h)`);
        return;
      }
    }

    // 5. Position déjà ouverte sur ce symbole
    const existing = await OpenPosition.findOne({ email: user.email, symbol });
    if (existing) return;

    // 6. Vérifier les fonds
    const currency = user.currency || 'USD';
    const available = balance[currency]?.free || 0;
    if (available < amount) {
      console.log(`[IA] ${user.email} — Fonds insuffisants (${available.toFixed(2)}$ / ${amount}$)`);
      return;
    }

    // ── EXÉCUTION ──
    const price = analysis.livePrice;
    const tpPct = analysis.tpPct || strategy.tpMultiplier;
    const tpPrice = +(price * (1 + tpPct)).toFixed(8);
    const slPrice = +(price * (1 - SL_PCT)).toFixed(8);
    const qty = amount / price;

    const orderParams = {};
    if (user.exchangeName === 'kraken') orderParams.oflags = 'fciq';

    console.log(`[IA_${mem.version}] BUY ${symbol} · Score:${analysis.score.toFixed(2)} · RSI:${analysis.rsi?.toFixed(0)} · $${amount} · TP:${tpPrice} · SL:${slPrice}`);

    const order = await exchange.createOrder(symbol, 'market', 'buy', qty, undefined, orderParams);
    console.log(`[IA_${mem.version}] Ordre exécuté: ${order.id}`);

    await new OpenPosition({
      email: user.email, symbol,
      exchange: 'Kraken', exchangeId: user.exchangeName,
      figure: `IA_${mem.version}`, entryPrice: price,
      tp: tpPrice, sl: slPrice,
      tpPct: +(tpPct * 100).toFixed(1),
      qty, amount, currency, botMode:'ai'
    }).save();

    await new Trade({
      email: user.email, symbol,
      exchange: 'Kraken', market: 'Spot',
      direction: 'Long', figure: `IA_${mem.version}`,
      entryPrice: price, exitPrice: null,
      amount, pnl: 0, currency,
      result: 'OPEN',
      exitReason: `IA_${mem.version} · Score:${analysis.score.toFixed(2)}`,
      botMode:'ai'
    }).save();

    // Mettre à jour la mémoire
    mem.dayTrades++;

  } catch(e) { console.log(`[IA] Erreur trade ${symbol}:`, e.message); }
}

// Scan IA — analyse toutes les paires disponibles
async function runAIScan() {
  try {
    const aiUsers = await User.find({ active: true, botMode:'ai', apiKey: { $exists: true } });
    if (aiUsers.length === 0) return;

    console.log(`[IA] Scan pour ${aiUsers.length} utilisateur(s) en mode IA`);

    for (const user of aiUsers) {
      try {
        const mem = await initAIMemory(user.email);
        const strategy = mem.strategy;

        const ExClass = ccxt[user.exchangeName] || ccxt['kraken'];
        const exchange = new ExClass({ apiKey: user.apiKey, secret: user.apiSecret, enableRateLimit: true });
        const balance = await exchange.fetchBalance();

        // Scanner les paires disponibles en mémoire
        const pairs = Object.keys(krakenCandles).filter(sym => {
          const candles = krakenCandles[sym];
          return candles && candles.length >= 100;
        });

        let bestSignal = null;
        let bestScore = 0;

        for (const symbol of pairs) {
          try {
            const candles = krakenCandles[symbol];
            const closes  = candles.map(c => c.c);
            const volumes = candles.map(c => c.v);
            const livePrice = livePrices[symbol];
            if (!livePrice) continue;

            const analysis = aiScore(closes, volumes, strategy, livePrice);
            analysis.livePrice = livePrice;
            analysis.symbol = symbol;

            if (analysis.shouldBuy && analysis.score > bestScore) {
              // Vérifier pas de position déjà ouverte
              const existing = await OpenPosition.findOne({ email: user.email, symbol });
              if (!existing) {
                bestScore = analysis.score;
                bestSignal = analysis;
              }
            }
          } catch(e) {}
        }

        // Entrer sur le meilleur signal trouvé
        if (bestSignal) {
          console.log(`[IA_${mem.version}] Meilleur signal: ${bestSignal.symbol} · Score:${bestSignal.score.toFixed(2)}`);
          await executeAITrade(user, bestSignal.symbol, bestSignal, exchange, balance);
        }

      } catch(e) { console.log(`[IA] Erreur utilisateur ${user.email}:`, e.message); }
    }
  } catch(e) { console.log('[IA] Erreur runAIScan:', e.message); }
}

// ══════════════════════════════════════════════════════════════════════
// WEBSOCKET KRAKEN (identique v9.0)
// ══════════════════════════════════════════════════════════════════════
const krakenCandles    = {};
const krakenCandles4h  = {};
let krakenPairsList    = [];
let wsConnected        = false;
let ws = null, ws4h = null, wsTicker = null;
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
    console.log(`[Ticker] Connecté — ${pairs.length} paires`);
    for (let i=0; i<pairs.length; i+=50)
      wsTicker.send(JSON.stringify({ method:'subscribe', params:{ channel:'ticker', symbol:pairs.slice(i,i+50) } }));
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
            const bc = breakoutConfirm[t.symbol];
            if (bc) {
              if (t.last > bc.resistance) {
                bc.count++;
                if (bc.count >= 3) {
                  console.log(`[Breakout] ${t.symbol} confirmé → ORDRE`);
                  delete breakoutConfirm[t.symbol];
                  executeTrade(bc.signal).catch(() => {});
                }
              } else { delete breakoutConfirm[t.symbol]; }
            }
          }
        }
      }
    } catch(e) {}
  });
  wsTicker.on('close', () => { setTimeout(() => connectKrakenTicker(krakenPairsList), 5000); });
  wsTicker.on('error', (err) => { console.log('[Ticker] Erreur:', err.message); });
}

function connectKrakenWS(pairs) {
  if (ws) { try { ws.terminate(); } catch(e) {} }
  ws = new WebSocket('wss://ws.kraken.com/v2');
  ws.on('open', () => {
    wsConnected = true;
    console.log(`WebSocket Kraken — ${pairs.length} paires`);
    for (let i=0; i<pairs.length; i+=50)
      ws.send(JSON.stringify({ method:'subscribe', params:{ channel:'ohlc', symbol:pairs.slice(i,i+50), interval:1440 } }));
  });
  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.channel==='ohlc' && (msg.type==='snapshot'||msg.type==='update') && msg.data) {
        for (const c of msg.data) {
          const sym = c.symbol;
          const arr = krakenCandles[sym] || (krakenCandles[sym]=[]);
          const lastC = arr[arr.length-1];
          if (lastC && Math.abs(lastC.c-c.close)/(c.close||1) < 0.5) { lastC.c=c.close; lastC.v=c.volume; }
          else { arr.push({c:c.close,v:c.volume}); if (arr.length>150) arr.shift(); }
          if (!lastC || arr[arr.length-1]!==lastC) setImmediate(() => scanSinglePair(sym,'1d','kraken'));
        }
      }
    } catch(e) {}
  });
  ws.on('close', () => { wsConnected=false; setTimeout(() => connectKrakenWS(krakenPairsList),5000); });
  ws.on('error', (err) => { console.log('Erreur WS:', err.message); });
}

function connectKrakenWS4h(pairs) {
  if (ws4h) { try { ws4h.terminate(); } catch(e) {} }
  ws4h = new WebSocket('wss://ws.kraken.com/v2');
  ws4h.on('open', () => {
    console.log(`[WS-4h] Connecté — ${pairs.length} paires`);
    for (let i=0; i<pairs.length; i+=50)
      ws4h.send(JSON.stringify({ method:'subscribe', params:{ channel:'ohlc', symbol:pairs.slice(i,i+50), interval:240 } }));
  });
  ws4h.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.channel==='ohlc' && (msg.type==='snapshot'||msg.type==='update') && msg.data) {
        for (const c of msg.data) {
          const sym = c.symbol;
          const arr = krakenCandles4h[sym] || (krakenCandles4h[sym]=[]);
          const lastC = arr[arr.length-1];
          if (lastC && Math.abs(lastC.c-c.close)/(c.close||1) < 0.5) { lastC.c=c.close; lastC.v=c.volume; }
          else { arr.push({c:c.close,v:c.volume}); if (arr.length>1000) arr.shift(); }
          if (!lastC || arr[arr.length-1]!==lastC) setImmediate(() => scanSinglePair(sym,'4h','kraken'));
        }
      }
    } catch(e) {}
  });
  ws4h.on('close', () => { setTimeout(() => connectKrakenWS4h(krakenPairsList),5000); });
  ws4h.on('error', (err) => { console.log('[WS-4h] Erreur:', err.message); });
}

async function preloadHistoricalCandles(pairs) {
  console.log(`Preloading ${pairs.length} paires...`);
  const exchange = new ccxt.kraken({ enableRateLimit:true, timeout:10000 });
  const BATCH = 50;
  for (let i=0; i<pairs.length; i+=BATCH) {
    const batch = pairs.slice(i,i+BATCH);
    await Promise.all(batch.map(async (symbol) => {
      try {
        const d = await exchange.fetchOHLCV(symbol,'1d',undefined,160);
        if (d && d.length>=10) krakenCandles[symbol] = d.map(c=>({c:c[4],v:c[6]||c[5]}));
      } catch(e) {}
      try {
        const h4 = await exchange.fetchOHLCV(symbol,'4h',undefined,1000);
        if (h4 && h4.length>=10) krakenCandles4h[symbol] = h4.map(c=>({c:c[4],v:c[6]||c[5]}));
      } catch(e) {}
    }));
    console.log(`Preloading... ${Math.min(i+BATCH,pairs.length)}/${pairs.length}`);
    if (i+BATCH<pairs.length) await new Promise(r=>setTimeout(r,500));
  }
  console.log('Preloading terminé!');
}

// ══════════════════════════════════════════════════════════════════════
// SCAN CLASSIQUE (Bot Classique uniquement)
// ══════════════════════════════════════════════════════════════════════
function scanSinglePair(symbol, timeframe='1d', exchangeId='kraken') {
  try {
    const candleStore = timeframe==='4h' ? krakenCandles4h : krakenCandles;
    const candles = candleStore[symbol];
    if (!candles || candles.length<100) return;
    const closes  = candles.map(c=>c.c);
    const volumes = candles.map(c=>c.v);
    const livePrice = livePrices[symbol];
    if (!livePrice) return;
    const sig = detectFigure(closes,volumes,livePrice);
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
    if (!breakoutConfirm[symbol]) {
      breakoutConfirm[symbol] = { count:1, resistance, figure:sig.fig.name, signal, startedAt:Date.now() };
      console.log(`[Breakout] ${symbol} · ${sig.fig.name} · 1/3`);
    }
    setTimeout(() => {
      if (breakoutConfirm[symbol]&&breakoutConfirm[symbol].figure===sig.fig.name) {
        console.log(`[Breakout] ${symbol} — timeout — annulé`);
        delete breakoutConfirm[symbol];
      }
    }, 30000);
  } catch(e) {}
}

// Exécution Bot Classique (mode='classic' uniquement)
async function executeTrade(signal) {
  try {
    const exchangeId = signal.exchangeId||'kraken';
    const exConfig = EXCHANGES_CONFIG.find(e=>e.id===exchangeId);
    // Seulement les utilisateurs en mode classique
    const users = await User.find({
      active:true, apiKey:{$exists:true},
      exchangeName:new RegExp(exchangeId,'i'),
      botMode:'classic' // ← IMPORTANT: seulement le bot classique
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
        console.log(`[Classic] BUY ${signal.symbol} · ${signal.figure} · ${amount}${currency}`);
        const order = await exchange.createOrder(signal.symbol,'market','buy',qty,undefined,orderParams);
        await new OpenPosition({email:user.email,symbol:signal.symbol,exchange:signal.exchange,exchangeId,
          figure:signal.figure,entryPrice:signal.entryPrice,tp:signal.tp,sl:signal.sl,
          tpPct:signal.tpPct,figureTarget:signal.figureTarget,qty,amount,currency,
          timeframe:signal.timeframe, botMode:'classic'}).save();
        await new Trade({email:user.email,symbol:signal.symbol,exchange:signal.exchange,market:'Spot',
          direction:signal.direction,figure:signal.figure,entryPrice:signal.entryPrice,
          exitPrice:null,amount,pnl:0,currency,result:'OPEN',
          exitReason:'Position ouverte — en attente TP/SL',botMode:'classic'}).save();
      } catch(e) { console.log(`[Classic] Erreur ${signal.symbol}:`,e.message); }
    }
  } catch(e) { console.log('[executeTrade] Erreur:',e.message); }
}

// ══════════════════════════════════════════════════════════════════════
// TP/SL — Fonctionne pour les deux modes
// ══════════════════════════════════════════════════════════════════════
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
      console.log(`[${pos.botMode==='ai'?'IA':'Classic'} ${reason}] ${pos.symbol} prix:${currentPrice}`);
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
           exitReason:hitTP?`TP +${pos.tpPct}% atteint`:'SL -2% touché'},{sort:{time:-1}});
        await OpenPosition.deleteOne({_id:pos._id});
        console.log(`[${reason}] PnL: ${pnl>=0?'+':''}$${pnl.toFixed(4)}`);
        // Mettre à jour la mémoire IA si mode IA
        if (pos.botMode==='ai' && aiMemory[pos.email]) {
          aiMemory[pos.email].dayPnl += pnl;
        }
      } catch(e) {
        console.log(`[TP/SL] Erreur ${pos.symbol}:`,e.message);
        if (e.message&&e.message.includes('Insufficient funds'))
          await OpenPosition.deleteOne({_id:pos._id}).catch(()=>{});
      } finally { positionsInProgress.delete(posId); }
    }
  } catch(e) { console.log('[TP/SL] Erreur:',e.message); }
}

// ══════════════════════════════════════════════════════════════════════
// SCAN ALL (classique + REST)
// ══════════════════════════════════════════════════════════════════════
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
  console.log(`\n=== SCAN — ${new Date().toLocaleTimeString()} ===`);
  signalsCache.length=0;
  Object.keys(signalsByExchange).forEach(k=>delete signalsByExchange[k]);
  if (typeof global.gc==='function') global.gc();
  try {
    const users=await User.find({active:true,apiKey:{$exists:true}});
    const krakenResults=scanKrakenFromMemory();
    signalsCache.push(...krakenResults);
    signalsByExchange['kraken']=krakenResults;
    lastScanTime=new Date();
    console.log(`[Kraken] ${krakenResults.length} signal(s) · ${Date.now()-startTime}ms`);
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
    console.log(`=== FIN · ${signalsCache.length} signaux · ${Date.now()-startTime}ms ===\n`);
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
              amount,pnl:0,currency,result:'OPEN',exitReason:'Position ouverte — en attente TP/SL',botMode:'classic'}).save();
            ordersPlaced++;
          } catch(e){console.log(`[Classic] Erreur ${sig.symbol}:`,e.message);}
        }
        if (ordersPlaced>0) console.log(`[Classic] ${ordersPlaced} ordre(s) pour ${user.email}`);
      } catch(e){console.log(`[Classic] Erreur ${user.email}:`,e.message);}
    }
  } finally {scanRunning=false;}
}

// ══════════════════════════════════════════════════════════════════════
// XLM PAYMENTS
// ══════════════════════════════════════════════════════════════════════
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
      console.log(`[XLM] Paiement — ${user.email} PAID`);
    }
  } catch(e){console.log('[XLM] Erreur:',e.message);}
}

// ══════════════════════════════════════════════════════════════════════
// ROUTES
// ══════════════════════════════════════════════════════════════════════
app.get('/', (req,res) => res.json({
  status:'Bender Pro v10.0 — Bot Classique + IA',
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
  res.json({success:true,message:'Scan lancé...'});
  scanAll().catch(console.error);
});

app.post('/register-email',async(req,res)=>{
  const {email}=req.body;
  if (!email||!email.includes('@')) return res.json({success:false,error:'Email invalide'});
  try {
    await User.findOneAndUpdate({email},{email,active:true},{upsert:true,new:true});
    res.json({success:true,message:'Email enregistré'});
  } catch(e){res.json({success:false,error:e.message});}
});

// Route connect — supporte le choix de mode
app.post('/connect',async(req,res)=>{
  const {email,apiKey,secret,exchangeName,tradeAmount,currency,botMode}=req.body;
  if (!email||!apiKey||!secret||!exchangeName)
    return res.json({success:false,error:'Données manquantes'});
  try {
    const exConfig=EXCHANGES_CONFIG.find(e=>e.id===exchangeName.toLowerCase()||e.name.toLowerCase()===exchangeName.toLowerCase());
    const selectedCurrency=currency||(exConfig?exConfig.currencies[0]:'USD');
    const selectedMode=botMode==='ai'?'ai':'classic';
    await User.findOneAndUpdate({email},
      {apiKey,apiSecret:secret,exchangeName:exConfig?exConfig.id:exchangeName.toLowerCase(),
       active:true,tradeAmount:tradeAmount||TRADE_AMOUNT,currency:selectedCurrency,botMode:selectedMode},
      {upsert:true,new:true});
    // Initialiser la mémoire IA si mode IA
    if (selectedMode==='ai') await initAIMemory(email);
    const modeLabel = selectedMode==='ai' ? '🧠 Mode IA Autonome' : '🤖 Mode Classique';
    res.json({success:true,message:`${modeLabel} · ${exConfig?.name||exchangeName} · ${selectedCurrency} · SL -2%`});
  } catch(e){res.json({success:false,error:e.message});}
});

// Route pour changer de mode sans déconnecter
app.post('/switch-mode',async(req,res)=>{
  try {
    const {email,botMode}=req.body;
    if (!email||!botMode) return res.json({success:false,error:'Données manquantes'});
    const mode=botMode==='ai'?'ai':'classic';
    await User.findOneAndUpdate({email},{botMode:mode});
    if (mode==='ai') await initAIMemory(email);
    res.json({success:true,mode,message:`Mode basculé vers: ${mode==='ai'?'IA Autonome':'Classique'}`});
  } catch(e){res.json({success:false,error:e.message});}
});

app.get('/status/:email',async(req,res)=>{
  try {
    const user=await User.findOne({email:req.params.email});
    if (!user) return res.json({connected:false});
    const trades=await Trade.countDocuments({email:req.params.email});
    const wins=await Trade.countDocuments({email:req.params.email,result:'WIN'});
    // Stats IA si applicable
    let aiInfo = null;
    if (user.botMode==='ai') {
      const mem = aiMemory[req.params.email];
      // Capital utilisé aujourd'hui
      const dayStart = new Date(); dayStart.setHours(0,0,0,0);
      const dayTrades = await Trade.find({email:req.params.email,botMode:'ai',time:{$gte:dayStart}});
      const dayCapitalUsed = dayTrades.reduce((a,t)=>a+(t.amount||0),0);
      const dayPnl = dayTrades.reduce((a,t)=>a+(t.pnl||0),0);
      const openAI = await OpenPosition.countDocuments({email:req.params.email,botMode:'ai'});
      aiInfo = {
        version:      mem?.version||1,
        dayPnl:       dayPnl,
        dayTrades:    dayTrades.length,
        dayCapitalUsed: +dayCapitalUsed.toFixed(2),
        openPositions: openAI,
        strategy:     mem?.strategy||null,
        totalVersions: await AIVersion.countDocuments({email:req.params.email}),
        // Config capital de l'utilisateur
        config: {
          aiTradeAmount:  user.aiTradeAmount  || 5,
          aiMaxTrades:    user.aiMaxTrades    || 1,
          aiDailyCapital: user.aiDailyCapital || 10,
        }
      };
    }
    res.json({connected:true,active:user.active,exchange:user.exchangeName,
      tradeAmount:user.tradeAmount,trades,botMode:user.botMode||'classic',
      winRate:trades>0?Math.round(wins/trades*100)+'%':'N/A',aiInfo});
  } catch(e){res.json({success:false,error:e.message});}
});

// Route stats IA
app.get('/ai-stats/:email',async(req,res)=>{
  try {
    const versions=await AIVersion.find({email:req.params.email}).sort({version:-1}).limit(10);
    const mem=aiMemory[req.params.email];
    res.json({success:true,
      currentVersion:mem?.version||1,
      dayPnl:mem?.dayPnl||0,
      strategy:mem?.strategy||null,
      history:versions
    });
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
    if (!user) return res.json({success:false,error:'Utilisateur non trouvé'});
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

app.post('/billing/paid/:email',async(req,res)=>{
  try {
    const {txHash}=req.body;
    const periodStart=new Date(Date.now()-BILLING_DAYS*24*3600*1000);
    const billing=await Billing.findOneAndUpdate(
      {email:req.params.email,periodStart:{$gte:new Date(periodStart.getTime()-3600000)}},
      {status:'PAID',paidAt:new Date(),txHash:txHash||''},
      {sort:{createdAt:-1},new:true});
    if (!billing) return res.json({success:false,error:'Facture non trouvée'});
    res.json({success:true,billing});
  } catch(e){res.json({success:false,error:e.message});}
});

app.get('/signals',async(req,res)=>{
  try{res.json({signals:await Signal.find().sort({time:-1}).limit(100)});}
  catch(e){res.json({success:false,error:e.message});}
});

app.get('/exchanges',(req,res)=>res.json({exchanges:EXCHANGES_CONFIG}));

let pricesCache={},pricesCacheTime=0;
function refreshPricesFromMemory(){
  const out={};
  const watch=['BTC/USD','ETH/USD','SOL/USD','XRP/USD','ADA/USD','AVAX/USD','DOGE/USD',
    'DOT/USD','LINK/USD','LTC/USD','ATOM/USD','UNI/USD','NEAR/USD','XLM/USD','ARB/USD','OP/USD'];
  for (const sym of watch) {
    const candles=krakenCandles[sym];
    if (candles&&candles.length>=2){
      const last=candles[candles.length-1],prev=candles[0];
      out[sym.split('/')[0]]={price:last.c,changePct:prev.c?((last.c-prev.c)/prev.c)*100:null};
    }
  }
  pricesCache=out;pricesCacheTime=Date.now();
}
app.get('/prices',(req,res)=>{refreshPricesFromMemory();res.json({success:true,prices:pricesCache,time:pricesCacheTime});});

app.get('/platform-signals/:email',async(req,res)=>{
  try {
    const user=await User.findOne({email:req.params.email});
    if (!user) return res.json({success:false,error:'Utilisateur non trouvé'});
    const exchangeId=user.exchangeName.toLowerCase();
    const exConfig=EXCHANGES_CONFIG.find(e=>e.id===exchangeId);
    const sigs=signalsByExchange[exchangeId]||[];
    const amount=user.tradeAmount||TRADE_AMOUNT;
    res.json({success:true,exchange:exConfig?.name||user.exchangeName,tradeAmount:amount,
      lastScan:lastScanTime,count:sigs.length,botMode:user.botMode||'classic',
      signals:sigs.map(s=>({...s,potentialGainUSD:+(amount*(s.tpPct||TP_PCT*100)/100).toFixed(4),potentialLossUSD:+(amount*SL_PCT).toFixed(4)}))});
  } catch(e){res.json({success:false,error:e.message});}
});

// Configurer les paramètres du bot IA
app.post('/ai-config',async(req,res)=>{
  try {
    const {email, aiTradeAmount, aiMaxTrades, aiDailyCapital} = req.body;
    if (!email) return res.json({success:false,error:'Email manquant'});

    // Garde-fous absolus
    const safeTradeAmount  = Math.min(Math.max(parseFloat(aiTradeAmount)||5, 1), 5);  // 1$ à 5$ max
    const safeMaxTrades    = Math.min(Math.max(parseInt(aiMaxTrades)||1, 1), 10);     // 1 à 10 max
    const safeDailyCapital = Math.min(Math.max(parseFloat(aiDailyCapital)||10, 5), 500); // 5$ à 500$

    await User.findOneAndUpdate({email},{
      aiTradeAmount:  safeTradeAmount,
      aiMaxTrades:    safeMaxTrades,
      aiDailyCapital: safeDailyCapital
    });

    console.log(`[IA Config] ${email} — ${safeTradeAmount}$/trade · ${safeMaxTrades} trades max · ${safeDailyCapital}$/jour`);
    res.json({success:true,
      aiTradeAmount:safeTradeAmount,
      aiMaxTrades:safeMaxTrades,
      aiDailyCapital:safeDailyCapital,
      message:`Config IA: ${safeTradeAmount}$/trade · ${safeMaxTrades} simultanés · ${safeDailyCapital}$/jour`
    });
  } catch(e){res.json({success:false,error:e.message});}
});

app.post('/save-xlm',async(req,res)=>{
  try {
    const {email,xlmWallet}=req.body;
    if (!email||!xlmWallet||!xlmWallet.startsWith('G')||xlmWallet.length<40)
      return res.json({success:false,error:'Adresse XLM invalide'});
    await User.findOneAndUpdate({email},{xlmWallet},{upsert:true});
    res.json({success:true,message:'Wallet XLM sauvegardé'});
  } catch(e){res.json({success:false,error:e.message});}
});

app.post('/disconnect',async(req,res)=>{
  try {
    const {email}=req.body;
    if (!email) return res.json({success:false,error:'Email manquant'});
    await User.findOneAndUpdate({email},{active:false});
    res.json({success:true,message:'Déconnecté'});
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

// ══════════════════════════════════════════════════════════════════════
// 🛡️ BOT WATCHDOG — Surveillance & Protection
// Tourne toutes les 30 secondes — surveille tout
// ══════════════════════════════════════════════════════════════════════

const watchdogAlerts = []; // historique des alertes
const tradeAttempts  = new Map(); // { key: [timestamps] } anti-boucle
const suspiciousIPs  = new Map(); // { ip: compteur }

// ── 1. Vérifier que le WebSocket Kraken est vivant
function watchWebSocket() {
  const wsOk     = wsTicker && wsTicker.readyState === 1;
  const wsOhlcOk = ws       && ws.readyState       === 1;
  const ws4hOk   = ws4h     && ws4h.readyState     === 1;
  const nPrices  = Object.keys(livePrices).length;

  if (!wsOk) {
    watchAlert('WEBSOCKET', 'Ticker WebSocket déconnecté — reconnexion');
    connectKrakenTicker(krakenPairsList);
  }
  if (!wsOhlcOk) {
    watchAlert('WEBSOCKET', 'OHLC Daily WebSocket déconnecté — reconnexion');
    connectKrakenWS(krakenPairsList);
  }
  if (!ws4hOk) {
    watchAlert('WEBSOCKET', 'OHLC 4h WebSocket déconnecté — reconnexion');
    connectKrakenWS4h(krakenPairsList);
  }
  if (nPrices === 0 && krakenPairsList.length > 0) {
    watchAlert('PRIX', 'livePrices vide — aucun prix reçu depuis le ticker');
  }
}

// ── 2. Vérifier les positions ouvertes (protocole SL/TP)
async function watchPositions() {
  try {
    const positions = await OpenPosition.find({});
    for (const pos of positions) {
      // Vérifier cohérence TP/SL
      if (pos.tp <= pos.entryPrice) {
        watchAlert('PROTOCOLE', `${pos.symbol} TP incohérent (${pos.tp} <= entrée ${pos.entryPrice}) — correction`);
        const correctedTP = +(pos.entryPrice * 1.15).toFixed(8);
        await OpenPosition.updateOne({ _id: pos._id }, { tp: correctedTP });
      }
      if (pos.sl >= pos.entryPrice) {
        watchAlert('PROTOCOLE', `${pos.symbol} SL incohérent (${pos.sl} >= entrée ${pos.entryPrice}) — correction`);
        const correctedSL = +(pos.entryPrice * (1 - SL_PCT)).toFixed(8);
        await OpenPosition.updateOne({ _id: pos._id }, { sl: correctedSL });
      }

      // Vérifier position orpheline (utilisateur inactif)
      const user = await User.findOne({ email: pos.email });
      if (!user || !user.active) {
        watchAlert('ORPHELINE', `Position orpheline ${pos.symbol} pour ${pos.email} — utilisateur inactif`);
        // Ne pas supprimer automatiquement — juste alerter
      }

      // Vérifier position trop ancienne (>7 jours sans TP/SL)
      const ageHours = (Date.now() - new Date(pos.openedAt).getTime()) / (1000 * 60 * 60);
      if (ageHours > 168) { // 7 jours
        watchAlert('POSITION', `${pos.symbol} ouverte depuis ${Math.floor(ageHours)}h — vérification recommandée`);
      }

      // Vérifier montant respecté (mode IA max 5$)
      if (pos.botMode === 'ai' && pos.amount > AI_MAX_TRADE + 0.01) {
        watchAlert('CAPITAL_IA', `${pos.symbol} montant IA ${pos.amount}$ > max autorisé ${AI_MAX_TRADE}$`);
      }
    }
  } catch(e) { console.log('[Watchdog] Erreur watchPositions:', e.message); }
}

// ── 3. Détecter les boucles de trade (même signal tenté trop souvent)
function watchTradeBoucle(symbol, exchangeId) {
  const key = symbol + '|' + exchangeId;
  const now = Date.now();
  if (!tradeAttempts.has(key)) tradeAttempts.set(key, []);
  const attempts = tradeAttempts.get(key);
  // Garder seulement les 60 dernières secondes
  const recent = attempts.filter(t => now - t < 60000);
  recent.push(now);
  tradeAttempts.set(key, recent);
  if (recent.length >= 5) {
    watchAlert('BOUCLE', `${symbol} tenté ${recent.length}x en 60s — possible boucle infinie`);
    return true; // bloquer
  }
  return false;
}

// ── 4. Détecter manipulation de prix (spike anormal)
function watchPriceManipulation(symbol, newPrice) {
  const candles = krakenCandles[symbol];
  if (!candles || candles.length < 10) return;
  const avgPrice = candles.slice(-10).reduce((a, c) => a + c.c, 0) / 10;
  const deviation = Math.abs(newPrice - avgPrice) / avgPrice;
  if (deviation > 0.15) { // +/-15% en un tick = suspect
    watchAlert('MANIPULATION', `${symbol} spike de prix détecté: ${(deviation*100).toFixed(1)}% d'écart vs moyenne`);
    // Invalider le prix suspect
    delete livePrices[symbol];
    return true;
  }
  return false;
}

// ── 5. Surveiller le capital IA journalier
async function watchIACapital() {
  try {
    const aiUsers = await User.find({ active: true, botMode: 'ai' });
    for (const user of aiUsers) {
      const dayStart = new Date(); dayStart.setHours(0,0,0,0);
      const dayTrades = await Trade.find({ email: user.email, botMode: 'ai', time: { $gte: dayStart } });
      const dayCapitalUsed = dayTrades.reduce((a,t) => a + (t.amount||0), 0);
      const limit = user.aiDailyCapital || 10;

      if (dayCapitalUsed > limit * 1.1) { // 10% de tolérance
        watchAlert('CAPITAL_IA', `${user.email} capital IA dépassé: $${dayCapitalUsed.toFixed(2)} / $${limit} autorisé`);
      }

      // Vérifier P&L négatif excessif
      const dayPnl = dayTrades.reduce((a,t) => a + (t.pnl||0), 0);
      const maxLoss = -(limit * AI_MAX_LOSS_DAY);
      if (dayPnl < maxLoss * 1.5) {
        watchAlert('CIRCUIT_BREAKER', `${user.email} pertes IA excessives: $${dayPnl.toFixed(2)} (max: $${maxLoss.toFixed(2)})`);
      }
    }
  } catch(e) { console.log('[Watchdog] Erreur watchIACapital:', e.message); }
}

// ── 6. Surveiller MongoDB (connexion active)
async function watchDatabase() {
  try {
    const state = mongoose.connection.readyState;
    // 0=déconnecté, 1=connecté, 2=connexion, 3=déconnexion
    if (state !== 1) {
      watchAlert('DATABASE', `MongoDB état anormal: ${state} — tentative de reconnexion`);
      await mongoose.connect(process.env.MONGODB_URI).catch(() => {});
    }
    // Vérifier accumulation de signaux (fuite mémoire potentielle)
    if (signalsCache.length > MAX_SIGNALS_CACHE * 1.5) {
      watchAlert('MEMOIRE', `Cache signaux trop grand: ${signalsCache.length} — nettoyage`);
      signalsCache.length = MAX_SIGNALS_CACHE;
    }
    // Vérifier recentSignals (fuite mémoire Map)
    if (recentSignals.size > 10000) {
      watchAlert('MEMOIRE', `recentSignals trop grand (${recentSignals.size}) — nettoyage`);
      // Supprimer les entrées de plus de 6h
      const cutoff = Date.now() - 6 * 60 * 60 * 1000;
      for (const [key, time] of recentSignals) {
        if (time < cutoff) recentSignals.delete(key);
      }
    }
  } catch(e) { console.log('[Watchdog] Erreur watchDatabase:', e.message); }
}

// ── 7. Vérifier la santé globale du bot
async function watchBotHealth() {
  try {
    const activeUsers = await User.countDocuments({ active: true, apiKey: { $exists: true } });
    const openPos     = await OpenPosition.countDocuments({});
    const nPrices     = Object.keys(livePrices).length;
    const wsOk        = wsTicker && wsTicker.readyState === 1;

    // Log santé toutes les 5 minutes
    console.log(`[Watchdog] ✅ Santé: ${activeUsers} users · ${openPos} positions · ${nPrices} prix live · WS:${wsOk?'OK':'KO'} · Alertes: ${watchdogAlerts.length}`);

    // Vérifier que les positions ont bien des prix live
    if (openPos > 0 && nPrices === 0) {
      watchAlert('CRITIQUE', `${openPos} positions ouvertes mais AUCUN prix live — TP/SL aveugle!`);
      // Forcer reconnexion ticker
      connectKrakenTicker(krakenPairsList);
    }

    // Vérifier positionsInProgress bloquées
    if (positionsInProgress.size > 10) {
      watchAlert('VERROU', `${positionsInProgress.size} positions en cours de traitement — possible blocage`);
      // Reset si bloqué depuis trop longtemps (normalement vide en quelques secondes)
    }
  } catch(e) { console.log('[Watchdog] Erreur watchBotHealth:', e.message); }
}

// ── Enregistrer une alerte Watchdog
function watchAlert(type, message) {
  const alert = { type, message, time: new Date() };
  watchdogAlerts.unshift(alert);
  if (watchdogAlerts.length > 200) watchdogAlerts.pop(); // garder 200 alertes max
  console.log(`[Watchdog ⚠ ${type}] ${message}`);
}

// ── Cycle principal du Watchdog (toutes les 30 secondes)
async function runWatchdog() {
  try {
    watchWebSocket();                  // WebSocket vivant ?
    await watchPositions();            // Positions cohérentes ?
    await watchIACapital();            // Capital IA respecté ?
    await watchDatabase();             // MongoDB et mémoire OK ?
    await watchBotHealth();            // Santé globale
  } catch(e) { console.log('[Watchdog] Erreur cycle:', e.message); }
}

// Route admin pour voir les alertes Watchdog
app.get('/admin/watchdog', (req, res) => {
  res.json({
    success:     true,
    alerts:      watchdogAlerts.slice(0, 50),
    count:       watchdogAlerts.length,
    wsStatus: {
      ticker: wsTicker?.readyState === 1 ? 'CONNECTED' : 'DISCONNECTED',
      ohlc1d: ws?.readyState       === 1 ? 'CONNECTED' : 'DISCONNECTED',
      ohlc4h: ws4h?.readyState     === 1 ? 'CONNECTED' : 'DISCONNECTED',
    },
    livePricesCount:    Object.keys(livePrices).length,
    recentSignalsCount: recentSignals.size,
    signalsCacheCount:  signalsCache.length,
    positionsInProgress: positionsInProgress.size,
    krakenPairs:         krakenPairsList.length,
  });
});

// ══════════════════════════════════════════════════════════════════════
// DÉMARRAGE
// ══════════════════════════════════════════════════════════════════════
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`\n🤖 Bender Pro v10.0 · Port ${PORT}`);
  console.log(` Mode Classique + Mode IA Autonome`);
  console.log(` 35 Plateformes · SL -2% · TP dynamique\n`);

  setImmediate(async () => {
    // TP/SL instantané (les deux modes)
    setTimeout(() => checkTPSLInstant().catch(console.error), 100);
    setInterval(() => checkTPSLInstant().catch(console.error), 2000);
    console.log(' TP/SL actif (2s)');

    // XLM toutes les 24h
    // Watchdog actif toutes les 30s
    setTimeout(() => runWatchdog().catch(console.error), 10000);
    setInterval(() => runWatchdog().catch(console.error), 30 * 1000);
    console.log(' Watchdog de surveillance actif');

    setTimeout(() => checkXlmPayments().catch(console.error), 5000);
    setInterval(() => checkXlmPayments().catch(console.error), 24*60*60*1000);

    // Vérification fin de journée IA (toutes les 5 minutes)
    setInterval(() => checkAIDayResult().catch(console.error), 5*60*1000);

    // Scan IA toutes les 15 minutes
    setInterval(() => runAIScan().catch(console.error), 15*60*1000);
    console.log(' Mode IA actif (scan toutes les 15min · reset quotidien à minuit)');

    // Kraken WebSocket
    krakenPairsList = await fetchKrakenUsdtPairs().catch(() => []);
    if (krakenPairsList.length > 0) {
      connectKrakenTicker(krakenPairsList);
      connectKrakenWS(krakenPairsList);
      connectKrakenWS4h(krakenPairsList);
      console.log(` ${krakenPairsList.length} paires Kraken · WebSocket actif`);
      preloadHistoricalCandles(krakenPairsList).then(() => {
        console.log(' Preloading terminé');
        scanAll().catch(console.error);
        // Premier scan IA après preloading
        runAIScan().catch(console.error);
      }).catch(console.error);
    }

    // Scan classique toutes les 60s
    setTimeout(() => setInterval(() => scanAll().catch(console.error), SCAN_INTERVAL), 65000);
  });
});
