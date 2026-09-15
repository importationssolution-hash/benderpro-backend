// Bender Pro v11.2 - Bot Classique Simple + Analyse 1 Robot
// npm install express cors mongoose ccxt helmet ws node-fetch stripe
const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const ccxt = require('ccxt');
const helmet = require('helmet');
const WebSocket = require('ws');
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY || '');
const fetch = (...args) => import('node-fetch').then(({default: f}) => f(...args));

const app = express();
app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors());

// IMPORTANT : la route webhook Stripe doit recevoir le corps BRUT (pas encore
// transforme en JSON) pour pouvoir verifier la signature. Elle est donc
// enregistree AVANT express.json() qui s'applique a toutes les autres routes.
app.post('/stripe-webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;
  try {
    event = stripe.webhooks.constructEvent(req.body, sig, process.env.STRIPE_WEBHOOK_SECRET || '');
  } catch (e) {
    console.log('[Stripe] Signature webhook invalide:', e.message);
    return res.status(400).send(`Webhook Error: ${e.message}`);
  }

  if (event.type === 'checkout.session.completed') {
    const session = event.data.object;
    const email = session.metadata?.email;
    const tier = session.metadata?.tier;
    if (email && tier) {
      const expires = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000);
      await User.findOneAndUpdate({ email }, { signalTier: tier, signalTierExpires: expires }, { upsert: true });
      await SignalPayment.findOneAndUpdate(
        { stripeSessionId: session.id },
        { status: 'PAID', paidAt: new Date() }
      );
      console.log(`[Stripe] Paiement confirme - ${email} - palier ${tier}`);
    }
  }
  res.json({ received: true });
});

app.use(express.json());

// ================================================================
// CONFIG GLOBALE - Parametres stricts fixes
// ================================================================
const TRADE_AMOUNT         = 5;
const SL_PCT               = 0.04;   // -4%
const VOL_CONFIRM          = 1.8;
const SCAN_INTERVAL        = 60 * 1000;
const MAX_PAIRS            = 500;
const MAX_SIGNALS_CACHE    = 200;
const MIN_DAILY_VOLUME_USD = 500000;
const SENTIMENT_MIN        = 60;     // 60% marche vert requis
const ENTRY_MAX_PCT        = 0.03;   // max 3% au-dessus resistance
const BLOCK_HOURS          = 100;    // blocage 100h apres trade

// ================================================================
// MONGODB
// ================================================================
mongoose.connect(process.env.MONGODB_URI)
  .then(() => console.log('MongoDB connecte!'))
  .catch(err => console.log('Erreur MongoDB:', err.message));

// ================================================================
// SCHEMAS
// ================================================================
const UserSchema = new mongoose.Schema({
  email:        { type: String, required: true, unique: true },
  exchangeName: String,
  apiKey:       String,
  apiSecret:    String,
  tradeAmount:  { type: Number, default: 5 },
  currency:     { type: String, default: 'USD' },
  active:       { type: Boolean, default: true },
  botMode:      { type: String, default: 'classic' },
  xlmWallet:    String,
  signalTier:         { type: String, default: null },     // starter | plus | pro | total
  signalTierExpires:  { type: Date, default: null },
  createdAt:    { type: Date, default: Date.now }
});

// Paiements d'abonnement aux signaux (carte via Stripe, ou crypto ETH/BTC)
const SignalPaymentSchema = new mongoose.Schema({
  email: String, tier: String, method: String, // 'stripe' | 'eth' | 'btc'
  amountUsd: Number, amountCrypto: Number, cryptoAddress: String,
  status: { type: String, default: 'PENDING' }, // PENDING | PAID | EXPIRED
  stripeSessionId: String, txHash: String,
  createdAt: { type: Date, default: Date.now },
  paidAt: Date,
});

const TradeSchema = new mongoose.Schema({
  email: String, symbol: String, exchange: String, market: String,
  direction: String, figure: String, entryPrice: Number, exitPrice: Number,
  amount: Number, pnl: Number, result: String, exitReason: String,
  timeframe: String, currency: String, botMode: String,
  estimatedDuration: String,
  resistanceBreakAt: Date,
  time: { type: Date, default: Date.now }
});

const SignalSchema = new mongoose.Schema({
  symbol: String, exchange: String, market: String, figure: String,
  direction: String, confidence: Number, entryPrice: Number,
  tp: Number, sl: Number, volumeRatio: Number, timeframe: String,
  resistanceBreakAt: Date,
  time: { type: Date, default: Date.now }
});

const OpenPositionSchema = new mongoose.Schema({
  email: String, symbol: String, exchange: String, exchangeId: String,
  figure: String, entryPrice: Number, tp: Number, sl: Number,
  tpPct: Number, figureTarget: Number, qty: Number, amount: Number,
  currency: String, timeframe: String, botMode: String,
  estimatedDuration: String,
  resistanceBreakAt: Date,
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
const SignalPayment = mongoose.model('SignalPayment', SignalPaymentSchema);

// ================================================================
// 35 PLATEFORMES
// ================================================================
const EXCHANGES_CONFIG = [
  { id:'kraken',      name:'Kraken',      geo:'BOTH',  currencies:['USD','CAD','EUR'], quoteFilter:['USD','CAD','EUR'], spot:true, futures:true,  ccxt:true  },
  { id:'coinbasepro', name:'Coinbase',    geo:'BOTH',  currencies:['USD','EUR','GBP'], quoteFilter:['USD','EUR'],       spot:true, futures:false, ccxt:true  },
  { id:'gemini',      name:'Gemini',      geo:'BOTH',  currencies:['USD','EUR'],       quoteFilter:['USD'],             spot:true, futures:false, ccxt:true  },
  { id:'bitbuy',      name:'Bitbuy',      geo:'CA',    currencies:['CAD'],             quoteFilter:['CAD'],             spot:true, futures:false, ccxt:true  },
  { id:'ndax',        name:'NDAX',        geo:'CA',    currencies:['CAD'],             quoteFilter:['CAD'],             spot:true, futures:false, ccxt:true  },
  { id:'binance',     name:'Binance',     geo:'WORLD', currencies:['USDT','USDC'],     quoteFilter:['USDT','USDC'],     spot:true, futures:true,  ccxt:true  },
  { id:'bybit',       name:'Bybit',       geo:'WORLD', currencies:['USDT','USDC'],     quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'bitget',      name:'Bitget',      geo:'WORLD', currencies:['USDT','USDC'],     quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'okx',         name:'OKX',         geo:'WORLD', currencies:['USDT','USDC'],     quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'kucoin',      name:'KuCoin',      geo:'WORLD', currencies:['USDT','BTC'],      quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'gateio',      name:'Gate.io',     geo:'WORLD', currencies:['USDT','USDC'],     quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'mexc',        name:'MEXC',        geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'bitfinex',    name:'Bitfinex',    geo:'WORLD', currencies:['USD','USDT'],      quoteFilter:['USD','USDT'],      spot:true, futures:true,  ccxt:true  },
  { id:'bitstamp',    name:'Bitstamp',    geo:'WORLD', currencies:['USD','EUR'],       quoteFilter:['USD','EUR'],       spot:true, futures:false, ccxt:true  },
  { id:'huobi',       name:'Huobi',       geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  // -- Entrees ajoutees pour le scan multi-exchange (id = id ccxt exact, utilise par scanMultiExchange) --
  { id:'coinbase',    name:'Coinbase',    geo:'BOTH',  currencies:['USD'],             quoteFilter:['USD'],             spot:true, futures:false, ccxt:true  },
  { id:'gate',        name:'Gate.io',     geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'htx',         name:'HTX',         geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'bingx',       name:'BingX',       geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'phemex',      name:'Phemex',      geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'cryptocom',   name:'Crypto.com',  geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'poloniex',    name:'Poloniex',    geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'deepcoin',    name:'Deepcoin',    geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'xt',          name:'XT.com',      geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'toobit',      name:'Toobit',      geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'lbank',       name:'LBank',       geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'deribit',     name:'Deribit',     geo:'WORLD', currencies:['USDC'],            quoteFilter:['USDC'],            spot:false,futures:true,  ccxt:true  },
  { id:'woo',         name:'WOO X',       geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:true,  ccxt:true  },
  { id:'bitrue',      name:'Bitrue',      geo:'WORLD', currencies:['USDT'],            quoteFilter:['USDT'],            spot:true, futures:false, ccxt:true  },
];

// ================================================================
// INDICATEURS TECHNIQUES
// ================================================================
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

// ================================================================
// FIGURES CHARTISTES - Parametres fixes stricts
// ================================================================
const FIGURES = [
  { name:'Cup & Handle',     code:'C&H',   dir:'Long', wr:0.84 },
  { name:'ETE Inverse',      code:'ETEi',  dir:'Long', wr:0.81 },
  { name:'Double Bottom',    code:'2Bot',  dir:'Long', wr:0.76 },
  { name:'Triangle Asc.',    code:'TriA',  dir:'Long', wr:0.74 },
  { name:'Drapeau Haussier', code:'DrapH', dir:'Long', wr:0.76 },
  { name:'Biseau Baissier',  code:'BisB',  dir:'Long', wr:0.73 },
];

// Duree estimee par timeframe
function estimateDuration(timeframe) {
  if (timeframe === '15m') return '30 minutes a 3 heures';
  if (timeframe === '1h') return '2 a 8 heures';
  if (timeframe === '4h') return '1 a 4 jours';
  return '5 a 20 jours';
}

function detectFigure(closes, volumes, livePrice, aiParams) {
  if (closes.length < 50) return null;
  const RSI_MIN    = (aiParams && aiParams.rsiMin)    || 33;
  const RSI_MAX    = (aiParams && aiParams.rsiMax)    || 72;
  const RSI_PERIOD = (aiParams && aiParams.rsiPeriod) || 14;
  const EMA_FAST   = (aiParams && aiParams.emaFast)   || 20;
  const EMA_SLOW   = (aiParams && aiParams.emaSlow)   || 50;
  const ADX_MIN    = (aiParams && aiParams.adxMin!=null) ? aiParams.adxMin : 20;
  const n = closes.length;
  const price = livePrice || closes[n-1];
  const volNow = volumes[n-1];
  const volAvg = avg(volumes.slice(-50));
  const volRatio = volNow / volAvg;
  if (volRatio < VOL_CONFIRM) return null;
  const rsi = calcRSI(closes, RSI_PERIOD);
  if (rsi < RSI_MIN || rsi > RSI_MAX) return null;
  const ema20 = calcEMA(closes.slice(-Math.max(EMA_FAST*3,60)), Math.min(EMA_FAST, closes.length-1));
  const ema50 = calcEMA(closes.slice(-Math.max(EMA_SLOW*3,100)), Math.min(EMA_SLOW, closes.length-1));
  if (ema20 <= ema50) return null;
  const adx = calcADX(closes);
  if (adx < ADX_MIN) return null;
  const slice = closes.slice(-150);
  const high = Math.max(...slice);
  const low  = Math.min(...slice);
  const figureTarget = (high - low) / low;
  if (figureTarget < 0.40) return null;
  let tpPct;
  if      (figureTarget >= 0.80) tpPct = 0.20;
  else if (figureTarget >= 0.71) tpPct = 0.17;
  else if (figureTarget >= 0.61) tpPct = 0.15;
  else                           tpPct = 0.13;
  function buildLevels() {
    return { tp: +(price*(1+tpPct)).toFixed(8), sl: +(price*(1-SL_PCT)).toFixed(8) };
  }
  if (n >= 100) {
    const cupLow = Math.min(...closes.slice(n-60,n-20));
    const resistance = Math.max(...closes.slice(n-30,n-1));
    if (cupLow < closes[n-70]*0.95 && price > resistance && volRatio > 1.8)
      return { fig:FIGURES[0], ...buildLevels(), figureTarget, tpPct, volRatio };
  }
  if (n >= 100) {
    const headLow = Math.min(...closes.slice(n-60,n-20));
    const shoulderLow = Math.min(...closes.slice(n-80,n-60));
    const neckline = Math.max(...closes.slice(n-80,n-2));
    if (headLow < shoulderLow*0.97 && price > neckline && volRatio > 1.5)
      return { fig:FIGURES[1], ...buildLevels(), figureTarget, tpPct, volRatio };
  }
  if (n >= 70) {
    const bot1 = Math.min(...closes.slice(n-50,n-25));
    const bot2 = Math.min(...closes.slice(n-25,n-1));
    const midTop = Math.max(...closes.slice(n-40,n-10));
    if (Math.abs(bot1-bot2)/bot1 < 0.02 && price > midTop && volRatio > 1.4)
      return { fig:FIGURES[2], ...buildLevels(), figureTarget, tpPct, volRatio };
  }
  const range = (high - low) / price;
  const trend10 = closes[n-51] ? (price - closes[n-51]) / closes[n-51] : 0;
  if (range < 0.04 && trend10 > 0.01 && price > high*0.999 && volRatio > 1.6)
    return { fig:FIGURES[3], ...buildLevels(), figureTarget, tpPct, volRatio };
  if (trend10 > 0.06 && range < 0.025 && price > high*0.999 && volRatio > 1.8)
    return { fig:FIGURES[4], ...buildLevels(), figureTarget, tpPct, volRatio };
  if (range < 0.035 && trend10 < -0.02 && trend10 > -0.05 && price > high*0.999 && volRatio > 1.7)
    return { fig:FIGURES[5], ...buildLevels(), figureTarget, tpPct, volRatio };
  return null;
}

// ================================================================
// WEBSOCKET KRAKEN - connexions permanentes
// ================================================================
const krakenCandles    = {};
const krakenCandles1h  = {};
const krakenCandles4h  = {};
const krakenCandles15m = {};
let krakenPairsList   = [];
let wsConnected       = false;
let ws = null, ws1h = null, ws4h = null, ws15m = null, wsTicker = null;
const livePrices      = {};
const pairDailyVolume = {};
const breakoutConfirm = {};
const recentSignals   = new Map();
const QUOTE_CURRENCIES = ['USD'];

function getMarketSentiment() {
  let up = 0, total = 0;
  for (const symbol of krakenPairsList) {
    const candles = krakenCandles[symbol];
    const livePrice = livePrices[symbol];
    if (!candles || candles.length < 2 || !livePrice) continue;
    const prev = candles[candles.length-2];
    if (!prev || !prev.c) continue;
    total++;
    if (livePrice > prev.c) up++;
  }
  if (total < 10) return 100;
  return Math.round(up / total * 100);
}

function getTimeBeforeExpiry(resistanceBreakAt, timeframe) {
  if (!resistanceBreakAt) return null;
  const tfMs = timeframe === '15m' ? 900000 : timeframe === '1h' ? 3600000 : timeframe === '4h' ? 14400000 : 86400000;
  const expiryAt = new Date(resistanceBreakAt).getTime() + tfMs;
  const msLeft = expiryAt - Date.now();
  if (msLeft <= 0) return 'Expire';
  const h = Math.floor(msLeft / 3600000);
  const m = Math.floor((msLeft % 3600000) / 60000);
  const s = Math.floor((msLeft % 60000) / 1000);
  return (h > 0 ? h + 'h ' : '') + (m > 0 ? m + 'm ' : '') + s + 's';
}

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
    console.log(`[Ticker] Connecte - ${pairs.length} paires`);
    for (let i=0;i<pairs.length;i+=50)
      wsTicker.send(JSON.stringify({method:'subscribe',params:{channel:'ticker',symbol:pairs.slice(i,i+50)}}));
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
          }
        }
      }
    } catch(e) {}
  });
  wsTicker.on('close',()=>{ setTimeout(()=>connectKrakenTicker(krakenPairsList),5000); });
  wsTicker.on('error', (err) => { console.log('[Ticker] Erreur:', err.message); });
}

function handleOhlcMessage(raw, store, tf) {
  try {
    const msg = JSON.parse(raw);
    if (msg.channel==='ohlc' && (msg.type==='snapshot'||msg.type==='update') && msg.data) {
      for (const c of msg.data) {
        const sym = c.symbol;
        const arr = store[sym] || (store[sym] = []);
        const last = arr[arr.length-1];
        const isNewCandle = !last || (c.timestamp && last.ts && c.timestamp !== last.ts);
        if (!isNewCandle) {
          if (last) { last.c = c.close; last.v = c.volume; }
        } else {
          const closedClose = last ? last.c : null;
          arr.push({ c: c.close, v: c.volume, ts: c.timestamp });
          if (arr.length > 500) arr.shift();
          const key = sym + '|' + tf;
          const bc  = breakoutConfirm[key] || breakoutConfirm[sym];
          if (bc && closedClose !== null && closedClose > bc.resistance) {
            console.log(`[CASSURE ${tf}] ${sym} - ${bc.figure} - ${closedClose.toFixed(4)} > ${bc.resistance.toFixed(4)} -> ORDRE`);
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

function connectKrakenWS(pairs) {
  if (ws) { try { ws.terminate(); } catch(e) {} }
  ws = new WebSocket('wss://ws.kraken.com/v2');
  ws.on('open', () => {
    wsConnected = true;
    console.log(`WebSocket Kraken - ${pairs.length} paires`);
    for (let i=0;i<pairs.length;i+=50)
      ws.send(JSON.stringify({method:'subscribe',params:{channel:'ohlc',symbol:pairs.slice(i,i+50),interval:1440}}));
  });
  ws.on('message', (raw) => handleOhlcMessage(raw, krakenCandles, '1d'));
  ws.on('close',()=>{ wsConnected=false; setTimeout(()=>connectKrakenWS(krakenPairsList),15000); });
  ws.on('error', (err) => { console.log('Erreur WS:', err.message); });
}

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

function connectKrakenWS15m(pairs) {
  if (ws15m) { try { ws15m.terminate(); } catch(e) {} }
  ws15m = new WebSocket('wss://ws.kraken.com/v2');
  ws15m.on('open', () => {
    console.log(`[WS-15M] ${pairs.length} paires`);
    let i = 0;
    const send = () => {
      if (i >= pairs.length || ws15m.readyState !== 1) return;
      ws15m.send(JSON.stringify({ method:'subscribe', params:{ channel:'ohlc', symbol:pairs.slice(i,i+25), interval:15 }}));
      i += 25;
      if (i < pairs.length) setTimeout(send, 200);
    };
    send();
  });
  ws15m.on('message', (raw) => handleOhlcMessage(raw, krakenCandles15m, '15m'));
  ws15m.on('close', () => { setTimeout(() => connectKrakenWS15m(krakenPairsList), 5000); });
  ws15m.on('error', (e) => console.log('[WS-15M]', e.message));
}

function tfToMs(tf) {
  return { '15m': 900000, '1h': 3600000, '4h': 14400000, '1d': 86400000 }[tf] || 86400000;
}

function storeForTimeframe(tf) {
  const t = (tf||'1d').toLowerCase();
  if (t==='15m'||t==='15'||t==='1') return krakenCandles15m;
  if (t==='1h'||t==='60') return krakenCandles1h;
  if (t==='4h'||t==='240') return krakenCandles4h;
  return krakenCandles;
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

// ================================================================
// MULTI-EXCHANGE (polling REST via ccxt) - extension au-dela de Kraken
// ================================================================
// Liste des plateformes a scanner en plus de Kraken (id ccxt + devise de cotation).
// Ces 23 plateformes ont ete verifiees comme reellement supportees par ccxt.
// maxPairs volontairement modere (50) vu le nombre de plateformes -- a ajuster
// selon la charge observee en production (rate limits, temps de cycle).
//
// DESACTIVEES -- blocage geographique Canada (confirme en production) :
// Binance et Bybit bloquent l'acces depuis des IP canadiennes suite a des
// actions reglementaires (Commission des valeurs mobilieres de l'Ontario).
// Bitget semble avoir un blocage similaire. Meme le serveur (pas seulement
// l'utilisateur final) est bloque si son IP est identifiee comme canadienne.
// Pour les reactiver: utiliser un proxy/VPN sortant hors Canada pour ces 3
// exchanges specifiquement, ou heberger le serveur hors Canada.
const MULTI_EXCHANGES_DISABLED_GEO_BLOCKED = [
  { id: 'binance', quote: 'USDT', maxPairs: 50 },
  { id: 'bybit',   quote: 'USDT', maxPairs: 50 },
  { id: 'bitget',  quote: 'USDT', maxPairs: 50 },
];

const MULTI_EXCHANGES = [
  { id: 'okx',       quote: 'USDT', maxPairs: 50 },
  { id: 'coinbase',  quote: 'USD',  maxPairs: 50 },
  { id: 'kucoin',    quote: 'USDT', maxPairs: 50 },
  { id: 'gate',      quote: 'USDT', maxPairs: 50 },
  { id: 'mexc',      quote: 'USDT', maxPairs: 50 },
  { id: 'bingx',     quote: 'USDT', maxPairs: 50 },
  { id: 'phemex',    quote: 'USDT', maxPairs: 50 },
  { id: 'htx',       quote: 'USDT', maxPairs: 50 },
  { id: 'cryptocom', quote: 'USDT', maxPairs: 50 },
  { id: 'bitstamp',  quote: 'USD',  maxPairs: 50 },
  { id: 'bitfinex',  quote: 'USD',  maxPairs: 50 },
  { id: 'poloniex',  quote: 'USDT', maxPairs: 50 },
  { id: 'deepcoin',  quote: 'USDT', maxPairs: 50 },
  { id: 'xt',        quote: 'USDT', maxPairs: 50 },
  { id: 'toobit',    quote: 'USDT', maxPairs: 50 },
  { id: 'lbank',     quote: 'USDT', maxPairs: 50 },
  { id: 'deribit',   quote: 'USDC', maxPairs: 50 }, // Deribit = surtout options/futures, peu de paires spot attendues
  { id: 'woo',       quote: 'USDT', maxPairs: 50 },
  { id: 'bitrue',    quote: 'USDT', maxPairs: 50 },
  { id: 'ndax',      quote: 'CAD',  maxPairs: 50 },
  // NON supportees par ccxt (necessitent une integration API separee) :
  // bitmart, ascendex, coinw, pionex, bitbuy, newton, coinsquare, coinberry, shakepay.
];

const POLL_INTERVAL_MS = 5 * 60 * 1000; // 5 min entre chaque cycle. Chaque cycle sonde UN timeframe
// (rotation 15m->1h->4h->1d, voir MULTI_TIMEFRAMES) sur les 23 plateformes -- meme charge reseau
// qu'avant, juste repartie sur les 4 timeframes au lieu de tout faire d'un coup. Rotation complete
// (les 4 timeframes) toutes les ~20 min. Ajuster si Render montre des signes de surcharge.
const multiExchangeCandles = {}; // { exchangeId: { '1d': { symbol: [candles] }, '4h': {...}, ... } }
const multiExchangeInstances = {}; // instances ccxt reutilisees (rate limit interne par exchange)
let multiExchangePairs = {}; // { exchangeId: [symbols] }

function getExchangeInstance(exchangeId) {
  if (!multiExchangeInstances[exchangeId]) {
    const ExClass = ccxt[exchangeId];
    if (!ExClass) return null;
    multiExchangeInstances[exchangeId] = new ExClass({ enableRateLimit: true, timeout: 15000 });
  }
  return multiExchangeInstances[exchangeId];
}

async function discoverPairs(exchangeId, quote, maxPairs) {
  const ex = getExchangeInstance(exchangeId);
  if (!ex) return [];
  try {
    const markets = await ex.loadMarkets();
    const pairs = Object.keys(markets).filter(s => {
      const m = markets[s];
      return s.endsWith('/' + quote) && m.active !== false && (m.spot === true || m.type === 'spot');
    });
    return pairs.slice(0, maxPairs);
  } catch (e) {
    console.log(`[Multi-${exchangeId}] Erreur discoverPairs:`, e.message);
    return [];
  }
}

// Sonde un exchange pour un timeframe donne, pour toutes ses paires configurees.
async function pollExchangeTimeframe(exchangeId, symbols, tf, store) {
  const ex = getExchangeInstance(exchangeId);
  if (!ex) return;
  if (!store[tf]) store[tf] = {};
  const BATCH = 10;
  for (let i = 0; i < symbols.length; i += BATCH) {
    const batch = symbols.slice(i, i + BATCH);
    await Promise.all(batch.map(async (symbol) => {
      try {
        const ohlcv = await ex.fetchOHLCV(symbol, tf, undefined, 200);
        if (ohlcv && ohlcv.length >= 20) {
          store[tf][symbol] = ohlcv.map(c => ({ c: c[4], v: c[5], ts: c[0] }));
        }
      } catch (e) { /* paire indisponible ou erreur reseau -> on ignore, on reessaie au prochain cycle */ }
    }));
    await new Promise(r => setTimeout(r, ex.rateLimit || 200));
  }
}

// Cycle complet : a chaque appel, on sonde UN SEUL timeframe pour toutes les
// plateformes (rotation 15m -> 1h -> 4h -> 1d -> 15m -> ...). Ca garde la charge
// reseau/CPU constante (equivalente a avant) tout en couvrant les 4 timeframes
// sur une fenetre glissante, au lieu de tout sonder d'un coup et risquer de
// surcharger Render (timeouts, memoire, rate limits cumules).
const MULTI_TIMEFRAMES = ['15m', '1h', '4h', '1d'];
let multiTimeframeRotationIndex = 0;

async function pollAllMultiExchanges() {
  const tf = MULTI_TIMEFRAMES[multiTimeframeRotationIndex];
  multiTimeframeRotationIndex = (multiTimeframeRotationIndex + 1) % MULTI_TIMEFRAMES.length;
  console.log(`[Multi] Debut du cycle de sondage - timeframe: ${tf}`);

  for (const cfg of MULTI_EXCHANGES) {
    try {
      if (!multiExchangePairs[cfg.id] || !multiExchangePairs[cfg.id].length) {
        multiExchangePairs[cfg.id] = await discoverPairs(cfg.id, cfg.quote, cfg.maxPairs);
        console.log(`[Multi-${cfg.id}] ${multiExchangePairs[cfg.id].length} paires /${cfg.quote} decouvertes`);
      }
      if (!multiExchangeCandles[cfg.id]) multiExchangeCandles[cfg.id] = {};
      await pollExchangeTimeframe(cfg.id, multiExchangePairs[cfg.id], tf, multiExchangeCandles[cfg.id]);
      console.log(`[Multi-${cfg.id}] Sondage ${tf} termine - ${Object.keys(multiExchangeCandles[cfg.id][tf]||{}).length} paires en memoire`);
    } catch (e) {
      console.log(`[Multi-${cfg.id}] Erreur cycle (${tf}):`, e.message);
    }
  }
  console.log(`[Multi] Cycle ${tf} termine. Prochain timeframe: ${MULTI_TIMEFRAMES[multiTimeframeRotationIndex]}`);
}

// Scanne un exchange (autre que Kraken) pour detecter des signaux, meme logique
// que scanKrakenTimeframe mais generalisee a n'importe quelle plateforme ccxt.
function scanMultiExchange(exchangeId, timeframeLabel, aiParams) {
  const store = (multiExchangeCandles[exchangeId] || {})[timeframeLabel] || {};
  const symbols = Object.keys(store);
  const results = [];
  const exConfig = EXCHANGES_CONFIG.find(e => e.id === exchangeId) || { name: exchangeId };
  for (const symbol of symbols) {
    const candles = store[symbol];
    if (!candles || candles.length < 20) continue;
    const closes = candles.filter(c => c.c > 0).map(c => c.c);
    const volumes = candles.filter(c => c.v > 0).map(c => c.v);
    if (closes.length < 20) continue;
    const price = closes[closes.length - 1];
    const sig = detectFigure(closes, volumes, price, aiParams);
    if (!sig) continue;
    const volRatio = volumes[volumes.length - 1] / avg(volumes.slice(-50));
    results.push({
      symbol, exchange: exConfig.name, exchangeId, timeframe: timeframeLabel, market: 'Spot',
      figure: sig.fig.name, figureCode: sig.fig.code, direction: sig.fig.dir,
      confidence: Math.round(sig.fig.wr * 100), reliable: sig.fig.wr >= 0.65,
      entryPrice: price, tp: sig.tp, sl: sig.sl,
      tpPct: +(sig.tpPct * 100).toFixed(1), slPct: +(SL_PCT * 100).toFixed(1),
      figureTarget: +(sig.figureTarget * 100).toFixed(1),
      volumeRatio: volRatio.toFixed(2), tradeAmount: TRADE_AMOUNT,
      estimatedDuration: estimateDuration(timeframeLabel),
      gain: (TRADE_AMOUNT * sig.tpPct).toFixed(4), time: new Date()
    });
  }
  return results;
}

// Scanne TOUS les exchanges multi-plateformes configures (agrege les resultats).
// timeframeLabel: '1d' (defaut) | '4h' | '1h' | '15m' -- utilise les donnees
// disponibles pour ce timeframe precis (alimentees par la rotation de sondage).
// Si aucune donnee n'est encore disponible pour ce timeframe (rotation pas encore
// passee dessus), retourne simplement un tableau vide pour cette plateforme --
// pas d'erreur, juste "pas encore de signal a montrer pour ce timeframe".
function scanAllMultiExchanges(aiParams, timeframeLabel) {
  const tf = timeframeLabel || '1d';
  let all = [];
  for (const cfg of MULTI_EXCHANGES) {
    all = all.concat(scanMultiExchange(cfg.id, tf, aiParams));
  }
  return all;
}

async function preloadHistoricalCandles(pairs) {
  console.log(`Preloading historique - ${pairs.length} paires...`);
  const exchange = new ccxt.kraken({ enableRateLimit: true, timeout: 20000 });
  const BATCH = 20;
  for (let i = 0; i < pairs.length; i += BATCH) {
    const batch = pairs.slice(i, i + BATCH);
    await Promise.all(batch.map(async (symbol) => {
      try { const d = await fetchMaxHistory(exchange, symbol, '1d', 5); if (d.length >= 10) krakenCandles[symbol] = d; } catch(e) {}
      try { const h4 = await fetchMaxHistory(exchange, symbol, '4h', 3); if (h4.length >= 10) krakenCandles4h[symbol] = h4; } catch(e) {}
      try { const h1 = await fetchMaxHistory(exchange, symbol, '1h', 2); if (h1.length >= 10) krakenCandles1h[symbol] = h1; } catch(e) {}
      try { const m15 = await fetchMaxHistory(exchange, symbol, '15m', 2); if (m15.length >= 10) krakenCandles15m[symbol] = m15; } catch(e) {}
    }));
    const done = Math.min(i + BATCH, pairs.length);
    console.log(`Preloading... ${done}/${pairs.length}`);
    if (done < pairs.length) await new Promise(r => setTimeout(r, 800));
  }
  console.log('Preloading termine!');
}

// ================================================================
// ANALYSE CONTINUE - 1 seul robot analyse en permanence
// ================================================================
const signalsCache = [];
let lastScanTime = null;
let latestRobotAnalyses = [];
let lastAnalysisTime = null;

function getMarketScore(symbol, closes, volumes, livePrice) {
  const rsi = calcRSI(closes);
  const ema20 = calcEMA(closes.slice(-60), Math.min(20, closes.length-1));
  const ema50 = calcEMA(closes.slice(-100), Math.min(50, closes.length-1));
  const adx = calcADX(closes);
  const volRatio = volumes[volumes.length-1] / avg(volumes.slice(-50));
  const sentiment = getMarketSentiment();
  let score = 0;
  if (rsi >= 33 && rsi <= 72) score += 25;
  if (ema20 > ema50) score += 20;
  if (adx >= 20) score += 20;
  if (volRatio >= 1.8) score += 20;
  if (sentiment >= 60) score += 15;
  return { score, rsi: +rsi.toFixed(1), adx: +adx.toFixed(1), volRatio: +volRatio.toFixed(2), sentiment, ema: ema20 > ema50 ? 'Haussiere' : 'Baissiere' };
}

async function runRobotAnalysis() {
  if (Object.keys(krakenCandles).length < 10) return;
  const analyses = [];
  const symbols = Object.keys(krakenCandles).filter(s => livePrices[s] && krakenCandles[s]?.length >= 100);

  for (let i = 0; i < symbols.length; i++) {
    if (i % 100 === 0) await new Promise(r => setImmediate(r));
    const symbol = symbols[i];
    const candles = krakenCandles[symbol];
    const livePrice = livePrices[symbol];
    if (!candles || !livePrice || livePrice < 0.001) continue;
    const closes = candles.map(c => c.c);
    const volumes = candles.map(c => c.v);
    const dv = pairDailyVolume[symbol] || 0;
    if (dv > 0 && dv < MIN_DAILY_VOLUME_USD) continue;
    const metrics = getMarketScore(symbol, closes, volumes, livePrice);
    const sig = detectFigure(closes, volumes, livePrice);
    if (!sig) continue;
    const resistance = Math.max(...closes.slice(-30));
    const pctAbove = ((livePrice - resistance) / resistance * 100);
    const breakAt = breakoutConfirm[symbol + '|1d']?.breakAt || breakoutConfirm[symbol]?.breakAt || null;
    const timeLeft = getTimeBeforeExpiry(breakAt, '1d');
    analyses.push({
      symbol, figure: sig.fig.name, score: metrics.score,
      recommendation: metrics.score >= 80 ? 'FORT' : metrics.score >= 60 ? 'MODERE' : 'FAIBLE',
      entryPrice: livePrice, tp: sig.tp, sl: sig.sl,
      tpPct: +(sig.tpPct*100).toFixed(1), slPct: 4,
      figureTarget: +(sig.figureTarget*100).toFixed(1),
      estimatedDuration: estimateDuration('1d'),
      pctAboveResistance: +pctAbove.toFixed(2),
      timeBeforeExpiry: timeLeft,
      indicators: metrics, timestamp: new Date()
    });
  }
  analyses.sort((a, b) => b.score - a.score);
  latestRobotAnalyses = analyses.slice(0, 50);
  lastAnalysisTime = new Date();
  console.log(`[Robot] ${analyses.length} signaux - Meilleur: ${analyses[0]?.symbol||'-'} - Score: ${analyses[0]?.score||0}/100`);
}

// ================================================================
// SCAN - Detection et cassure
// ================================================================
function scanSinglePair(symbol, timeframe='1d', exchangeId='kraken', candleStoreOverride=null) {
  try {
    const candleStore = candleStoreOverride || krakenCandles;
    const candles = candleStore[symbol];
    if (!candles || candles.length < 100) return;
    const closes  = candles.map(c=>c.c);
    const volumes = candles.map(c=>c.v);
    const livePrice = livePrices[symbol];
    if (!livePrice || livePrice < 0.001) return;
    if (!pairDailyVolume[symbol]) {
      const rv = candles.slice(-30).map(b=>b.c*b.v).filter(v=>v>0);
      if (rv.length > 0) pairDailyVolume[symbol] = rv.reduce((a,b)=>a+b,0)/rv.length;
    }
    const dv = pairDailyVolume[symbol] || 0;
    if (dv > 0 && dv < MIN_DAILY_VOLUME_USD) return;
    const sentiment = getMarketSentiment();
    if (sentiment < SENTIMENT_MIN) return;
    const sig = detectFigure(closes, volumes, livePrice);
    if (!sig) return;
    const sigKey = symbol+'|'+sig.fig.name+'|'+timeframe;
    const lastSigTime = recentSignals.get(sigKey);
    if (lastSigTime && Date.now()-lastSigTime < 60*60*1000) return;
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
      estimatedDuration: estimateDuration(timeframe),
      time:new Date()
    };
    const idx = signalsCache.findIndex(s=>s.symbol===symbol&&s.exchangeId===exchangeId);
    if (idx>=0) signalsCache[idx]=signal;
    else if (signalsCache.length<MAX_SIGNALS_CACHE) signalsCache.push(signal);
    new Signal({ symbol, exchange:exConfig.name, market:'Spot', figure:sig.fig.name,
      direction:sig.fig.dir, confidence:signal.confidence, entryPrice:livePrice,
      tp:sig.tp, sl:sig.sl, volumeRatio:volRatio, timeframe,
      resistanceBreakAt: new Date() }).save().catch(()=>{});
    const resistance = Math.max(...closes.slice(-30));
    const pctAbove = (livePrice - resistance) / resistance;
    if (pctAbove > ENTRY_MAX_PCT) return;
    const bcKey = symbol + '|' + timeframe;
    if (!breakoutConfirm[bcKey] || breakoutConfirm[bcKey].figure !== sig.fig.name) {
      breakoutConfirm[bcKey] = { resistance, figure:sig.fig.name, signal, breakAt: new Date() };
      console.log(`[Figure ${timeframe}] ${symbol} - ${sig.fig.name} - Resistance ${resistance.toFixed(4)}`);
    }
    setTimeout(() => {
      if (breakoutConfirm[bcKey]?.figure === sig.fig.name) delete breakoutConfirm[bcKey];
    }, 7*24*60*60*1000);
  } catch(e) {}
}

// ================================================================
// EXECUTION DU TRADE
// ================================================================
async function executeTrade(signal) {
  console.log(`[PAUSE] Trade ignore: ${signal.symbol} - ${signal.figure} - Trading suspendu`);
  return;

  try {
    const exchangeId = signal.exchangeId||'kraken';
    const exConfig = EXCHANGES_CONFIG.find(e=>e.id===exchangeId);
    const users = await User.find({
      active:true, apiKey:{$exists:true},
      exchangeName:new RegExp(exchangeId,'i'),
      botMode:'classic'
    });
    for (const user of users) {
      try {
        const existingPos = await OpenPosition.findOne({email:user.email,symbol:signal.symbol,exchangeId});
        if (existingPos) continue;
        const recentTrade = await Trade.findOne({email:user.email,symbol:signal.symbol,exchange:signal.exchange,time:{$gte:new Date(Date.now()-BLOCK_HOURS*60*60*1000)}});
        if (recentTrade) continue;
        if (!exConfig||!exConfig.ccxt) continue;
        const ExClass = ccxt[exchangeId];
        if (!ExClass) continue;
        const exchange = new ExClass({apiKey:user.apiKey,secret:user.apiSecret,enableRateLimit:true});
        const balance = await exchange.fetchBalance();
        const currency = user.currency||exConfig.currencies[0];
        const available = balance[currency]?.free||0;
        const amount = Math.max(user.tradeAmount||TRADE_AMOUNT,5);
        if (available < amount) {
          console.log(`[Trade] Fonds insuffisants: ${available}${currency} < ${amount}$ - Signal expire: ${getTimeBeforeExpiry(signal.resistanceBreakAt||new Date(), signal.timeframe)}`);
          continue;
        }
        const qty = amount/signal.entryPrice;
        const orderParams = {};
        if (exchangeId==='kraken') orderParams.oflags='fciq';
        console.log(`[Trade] BUY ${signal.symbol} - ${signal.figure} - ${amount}${currency}`);
        await exchange.createOrder(signal.symbol,'market','buy',qty,undefined,orderParams);
        await new OpenPosition({email:user.email,symbol:signal.symbol,exchange:signal.exchange,exchangeId,
          figure:signal.figure,entryPrice:signal.entryPrice,tp:signal.tp,sl:signal.sl,
          tpPct:signal.tpPct,figureTarget:signal.figureTarget,qty,amount,currency,
          timeframe:signal.timeframe,estimatedDuration:signal.estimatedDuration,
          resistanceBreakAt:signal.resistanceBreakAt||new Date(),botMode:'classic'}).save();
        await new Trade({email:user.email,symbol:signal.symbol,exchange:signal.exchange,market:'Spot',
          direction:signal.direction,figure:signal.figure,entryPrice:signal.entryPrice,
          exitPrice:null,amount,pnl:0,currency,result:'OPEN',
          exitReason:'Position ouverte - en attente TP/SL',botMode:'classic',
          estimatedDuration:signal.estimatedDuration,
          resistanceBreakAt:signal.resistanceBreakAt||new Date()}).save();
      } catch(e) { console.log(`[Trade] Erreur ${signal.symbol}:`,e.message); }
    }
  } catch(e) { console.log('[executeTrade] Erreur:',e.message); }
}

// ================================================================
// TP/SL - Verification toutes les 2 secondes
// ================================================================
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
        await exchange.createOrder(pos.symbol,'market','sell',baseBalance,undefined,orderParams);
        const posTpPct = pos.tpPct ? pos.tpPct/100 : 0.12;
        const pnl = hitTP ? pos.amount*posTpPct : -(pos.amount*SL_PCT);
        await Trade.findOneAndUpdate({email:pos.email,symbol:pos.symbol,result:'OPEN'},
          {exitPrice:currentPrice,pnl,result:hitTP?'WIN':'LOSS',
           exitReason:hitTP?`TP +${pos.tpPct}% atteint`:'SL -4% touche'},{sort:{time:-1}});
        await OpenPosition.deleteOne({_id:pos._id});
        console.log(`[${reason}] ${pos.symbol} PnL: ${pnl>=0?'+':''}$${pnl.toFixed(4)}`);
      } catch(e) {
        console.log(`[TP/SL] Erreur ${pos.symbol}:`,e.message);
        if (e.message&&e.message.includes('Insufficient funds'))
          await OpenPosition.deleteOne({_id:pos._id}).catch(()=>{});
      } finally { positionsInProgress.delete(posId); }
    }
  } catch(e) { console.log('[TP/SL] Erreur:',e.message); }
}

// ================================================================
// SCAN CLASSIQUE
// ================================================================
const marketsCache = {};
let scanRunning = false;

function scanKrakenFromMemory() {
  return scanKrakenTimeframe(krakenCandles, '1d', null);
}

function scanKrakenTimeframe(store, timeframeLabel, aiParams) {
  const results = [];
  for (const symbol of krakenPairsList) {
    const candles = store[symbol];
    if (!candles||candles.length<20) continue;
    const closes = candles.filter(c=>c.c>0).map(c=>c.c);
    const volumes = candles.filter(c=>c.v>0).map(c=>c.v);
    if (closes.length<20) continue;
    const price = closes[closes.length-1];
    const sig = detectFigure(closes,volumes,price,aiParams);
    if (!sig) continue;
    const volRatio = volumes[volumes.length-1]/avg(volumes.slice(-50));
    results.push({
      symbol,exchange:'Kraken',exchangeId:'kraken',timeframe:timeframeLabel,market:'Spot',
      figure:sig.fig.name,figureCode:sig.fig.code,direction:sig.fig.dir,
      confidence:Math.round(sig.fig.wr*100),reliable:sig.fig.wr>=0.65,
      entryPrice:price,tp:sig.tp,sl:sig.sl,
      tpPct:+(sig.tpPct*100).toFixed(1),slPct:+(SL_PCT*100).toFixed(1),
      figureTarget:+(sig.figureTarget*100).toFixed(1),
      volumeRatio:volRatio.toFixed(2),tradeAmount:TRADE_AMOUNT,
      estimatedDuration: estimateDuration(timeframeLabel),
      gain:(TRADE_AMOUNT*sig.tpPct).toFixed(4),time:new Date()
    });
  }
  return results;
}

async function scanAll() {
  if (scanRunning) return;
  scanRunning = true;
  try {
    const krakenResults = scanKrakenFromMemory();
    signalsCache.length = 0;
    signalsCache.push(...krakenResults);
    lastScanTime = new Date();
    console.log(`[Scan] ${krakenResults.length} signaux`);

    return;

    const users = await User.find({active:true,apiKey:{$exists:true},botMode:'classic'});
    for (const user of users) {
      const userExchangeId = user.exchangeName.toLowerCase();
      const exConfig = EXCHANGES_CONFIG.find(e=>e.id===userExchangeId);
      if (!exConfig||!exConfig.ccxt) continue;
      const userSignals = signalsCache.filter(s=>s.exchangeId===userExchangeId);
      if (userSignals.length===0) continue;
      try {
        const ExClass = ccxt[userExchangeId]; if(!ExClass) continue;
        const exchange = new ExClass({apiKey:user.apiKey,secret:user.apiSecret,enableRateLimit:true});
        const balance = await exchange.fetchBalance();
        const currency = user.currency||exConfig.currencies[0];
        const available = balance[currency]?.free||0;
        const amount = Math.max(user.tradeAmount||TRADE_AMOUNT,5);
        if (available<amount) continue;
        for (const sig of userSignals.slice(0,20)) {
          const existingPos = await OpenPosition.findOne({email:user.email,symbol:sig.symbol,exchangeId:userExchangeId});
          if (existingPos) continue;
          const recentTrade = await Trade.findOne({email:user.email,symbol:sig.symbol,time:{$gte:new Date(Date.now()-BLOCK_HOURS*60*60*1000)}});
          if (recentTrade) continue;
          const qty = amount/sig.entryPrice;
          const orderParams = {}; if(userExchangeId==='kraken') orderParams.oflags='fciq';
          await exchange.createOrder(sig.symbol,'market','buy',qty,undefined,orderParams);
          await new OpenPosition({email:user.email,symbol:sig.symbol,exchange:sig.exchange,exchangeId:userExchangeId,
            figure:sig.figure,entryPrice:sig.entryPrice,tp:sig.tp,sl:sig.sl,tpPct:sig.tpPct,
            figureTarget:sig.figureTarget,qty,amount,currency,botMode:'classic',
            estimatedDuration:sig.estimatedDuration}).save();
          await new Trade({email:user.email,symbol:sig.symbol,exchange:sig.exchange,market:'Spot',
            direction:sig.direction,figure:sig.figure,entryPrice:sig.entryPrice,exitPrice:null,
            amount,pnl:0,currency,result:'OPEN',exitReason:'Position ouverte',botMode:'classic',
            estimatedDuration:sig.estimatedDuration}).save();
        }
      } catch(e) { console.log(`[Scan] Erreur ${user.email}:`,e.message); }
    }
  } finally { scanRunning = false; }
}

// ================================================================
// XLM PAYMENTS
// ================================================================
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
    }
  } catch(e) { console.log('[XLM] Erreur:',e.message); }
}

// ================================================================
// WATCHDOG
// ================================================================
const watchdogAlerts = [];

function watchWebSocket() {
  const wsOk   = wsTicker && wsTicker.readyState===1;
  const wsOk1d = ws && ws.readyState===1;
  const wsOk1h = ws1h && ws1h.readyState===1;
  const wsOk4h = ws4h && ws4h.readyState===1;
  const wsOk15m = ws15m && ws15m.readyState===1;
  if(!wsOk)  { watchAlert('WEBSOCKET','Ticker KO');   connectKrakenTicker(krakenPairsList); }
  if(!wsOk1d){ watchAlert('WEBSOCKET','Daily KO');    connectKrakenWS(krakenPairsList); }
  if(!wsOk1h){ watchAlert('WEBSOCKET','1H KO');       connectKrakenWS1h(krakenPairsList); }
  if(!wsOk4h){ watchAlert('WEBSOCKET','4H KO');       connectKrakenWS4h(krakenPairsList); }
  if(!wsOk15m){ watchAlert('WEBSOCKET','15M KO');     connectKrakenWS15m(krakenPairsList); }
}

async function watchPositions() {
  try {
    const positions = await OpenPosition.find({});
    for (const pos of positions) {
      if (pos.tp <= pos.entryPrice) {
        await OpenPosition.updateOne({_id:pos._id},{tp:+(pos.entryPrice*1.15).toFixed(8)});
      }
      if (pos.sl >= pos.entryPrice) {
        await OpenPosition.updateOne({_id:pos._id},{sl:+(pos.entryPrice*(1-SL_PCT)).toFixed(8)});
      }
      const ageHours = (Date.now()-new Date(pos.openedAt).getTime())/(1000*60*60);
      if (ageHours > 240) {
        const curP = livePrices[pos.symbol];
        if (curP) {
          const pnl = (curP-pos.entryPrice)/pos.entryPrice*pos.amount;
          await Trade.findOneAndUpdate({email:pos.email,symbol:pos.symbol,result:'OPEN'},
            {exitPrice:curP,pnl:+pnl.toFixed(4),result:pnl>=0?'WIN':'LOSS',exitReason:'Position fantome >10j'},{sort:{time:-1}}).catch(()=>{});
          await OpenPosition.deleteOne({_id:pos._id}).catch(()=>{});
        }
      }
    }
  } catch(e) { console.log('[Watchdog] Erreur positions:', e.message); }
}

async function watchDatabase() {
  if (mongoose.connection.readyState !== 1) {
    watchAlert('DATABASE','MongoDB deconnecte');
    await mongoose.connect(process.env.MONGODB_URI).catch(()=>{});
  }
  if (recentSignals.size > 10000) {
    const cutoff = Date.now() - 6*60*60*1000;
    for (const [key, time] of recentSignals) {
      if (time < cutoff) recentSignals.delete(key);
    }
  }
}

async function watchBotHealth() {
  const nPrices = Object.keys(livePrices).length;
  const wsOk = wsTicker && wsTicker.readyState === 1;
  console.log(`[Watchdog] ${nPrices} prix live - WS:${wsOk?'OK':'KO'} - Sentiment:${getMarketSentiment()}% - Signaux:${latestRobotAnalyses.length}`);
}

function watchAlert(type, message) {
  const isDup = watchdogAlerts.some(a=>a.type===type&&a.message===message&&Date.now()-new Date(a.time).getTime()<3600000);
  if (isDup) return;
  watchdogAlerts.unshift({ type, message, time: new Date() });
  if (watchdogAlerts.length > 100) watchdogAlerts.pop();
  console.log(`[Watchdog ${type}] ${message}`);
}

async function runWatchdog() {
  try {
    watchWebSocket();
    await watchPositions();
    await watchDatabase();
    await watchBotHealth();
  } catch(e) { console.log('[Watchdog] Erreur:', e.message); }
}

// ================================================================
// ROUTES API
// ================================================================
app.get('/', (req,res) => res.json({
  status: 'Bender Pro v11.2',
  krakenWsConnected: wsConnected,
  krakenPairsTracked: krakenPairsList.length,
  lastScan: lastScanTime,
  signalsActive: signalsCache.length,
  marketSentiment: getMarketSentiment()+'%',
  tradingEnabled: getMarketSentiment() >= SENTIMENT_MIN,
}));

app.get('/market', (req,res) => {
  const q = req.query;
  const hasCustomParams = !!(q.timeframe || q.rsi || q.emaFast || q.emaSlow || q.adxMin);

  if (hasCustomParams) {
    const tf = (q.timeframe || '1d').toLowerCase();
    const store = storeForTimeframe(tf);
    const timeframeLabel = (tf==='15'||tf==='1') ? '15m' : (tf==='60') ? '1h' : (tf==='240') ? '4h' : tf;
    const aiParams = {
      rsiPeriod: q.rsi ? parseInt(q.rsi) : undefined,
      emaFast: q.emaFast ? parseInt(q.emaFast) : undefined,
      emaSlow: q.emaSlow ? parseInt(q.emaSlow) : undefined,
      adxMin: q.adxMin ? parseFloat(q.adxMin) : undefined,
      rsiMin: q.rsiMin ? parseFloat(q.rsiMin) : undefined,
      rsiMax: q.rsiMax ? parseFloat(q.rsiMax) : undefined,
    };
    let sigs = scanKrakenTimeframe(store, timeframeLabel, aiParams);
    // Applique les memes parametres personnalises ET le meme timeframe demande
    // aux plateformes multi-exchange. Grace a la rotation de sondage (15m/1h/4h/1d),
    // les donnees pour ce timeframe seront disponibles au plus tard apres un cycle
    // complet de rotation -- si rien n'est encore en memoire pour ce timeframe precis,
    // ces plateformes retournent simplement 0 signal pour l'instant (pas d'erreur).
    sigs = sigs.concat(scanAllMultiExchanges(aiParams, timeframeLabel));
    if (q.exchange) sigs=sigs.filter(s=>s.exchange.toLowerCase().includes(q.exchange.toLowerCase()));
    if (q.direction) sigs=sigs.filter(s=>s.direction.toLowerCase()===q.direction.toLowerCase());
    if (q.figures) {
      const wanted = q.figures.split(',').map(f=>f.trim().toLowerCase());
      sigs=sigs.filter(s=>wanted.includes(s.figure.toLowerCase()));
    }
    return res.json({success:true,signals:sigs,count:sigs.length,lastScan:new Date(),timeframe:timeframeLabel,custom:true});
  }

  let sigs = [...signalsCache, ...scanAllMultiExchanges(null)];
  if (req.query.exchange) sigs = sigs.filter(s=>s.exchange.toLowerCase().includes(req.query.exchange.toLowerCase()));
  res.json({success:true,signals:sigs,count:sigs.length,lastScan:lastScanTime});
});

app.get('/scan', async(req,res) => {
  res.json({success:true,message:'Scan lance...'});
  scanAll().catch(console.error);
});

app.get('/robot-analysis', (req,res) => {
  res.json({
    success: true,
    lastUpdate: lastAnalysisTime,
    nextUpdate: lastAnalysisTime ? new Date(new Date(lastAnalysisTime).getTime()+30*60*1000) : null,
    totalRobots: 1,
    activeRobots: 1,
    analyses: latestRobotAnalyses,
    marketSentiment: getMarketSentiment()
  });
});

app.post('/register-email', async(req,res) => {
  const {email} = req.body;
  if (!email||!email.includes('@')) return res.json({success:false,error:'Email invalide'});
  try {
    await User.findOneAndUpdate({email},{email,active:true},{upsert:true,new:true});
    res.json({success:true,message:'Email enregistre'});
  } catch(e) { res.json({success:false,error:e.message}); }
});

app.post('/connect', async(req,res) => {
  const {email,apiKey,secret,exchangeName,tradeAmount,currency} = req.body;
  if (!email||!apiKey||!secret||!exchangeName)
    return res.json({success:false,error:'Donnees manquantes'});
  try {
    const exConfig = EXCHANGES_CONFIG.find(e=>e.id===exchangeName.toLowerCase()||e.name.toLowerCase()===exchangeName.toLowerCase());
    const selectedCurrency = currency||(exConfig?exConfig.currencies[0]:'USD');
    await User.findOneAndUpdate({email},
      {apiKey,apiSecret:secret,exchangeName:exConfig?exConfig.id:exchangeName.toLowerCase(),
       active:true,tradeAmount:tradeAmount||TRADE_AMOUNT,currency:selectedCurrency,botMode:'classic'},
      {upsert:true,new:true});
    res.json({success:true,message:`Bot Classique - ${exConfig?.name||exchangeName} - ${selectedCurrency} - SL -4%`});
  } catch(e) { res.json({success:false,error:e.message}); }
});

app.get('/status/:email', async(req,res) => {
  try {
    const user = await User.findOne({email:req.params.email});
    if (!user) return res.json({connected:false});
    const trades = await Trade.countDocuments({email:req.params.email});
    const wins = await Trade.countDocuments({email:req.params.email,result:'WIN'});
    res.json({connected:true,active:user.active,exchange:user.exchangeName,
      tradeAmount:user.tradeAmount,trades,
      winRate:trades>0?Math.round(wins/trades*100)+'%':'N/A'});
  } catch(e) { res.json({success:false,error:e.message}); }
});

app.get('/positions/:email', async(req,res) => {
  try {
    const positions = await OpenPosition.find({email:req.params.email});
    const enriched = positions.map(pos => {
      const currentPrice = livePrices[pos.symbol]||pos.entryPrice;
      const timeLeft = getTimeBeforeExpiry(pos.resistanceBreakAt, pos.timeframe||'1d');
      return {
        ...pos.toObject(),
        currentPrice,
        pnlPct: ((currentPrice-pos.entryPrice)/pos.entryPrice*100).toFixed(2),
        pnlUsd: ((currentPrice-pos.entryPrice)/pos.entryPrice*pos.amount).toFixed(4),
        estimatedDuration: pos.estimatedDuration || estimateDuration(pos.timeframe||'1d'),
        timeBeforeSignalExpiry: timeLeft,
      };
    });
    res.json({success:true,positions:enriched,count:enriched.length});
  } catch(e) { res.json({success:false,error:e.message}); }
});

app.get('/trades/:email', async(req,res) => {
  try {
    const email = req.params.email;
    const allTrades = await Trade.find({email});
    const totalPnl = allTrades.reduce((a,t)=>a+t.pnl,0);
    const totalWins = allTrades.filter(t=>t.result==='WIN').length;
    const trades = await Trade.find({email}).sort({time:-1}).limit(100);
    res.json({trades,totalTradesCount:allTrades.length,totalPnl:totalPnl.toFixed(4),
      wins:totalWins,losses:allTrades.length-totalWins,displayedCount:trades.length});
  } catch(e) { res.json({success:false,error:e.message}); }
});

const COMMISSION_RATE = 0.0025;
const BILLING_WALLET = process.env.BILLING_WALLET||'GDIZP4VPNBZLV7CCDUG4BORFYERX3NQVBE3N6W2FAFAIT3OTTRUIUCBR';
const BILLING_DAYS = 30;

app.get('/billing/:email', async(req,res) => {
  try {
    const email = req.params.email;
    const user = await User.findOne({email});
    if (!user) return res.json({success:false,error:'Utilisateur non trouve'});
    const periodEnd = new Date();
    const periodStart = new Date(periodEnd-BILLING_DAYS*24*3600*1000);
    const trades = await Trade.find({email,time:{$gte:periodStart,$lte:periodEnd},result:{$in:['WIN','LOSS']}});
    const totalVolume = trades.reduce((a,t)=>a+t.amount,0);
    const totalPnl = trades.reduce((a,t)=>a+t.pnl,0);
    const wins = trades.filter(t=>t.result==='WIN').length;
    const commission = +(totalVolume*COMMISSION_RATE).toFixed(4);
    let billing = await Billing.findOne({email,periodStart:{$gte:new Date(periodStart.getTime()-3600000)}});
    if (!billing) billing = await new Billing({email,periodStart,periodEnd,totalPnl:+totalPnl.toFixed(4),totalVolume:+totalVolume.toFixed(4),commission,status:'PENDING'}).save();
    res.json({success:true,billing:{email,trades:trades.length,wins,commission,status:billing.status,wallet:BILLING_WALLET}});
  } catch(e) { res.json({success:false,error:e.message}); }
});

app.post('/save-xlm', async(req,res) => {
  try {
    const {email,xlmWallet} = req.body;
    if (!email||!xlmWallet||!xlmWallet.startsWith('G')||xlmWallet.length<40)
      return res.json({success:false,error:'Adresse XLM invalide'});
    await User.findOneAndUpdate({email},{xlmWallet},{upsert:true});
    res.json({success:true,message:'Wallet XLM sauvegarde'});
  } catch(e) { res.json({success:false,error:e.message}); }
});

app.post('/disconnect', async(req,res) => {
  try {
    const {email} = req.body;
    await User.findOneAndUpdate({email},{active:false});
    res.json({success:true,message:'Deconnecte'});
  } catch(e) { res.json({success:false,error:e.message}); }
});

app.post('/toggle', async(req,res) => {
  try { const{email,active}=req.body; await User.findOneAndUpdate({email},{active}); res.json({success:true,active}); }
  catch(e) { res.json({success:false,error:e.message}); }
});

app.get('/admin/stats', async(req,res) => {
  try {
    const users = await User.countDocuments();
    const active = await User.countDocuments({active:true});
    const trades = await Trade.countDocuments();
    const wins = await Trade.countDocuments({result:'WIN'});
    res.json({users,active,trades,winRate:trades>0?Math.round(wins/trades*100)+'%':'N/A',
      signalsActive:signalsCache.length,lastScan:lastScanTime,
      krakenWsConnected:wsConnected,krakenPairsTracked:krakenPairsList.length,
      marketSentiment:getMarketSentiment()+'%'});
  } catch(e) { res.json({success:false,error:e.message}); }
});

app.get('/admin/watchdog', (req,res) => {
  res.json({
    success: true,
    alerts: watchdogAlerts.slice(0,50),
    count: watchdogAlerts.length,
    wsStatus: {
      ticker: wsTicker?.readyState===1?'CONNECTED':'DISCONNECTED',
      ohlc1d: ws?.readyState===1?'CONNECTED':'DISCONNECTED',
      ohlc1h: ws1h?.readyState===1?'CONNECTED':'DISCONNECTED',
      ohlc4h: ws4h?.readyState===1?'CONNECTED':'DISCONNECTED',
      ohlc15m: ws15m?.readyState===1?'CONNECTED':'DISCONNECTED',
    },
    livePricesCount: Object.keys(livePrices).length,
    signalsCacheCount: signalsCache.length,
    krakenPairs: krakenPairsList.length,
    marketSentiment: getMarketSentiment()+'%',
    tradingEnabled: getMarketSentiment() >= SENTIMENT_MIN,
  });
});

app.post('/admin/clear-alerts', (req,res) => {
  watchdogAlerts.length = 0;
  res.json({success:true,message:'Alertes effacees'});
});

// ================================================================
// PAIEMENTS DES ABONNEMENTS AUX SIGNAUX (carte via Stripe, ou crypto ETH/BTC)
// ================================================================
const SIGNAL_TIER_PRICES_USD = { starter: 4, plus: 7, pro: 10, total: 12 };
const SIGNAL_ETH_WALLET = '0x335a86429321361062CC18eC99Eba1b2C9b1E540';
const SIGNAL_BTC_WALLET = 'bc1qdft5d3355dpjzdcsrqvyeqtn6se9ththr073jh';
const CRYPTO_PAYMENT_TOLERANCE = 0.02; // 2% de marge (fluctuation du prix entre la demande et l'envoi)

async function getCryptoPriceUSD(coingeckoId) {
  try {
    const r = await fetch(`https://api.coingecko.com/api/v3/simple/price?ids=${coingeckoId}&vs_currencies=usd`);
    const data = await r.json();
    return (data[coingeckoId] && data[coingeckoId].usd) || null;
  } catch (e) { return null; }
}

// -- Paiement par carte (Stripe Checkout) --
app.post('/create-checkout-session', async (req, res) => {
  try {
    const { email, tier } = req.body;
    const priceUsd = SIGNAL_TIER_PRICES_USD[tier];
    if (!email || !priceUsd) return res.json({ success: false, error: 'Palier ou email invalide' });

    const session = await stripe.checkout.sessions.create({
      mode: 'subscription',
      payment_method_types: ['card'],
      customer_email: email,
      line_items: [{
        price_data: {
          currency: 'usd',
          product_data: { name: `Bender Pro - Palier ${tier}` },
          unit_amount: Math.round(priceUsd * 100),
          recurring: { interval: 'month' },
        },
        quantity: 1,
      }],
      metadata: { email, tier },
      success_url: (process.env.SITE_URL || 'https://benderpro.netlify.app') + '/index.html?payment=success',
      cancel_url: (process.env.SITE_URL || 'https://benderpro.netlify.app') + '/index.html?payment=cancelled',
    });

    await new SignalPayment({ email, tier, method: 'stripe', amountUsd: priceUsd, stripeSessionId: session.id }).save();
    res.json({ success: true, url: session.url });
  } catch (e) {
    console.log('[Stripe] Erreur creation session:', e.message);
    res.json({ success: false, error: e.message });
  }
});

// -- Paiement crypto (ETH ou BTC) : demande d'adresse + montant --
app.post('/signal-payment/request', async (req, res) => {
  try {
    const { email, tier, method } = req.body;
    const priceUsd = SIGNAL_TIER_PRICES_USD[tier];
    if (!email || !priceUsd || !['eth', 'btc'].includes(method)) {
      return res.json({ success: false, error: 'Parametres invalides' });
    }

    const coingeckoId = method === 'eth' ? 'ethereum' : 'bitcoin';
    const priceCrypto = await getCryptoPriceUSD(coingeckoId);
    if (!priceCrypto) return res.json({ success: false, error: 'Prix indisponible, reessaie dans un instant' });

    const amountCrypto = +(priceUsd / priceCrypto).toFixed(8);
    const address = method === 'eth' ? SIGNAL_ETH_WALLET : SIGNAL_BTC_WALLET;

    const payment = await new SignalPayment({
      email, tier, method, amountUsd: priceUsd, amountCrypto, cryptoAddress: address
    }).save();

    res.json({ success: true, paymentId: payment._id, address, amount: amountCrypto, method });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// -- Verifier si un paiement crypto est arrive --
app.get('/signal-payment/status/:id', async (req, res) => {
  try {
    const payment = await SignalPayment.findById(req.params.id);
    if (!payment) return res.json({ success: false, error: 'Paiement introuvable' });
    res.json({ success: true, status: payment.status });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// -- Sondage periodique : verifie les paiements crypto en attente sur la blockchain --
async function checkCryptoSignalPayments() {
  try {
    const pending = await SignalPayment.find({ status: 'PENDING', method: { $in: ['eth', 'btc'] } });
    if (!pending.length) return;

    for (const payment of pending) {
      // Expire apres 2h sans paiement recu
      if (Date.now() - new Date(payment.createdAt).getTime() > 2 * 60 * 60 * 1000) {
        payment.status = 'EXPIRED'; await payment.save(); continue;
      }

      let found = false;
      if (payment.method === 'eth') {
        found = await checkEthPaymentReceived(payment);
      } else {
        found = await checkBtcPaymentReceived(payment);
      }

      if (found) {
        const expires = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000);
        await User.findOneAndUpdate({ email: payment.email }, { signalTier: payment.tier, signalTierExpires: expires }, { upsert: true });
        payment.status = 'PAID'; payment.paidAt = new Date();
        await payment.save();
        console.log(`[Crypto] Paiement confirme - ${payment.email} - palier ${payment.tier} (${payment.method})`);
      }
    }
  } catch (e) { console.log('[Crypto Payment] Erreur:', e.message); }
}

async function checkEthPaymentReceived(payment) {
  try {
    const apiKey = process.env.ETHERSCAN_API_KEY || '';
    const url = `https://api.etherscan.io/api?module=account&action=txlist&address=${payment.cryptoAddress}&startblock=0&endblock=99999999&sort=desc&apikey=${apiKey}`;
    const r = await fetch(url);
    const data = await r.json();
    if (!data.result || !Array.isArray(data.result)) return false;
    const createdTs = Math.floor(new Date(payment.createdAt).getTime() / 1000);
    return data.result.some(tx => {
      const valueEth = parseFloat(tx.value) / 1e18;
      const withinAmount = Math.abs(valueEth - payment.amountCrypto) / payment.amountCrypto < CRYPTO_PAYMENT_TOLERANCE;
      return withinAmount && parseInt(tx.timeStamp) >= createdTs - 300; // marge de 5 min
    });
  } catch (e) { return false; }
}

async function checkBtcPaymentReceived(payment) {
  try {
    const r = await fetch(`https://blockstream.info/api/address/${payment.cryptoAddress}/txs`);
    const txs = await r.json();
    if (!Array.isArray(txs)) return false;
    const createdTs = Math.floor(new Date(payment.createdAt).getTime() / 1000);
    return txs.some(tx => {
      const received = (tx.vout || []).filter(o => o.scriptpubkey_address === payment.cryptoAddress)
        .reduce((sum, o) => sum + o.value, 0) / 1e8; // satoshis -> BTC
      const withinAmount = Math.abs(received - payment.amountCrypto) / payment.amountCrypto < CRYPTO_PAYMENT_TOLERANCE;
      const txTime = tx.status?.block_time || Math.floor(Date.now() / 1000);
      return withinAmount && txTime >= createdTs - 300;
    });
  } catch (e) { return false; }
}

// -- Etat de l'abonnement d'un utilisateur (utilise par le frontend) --
app.get('/signal-tier/:email', async (req, res) => {
  try {
    const user = await User.findOne({ email: req.params.email });
    if (!user || !user.signalTier) return res.json({ success: true, tier: null });
    if (user.signalTierExpires && new Date(user.signalTierExpires) < new Date()) {
      return res.json({ success: true, tier: null, expired: true });
    }
    res.json({ success: true, tier: user.signalTier, expires: user.signalTierExpires });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

app.post('/chat-builder', async(req,res) => {
  try {
    const {system,messages} = req.body;
    if (!messages||!Array.isArray(messages)) return res.json({success:false,error:'Messages manquants'});
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method:'POST',
      headers:{'Content-Type':'application/json','x-api-key':process.env.ANTHROPIC_API_KEY||'','anthropic-version':'2023-06-01'},
      body: JSON.stringify({model:'claude-sonnet-4-6',max_tokens:1000,system:system||'Tu es Bender, assistant trading.',messages:messages.slice(-10)})
    });
    const data = await response.json();
    if (data.error) return res.json({success:false,error:data.error.message,reply:'Erreur API.'});
    res.json({success:true,reply:data.content?.[0]?.text||'Erreur.'});
  } catch(e) { res.json({success:false,error:e.message,reply:'Service indisponible.'}); }
});

// ================================================================
// SWAP (1inch) - meme modele que MetaMask/Trust Wallet : frais de
// referencement preleves automatiquement par l'API sur chaque swap,
// verses a l'adresse SWAP_FEE_WALLET. La cle API 1inch reste secrete
// cote serveur (jamais exposee au navigateur) -- meme pattern que
// /chat-builder ci-dessus pour la cle Anthropic.
// ================================================================
const ONEINCH_API_KEY = process.env.ONEINCH_API_KEY || '';
const SWAP_FEE_WALLET = process.env.SWAP_FEE_WALLET || ''; // adresse EVM qui recoit les frais
const SWAP_FEE_PCT = 0.5; // 0.5% -- ajustable

// Recuperer un devis de swap (prix, montant recu estime, frais inclus)
app.get('/swap/quote', async (req, res) => {
  try {
    const { chainId, fromToken, toToken, amount } = req.query;
    if (!chainId || !fromToken || !toToken || !amount) {
      return res.json({ success: false, error: 'Parametres manquants (chainId, fromToken, toToken, amount)' });
    }
    if (!ONEINCH_API_KEY) return res.json({ success: false, error: 'Service de swap non configure (cle API manquante)' });

    const params = new URLSearchParams({
      src: fromToken, dst: toToken, amount,
      fee: SWAP_FEE_PCT.toString(),
      ...(SWAP_FEE_WALLET ? { referrer: SWAP_FEE_WALLET } : {})
    });
    const url = `https://api.1inch.dev/swap/v6.0/${chainId}/quote?${params}`;
    const r = await fetch(url, { headers: { Authorization: `Bearer ${ONEINCH_API_KEY}` } });
    const data = await r.json();
    if (data.error) return res.json({ success: false, error: data.description || data.error });
    res.json({ success: true, quote: data, feePct: SWAP_FEE_PCT });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// Construire la transaction de swap prete a signer (l'utilisateur signe
// lui-meme dans son navigateur -- le serveur ne touche jamais aux fonds).
app.get('/swap/tx', async (req, res) => {
  try {
    const { chainId, fromToken, toToken, amount, fromAddress, slippage } = req.query;
    if (!chainId || !fromToken || !toToken || !amount || !fromAddress) {
      return res.json({ success: false, error: 'Parametres manquants' });
    }
    if (!ONEINCH_API_KEY) return res.json({ success: false, error: 'Service de swap non configure (cle API manquante)' });

    const params = new URLSearchParams({
      src: fromToken, dst: toToken, amount, from: fromAddress,
      slippage: slippage || '1',
      fee: SWAP_FEE_PCT.toString(),
      ...(SWAP_FEE_WALLET ? { referrer: SWAP_FEE_WALLET } : {}),
      disableEstimate: 'false'
    });
    const url = `https://api.1inch.dev/swap/v6.0/${chainId}/swap?${params}`;
    const r = await fetch(url, { headers: { Authorization: `Bearer ${ONEINCH_API_KEY}` } });
    const data = await r.json();
    if (data.error) return res.json({ success: false, error: data.description || data.error });
    res.json({ success: true, tx: data.tx, feePct: SWAP_FEE_PCT });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// Verifier si une allocation (approve) ERC-20 est necessaire avant le swap
app.get('/swap/allowance', async (req, res) => {
  try {
    const { chainId, tokenAddress, walletAddress } = req.query;
    if (!ONEINCH_API_KEY) return res.json({ success: false, error: 'Service de swap non configure' });
    const url = `https://api.1inch.dev/swap/v6.0/${chainId}/approve/allowance?tokenAddress=${tokenAddress}&walletAddress=${walletAddress}`;
    const r = await fetch(url, { headers: { Authorization: `Bearer ${ONEINCH_API_KEY}` } });
    const data = await r.json();
    res.json({ success: true, allowance: data.allowance });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// Construire la transaction d'autorisation (approve) ERC-20 si necessaire
app.get('/swap/approve-tx', async (req, res) => {
  try {
    const { chainId, tokenAddress, amount } = req.query;
    if (!ONEINCH_API_KEY) return res.json({ success: false, error: 'Service de swap non configure' });
    const params = new URLSearchParams({ tokenAddress, ...(amount ? { amount } : {}) });
    const url = `https://api.1inch.dev/swap/v6.0/${chainId}/approve/transaction?${params}`;
    const r = await fetch(url, { headers: { Authorization: `Bearer ${ONEINCH_API_KEY}` } });
    const data = await r.json();
    res.json({ success: true, tx: data });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// ================================================================
// DEMARRAGE
// ================================================================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`\nBender Pro v11.2 - Port ${PORT}`);
  console.log(` 1 Robot - Signaux uniquement - Trading en pause\n`);

  setImmediate(async () => {
    setTimeout(() => checkTPSLInstant().catch(console.error), 100);
    setInterval(() => checkTPSLInstant().catch(console.error), 2000);

    setTimeout(() => runWatchdog().catch(console.error), 10000);
    setInterval(() => runWatchdog().catch(console.error), 30000);

    setInterval(() => {
      const ping = JSON.stringify({method:'ping'});
      if (wsTicker&&wsTicker.readyState===1) wsTicker.send(ping);
      if (ws&&ws.readyState===1) ws.send(ping);
      if (ws1h&&ws1h.readyState===1) ws1h.send(ping);
      if (ws4h&&ws4h.readyState===1) ws4h.send(ping);
      if (ws15m&&ws15m.readyState===1) ws15m.send(ping);
    }, 15000);

    setInterval(() => {
      if (!krakenPairsList.length) return;
      if (!wsTicker||wsTicker.readyState!==1) connectKrakenTicker(krakenPairsList);
      if (!ws||ws.readyState!==1) connectKrakenWS(krakenPairsList);
      if (!ws1h||ws1h.readyState!==1) connectKrakenWS1h(krakenPairsList);
      if (!ws4h||ws4h.readyState!==1) connectKrakenWS4h(krakenPairsList);
      if (!ws15m||ws15m.readyState!==1) connectKrakenWS15m(krakenPairsList);
    }, 30000);

    setTimeout(() => checkXlmPayments().catch(console.error), 5000);
    setInterval(() => checkXlmPayments().catch(console.error), 24*60*60*1000);

    // Verifie les paiements crypto en attente (abonnements signaux) toutes les 2 minutes
    setTimeout(() => checkCryptoSignalPayments().catch(console.error), 10000);
    setInterval(() => checkCryptoSignalPayments().catch(console.error), 2*60*1000);

    // Sondage multi-exchange (Binance, Bybit, Coinbase - extensible via MULTI_EXCHANGES)
    setTimeout(() => pollAllMultiExchanges().catch(console.error), 15000);
    setInterval(() => pollAllMultiExchanges().catch(console.error), POLL_INTERVAL_MS);

    krakenPairsList = await fetchKrakenUsdtPairs().catch(() => []);
    if (krakenPairsList.length > 0) {
      connectKrakenTicker(krakenPairsList);
      connectKrakenWS(krakenPairsList);
      connectKrakenWS1h(krakenPairsList);
      connectKrakenWS4h(krakenPairsList);
      connectKrakenWS15m(krakenPairsList);
      console.log(` ${krakenPairsList.length} paires - WebSocket actif`);

      preloadHistoricalCandles(krakenPairsList).then(() => {
        console.log(' Preloading termine - Analyse lancee');
        runRobotAnalysis().catch(console.error);
        setInterval(() => runRobotAnalysis().catch(console.error), 30*60*1000);
        scanAll().catch(console.error);
        setTimeout(() => setInterval(() => scanAll().catch(console.error), SCAN_INTERVAL), 65000);
      }).catch(console.error);
    }
  });
});
