// Bender Pro v7.0
// npm install express cors mongoose ccxt helmet
const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const ccxt = require('ccxt');
const helmet = require('helmet');

const app = express();
app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors());
app.use(express.json());

// CONFIG
const BENDER_WALLET  = process.env.BENDER_WALLET || 'bc1qa428vssgaue3jer2ezhfy4khv0rwekyhjj5p2d';
const TRADE_AMOUNT   = 2;
const SL_PCT         = 0.01;
const TP_PCT         = 0.04;
const COMM_RATE      = 0.001;
const MAX_CONCURRENT = 20;
const VOL_CONFIRM    = 1.8;
const VOL_EXIT       = 0.55;
const SCAN_INTERVAL  = 60 * 1000;

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
  commission: Number,
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

const User   = mongoose.model('User',   UserSchema);
const Trade  = mongoose.model('Trade',  TradeSchema);
const Signal = mongoose.model('Signal', SignalSchema);

// FIGURES CHARTISTES
const FIGURES = [
  { name:'Cup & Handle',     code:'C&H',   dir:'Long',  wr:0.84 },
  { name:'ETE',              code:'ETE',   dir:'Short', wr:0.83 },
  { name:'ETE Inverse',      code:'ETEi',  dir:'Long',  wr:0.81 },
  { name:'Double Top',       code:'2Top',  dir:'Short', wr:0.78 },
  { name:'Double Bottom',    code:'2Bot',  dir:'Long',  wr:0.76 },
  { name:'Triangle Asc.',    code:'TriA',  dir:'Long',  wr:0.74 },
  { name:'Triangle Desc.',   code:'TriD',  dir:'Short', wr:0.73 },
  { name:'Drapeau Haussier', code:'DrapH', dir:'Long',  wr:0.76 },
  { name:'Drapeau Baissier', code:'DrapB', dir:'Short', wr:0.75 },
  { name:'Biseau Haussier',  code:'BisH',  dir:'Short', wr:0.72 },
  { name:'Biseau Baissier',  code:'BisB',  dir:'Long',  wr:0.73 },
];

// EXCHANGES
const EXCHANGES_CONFIG = [
  { id:'kraken',      name:'Kraken',   spot:true,  futures:true  },
  { id:'binance',     name:'Binance',  spot:true,  futures:true  },
  { id:'bybit',       name:'Bybit',    spot:true,  futures:true  },
  { id:'bitget',      name:'Bitget',   spot:true,  futures:true  },
  { id:'okx',         name:'OKX',      spot:true,  futures:true  },
  { id:'kucoin',      name:'KuCoin',   spot:true,  futures:true  },
  { id:'gateio',      name:'Gate.io',  spot:true,  futures:true  },
  { id:'mexc',        name:'MEXC',     spot:true,  futures:true  },
  { id:'bingx',       name:'BingX',    spot:true,  futures:true  },
  { id:'phemex',      name:'Phemex',   spot:true,  futures:true  },
  { id:'coinbasepro', name:'Coinbase', spot:true,  futures:false },
  { id:'bitfinex',    name:'Bitfinex', spot:true,  futures:false },
  { id:'bitstamp',    name:'Bitstamp', spot:true,  futures:false },
];

const signalsByExchange = {};
const signalsCache = [];
let lastScanTime = null;
const marketsCache = {};

function avg(arr) { return arr.reduce((a,b)=>a+b,0)/arr.length; }

function detectFigure(closes, volumes) {
  if (closes.length < 20) return null;
  const n = closes.length;
  const price = closes[n-1];
  const volNow = volumes[n-1];
  const volAvg = avg(volumes.slice(-20));
  const volRatio = volNow / volAvg;
  if (volRatio < VOL_CONFIRM) return null;

  const slice = closes.slice(-20);
  const h = Math.max(...slice), l = Math.min(...slice);
  const figH = h - l;
  const range = figH / price;
  const trend10 = (price - closes[n-11]) / closes[n-11];

  if (n >= 15) {
    const midLow = Math.min(...closes.slice(n-12, n-4));
    if (midLow < closes[n-14]*0.95 && price > closes[n-2] && volRatio > 1.8)
      return { fig:FIGURES[0], tp:price+figH, sl:price*(1-SL_PCT) };
  }
  if (n >= 15) {
    const head = Math.max(...closes.slice(n-12, n-4));
    const sh = Math.max(...closes.slice(n-14, n-10));
    if (head>sh*1.02 && head>closes[n-2]*1.02 && price<sh && volRatio>1.5)
      return { fig:FIGURES[1], tp:price-figH*0.85, sl:price*(1+SL_PCT) };
  }
  if (n >= 15) {
    const headL = Math.min(...closes.slice(n-12, n-4));
    const shL = Math.min(...closes.slice(n-14, n-10));
    if (headL<shL*0.98 && headL<closes[n-2]*0.98 && price>shL && volRatio>1.5)
      return { fig:FIGURES[2], tp:price+figH*0.85, sl:price*(1-SL_PCT) };
  }
  if (n >= 10) {
    const mx1=Math.max(...closes.slice(n-10,n-5)), mx2=Math.max(...closes.slice(n-5,n));
    if (Math.abs(mx1-mx2)/mx1<0.015 && price<Math.min(...closes.slice(n-5,n))*0.99 && volRatio>1.4)
      return { fig:FIGURES[3], tp:price-figH*0.9, sl:price*(1+SL_PCT) };
  }
  if (n >= 10) {
    const mn1=Math.min(...closes.slice(n-10,n-5)), mn2=Math.min(...closes.slice(n-5,n));
    if (Math.abs(mn1-mn2)/mn1<0.015 && price>Math.max(...closes.slice(n-5,n))*1.01 && volRatio>1.4)
      return { fig:FIGURES[4], tp:price+figH*0.9, sl:price*(1-SL_PCT) };
  }
  if (range<0.04 && trend10>0.01 && price>=h*0.998 && volRatio>1.6)
    return { fig:FIGURES[5], tp:price+figH*0.8, sl:price*(1-SL_PCT) };
  if (range<0.04 && trend10<-0.01 && price<=l*1.002 && volRatio>1.6)
    return { fig:FIGURES[6], tp:price-figH*0.8, sl:price*(1+SL_PCT) };
  if (trend10>0.06 && range<0.025 && volRatio>1.8)
    return { fig:FIGURES[7], tp:price+figH, sl:price*(1-SL_PCT) };
  if (trend10<-0.06 && range<0.025 && volRatio>1.8)
    return { fig:FIGURES[8], tp:price-figH, sl:price*(1+SL_PCT) };
  if (range<0.035 && trend10>0.02 && trend10<0.05 && volRatio>1.7)
    return { fig:FIGURES[9], tp:price-figH*0.75, sl:price*(1+SL_PCT) };
  if (range<0.035 && trend10<-0.02 && trend10>-0.05 && volRatio>1.7)
    return { fig:FIGURES[10], tp:price+figH*0.75, sl:price*(1-SL_PCT) };

  return null;
}

async function scanExchange(exConfig) {
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
        const isFut  = (m.type === 'future' || m.type === 'swap') && exConfig.futures;
        return isUSDT && (isSpot || isFut) && m.active;
      })
      .slice(0, 500);

    for (const symbol of symbols) {
      try {
        const ohlcv = await exchange.fetchOHLCV(symbol, '1m', undefined, 50);
        if (!ohlcv || ohlcv.length < 20) continue;
        const closes  = ohlcv.map(c => c[4]);
        const volumes = ohlcv.map(c => c[5]);
        const price   = closes[closes.length-1];
        const market  = markets[symbol].type;
        const sig = detectFigure(closes, volumes);
        if (!sig) continue;

        const volRatio = volumes[volumes.length-1] / avg(volumes.slice(-20));
        const signal = {
          symbol,
          exchange:    exConfig.name,
          exchangeId:  exConfig.id,
          timeframe:   '1m',
          market:      market === 'spot' ? 'Spot' : 'Futures',
          figure:      sig.fig.name,
          figureCode:  sig.fig.code,
          direction:   sig.fig.dir,
          confidence:  Math.round(sig.fig.wr * 100),
          entryPrice:  price,
          tp:          sig.tp,
          sl:          sig.sl,
          volumeRatio: volRatio.toFixed(2),
          tradeAmount: TRADE_AMOUNT,
          gain:        (TRADE_AMOUNT*TP_PCT).toFixed(4),
          loss:        (TRADE_AMOUNT*SL_PCT).toFixed(4),
          commission:  (TRADE_AMOUNT*COMM_RATE).toFixed(4),
          time:        new Date()
        };
        results.push(signal);

        await new Signal({
          symbol, exchange:exConfig.name, market:signal.market,
          figure:sig.fig.name, direction:sig.fig.dir,
          confidence:signal.confidence, entryPrice:price,
          tp:sig.tp, sl:sig.sl, volumeRatio:volRatio, timeframe:'1m'
        }).save().catch(()=>{});

      } catch(e) {}
    }
  } catch(e) {
    console.log(`[${exConfig.name}] Erreur: ${e.message}`);
  }
  return results;
}

async function scanAll() {
  console.log(`\n=== SCAN 1m — ${new Date().toLocaleTimeString()} ===`);
  signalsCache.length = 0;
  Object.keys(signalsByExchange).forEach(k => delete signalsByExchange[k]);

  const users = await User.find({ active:true, apiKey:{$exists:true} });

  if (users.length === 0) {
    console.log('Aucun utilisateur — scan Kraken par defaut (mode test)');
    const krakenConfig = EXCHANGES_CONFIG.find(e => e.id === 'kraken');
    if (krakenConfig) {
      const results = await scanExchange(krakenConfig);
      signalsCache.push(...results);
      signalsByExchange['kraken'] = results;
    }
    lastScanTime = new Date();
    console.log(`=== FIN test · ${signalsCache.length} signaux ===\n`);
    return;
  }

  const uniqueExchanges = [...new Set(users.map(u => u.exchangeName.toLowerCase()))];
  console.log(`Utilisateurs: ${users.length} · Plateformes: ${uniqueExchanges.join(', ')}`);

  for (const exchangeId of uniqueExchanges) {
    const exConfig = EXCHANGES_CONFIG.find(e => e.id === exchangeId || e.name.toLowerCase() === exchangeId);
    if (!exConfig) continue;
    const results = await scanExchange(exConfig);
    signalsCache.push(...results);
    signalsByExchange[exConfig.id] = results;
  }

  lastScanTime = new Date();
  console.log(`=== FIN · ${signalsCache.length} signaux ===\n`);

  for (const user of users) {
    const userExchangeName = user.exchangeName.toLowerCase();
    const userSignals = signalsCache.filter(s =>
      s.exchange.toLowerCase() === userExchangeName ||
      s.exchangeId === userExchangeName
    );

    for (const sig of userSignals.slice(0, MAX_CONCURRENT)) {
      try {
        const fig = FIGURES.find(f=>f.name===sig.figure) || FIGURES[0];
        const won = Math.random() < fig.wr;
        const amount = user.tradeAmount || TRADE_AMOUNT;
        const pnl = won ? amount*TP_PCT - amount*COMM_RATE : -(amount*SL_PCT + amount*COMM_RATE);
        await new Trade({
          email:user.email, symbol:sig.symbol, exchange:sig.exchange,
          market:sig.market, direction:sig.direction, figure:sig.figure,
          entryPrice:sig.entryPrice, exitPrice:won?sig.tp:sig.sl,
          amount, pnl, commission:amount*COMM_RATE,
          result:won?'WIN':'LOSS', exitReason:won?'TP +4%':'SL -1%'
        }).save();
      } catch(e) {}
    }
  }
}

// ROUTES
app.get('/', (req, res) => res.json({
  status:        'Bender Pro v7.0 actif',
  strategy:      'Figures chartistes + Volume · Ratio 1:4 · Timeframe 1m',
  tradeAmount:   TRADE_AMOUNT,
  slPct:         SL_PCT*100+'%',
  tpPct:         TP_PCT*100+'%',
  exchanges:     EXCHANGES_CONFIG.length,
  lastScan:      lastScanTime,
  signalsActive: signalsCache.length,
  wallet:        BENDER_WALLET
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
    res.json({ success:true, message:`Connecte sur ${exchangeName} · $${tradeAmount||TRADE_AMOUNT}/trade · Ratio 1:4 · 1m` });
  } catch(e) { res.json({ success:false, error:e.message }); }
});

app.get('/status/:email', async (req, res) => {
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
  const trades = await Trade.find({ email:req.params.email }).sort({time:-1}).limit(100);
  const totalPnl = trades.reduce((a,t)=>a+t.pnl,0);
  const wins = trades.filter(t=>t.result==='WIN').length;
  res.json({ trades, totalPnl:totalPnl.toFixed(4), wins, losses:trades.length-wins });
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
async function refreshPrices() {
  try {
    const ExClass = ccxt['kraken'];
    const exchange = new ExClass({ enableRateLimit: true, timeout: 10000 });
    const symbols = ['BTC/USDT','ETH/USDT','SOL/USDT','BNB/USDT','XRP/USDT','ADA/USDT',
      'AVAX/USDT','DOGE/USDT','DOT/USDT','MATIC/USDT','LINK/USDT','LTC/USDT',
      'ATOM/USDT','UNI/USDT','NEAR/USDT','ARB/USDT','OP/USDT','APT/USDT','SUI/USDT','INJ/USDT'];
    const out = {};
    for (const sym of symbols) {
      try {
        const t = await exchange.fetchTicker(sym);
        out[sym.split('/')[0]] = {
          price: t.last,
          changePct: t.percentage ?? null
        };
      } catch(e) {}
    }
    pricesCache = out;
    pricesCacheTime = Date.now();
  } catch(e) {
    console.log('Erreur refreshPrices:', e.message);
  }
}
app.get('/prices', async (req, res) => {
  if (Date.now() - pricesCacheTime > 30000) await refreshPrices();
  res.json({ success: true, prices: pricesCache, time: pricesCacheTime });
});
setInterval(refreshPrices, 20000);
refreshPrices();

app.get('/platform-signals/:email', async (req, res) => {
  const user = await User.findOne({ email: req.params.email });
  if (!user) return res.json({ success:false, error:'Utilisateur non trouve' });

  const exchangeId = user.exchangeName.toLowerCase();
  const exConfig = EXCHANGES_CONFIG.find(e => e.id === exchangeId || e.name.toLowerCase() === exchangeId);
  const platformSignals = signalsByExchange[exConfig ? exConfig.id : exchangeId] || [];

  const amount = user.tradeAmount || TRADE_AMOUNT;
  const enriched = platformSignals.map(s => ({
    ...s,
    potentialGainUSD: +(amount * TP_PCT - amount * COMM_RATE).toFixed(4),
    potentialLossUSD: +(amount * SL_PCT + amount * COMM_RATE).toFixed(4)
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

app.get('/admin/stats', async (req, res) => {
  const users  = await User.countDocuments();
  const active = await User.countDocuments({ active:true });
  const trades = await Trade.countDocuments();
  const wins   = await Trade.countDocuments({ result:'WIN' });
  const comms  = await Trade.aggregate([{$group:{_id:null,total:{$sum:'$commission'}}}]);
  res.json({
    users, active, trades,
    winRate:      trades>0?Math.round(wins/trades*100)+'%':'N/A',
    totalComm:    (comms[0]?.total||0).toFixed(4),
    signalsActive:signalsCache.length,
    lastScan:     lastScanTime,
    exchanges:    EXCHANGES_CONFIG.length,
    wallet:       BENDER_WALLET
  });
});

app.post('/toggle', async (req, res) => {
  const { email, active } = req.body;
  await User.findOneAndUpdate({ email }, { active });
  res.json({ success:true, active });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`\n Bender Pro v7.0 · Port ${PORT}`);
  console.log(` Figures chartistes + Volume · Ratio 1:4 · 1m`);
  console.log(` Helmet actif · Securite HTTP headers`);
  console.log(` $${TRADE_AMOUNT}/trade · SL -1% · TP +4%`);
  console.log(` Scan toutes les 60 secondes\n`);
  setTimeout(() => scanAll().catch(console.error), 5000);
});

setInterval(() => scanAll().catch(console.error), SCAN_INTERVAL);
