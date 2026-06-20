// Bender Pro v8.0 â€” Scan via WebSocket Kraken (quasi instantane)
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
const BENDER_WALLET  = process.env.BENDER_WALLET || 'bc1qa428vssgaue3jer2ezhfy4khv0rwekyhjj5p2d';
const TRADE_AMOUNT   = 2;
const SL_PCT         = 0.01;
const TP_PCT         = 0.04;
const COMM_RATE      = 0.001;
const MAX_CONCURRENT = 20;
const VOL_CONFIRM    = 1.8;
const SCAN_INTERVAL  = 60 * 1000;
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

// EXCHANGES (affichage seulement â€” le scan WebSocket rapide ne couvre que Kraken pour l'instant)
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

// â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• 
// WEBSOCKET KRAKEN â€” donnees publiques en continu (sans cle API)
// On garde en memoire les 50 dernieres bougies 1m de chaque paire,
// mises a jour en temps reel par le flux WebSocket.
// Le "scan" devient alors instantane: on lit juste la memoire.
// â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• 
const krakenCandles = {}; // { 'BTC/USDT': [{o,h,l,c,v}, ...] }
let krakenPairsList = [];
let wsConnected = false;
let ws = null;

const QUOTE_CURRENCIES = ['USDC', 'USD']; // USDT retire: bloque sur Kraken Canada

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
    console.log(`[Diagnostic] ${pairs.length} paires au total (USDC+USD, USDT exclu - bloque au Canada)`);

    return pairs.slice(0, MAX_PAIRS);
  } catch (e) {
    console.log('Erreur fetchKrakenUsdtPairs:', e.message);
    return [];
  }
}

function connectKrakenWS(pairs) {
  if (ws) {
    try { ws.terminate(); } catch(e) {}
  }
  ws = new WebSocket('wss://ws.kraken.com/v2');

  ws.on('open', () => {
    wsConnected = true;
    console.log(`WebSocket Kraken connecte â€” abonnement a ${pairs.length} paires`);
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
          interval: 1
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
            if (arr.length > 60) arr.shift();
          }
        }
      }
    } catch (e) {}
  });

  ws.on('close', () => {
    wsConnected = false;
    console.log('WebSocket Kraken deconnecte â€” reconnexion dans 5s');
    setTimeout(() => connectKrakenWS(krakenPairsList), 5000);
  });

  ws.on('error', (err) => {
    console.log('Erreur WebSocket Kraken:', err.message);
  });
}

// Precharge les 50 dernieres bougies historiques via REST au demarrage
// pour ne pas attendre 50 minutes que le WebSocket les accumule.
async function preloadHistoricalCandles(pairs) {
  console.log(`Preloading ${pairs.length} paires via REST (bougies historiques)...`);
  const exchange = new ccxt.kraken({ enableRateLimit: true, timeout: 10000 });
  const BATCH = 20;
  let loaded = 0;
  for (let i = 0; i < pairs.length; i += BATCH) {
    const batch = pairs.slice(i, i + BATCH);
    await Promise.all(batch.map(async (symbol) => {
      try {
        const ohlcv = await exchange.fetchOHLCV(symbol, '1m', undefined, 55);
        if (!ohlcv || ohlcv.length < 10) return;
        krakenCandles[symbol] = ohlcv.map(c => ({
          t: String(c[0]),
          o: c[1], h: c[2], l: c[3], c: c[4], v: c[6] || c[5]
        }));
        loaded++;
      } catch(e) {}
    }));
    // Petite pause entre les batches pour eviter le rate limit
    if (i + BATCH < pairs.length) {
      await new Promise(r => setTimeout(r, 1500));
    }
  }
  console.log(`Preloading termine â€” ${loaded}/${pairs.length} paires chargees avec historique`);
}

async function initKrakenWS() {
  krakenPairsList = await fetchKrakenUsdtPairs();
  if (krakenPairsList.length === 0) {
    console.log('Aucune paire Kraken trouvee â€” retry dans 15s');
    setTimeout(initKrakenWS, 15000);
    return;
  }
  console.log(`${krakenPairsList.length} paires (USDC/USD) Kraken trouvees pour le flux WebSocket`);
  // Precharge les bougies historiques avant de connecter le WebSocket
  await preloadHistoricalCandles(krakenPairsList);
  connectKrakenWS(krakenPairsList);
}

// Scan instantane: lit les bougies deja en memoire (mises a jour par le WebSocket)
// au lieu de faire des requetes HTTP une par une.
function scanKrakenFromMemory() {
  const results = [];
  for (const symbol of krakenPairsList) {
    const candles = krakenCandles[symbol];
    if (!candles || candles.length < 20) continue;
    const closes  = candles.map(c => c.c);
    const volumes = candles.map(c => c.v);
    const price   = closes[closes.length - 1];
    const sig = detectFigure(closes, volumes);
    if (!sig) continue;

    const volRatio = volumes[volumes.length-1] / avg(volumes.slice(-20));
    const signal = {
      symbol,
      exchange:    'Kraken',
      exchangeId:  'kraken',
      timeframe:   '1m',
      market:      'Spot',
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

    new Signal({
      symbol, exchange:'Kraken', market:'Spot',
      figure:sig.fig.name, direction:sig.fig.dir,
      confidence:signal.confidence, entryPrice:price,
      tp:sig.tp, sl:sig.sl, volumeRatio:volRatio, timeframe:'1m'
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
        const isFut  = (m.type === 'future' || m.type === 'swap') && exConfig.futures;
        return isUSDT && (isSpot || isFut) && m.active;
      })
      .slice(0, 100); // limite plus basse en REST pour rester sous 60s

    const BATCH = 15;
    for (let i = 0; i < symbols.length; i += BATCH) {
      const batch = symbols.slice(i, i + BATCH);
      const batchResults = await Promise.all(batch.map(async (symbol) => {
        try {
          const ohlcv = await exchange.fetchOHLCV(symbol, '1m', undefined, 50);
          if (!ohlcv || ohlcv.length < 20) return null;
          const closes  = ohlcv.map(c => c[4]);
          const volumes = ohlcv.map(c => c[5]);
          const price   = closes[closes.length-1];
          const market  = markets[symbol].type;
          const sig = detectFigure(closes, volumes);
          if (!sig) return null;
          const volRatio = volumes[volumes.length-1] / avg(volumes.slice(-20));
          return {
            symbol, exchange: exConfig.name, exchangeId: exConfig.id, timeframe: '1m',
            market: market === 'spot' ? 'Spot' : 'Futures',
            figure: sig.fig.name, figureCode: sig.fig.code, direction: sig.fig.dir,
            confidence: Math.round(sig.fig.wr * 100), entryPrice: price,
            tp: sig.tp, sl: sig.sl, volumeRatio: volRatio.toFixed(2),
            tradeAmount: TRADE_AMOUNT, gain: (TRADE_AMOUNT*TP_PCT).toFixed(4),
            loss: (TRADE_AMOUNT*SL_PCT).toFixed(4), commission: (TRADE_AMOUNT*COMM_RATE).toFixed(4),
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

let scanRunning = false;
async function scanAll() {
  if (scanRunning) {
    console.log('Scan precedent encore en cours â€” on attend le prochain cycle');
    return;
  }
  scanRunning = true;
  const startTime = Date.now();
  console.log(`\n=== SCAN 1m â€” ${new Date().toLocaleTimeString()} ===`);
  signalsCache.length = 0;
  Object.keys(signalsByExchange).forEach(k => delete signalsByExchange[k]);

  try {
    const users = await User.find({ active:true, apiKey:{$exists:true} });

    if (users.length === 0) {
      console.log('Aucun utilisateur â€” scan Kraken par defaut (mode test, via WebSocket)');
      const results = scanKrakenFromMemory();
      signalsCache.push(...results);
      signalsByExchange['kraken'] = results;
      lastScanTime = new Date();
      console.log(`[Kraken-WS] ${krakenPairsList.length} paires en memoire Â· ${results.length} signal(s)`);
      console.log(`=== FIN test Â· ${signalsCache.length} signaux Â· ${Date.now()-startTime}ms ===\n`);
      return;
    }

    const uniqueExchanges = [...new Set(users.map(u => u.exchangeName.toLowerCase()))];
    console.log(`Utilisateurs: ${users.length} Â· Plateformes: ${uniqueExchanges.join(', ')}`);

    for (const exchangeId of uniqueExchanges) {
      const exConfig = EXCHANGES_CONFIG.find(e => e.id === exchangeId || e.name.toLowerCase() === exchangeId);
      if (!exConfig) continue;

      let results;
      if (exConfig.id === 'kraken') {
        // Scan quasi instantane via WebSocket (donnees deja en memoire)
        results = scanKrakenFromMemory();
        console.log(`[Kraken-WS] ${krakenPairsList.length} paires en memoire Â· ${results.length} signal(s)`);
      } else {
        // Repli REST plus lent pour les autres plateformes
        results = await scanExchangeRest(exConfig);
        console.log(`[${exConfig.name}-REST] ${results.length} signal(s)`);
      }
      signalsCache.push(...results);
      signalsByExchange[exConfig.id] = results;
    }

    lastScanTime = new Date();
    console.log(`=== FIN Â· ${signalsCache.length} signaux Â· ${Date.now()-startTime}ms ===\n`);

    // â”€â”€ EXECUTION REELLE DES TRADES (plus de simulation Math.random) â”€â”€
    // On utilise directement les signaux deja detectes par le WebSocket
    // pour placer de vrais ordres sur le compte de chaque utilisateur.
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

        const usdc = balance.USDC?.free || 0;
        const usd  = balance.USD?.free  || 0;
        if (usdc + usd <= 0) {
          console.log(`[Bot] Aucun fonds pour ${user.email}`);
          continue;
        }

        const rawAmount  = user.tradeAmount || TRADE_AMOUNT;
        const amount     = Math.min(Math.max(rawAmount, 5), 50); // min 5$, max 50$
        let ordersPlaced = 0;

        for (const sig of userSignals.slice(0, MAX_CONCURRENT)) {
          if (ordersPlaced >= MAX_CONCURRENT) break;
          try {
            const [base, quote] = sig.symbol.split('/');
            const quoteBalance  = balance[quote]?.free || 0;
            const price         = sig.entryPrice;

            // â”€â”€ LONG â†’ BUY â”€â”€
            if (sig.direction === 'Long' && quoteBalance >= amount) {
              const qty           = amount / price;
              const commissionUSD = amount * COMM_RATE;
              console.log(`[Bot] ORDRE BUY REEL: ${sig.symbol} Â· ${sig.figure} Â· $${amount}`);
              const order = await exchange.createMarketBuyOrder(sig.symbol, qty);
              console.log(`[Bot] Ordre execute: ${order.id}`);
              await new Trade({
                email: user.email, symbol: sig.symbol, exchange: sig.exchange,
                market: sig.market, direction: sig.direction, figure: sig.figure,
                entryPrice: price, exitPrice: sig.tp,
                amount, pnl: amount*TP_PCT - commissionUSD,
                commission: commissionUSD, result: 'OPEN', exitReason: 'Ordre place'
              }).save();
              ordersPlaced++;
            }

            // â”€â”€ SHORT â†’ SELL (si position ouverte sur cette paire) â”€â”€
            if (sig.direction === 'Short') {
              const baseBalance = balance[base]?.free || 0;
              if (baseBalance > 0.000001) {
                const tradeValue    = baseBalance * price;
                const commissionUSD = tradeValue * COMM_RATE;
                console.log(`[Bot] ORDRE SELL REEL: ${sig.symbol} Â· ${sig.figure} Â· ${baseBalance} ${base}`);
                const order = await exchange.createMarketSellOrder(sig.symbol, baseBalance);
                console.log(`[Bot] Ordre execute: ${order.id}`);
                await new Trade({
                  email: user.email, symbol: sig.symbol, exchange: sig.exchange,
                  market: sig.market, direction: sig.direction, figure: sig.figure,
                  entryPrice: price, exitPrice: sig.sl,
                  amount: tradeValue, pnl: -(tradeValue*SL_PCT + commissionUSD),
                  commission: commissionUSD, result: 'OPEN', exitReason: 'Ordre place'
                }).save();
                ordersPlaced++;
              }
            }

          } catch(e) {
            console.log(`[Bot] Erreur ordre ${sig.symbol}:`, e.message);
          }
        }

        if (ordersPlaced > 0) console.log(`[Bot] ${ordersPlaced} ordre(s) place(s) pour ${user.email}`);

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
  strategy:      'Figures chartistes + Volume Â· Ratio 1:4 Â· Timeframe 1m',
  scanMethod:    'WebSocket Kraken (temps reel) + REST en repli pour autres plateformes',
  tradeAmount:   TRADE_AMOUNT,
  slPct:         SL_PCT*100+'%',
  tpPct:         TP_PCT*100+'%',
  exchanges:     EXCHANGES_CONFIG.length,
  krakenWsConnected: wsConnected,
  krakenPairsTracked: krakenPairsList.length,
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
    res.json({ success:true, message:`Connecte sur ${exchangeName} Â· $${tradeAmount||TRADE_AMOUNT}/trade Â· Ratio 1:4 Â· 1m` });
  } catch(e) { res.json({ success:false, error:e.message }); }
});

// â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• 
// ROUTES FUTURES â€” systeme separe de Kraken Spot, cle API differente
// â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• 
const { FuturesUser, FuturesTrade, MAX_TRADE_AMOUNT_USD: FUTURES_MAX_AMOUNT, MAX_LEVERAGE: FUTURES_MAX_LEVERAGE } = require('./bot-futures');

app.post('/connect-futures', async (req, res) => {
  const { email, apiKey, secret, tradeAmount, leverage } = req.body;
  if (!email || !apiKey || !secret)
    return res.json({ success:false, error:'Donnees manquantes (cle API Futures requise, differente de la cle Spot)' });
  try {
    await FuturesUser.findOneAndUpdate(
      { email },
      { apiKey, apiSecret:secret, active:true, tradeAmount: tradeAmount || 2, leverage: leverage || 1 },
      { upsert:true, new:true }
    );
    res.json({
      success:true,
      message:`Connecte sur Kraken Futures Â· $${tradeAmount||2}/trade Â· Levier ${leverage||1}x (max ${FUTURES_MAX_LEVERAGE}x)`
    });
  } catch(e) { res.json({ success:false, error:e.message }); }
});

app.get('/futures-trades/:email', async (req, res) => {
  const trades = await FuturesTrade.find({ email: req.params.email }).sort({ time: -1 }).limit(100);
  res.json({ success: true, trades, count: trades.length });
});

app.get('/futures-status/:email', async (req, res) => {
  const user = await FuturesUser.findOne({ email: req.params.email });
  if (!user) return res.json({ connected: false });
  const trades = await FuturesTrade.countDocuments({ email: req.params.email });
  res.json({
    connected: true,
    active: user.active,
    tradeAmount: user.tradeAmount,
    leverage: user.leverage,
    maxAllowedAmount: FUTURES_MAX_AMOUNT,
    maxAllowedLeverage: FUTURES_MAX_LEVERAGE,
    trades
  });
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
  const email = req.params.email;
  // Totaux calcules sur TOUT l'historique (pas seulement les 100 affiches)
  const totalCount = await Trade.countDocuments({ email });
  const allTrades = await Trade.find({ email });
  const totalPnl = allTrades.reduce((a,t)=>a+t.pnl,0);
  const totalWins = allTrades.filter(t=>t.result==='WIN').length;
  const totalLosses = totalCount - totalWins;

  // Liste detaillee: seulement les 100 plus recents (pour l'affichage)
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

app.get('/signals', async (req, res) => {
  const signals = await Signal.find().sort({time:-1}).limit(100);
  res.json({ signals });
});

app.get('/exchanges', (req, res) => {
  res.json({ exchanges: EXCHANGES_CONFIG });
});
