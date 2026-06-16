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

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// ROUTES FUTURES â€” systeme separe de Kraken Spot, cle API differente
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
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
function refreshPricesFromMemory() {
  const out = {};
  const watch = ['BTC/USDT','ETH/USDT','SOL/USDT','XRP/USDT','ADA/USDT',
    'AVAX/USDT','DOGE/USDT','DOT/USDT','LINK/USDT','LTC/USDT',
    'ATOM/USDT','UNI/USDT','NEAR/USDT','ARB/USDT','OP/USDT','APT/USDT','SUI/USDT','INJ/USDT'];
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
    wallet:       BENDER_WALLET,
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
  console.log(`\n Bender Pro v8.0 Â· Port ${PORT}`);
  console.log(` Figures chartistes + Volume Â· Ratio 1:4 Â· 1m`);
  console.log(` Scan Kraken via WebSocket (quasi instantane)`);
  console.log(` Helmet actif Â· Securite HTTP headers`);
  console.log(` $${TRADE_AMOUNT}/trade Â· SL -1% Â· TP +4%`);
  console.log(` Scan toutes les 60 secondes\n`);
  initKrakenWS();
  setTimeout(() => scanAll().catch(console.error), 8000);
});

setInterval(() => scanAll().catch(console.error), SCAN_INTERVAL);

// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// EXECUTION REELLE DES TRADES (bot.js) â€” declenchee automatiquement
// chaque minute, juste apres le scan de signaux.
// bot.js utilise sa propre logique RSI/EMA et place de VRAIS ordres
// sur la plateforme connectee par l'utilisateur (Kraken, etc).
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
let botRunning = false;
async function runBotCycle() {
  if (botRunning) {
    console.log('[Bot] Cycle precedent encore en cours â€” on attend le prochain');
    return;
  }
  botRunning = true;
  try {
    const { execSync } = require('child_process');
    console.log('[Bot] Lancement du cycle de trading reel...');
    const output = execSync('node bot.js --once', {
      encoding: 'utf-8',
      timeout: 50000,
      env: process.env
    });
    console.log(output);
  } catch (e) {
    console.log('[Bot] Erreur cycle:', e.message);
  } finally {
    botRunning = false;
  }
}
setTimeout(() => runBotCycle().catch(console.error), 15000);
setInterval(() => runBotCycle().catch(console.error), SCAN_INTERVAL);

// Cycle Futures â€” execution reelle separee, declenchee aussi chaque minute
let futuresBotRunning = false;
async function runFuturesBotCycle() {
  if (futuresBotRunning) {
    console.log('[Futures] Cycle precedent encore en cours â€” on attend le prochain');
    return;
  }
  futuresBotRunning = true;
  try {
    const { execSync } = require('child_process');
    console.log('[Futures] Lancement du cycle de trading futures reel...');
    const output = execSync('node bot-futures.js --once', {
      encoding: 'utf-8',
      timeout: 50000,
      env: process.env
    });
    console.log(output);
  } catch (e) {
    console.log('[Futures] Erreur cycle:', e.message);
  } finally {
    futuresBotRunning = false;
  }
}
setTimeout(() => runFuturesBotCycle().catch(console.error), 25000);
setInterval(() => runFuturesBotCycle().catch(console.error), SCAN_INTERVAL);
