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

function connectKrakenWS(pairs) {
  if (ws) {
    try { ws.terminate(); } catch(e) {}
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
  console.log(`Preloading ${pairs.length} paires via REST (bougies historiques)...`);
  const exchange = new ccxt.kraken({ enableRateLimit: true, timeout: 10000 });
  const BATCH = 50; // Batches plus grands pour aller plus vite
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
      const commissionUSD = pos.amount * COMM_RATE;

      if (currentPrice >= pos.tp) {
        // TP ATTEINT — vendre
        const baseBalance = balance[base]?.free || pos.qty;
        if (baseBalance > 0.000001) {
          console.log(`[TP/SL] TP ATTEINT sur ${pos.symbol} — prix:${currentPrice} >= TP:${pos.tp} — VENTE`);
          const order = await exchange.createMarketSellOrder(pos.symbol, baseBalance);
          console.log(`[TP/SL] Ordre SELL execute: ${order.id}`);
          const pnl = pos.amount * TP_PCT - commissionUSD * 2;
          await Trade.findOneAndUpdate(
            { email: user.email, symbol: pos.symbol, result: 'OPEN' },
            { exitPrice: currentPrice, pnl, result: 'WIN', exitReason: 'TP +2% atteint' },
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
          const order = await exchange.createMarketSellOrder(pos.symbol, baseBalance);
          console.log(`[TP/SL] Ordre SELL execute: ${order.id}`);
          const pnl = -(pos.amount * SL_PCT + commissionUSD * 2);
          await Trade.findOneAndUpdate(
            { email: user.email, symbol: pos.symbol, result: 'OPEN' },
            { exitPrice: currentPrice, pnl, result: 'LOSS', exitReason: 'SL -0.5% touche' },
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

let scanRunning = false;
async function scanAll() {
  if (scanRunning) {
    console.log('Scan precedent encore en cours — on attend le prochain cycle');
    return;
  }
  scanRunning = true;
  const startTime = Date.now();
  console.log(`\n=== SCAN 1m — ${new Date().toLocaleTimeString()} ===`);
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
              const commissionUSD = amount * COMM_RATE;
              const tpPrice       = +(price * (1 + TP_PCT)).toFixed(8);
              const slPrice       = +(price * (1 - SL_PCT)).toFixed(8);
              console.log(`[Bot] ORDRE BUY REEL: ${sig.symbol} · ${sig.figure} · $${amount} · TP:${tpPrice} · SL:${slPrice}`);
              const order = await exchange.createMarketBuyOrder(sig.symbol, qty);
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
                amount, pnl: 0, commission: commissionUSD,
                result: 'OPEN', exitReason: 'Position ouverte — en attente TP/SL'
              }).save();
              ordersPlaced++;
            }

          } catch(e) {
            console.log(`[Bot] Erreur ordre ${sig.symbol}:`, e.message);
          }
        }

        if (ordersPlaced > 0) console.log(`[Bot] ${ordersPlaced} ordre(s) place(s) pour ${user.email}`);

        // ── SUIVI TP/SL DES POSITIONS OUVERTES ──
        await checkOpenPositions(user, exchange, balance);

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

// ═══════════════════════════════════════════════════════════════════
// ROUTES FUTURES — systeme separe de Kraken Spot, cle API differente
// ═══════════════════════════════════════════════════════════════════
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
      message:`Connecte sur Kraken Futures · $${tradeAmount||2}/trade · Levier ${leverage||1}x (max ${FUTURES_MAX_LEVERAGE}x)`
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
  console.log(`\n Bender Pro v8.0 · Port ${PORT}`);
  console.log(` Figures chartistes + Volume · Ratio 1:4 · 1m`);
  console.log(` Scan Kraken via WebSocket (quasi instantane)`);
  console.log(` Helmet actif · Securite HTTP headers`);
  console.log(` $${TRADE_AMOUNT}/trade · SL -0.5% · TP +2%`);
  console.log(` Scan toutes les 60 secondes\n`);
  setImmediate(() => {
    initKrakenWS().then(() => {
      setTimeout(() => scanAll().catch(console.error), 5000);
      setInterval(() => scanAll().catch(console.error), SCAN_INTERVAL);
    }).catch(console.error);
  });
});
