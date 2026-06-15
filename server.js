// Bender Pro v7.3 — Ultra-faible latence · WebSocket · Pool de connexions
// npm install express cors mongoose ccxt
const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const ccxt = require('ccxt');
const crypto = require('crypto');
const http = require('http');

const app = express();
app.use(cors({
  origin: process.env.ALLOWED_ORIGIN || '*',
  methods: ['GET','POST'],
  allowedHeaders: ['Content-Type','Authorization']
}));
app.use(express.json({ limit: '10kb' }));

// Rate limiting — anti DDoS

// Signaux en direct — tous les exchanges
app.get('/market', (req, res) => {
  const token = req.headers['x-admin-token'];
  const isAdmin = token === process.env.ADMIN_TOKEN;
  const { exchange, direction, figure } = req.query;
  let sigs = [...signalsCache];
  if (exchange) sigs = sigs.filter(s=>s.exchange.toLowerCase().includes(exchange.toLowerCase()));
  if (direction) sigs = sigs.filter(s=>s.direction===direction);
  if (figure) sigs = sigs.filter(s=>s.figure.includes(figure));
  // Masquer les details sensibles pour les non-admins
  const safeSigs = isAdmin ? sigs : sigs.map(s=>({
    direction: s.direction,
    figure: s.figure,
    market: s.market,
    confidence: s.confidence,
    time: s.time
    // symbol, entryPrice, tp, sl, exchange masques
  }));
  res.json({ success:true, signals:safeSigs, count:safeSigs.length, lastScan:lastScanTime });
});

// Scan immédiat
app.post('/scan', async (req, res) => {
  res.json({ success:true, message:'Scan lancé en arrière-plan...' });
  scanAll().catch(console.error);
});

// Stats par exchange
app.get('/exchanges', async (req, res) => {
  const logs = await ScanLog.find().sort({time:-1}).limit(EXCHANGES_CONFIG.length*2);
  const stats = {};
  logs.forEach(l => {
    if (!stats[l.exchange]) stats[l.exchange] = l;
  });
  res.json({ exchanges: EXCHANGES_CONFIG.map(e => ({
    name: e.name,
    region: e.region,
    spot: e.spot,
    futures: e.futures,
    lastScan: stats[e.name] || null
  }))});
});

// Connexion utilisateur
app.post('/connect', strictLimiter, async (req, res) => {
  const { email, apiKey, secret, exchangeName, tradeAmount } = req.body;
  if (!email || !apiKey || !secret || !exchangeName)
    return res.json({ success:false, error:'Données manquantes' });
  try {
    // Validation des entrees
    if (!validateApiKey(apiKey) || !validateApiKey(secret)) {
      return res.json({ success:false, error:'Cle API invalide' });
    }
    if (!/^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/.test(email)) {
      return res.json({ success:false, error:'Email invalide' });
    }

    // Chiffrement AES-256 des cles API
    const encryptedKey = encrypt(apiKey);
    const encryptedSecret = encrypt(secret);

    await User.findOneAndUpdate(
      { email: sanitize(email) },
      { apiKey: encryptedKey, apiSecret: encryptedSecret,
        exchangeName: sanitize(exchangeName), active:true,
        tradeAmount:tradeAmount||TRADE_AMOUNT },
      { upsert:true, new:true }
    );
    res.json({ success:true, message:`Connecté sur ${exchangeName} · $${tradeAmount||TRADE_AMOUNT}/trade · Ratio 1:4 · Scanner 35 plateformes actif` });
  } catch(e) { res.json({ success:false, error:e.message }); }
});

app.get('/status/:email', async (req, res) => {
  const user = await User.findOne({ email:req.params.email });
  if (!user) return res.json({ connected:false });
  const trades = await Trade.countDocuments({ email:req.params.email });
  const wins   = await Trade.countDocuments({ email:req.params.email, result:'WIN' });
  res.json({ connected:true, active:user.active, exchange:user.exchangeName,
    tradeAmount:user.tradeAmount, trades, winRate:trades>0?Math.round(wins/trades*100)+'%':'N/A' });
});

app.get('/trades/:email', async (req, res) => {
  const email = sanitize(req.params.email);
  const trades = await Trade.find({ email }).sort({time:-1}).limit(100);
  const totalPnl = trades.reduce((a,t)=>a+t.pnl,0);
  const wins = trades.filter(t=>t.result==='WIN').length;
  // Masquer les details sensibles — prix et symboles proteges
  const safeTrades = trades.map(t => ({
    id: t._id,
    direction: t.direction,
    figure: t.figure,
    result: t.result,
    pnl: t.pnl,
    time: t.time,
    market: t.market
    // entryPrice, exitPrice, symbol, exchange masques
  }));
  res.json({ trades: safeTrades, totalPnl:totalPnl.toFixed(4), wins, losses:trades.length-wins });
});

app.get('/signals', async (req, res) => {
  const token = req.headers['x-admin-token'];
  if (token !== process.env.ADMIN_TOKEN) {
    return res.status(403).json({ error: 'Acces refuse' });
  }
  const signals = await Signal.find().sort({time:-1}).limit(50)
    .select('-symbol -entryPrice -tp -sl -exchange');
  res.json({ signals });
});

app.get('/admin/stats', async (req, res) => {
  const token = req.headers['x-admin-token'];
  if (token !== process.env.ADMIN_TOKEN) {
    return res.status(403).json({ error: 'Acces refuse' });
  }
  const users  = await User.countDocuments();
  const active = await User.countDocuments({ active:true });
  const trades = await Trade.countDocuments();
  const wins   = await Trade.countDocuments({ result:'WIN' });
  const comms  = await Trade.aggregate([{$group:{_id:null,total:{$sum:'$commission'}}}]);
  res.json({ users, active, trades, winRate:trades>0?Math.round(wins/trades*100)+'%':'N/A',
    totalCommissions:(comms[0]?.total||0).toFixed(4),
    signalsActive:signalsCache.length, lastScan:lastScanTime,
    exchanges:EXCHANGES_CONFIG.length, wallet:BENDER_WALLET });
});

app.post('/toggle', async (req, res) => {
  const { email, active } = req.body;
  await User.findOneAndUpdate({ email }, { active });
  res.json({ success:true, active });
});

// ── DÉMARRAGE ──
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log(`\n🤖 Bender Pro v7.0 démarré · Port ${PORT}`);
  console.log(`📊 ${EXCHANGES_CONFIG.length} plateformes · Spot + Futures`);
  console.log(`💰 $${TRADE_AMOUNT}/trade · SL -1% · TP +4% · Ratio 1:4`);
  console.log(`🔄 Scan toutes les 15 minutes\n`);

  // Premier scan au démarrage (délai 30s pour que MongoDB soit prêt)
  setTimeout(() => scanAll().catch(console.error), 30000);
});

// Scan toutes les 5 minutes — continu
setInterval(() => scanAll().catch(console.error), SCAN_INTERVAL);
