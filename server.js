const express = require('express');
const cors = require('cors');
const bot = require('./bot');

const app = express();
app.use(cors());
app.use(express.json());

const users = {};
const trades = {};

// ── ROUTE TEST ──
app.get('/', (req, res) => {
  res.json({ 
    status: 'Bender Pro Backend actif',
    users: Object.keys(users).length,
    version: '2.0'
  });
});

// ── ANALYSE MARCHÉ EN DIRECT ──
app.get('/market', (req, res) => {
  const analysis = bot.analyzeMarket();
  res.json({ success: true, data: analysis });
});

// ── CONNEXION PLATEFORME ──
app.post('/connect', (req, res) => {
  const { email, apiKey, secret, exchangeName } = req.body;
  if (!email || !apiKey || !secret || !exchangeName) {
    return res.json({ success: false, error: 'Donnees manquantes' });
  }
  users[email] = { apiKey, secret, exchangeName, active: true, connectedAt: new Date().toISOString() };
  if (!trades[email]) trades[email] = [];
  console.log('Nouvel utilisateur connecte:', email, 'sur', exchangeName);
  res.json({ success: true, message: 'Connecte avec succes sur ' + exchangeName });
});

// ── STATUT ──
app.get('/status/:email', (req, res) => {
  const user = users[req.params.email];
  if (!user) return res.json({ connected: false });
  res.json({ connected: true, active: user.active, exchange: user.exchangeName, connectedAt: user.connectedAt });
});

// ── HISTORIQUE TRADES ──
app.get('/trades/:email', (req, res) => {
  res.json({ trades: trades[req.params.email] || [] });
});

// ── ACTIVER / DÉSACTIVER ──
app.post('/toggle', (req, res) => {
  const { email, active } = req.body;
  if (!users[email]) return res.json({ success: false, error: 'Non connecte' });
  users[email].active = active;
  console.log(email, active ? 'Bot ACTIVE' : 'Bot DESACTIVE');
  res.json({ success: true, active });
});

// ── SIGNAL EN DIRECT ──
app.get('/signal/:symbol', (req, res) => {
  const symbol = req.params.symbol.replace('-', '/').toUpperCase();
  const bases = { 'BTC/USDT': 65000, 'ETH/USDT': 3400, 'SOL/USDT': 145, 'BNB/USDT': 580 };
  const base = bases[symbol] || 1000;
  const prices = [];
  let p = base;
  for (let i = 0; i < 100; i++) {
    p = +(p * (1 + (Math.random() - 0.48) * 0.018)).toFixed(2);
    prices.push(p);
  }
  const signal = bot.getSignal(prices);
  res.json({ symbol, price: prices[prices.length-1], ...signal, commission: bot.COMMISSION });
});

// ── CRON INTERNE : toutes les 30 minutes ──
setInterval(() => {
  console.log('Cycle de trading - ' + new Date().toLocaleTimeString());
  const activeUsers = Object.entries(users).filter(([, u]) => u.active);
  console.log('Utilisateurs actifs:', activeUsers.length);
  
  if (activeUsers.length > 0) {
    const market = bot.analyzeMarket();
    activeUsers.forEach(([email, user]) => {
      Object.entries(market).forEach(([symbol, data]) => {
        if (data.signal !== 'WAIT' && data.confidence > 65) {
          const trade = {
            type: data.signal,
            symbol,
            price: data.price,
            confidence: data.confidence,
            commission: data.commission,
            exchange: user.exchangeName,
            time: new Date().toISOString()
          };
          trades[email].push(trade);
          console.log('[' + email + ']', data.signal, symbol, '@$' + data.price, '| Conf:', data.confidence + '%');
        }
      });
    });
