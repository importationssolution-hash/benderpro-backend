const express = require('express');
const cors = require('cors');
const app = express();
app.use(cors());
app.use(express.json());

const users = {};
const trades = {};

function calcRSI(prices) {
  if (prices.length < 15) return 50;
  let g = 0, l = 0;
  for (let i = prices.length - 14; i < prices.length; i++) {
    const d = prices[i] - prices[i-1];
    if (d > 0) g += d; else l -= d;
  }
  return 100 - (100 / (1 + (g/14) / ((l/14) || 0.001)));
}

function calcEMA(prices, n) {
  if (prices.length < n) return prices[prices.length-1];
  const k = 2/(n+1);
  let e = prices.slice(0,n).reduce((a,b)=>a+b)/n;
  for (let i = n; i < prices.length; i++) e = prices[i]*k + e*(1-k);
  return e;
}

function getSignal(prices) {
  const p = prices[prices.length-1];
  const rsi = calcRSI(prices);
  const e20 = calcEMA(prices, 20);
  const e50 = calcEMA(prices, 50);
  let score = 0;
  if (rsi < 30) score += 3;
  else if (rsi < 40) score += 1;
  else if (rsi > 70) score -= 3;
  else if (rsi > 60) score -= 1;
  if (e20 > e50) score += 1; else score -= 1;
  if (p > e50) score += 1; else score -= 1;
  const conf = Math.min(92, Math.abs(score) * 10 + 55);
  if (score >= 3) return { signal:'BUY', confidence:Math.round(conf), rsi:Math.round(rsi) };
  if (score <= -3) return { signal:'SELL', confidence:Math.round(conf), rsi:Math.round(rsi) };
  return { signal:'WAIT', confidence:Math.round(conf), rsi:Math.round(rsi) };
}

function genPrices(base) {
  const p = [base];
  for (let i = 1; i < 100; i++) p.push(+(p[i-1]*(1+(Math.random()-0.48)*0.018)).toFixed(2));
  return p;
}

function analyzeMarket() {
  const bases = {'BTC/USDT':65000,'ETH/USDT':3400,'SOL/USDT':145,'BNB/USDT':580};
  const results = {};
  Object.entries(bases).forEach(([sym, base]) => {
    const prices = genPrices(base);
    const sig = getSignal(prices);
    results[sym] = { price: prices[prices.length-1].toFixed(2), ...sig, time: new Date().toISOString() };
  });
  return results;
}

app.get('/', (req, res) => res.json({ status:'Bender Pro Backend actif', users:Object.keys(users).length }));

app.get('/market', (req, res) => res.json({ success:true, data:analyzeMarket() }));

app.post('/connect', (req, res) => {
  const { email, apiKey, secret, exchangeName } = req.body;
  if (!email || !apiKey || !secret || !exchangeName)
    return res.json({ success:false, error:'Donnees manquantes' });
  users[email] = { apiKey, secret, exchangeName, active:true, connectedAt:new Date().toISOString() };
  if (!trades[email]) trades[email] = [];
  console.log('Connecte:', email, 'sur', exchangeName);
  res.json({ success:true, message:'Connecte sur ' + exchangeName });
});

app.get('/status/:email', (req, res) => {
  const u = users[req.params.email];
  if (!u) return res.json({ connected:false });
  res.json({ connected:true, active:u.active, exchange:u.exchangeName, connectedAt:u.connectedAt });
});

app.get('/trades/:email', (req, res) => res.json({ trades: trades[req.params.email] || [] }));

app.post('/toggle', (req, res) => {
  const { email, active } = req.body;
  if (!users[email]) return res.json({ success:false, error:'Non connecte' });
  users[email].active = active;
  res.json({ success:true, active });
});

setInterval(() => {
  const active = Object.entries(users).filter(([,u]) => u.active);
  if (active.length === 0) return;
  console.log('Cycle trading -', active.length, 'utilisateurs actifs');
  const market = analyzeMarket();
  active.forEach(([email, user]) => {
    Object.entries(market).forEach(([symbol, data]) => {
      if (data.signal !== 'WAIT' && data.confidence > 65) {
        trades[email].push({
          type:data.signal, symbol, price:data.price,
          confidence:data.confidence, exchange:user.exchangeName,
          commission:(parseFloat(data.price)*0.001).toFixed(4),
          time:new Date().toISOString()
        });
        console.log('['+email+']', data.signal, symbol, '@$'+data.price);
      }
    });
  });
}, 30*60*1000);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('Bender Pro Backend demarre port', PORT));
