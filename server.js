const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');

const app = express();
app.use(cors());
app.use(express.json());

// â”€â”€ CONNEXION MONGODB â”€â”€
mongoose.connect(process.env.MONGODB_URI)
  .then(() => console.log('MongoDB connecte !'))
  .catch(err => console.log('Erreur MongoDB:', err.message));

// â”€â”€ MODÃˆLES â”€â”€
const UserSchema = new mongoose.Schema({
  email: { type: String, required: true, unique: true },
  password: String,
  exchangeName: String,
  apiKey: String,
  apiSecret: String,
  active: { type: Boolean, default: true },
  createdAt: { type: Date, default: Date.now }
});

const TradeSchema = new mongoose.Schema({
  email: String,
  type: String,
  symbol: String,
  price: String,
  amount: String,
  confidence: Number,
  commission: String,
  exchange: String,
  time: { type: Date, default: Date.now }
});

const CommissionSchema = new mongoose.Schema({
  email: String,
  amount: Number,
  symbol: String,
  tradeValue: Number,
  time: { type: Date, default: Date.now }
});

const User = mongoose.model('User', UserSchema);
const Trade = mongoose.model('Trade', TradeSchema);
const Commission = mongoose.model('Commission', CommissionSchema);

// â”€â”€ ALGORITHME â”€â”€
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

// â”€â”€ ROUTES â”€â”€
app.get('/', (req, res) => res.json({ status:'Bender Pro Backend actif avec MongoDB', version:'3.0' }));

app.get('/market', (req, res) => res.json({ success:true, data:analyzeMarket() }));

// Inscription
app.post('/register', async (req, res) => {
  const { email, password } = req.body;
  if (!email || !password) return res.json({ success:false, error:'Email et mot de passe requis' });
  try {
    const existing = await User.findOne({ email });
    if (existing) return res.json({ success:false, error:'Email deja utilise' });
    const user = new User({ email, password });
    await user.save();
    console.log('Nouvel utilisateur:', email);
    res.json({ success:true, message:'Compte cree !' });
  } catch(e) {
    res.json({ success:false, error:e.message });
  }
});

// Connexion plateforme
app.post('/connect', async (req, res) => {
  const { email, apiKey, secret, exchangeName } = req.body;
  if (!email || !apiKey || !secret || !exchangeName)
    return res.json({ success:false, error:'Donnees manquantes' });
  try {
    await User.findOneAndUpdate(
      { email },
      { apiKey, apiSecret:secret, exchangeName, active:true },
      { upsert:true, new:true }
    );
    console.log('Plateforme connectee:', email, '-', exchangeName);
    res.json({ success:true, message:'Connecte sur ' + exchangeName + ' ! Bot actif.' });
  } catch(e) {
    res.json({ success:false, error:e.message });
  }
});

// Statut
app.get('/status/:email', async (req, res) => {
  const user = await User.findOne({ email:req.params.email });
  if (!user) return res.json({ connected:false });
  res.json({ connected:true, active:user.active, exchange:user.exchangeName, createdAt:user.createdAt });
});

// Historique trades
app.get('/trades/:email', async (req, res) => {
  const trades = await Trade.find({ email:req.params.email }).sort({ time:-1 }).limit(50);
  res.json({ trades });
});

// Commissions totales
app.get('/commissions/:email', async (req, res) => {
  const comms = await Commission.find({ email:req.params.email });
  const total = comms.reduce((a,c) => a + c.amount, 0);
  res.json({ total:total.toFixed(4), count:comms.length, commissions:comms });
});

// Stats admin
app.get('/admin/stats', async (req, res) => {
  const users = await User.countDocuments();
  const trades = await Trade.countDocuments();
  const comms = await Commission.find();
  const totalComm = comms.reduce((a,c) => a + c.amount, 0);
  res.json({ users, trades, totalCommissions:totalComm.toFixed(4) });
});

// Activer/Desactiver
app.post('/toggle', async (req, res) => {
  const { email, active } = req.body;
  await User.findOneAndUpdate({ email }, { active });
  res.json({ success:true, active });
});

// â”€â”€ CRON : toutes les 30 minutes â”€â”€
setInterval(async () => {
  try {
    const activeUsers = await User.find({ active:true, apiKey:{ $exists:true } });
    if (activeUsers.length === 0) return;
    console.log('Cycle trading -', activeUsers.length, 'utilisateurs actifs');
    const market = analyzeMarket();

    for (const user of activeUsers) {
      for (const [symbol, data] of Object.entries(market)) {
        if (data.signal !== 'WAIT' && data.confidence > 65) {
          const tradeValue = parseFloat(data.price) * 0.001;
          const commission = tradeValue * 0.001;

          // Sauvegarder le trade
          await new Trade({
            email:user.email, type:data.signal, symbol,
            price:data.price, confidence:data.confidence,
            commission:commission.toFixed(4), exchange:user.exchangeName
          }).save();

          // Sauvegarder la commission
          await new Commission({
            email:user.email, amount:commission,
            symbol, tradeValue
          }).save();

          console.log('['+user.email+']', data.signal, symbol, '| Commission: $'+commission.toFixed(4));
        }
      }
    }
  } catch(e) {
    console.log('Erreur cycle:', e.message);
  }
}, 30 * 60 * 1000);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('Bender Pro Backend v3.0 avec MongoDB demarre port', PORT));
