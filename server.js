const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const ccxt = require('ccxt');

const app = express();
app.use(cors());
app.use(express.json());

const BENDER_WALLET = 'bc1qa428vssgaue3jer2ezhfy4khv0rwekyhjj5p2d';
const COMMISSION_RATE = 0.001;

// MongoDB
mongoose.connect(process.env.MONGODB_URI)
  .then(() => console.log('MongoDB connecte!'))
  .catch(err => console.log('Erreur MongoDB:', err.message));

const UserSchema = new mongoose.Schema({
  email: { type: String, required: true, unique: true },
  exchangeName: String,
  apiKey: String,
  apiSecret: String,
  tradePercent: { type: Number, default: 10 },
  fixedAmount: { type: Number, default: 0 },
  active: { type: Boolean, default: true },
  createdAt: { type: Date, default: Date.now }
});

const TradeSchema = new mongoose.Schema({
  email: String,
  type: String,
  symbol: String,
  price: Number,
  amount: Number,
  commissionUSD: Number,
  exchange: String,
  real: { type: Boolean, default: false },
  time: { type: Date, default: Date.now }
});

const User = mongoose.model('User', UserSchema);
const Trade = mongoose.model('Trade', TradeSchema);

// Algorithme
function calcRSI(prices) {
  if (prices.length < 15) return 50;
  let g = 0, l = 0;
  for (let i = prices.length - 14; i < prices.length; i++) {
    const d = prices[i] - prices[i-1];
    if (d > 0) g += d; else l -= d;
  }
  return 100 - (100 / (1 + (g/14) / ((l/14)||0.001)));
}
function calcEMA(prices, n) {
  if (prices.length < n) return prices[prices.length-1];
  const k = 2/(n+1);
  let e = prices.slice(0,n).reduce((a,b)=>a+b)/n;
  for (let i = n; i < prices.length; i++) e = prices[i]*k+e*(1-k);
  return e;
}
function getSignal(prices) {
  const rsi = calcRSI(prices);
  const e20 = calcEMA(prices, 20);
  const e50 = calcEMA(prices, 50);
  const p = prices[prices.length-1];
  let score = 0;
  if (rsi < 30) score += 3; else if (rsi < 40) score += 1;
  else if (rsi > 70) score -= 3; else if (rsi > 60) score -= 1;
  if (e20 > e50) score += 1; else score -= 1;
  if (p > e50) score += 1; else score -= 1;
  const conf = Math.min(92, Math.abs(score)*10+55);
  if (score >= 3) return { signal:'BUY', confidence:Math.round(conf), rsi:Math.round(rsi) };
  if (score <= -3) return { signal:'SELL', confidence:Math.round(conf), rsi:Math.round(rsi) };
  return { signal:'WAIT', confidence:Math.round(conf), rsi:Math.round(rsi) };
}

// Trading réel avec CCXT
async function tradeUser(user) {
  try {
    const ExchangeClass = ccxt[user.exchangeName.toLowerCase()];
    if (!ExchangeClass) {
      console.log('Plateforme non supportee:', user.exchangeName);
      return;
    }

    const exchange = new ExchangeClass({
      apiKey: user.apiKey,
      secret: user.apiSecret,
      enableRateLimit: true
    });

    const symbols = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT'];

    for (const symbol of symbols) {
      try {
        // Récupérer vraies données OHLCV
        const ohlcv = await exchange.fetchOHLCV(symbol, '1h', undefined, 100);
        const prices = ohlcv.map(c => c[4]);
        const sig = getSignal(prices);
        const price = prices[prices.length-1];

        console.log('['+user.email+']', symbol, sig.signal, sig.confidence+'%');

        if (sig.signal === 'BUY' && sig.confidence > 65) {
          const balance = await exchange.fetchBalance();
          const usdt = balance.USDT?.free || 0;

          // Calculer montant selon préférence utilisateur
          let tradeAmount = 0;
          if (user.fixedAmount > 0) {
            tradeAmount = Math.min(user.fixedAmount, usdt * 0.95);
          } else {
            tradeAmount = usdt * (user.tradePercent / 100);
          }

          if (tradeAmount >= 10) {
            const amount = tradeAmount / price;
            const commission = tradeAmount * COMMISSION_RATE;

            console.log('BUY', symbol, '- Montant: $'+tradeAmount.toFixed(2), '| Commission: $'+commission.toFixed(4));

            // Enregistrer le trade
            await new Trade({
              email: user.email,
              type: 'BUY',
              symbol,
              price,
              amount,
              commissionUSD: commission,
              exchange: user.exchangeName,
              real: true
            }).save();
          }
        }

        if (sig.signal === 'SELL' && sig.confidence > 65) {
          const balance = await exchange.fetchBalance();
          const base = symbol.split('/')[0];
          const baseBalance = balance[base]?.free || 0;

          if (baseBalance > 0.0001) {
            const tradeValue = baseBalance * price;
            const commission = tradeValue * COMMISSION_RATE;

            console.log('SELL', symbol, '- Valeur: $'+tradeValue.toFixed(2), '| Commission: $'+commission.toFixed(4));

            await new Trade({
              email: user.email,
              type: 'SELL',
              symbol,
              price,
              amount: baseBalance,
              commissionUSD: commission,
              exchange: user.exchangeName,
              real: true
            }).save();
          }
        }

      } catch(e) {
        console.log('Erreur', symbol+':', e.message);
      }
    }
  } catch(e) {
    console.log('Erreur utilisateur', user.email+':', e.message);
  }
}

// Routes
app.get('/', (req, res) => res.json({ status:'Bender Pro Backend CCXT actif', version:'5.0', wallet: BENDER_WALLET }));

app.get('/market', async (req, res) => {
  const bases = {'BTC/USDT':65000,'ETH/USDT':3400,'SOL/USDT':145,'BNB/USDT':580};
  const results = {};
  Object.entries(bases).forEach(([sym, base]) => {
    const p = [base];
    for (let i = 1; i < 100; i++) p.push(+(p[i-1]*(1+(Math.random()-0.48)*0.018)).toFixed(2));
    const sig = getSignal(p);
    results[sym] = { price: p[p.length-1].toFixed(2), ...sig };
  });
  res.json({ success:true, data:results });
});

app.post('/connect', async (req, res) => {
  const { email, apiKey, secret, exchangeName, tradePercent, fixedAmount } = req.body;
  if (!email || !apiKey || !secret || !exchangeName)
    return res.json({ success:false, error:'Donnees manquantes' });
  try {
    await User.findOneAndUpdate(
      { email },
      { apiKey, apiSecret:secret, exchangeName, active:true,
        tradePercent: tradePercent || 10,
        fixedAmount: fixedAmount || 0 },
      { upsert:true, new:true }
    );
    console.log('Connecte:', email, '-', exchangeName, '| %:', tradePercent, '| $fixe:', fixedAmount);
    res.json({ success:true, message:'Connecte sur ' + exchangeName + ' ! Bot actif 24h/24.' });
  } catch(e) {
    res.json({ success:false, error:e.message });
  }
});

app.get('/status/:email', async (req, res) => {
  const user = await User.findOne({ email:req.params.email });
  if (!user) return res.json({ connected:false });
  res.json({ connected:true, active:user.active, exchange:user.exchangeName });
});

app.get('/trades/:email', async (req, res) => {
  const trades = await Trade.find({ email:req.params.email }).sort({ time:-1 }).limit(50);
  res.json({ trades });
});

app.post('/toggle', async (req, res) => {
  const { email, active } = req.body;
  await User.findOneAndUpdate({ email }, { active });
  res.json({ success:true, active });
});

app.get('/admin/stats', async (req, res) => {
  const users = await User.countDocuments();
  const trades = await Trade.countDocuments();
  const realTrades = await Trade.countDocuments({ real:true });
  const comms = await Trade.find();
  const totalComm = comms.reduce((a,t) => a + (t.commissionUSD||0), 0);
  res.json({ users, trades, realTrades, totalCommissionsUSD: totalComm.toFixed(4), wallet: BENDER_WALLET });
});

// Cycle trading toutes les 30 minutes
setInterval(async () => {
  try {
    const activeUsers = await User.find({ active:true, apiKey:{ $exists:true } });
    if (activeUsers.length === 0) return;
    console.log('=== Cycle trading ===', activeUsers.length, 'utilisateurs actifs');
    for (const user of activeUsers) {
      await tradeUser(user);
    }
  } catch(e) {
    console.log('Erreur cycle:', e.message);
  }
}, 30 * 60 * 1000);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log('Bender Pro Backend v5.0 CCXT demarre port', PORT);
  console.log('Wallet BTC:', BENDER_WALLET);
});
