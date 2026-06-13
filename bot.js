const ccxt = require('ccxt');
const mongoose = require('mongoose');

// ── CONFIG ──
const COMMISSION_RATE = 0.001; // 0.1%
const BENDER_WALLET = process.env.BENDER_WALLET;
const MONGODB_URI = process.env.MONGODB_URI;
const SYMBOLS = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT'];

// ── MONGODB ──
const UserSchema = new mongoose.Schema({
  email: String,
  exchangeName: String,
  apiKey: String,
  apiSecret: String,
  active: Boolean
});

const TradeSchema = new mongoose.Schema({
  email: String,
  type: String,
  symbol: String,
  price: String,
  commissionUSD: Number,
  commissionBTC: Number,
  walletDestination: String,
  time: { type: Date, default: Date.now }
});

const User = mongoose.model('User', UserSchema);
const Trade = mongoose.model('Trade', TradeSchema);

// ── ALGORITHME ──
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
  if (rsi < 30) score += 3;
  else if (rsi < 40) score += 1;
  else if (rsi > 70) score -= 3;
  else if (rsi > 60) score -= 1;
  if (e20 > e50) score += 1; else score -= 1;
  if (p > e50) score += 1; else score -= 1;
  const conf = Math.min(92, Math.abs(score)*10+55);
  if (score >= 3) return { signal:'BUY', confidence:Math.round(conf) };
  if (score <= -3) return { signal:'SELL', confidence:Math.round(conf) };
  return { signal:'WAIT', confidence:Math.round(conf) };
}

// ── TRADING RÉEL ──
async function tradeUser(user) {
  try {
    const ExchangeClass = ccxt[user.exchangeName.toLowerCase()];
    if (!ExchangeClass) return;

    const exchange = new ExchangeClass({
      apiKey: user.apiKey,
      secret: user.apiSecret,
      enableRateLimit: true
    });

    console.log('Trading pour:', user.email, 'sur', user.exchangeName);
    const balance = await exchange.fetchBalance();
    const usdt = balance.USDT?.free || 0;
    const btcPrice = (await exchange.fetchTicker('BTC/USDT')).last;

    console.log('Balance USDT:', usdt, '| BTC Price: $'+btcPrice);

    for (const symbol of SYMBOLS) {
      try {
        const ohlcv = await exchange.fetchOHLCV(symbol, '1h', undefined, 100);
        const prices = ohlcv.map(c => c[4]);
        const sig = getSignal(prices);
        const price = prices[prices.length-1];

        console.log(symbol, '-', sig.signal, sig.confidence+'%');

        if (sig.signal === 'BUY' && sig.confidence > 65 && usdt >= 10) {
          // Calculer commission 0.1%
          const tradeAmount = usdt * 0.3;
          const commissionUSD = tradeAmount * COMMISSION_RATE;
          const commissionBTC = commissionUSD / btcPrice;

          console.log('Commission:', commissionUSD.toFixed(4), 'USD =', commissionBTC.toFixed(8), 'BTC');
          console.log('Vers wallet:', BENDER_WALLET);

          // Sauvegarder dans MongoDB
          await new Trade({
            email: user.email,
            type: 'BUY',
            symbol,
            price: price.toFixed(2),
            commissionUSD,
            commissionBTC,
            walletDestination: BENDER_WALLET
          }).save();

          console.log('Trade BUY enregistre pour', user.email);
        }

        if (sig.signal === 'SELL' && sig.confidence > 65) {
          const base = symbol.split('/')[0];
          const baseBalance = balance[base]?.free || 0;

          if (baseBalance > 0.0001) {
            const tradeValue = baseBalance * price;
            const commissionUSD = tradeValue * COMMISSION_RATE;
            const commissionBTC = commissionUSD / btcPrice;

            await new Trade({
              email: user.email,
              type: 'SELL',
              symbol,
              price: price.toFixed(2),
              commissionUSD,
              commissionBTC,
              walletDestination: BENDER_WALLET
            }).save();

            console.log('Trade SELL enregistre pour', user.email);
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

// ── MAIN ──
async function main() {
  console.log('=== Bender Pro Bot - Cycle de trading ===');
  console.log('Heure:', new Date().toISOString());
  console.log('Wallet BTC:', BENDER_WALLET);

  if (!MONGODB_URI) {
    console.log('MONGODB_URI manquant !');
    return;
  }

  await mongoose.connect(MONGODB_URI);
  console.log('MongoDB connecte !');

  const users = await User.find({ active: true, apiKey: { $exists: true } });
  console.log('Utilisateurs actifs:', users.length);

  for (const user of users) {
    await tradeUser(user);
  }

  console.log('=== Cycle termine ===');
  process.exit(0);
}

main().catch(err => {
  console.error('Erreur fatale:', err.message);
  process.exit(1);
});
