const ccxt = require('ccxt');
const mongoose = require('mongoose');

// â”€â”€ CONFIG â”€â”€
const COMMISSION_RATE = 0.001; // 0.1%
const BENDER_WALLET = process.env.BENDER_WALLET;
const MONGODB_URI = process.env.MONGODB_URI;
// Paires analysees: USDC et USD seulement (USDT bloque sur Kraken Canada)
const SYMBOLS = [
  'BTC/USDC', 'ETH/USDC', 'SOL/USDC',
  'BTC/USDC', 'ETH/USDC', 'SOL/USDC',
  'BTC/USD',  'ETH/USD',  'SOL/USD'
];
// LIMITES DE SECURITE CODEES EN DUR
const MAX_TRADE_AMOUNT_USD = 50;
const MIN_TRADE_AMOUNT_USD = 5; // Minimum 5 USDC par trade

// â”€â”€ MONGODB â”€â”€
const UserSchema = new mongoose.Schema({
  email: String,
  exchangeName: String,
  apiKey: String,
  apiSecret: String,
  tradeAmount: Number,
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

// â”€â”€ ALGORITHME â”€â”€
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

// Applique la limite de securite codee en dur, peu importe le tradeAmount configure
function getSafeTradeAmount(requestedAmount) {
  const amount = Number(requestedAmount) || 0;
  if (amount <= 0) return 0;
  if (amount < MIN_TRADE_AMOUNT_USD) {
    console.log(`Montant ${amount}$ inferieur au minimum de ${MIN_TRADE_AMOUNT_USD}$ â€” plafonne a ${MIN_TRADE_AMOUNT_USD}$`);
    return MIN_TRADE_AMOUNT_USD;
  }
  return Math.min(amount, MAX_TRADE_AMOUNT_USD);
}

// â”€â”€ TRADING RÃ‰EL â”€â”€
async function tradeUser(user) {
  try {
    const ExchangeClass = ccxt[user.exchangeName.toLowerCase()];
    if (!ExchangeClass) {
      console.log('Exchange non supporte:', user.exchangeName);
      return;
    }

    const exchange = new ExchangeClass({
      apiKey: user.apiKey,
      secret: user.apiSecret,
      enableRateLimit: true
    });

    console.log('Trading pour:', user.email, 'sur', user.exchangeName);

    let balance;
    try {
      balance = await exchange.fetchBalance();
    } catch (e) {
      console.log('Erreur fetchBalance pour', user.email, ':', e.message);
      return;
    }

    const usdc = balance.USDC?.free || 0;
    const usd  = balance.USD?.free  || 0;
    console.log(`Balance ${user.email} â€” USDC:${usdc} USD:${usd}`);

    if (usdc + usd <= 0) {
      console.log('Aucun fonds disponible (USDC/USD) pour', user.email, 'â€” aucun trade possible');
      return;
    }

    const safeTradeAmount = getSafeTradeAmount(user.tradeAmount);
    if (safeTradeAmount <= 0) {
      console.log('Montant de trade invalide pour', user.email);
      return;
    }
    if (Number(user.tradeAmount) > MAX_TRADE_AMOUNT_USD) {
      console.log(`Attention: tradeAmount configure (${user.tradeAmount}$) depasse la limite de securite â€” plafonne a ${MAX_TRADE_AMOUNT_USD}$`);
    }

    for (const symbol of SYMBOLS) {
      try {
        const [base, quote] = symbol.split('/');
        // On ne tente le symbole que si l'utilisateur a des fonds dans la devise de cotation
        const quoteBalance = balance[quote]?.free || 0;

        const ohlcv = await exchange.fetchOHLCV(symbol, '1h', undefined, 100);
        const prices = ohlcv.map(c => c[4]);
        const sig = getSignal(prices);
        const price = prices[prices.length-1];

        console.log(symbol, '-', sig.signal, sig.confidence+'%');

        if (sig.signal === 'BUY' && sig.confidence > 65 && quoteBalance >= safeTradeAmount) {
          const commissionUSD = safeTradeAmount * COMMISSION_RATE;
          const commissionBTC = commissionUSD / price;

          console.log('ORDRE REEL BUY:', symbol, 'Â·', safeTradeAmount, quote, 'Â· Commission:', commissionUSD.toFixed(4));

          // Execution reelle de l'ordre sur la plateforme connectee
          const order = await exchange.createMarketBuyOrder(symbol, safeTradeAmount / price);
          console.log('Ordre execute:', order.id);

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
          const baseBalance = balance[base]?.free || 0;

          if (baseBalance > 0.0001) {
            const tradeValue = baseBalance * price;
            const commissionUSD = tradeValue * COMMISSION_RATE;
            const commissionBTC = commissionUSD / price;

            console.log('ORDRE REEL SELL:', symbol, 'Â·', baseBalance, base);

            const order = await exchange.createMarketSellOrder(symbol, baseBalance);
            console.log('Ordre execute:', order.id);

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

// â”€â”€ CYCLE PRINCIPAL â”€â”€
async function runCycle() {
  console.log('=== Bender Pro Bot - Cycle de trading ===');
  console.log('Heure:', new Date().toISOString());
  console.log('Wallet BTC:', BENDER_WALLET);
  console.log('Limite de securite par trade:', MAX_TRADE_AMOUNT_USD, 'USD');

  const users = await User.find({ active: true, apiKey: { $exists: true } });
  console.log('Utilisateurs actifs:', users.length);

  for (const user of users) {
    await tradeUser(user);
  }

  console.log('=== Cycle termine ===');
}

// â”€â”€ MAIN â”€â”€
// Mode boucle: tourne en continu (declenche automatiquement chaque minute).
// Utiliser `node bot.js --once` pour un seul cycle puis arret (utile pour tester).
async function main() {
  if (!MONGODB_URI) {
    console.log('MONGODB_URI manquant !');
    process.exit(1);
  }

  await mongoose.connect(MONGODB_URI);
  console.log('MongoDB connecte !');

  const runOnce = process.argv.includes('--once');

  if (runOnce) {
    await runCycle();
    process.exit(0);
  } else {
    await runCycle();
    setInterval(() => {
      runCycle().catch(err => console.error('Erreur cycle:', err.message));
    }, 60 * 1000);
  }
}

main().catch(err => {
  console.error('Erreur fatale:', err.message);
  process.exit(1);
});
