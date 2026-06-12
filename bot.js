ender Pro â€” Moteur de trading automatique
// Tourne toutes les 30 minutes et analyse le marchÃ©

const https = require('https');

// â”€â”€ CONFIGURATION â”€â”€
const SYMBOLS = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT'];
const COMMISSION = 0.001; // 0.1%
const CHECK_INTERVAL = 30 * 60 * 1000; // 30 minutes

// â”€â”€ ALGORITHME RSI â”€â”€
function calcRSI(prices, period = 14) {
  if (prices.length < period + 1) return 50;
  let gains = 0, losses = 0;
  for (let i = prices.length - period; i < prices.length; i++) {
    const diff = prices[i] - prices[i - 1];
    if (diff > 0) gains += diff;
    else losses -= diff;
  }
  const rs = (gains / period) / ((losses / period) || 0.001);
  return 100 - (100 / (1 + rs));
}

// â”€â”€ ALGORITHME EMA â”€â”€
function calcEMA(prices, period) {
  if (prices.length < period) return prices[prices.length - 1];
  const k = 2 / (period + 1);
  let ema = prices.slice(0, period).reduce((a, b) => a + b) / period;
  for (let i = period; i < prices.length; i++) {
    ema = prices[i] * k + ema * (1 - k);
  }
  return ema;
}

// â”€â”€ ALGORITHME BOLLINGER â”€â”€
function calcBollinger(prices, period = 20) {
  if (prices.length < period) return null;
  const slice = prices.slice(-period);
  const mean = slice.reduce((a, b) => a + b) / period;
  const variance = slice.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / period;
  const std = Math.sqrt(variance);
  return { upper: mean + 2 * std, lower: mean - 2 * std, middle: mean };
}

// â”€â”€ SIGNAL COMBINÃ‰ (6 stratÃ©gies) â”€â”€
function getSignal(prices) {
  const price = prices[prices.length - 1];
  const rsi = calcRSI(prices);
  const ema20 = calcEMA(prices, 20);
  const ema50 = calcEMA(prices, 50);
  const ema200 = calcEMA(prices, Math.min(200, prices.length));
  const boll = calcBollinger(prices);
  let score = 0;

  // RSI
  if (rsi < 30) score += 2;
  else if (rsi < 40) score += 1;
  else if (rsi > 70) score -= 2;
  else if (rsi > 60) score -= 1;

  // EMA Cross
  if (ema20 > ema50) score += 1;
  else score -= 1;

  // Trend (EMA200)
  if (price > ema200) score += 1;
  else score -= 1;

  // Bollinger
  if (boll) {
    if (price <= boll.lower) score += 2;
    else if (price >= boll.upper) score -= 2;
  }

  // Momentum
  const recent = prices.slice(-5);
  const momentum = (recent[recent.length-1] - recent[0]) / recent[0] * 100;
  if (momentum > 1) score += 1;
  else if (momentum < -1) score -= 1;

  // Volume simulation
  const volatility = Math.abs(prices[prices.length-1] - prices[prices.length-2]) / prices[prices.length-2];
  if (volatility < 0.02) score += 0.5;

  const confidence = Math.min(95, Math.abs(score) * 12 + 50);

  if (score >= 3) return { signal: 'BUY', confidence: Math.round(confidence), rsi: Math.round(rsi) };
  if (score <= -3) return { signal: 'SELL', confidence: Math.round(confidence), rsi: Math.round(rsi) };
  return { signal: 'WAIT', confidence: Math.round(confidence), rsi: Math.round(rsi) };
}

// â”€â”€ PRIX SIMULÃ‰S (remplacer par vraie API quand backend prÃªt) â”€â”€
function generatePrices(base, count = 100) {
  const prices = [base];
  for (let i = 1; i < count; i++) {
    prices.push(+(prices[i-1] * (1 + (Math.random() - 0.48) * 0.018)).toFixed(2));
  }
  return prices;
}

// â”€â”€ ANALYSE MARCHÃ‰ â”€â”€
function analyzeMarket() {
  const bases = { 'BTC/USDT': 65000, 'ETH/USDT': 3400, 'SOL/USDT': 145, 'BNB/USDT': 580 };
  const results = {};

  SYMBOLS.forEach(symbol => {
    const prices = generatePrices(bases[symbol]);
    const analysis = getSignal(prices);
    const price = prices[prices.length - 1];
    results[symbol] = {
      price: price.toFixed(2),
      signal: analysis.signal,
      confidence: analysis.confidence,
      rsi: analysis.rsi,
      commission: (price * COMMISSION).toFixed(4),
      time: new Date().toISOString()
    };
  });

  return results;
}

// â”€â”€ EXPORT POUR SERVER.JS â”€â”€
module.exports = { analyzeMarket, getSignal, COMMISSION };

// â”€â”€ LOG DÃ‰MARRAGE â”€â”€
console.log('Bot Bender Pro initialisÃ© â€” 6 stratÃ©gies actives');
console.log('Symboles:', SYMBOLS.join(', '));
console.log('Commission:', (COMMISSION * 100) + '%');
