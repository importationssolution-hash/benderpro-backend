// Bender Pro â€” Backtest REEL sur donnees historiques Kraken
// Recupere les vraies donnees (max 12h en 1m, limite de l'API Kraken),
// applique la VRAIE logique de detection de figures chartistes,
// puis VERIFIE pour chaque signal si le prix a vraiment touche le TP ou le SL
// en regardant les bougies suivantes (pas de tirage au hasard).
//
// USAGE: node backtest.js
// (A executer une seule fois, pas en continu â€” script independant de server.js)

const ccxt = require('ccxt');

// â”€â”€ CONFIG (identique a server.js) â”€â”€
const SL_PCT = 0.01;
const TP_PCT = 0.04;
const TRADE_AMOUNT = 2;
const COMM_RATE = 0.001;
const VOL_CONFIRM = 1.8;
const QUOTE_CURRENCIES = ['USDC', 'USD']; // USDT exclu: bloque sur Kraken Canada
const MAX_PAIRS = 500;

const FIGURES = [
  { name:'Cup & Handle',     code:'C&H',   dir:'Long',  wr:0.84 },
  { name:'ETE',              code:'ETE',   dir:'Short', wr:0.83 },
  { name:'ETE Inverse',      code:'ETEi',  dir:'Long',  wr:0.81 },
  { name:'Double Top',       code:'2Top',  dir:'Short', wr:0.78 },
  { name:'Double Bottom',    code:'2Bot',  dir:'Long',  wr:0.76 },
  { name:'Triangle Asc.',    code:'TriA',  dir:'Long',  wr:0.74 },
  { name:'Triangle Desc.',   code:'TriD',  dir:'Short', wr:0.73 },
  { name:'Drapeau Haussier', code:'DrapH', dir:'Long',  wr:0.76 },
  { name:'Drapeau Baissier', code:'DrapB', dir:'Short', wr:0.75 },
  { name:'Biseau Haussier',  code:'BisH',  dir:'Short', wr:0.72 },
  { name:'Biseau Baissier',  code:'BisB',  dir:'Long',  wr:0.73 },
];

function avg(arr) { return arr.reduce((a,b)=>a+b,0)/arr.length; }

// â”€â”€ Detection de figures (copie exacte de la logique server.js) â”€â”€
function detectFigure(closes, volumes, idx) {
  const n = idx + 1;
  if (n < 20) return null;
  const price = closes[idx];
  const volNow = volumes[idx];
  const volAvg = avg(volumes.slice(Math.max(0, idx - 19), idx + 1));
  const volRatio = volNow / volAvg;
  if (volRatio < VOL_CONFIRM) return null;

  const slice = closes.slice(Math.max(0, idx - 19), idx + 1);
  const h = Math.max(...slice), l = Math.min(...slice);
  const figH = h - l;
  const range = figH / price;
  const trend10 = (price - closes[idx-10]) / closes[idx-10];

  if (n >= 15) {
    const midLow = Math.min(...closes.slice(idx-11, idx-3));
    if (midLow < closes[idx-13]*0.95 && price > closes[idx-1] && volRatio > 1.8)
      return { fig:FIGURES[0], tp:price+figH, sl:price*(1-SL_PCT) };
  }
  if (n >= 15) {
    const head = Math.max(...closes.slice(idx-11, idx-3));
    const sh = Math.max(...closes.slice(idx-13, idx-9));
    if (head>sh*1.02 && head>closes[idx-1]*1.02 && price<sh && volRatio>1.5)
      return { fig:FIGURES[1], tp:price-figH*0.85, sl:price*(1+SL_PCT) };
  }
  if (n >= 15) {
    const headL = Math.min(...closes.slice(idx-11, idx-3));
    const shL = Math.min(...closes.slice(idx-13, idx-9));
    if (headL<shL*0.98 && headL<closes[idx-1]*0.98 && price>shL && volRatio>1.5)
      return { fig:FIGURES[2], tp:price+figH*0.85, sl:price*(1-SL_PCT) };
  }
  if (n >= 10) {
    const mx1=Math.max(...closes.slice(idx-9,idx-4)), mx2=Math.max(...closes.slice(idx-4,idx+1));
    if (Math.abs(mx1-mx2)/mx1<0.015 && price<Math.min(...closes.slice(idx-4,idx+1))*0.99 && volRatio>1.4)
      return { fig:FIGURES[3], tp:price-figH*0.9, sl:price*(1+SL_PCT) };
  }
  if (n >= 10) {
    const mn1=Math.min(...closes.slice(idx-9,idx-4)), mn2=Math.min(...closes.slice(idx-4,idx+1));
    if (Math.abs(mn1-mn2)/mn1<0.015 && price>Math.max(...closes.slice(idx-4,idx+1))*1.01 && volRatio>1.4)
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

// Pour chaque signal detecte, regarde les bougies SUIVANTES pour voir
// si le prix a reellement touche le TP ou le SL en premier (vrai resultat, pas de hasard)
function checkRealOutcome(candles, signalIdx, dir, tp, sl) {
  for (let i = signalIdx + 1; i < candles.length; i++) {
    const c = candles[i];
    if (dir === 'Long') {
      if (c.h >= tp) return { result: 'WIN', closedAt: i };
      if (c.l <= sl) return { result: 'LOSS', closedAt: i };
    } else {
      if (c.l <= tp) return { result: 'WIN', closedAt: i };
      if (c.h >= sl) return { result: 'LOSS', closedAt: i };
    }
  }
  return { result: 'OPEN', closedAt: null }; // ni TP ni SL touche dans la fenetre disponible
}

async function fetchKrakenPairs() {
  const exchange = new ccxt.kraken({ enableRateLimit: true, timeout: 15000 });
  const markets = await exchange.loadMarkets();
  const pairs = Object.keys(markets).filter(s => {
    const m = markets[s];
    const matchesQuote = QUOTE_CURRENCIES.some(q => s.endsWith('/' + q));
    const isActive = m.active !== false;
    const isSpot = m.spot === true || m.type === 'spot';
    return matchesQuote && isActive && isSpot;
  });
  return { exchange, pairs: pairs.slice(0, MAX_PAIRS) };
}

async function backtestPair(exchange, symbol) {
  const results = [];
  try {
    // 720 bougies de 1m = 12h, maximum disponible via l'API Kraken (limite documentee)
    const ohlcv = await exchange.fetchOHLCV(symbol, '1m', undefined, 720);
    if (!ohlcv || ohlcv.length < 25) return results;

    const candles = ohlcv.map(c => ({ o:c[1], h:c[2], l:c[3], c:c[4], v:c[5] }));
    const closes = candles.map(c => c.c);
    const volumes = candles.map(c => c.v);

    // Parcourt chaque minute comme si on scannait en direct a ce moment-la
    for (let i = 20; i < candles.length - 1; i++) {
      const sig = detectFigure(closes.slice(0, i+1), volumes.slice(0, i+1), i);
      if (!sig) continue;

      const outcome = checkRealOutcome(candles, i, sig.fig.dir, sig.tp, sig.sl);
      const gainUSD = TRADE_AMOUNT * TP_PCT - TRADE_AMOUNT * COMM_RATE;
      const lossUSD = -(TRADE_AMOUNT * SL_PCT + TRADE_AMOUNT * COMM_RATE);

      results.push({
        symbol,
        figure: sig.fig.name,
        dir: sig.fig.dir,
        entryIdx: i,
        entryPrice: closes[i],
        result: outcome.result,
        pnl: outcome.result === 'WIN' ? gainUSD : outcome.result === 'LOSS' ? lossUSD : 0
      });
    }
  } catch (e) {
    console.log(`Erreur ${symbol}: ${e.message}`);
  }
  return results;
}

async function main() {
  console.log('=== BACKTEST REEL â€” Bender Pro ===');
  console.log('Recuperation des paires Kraken (USDC/USD)...');
  const { exchange, pairs } = await fetchKrakenPairs();
  console.log(`${pairs.length} paires trouvees. Fenetre disponible: 12h max (limite API Kraken 720 bougies 1m).\n`);

  let allResults = [];
  const BATCH = 10;
  for (let i = 0; i < pairs.length; i += BATCH) {
    const batch = pairs.slice(i, i + BATCH);
    console.log(`Backtest paires ${i+1}-${Math.min(i+BATCH, pairs.length)} / ${pairs.length}...`);
    const batchResults = await Promise.all(batch.map(p => backtestPair(exchange, p)));
    batchResults.forEach(r => allResults.push(...r));
    await new Promise(r => setTimeout(r, 1000)); // respect rate limit Kraken
  }

  const closed = allResults.filter(r => r.result !== 'OPEN');
  const wins = closed.filter(r => r.result === 'WIN');
  const losses = closed.filter(r => r.result === 'LOSS');
  const stillOpen = allResults.filter(r => r.result === 'OPEN');
  const totalPnl = closed.reduce((a, r) => a + r.pnl, 0);

  console.log('\n=== RESULTATS REELS (12 dernieres heures, donnees Kraken) ===');
  console.log(`Signaux detectes au total: ${allResults.length}`);
  console.log(`Signaux conclus (TP ou SL touche): ${closed.length}`);
  console.log(`  Gagnants (TP touche): ${wins.length}`);
  console.log(`  Perdants (SL touche): ${losses.length}`);
  console.log(`  Encore ouverts (ni TP ni SL touche dans la fenetre): ${stillOpen.length}`);
  console.log(`Taux de reussite reel: ${closed.length > 0 ? Math.round(wins.length/closed.length*100) : 0}%`);
  console.log(`Gain/Perte total reel: ${totalPnl >= 0 ? '+' : ''}$${totalPnl.toFixed(2)}`);

  console.log('\n=== DETAIL PAR FIGURE ===');
  const byFigure = {};
  closed.forEach(r => {
    if (!byFigure[r.figure]) byFigure[r.figure] = { wins: 0, losses: 0, pnl: 0 };
    if (r.result === 'WIN') byFigure[r.figure].wins++;
    else byFigure[r.figure].losses++;
    byFigure[r.figure].pnl += r.pnl;
  });
  Object.entries(byFigure).forEach(([fig, stats]) => {
    const total = stats.wins + stats.losses;
    const wr = total > 0 ? Math.round(stats.wins/total*100) : 0;
    console.log(`${fig}: ${stats.wins}W/${stats.losses}L (${wr}%) Â· ${stats.pnl >= 0 ? '+' : ''}$${stats.pnl.toFixed(2)}`);
  });

  console.log('\nNote: periode limitee a 12h (et non 24h) car l\'API publique Kraken');
  console.log('ne fournit pas plus de 720 bougies de 1 minute, peu importe la demande.');
}

main().catch(err => {
  console.error('Erreur fatale:', err.message);
  process.exit(1);
});
