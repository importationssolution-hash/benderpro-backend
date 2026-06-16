/**
 * Bender Pro v7.0 â€” Kraken Full-Market Scanner
 * Scan toutes les paires USDT actives sur Kraken chaque minute
 * DÃ©tection figures chartistes + volume Â· Ratio 1:4 Â· $2/trade
 *
 * INSTALLATION:
 *   1. Copie ce fichier dans ton projet backend Render
 *   2. npm install node-fetch (si pas dÃ©jÃ  installÃ©)
 *   3. Dans ton server.js principal: require('./kraken-scanner')
 *   4. Les routes /api/kraken/* seront automatiquement disponibles
 */

const fetch = (...args) => import('node-fetch').then(({default: f}) => f(...args));

// â”€â”€â”€ Config â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
const CONFIG = {
  TRADE_AMOUNT_USD : 2,
  RATIO            : 4,          // TP = SL * 4
  SL_PCT           : 1.0,        // Stop loss 1%
  TP_PCT           : 4.0,        // Take profit 4%
  SCAN_INTERVAL_MS : 60_000,     // Scan toutes les 60 secondes
  OHLC_CANDLES     : 60,         // Nombre de bougies analysÃ©es
  OHLC_INTERVAL    : 1,          // Timeframe: 1 minute
  KRAKEN_BASE      : 'https://api.kraken.com/0/public',
  MAX_PAIRS        : 80,         // Limite sÃ©curitÃ© (Kraken ~70 paires USDT)
  MIN_VOLUME_USD   : 50_000,     // Volume min 24h pour filtrer les paires illiquides
};

// â”€â”€â”€ Ã‰tat global du scanner â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
const state = {
  running        : false,
  lastScan       : null,
  scanCount      : 0,
  signals        : [],            // Signaux actifs du dernier scan
  history        : [],            // Historique des 50 derniers signaux
  pairs          : [],            // Paires actives trouvÃ©es sur Kraken
  priceCache     : {},            // Cache prix { XBTUSDT: 67234.5, ... }
  errors         : [],            // DerniÃ¨res erreurs (max 20)
  scanDuration   : 0,            // DurÃ©e du dernier scan en ms
};

// â”€â”€â”€ Helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function log(msg, level = 'info') {
  const ts = new Date().toISOString();
  console.log(`[BenderPro][${level.toUpperCase()}][${ts}] ${msg}`);
}

function addError(msg) {
  state.errors.unshift({ ts: new Date().toISOString(), msg });
  if (state.errors.length > 20) state.errors.pop();
}

async function krakenGet(path, params = {}) {
  const url = new URL(CONFIG.KRAKEN_BASE + path);
  Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  const res = await fetch(url.toString(), {
    headers: { 'User-Agent': 'BenderPro/7.0' },
    timeout: 10_000,
  });
  if (!res.ok) throw new Error(`Kraken HTTP ${res.status} on ${path}`);
  const data = await res.json();
  if (data.error && data.error.length > 0) throw new Error(`Kraken API: ${data.error[0]}`);
  return data.result;
}

// â”€â”€â”€ 1. RÃ©cupÃ©ration des paires actives â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function fetchActivePairs() {
  log('RÃ©cupÃ©ration des paires USDT actives sur Krakenâ€¦');
  const result = await krakenGet('/AssetPairs');
  const pairs = [];

  for (const [name, info] of Object.entries(result)) {
    // Filtre: paires USDT spot uniquement (pas les futures .d)
    if (
      info.quote === 'USDT' &&
      !name.endsWith('.d') &&
      info.status === 'online'
    ) {
      pairs.push({
        wsname  : info.wsname,       // ex: "XBT/USDT"
        apiname : name,              // ex: "XBTUSDT"
        base    : info.base,
        decimals: info.pair_decimals,
      });
    }
  }

  log(`${pairs.length} paires USDT trouvÃ©es sur Kraken`);
  return pairs.slice(0, CONFIG.MAX_PAIRS);
}

// â”€â”€â”€ 2. Filtre volume minimum â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function filterByVolume(pairs) {
  const names = pairs.map(p => p.apiname).join(',');
  const result = await krakenGet('/Ticker', { pair: names });
  const liquid = [];

  for (const pair of pairs) {
    const key = Object.keys(result).find(k =>
      k === pair.apiname || k.replace('XBT', 'BTC') === pair.apiname
    );
    if (!key) continue;
    const ticker = result[key];
    const price  = parseFloat(ticker.c[0]);
    const vol24  = parseFloat(ticker.v[1]);
    const volUSD = price * vol24;

    if (volUSD >= CONFIG.MIN_VOLUME_USD) {
      liquid.push({ ...pair, price, vol24USD: volUSD });
      state.priceCache[pair.apiname] = price;
    }
  }

  log(`${liquid.length} paires avec volume > $${CONFIG.MIN_VOLUME_USD.toLocaleString()}`);
  return liquid;
}

// â”€â”€â”€ 3. RÃ©cupÃ©ration OHLC â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function fetchOHLC(apiname) {
  const result = await krakenGet('/OHLC', {
    pair     : apiname,
    interval : CONFIG.OHLC_INTERVAL,
    count    : CONFIG.OHLC_CANDLES,
  });
  const key = Object.keys(result).find(k => k !== 'last');
  if (!key) return [];
  return result[key].map(c => ({
    t : c[0],
    o : parseFloat(c[1]),
    h : parseFloat(c[2]),
    l : parseFloat(c[3]),
    c : parseFloat(c[4]),
    v : parseFloat(c[6]),
  }));
}

// â”€â”€â”€ 4. DÃ©tection des figures chartistes â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function detectFigures(ohlc, price, pairInfo) {
  if (ohlc.length < 25) return [];
  const signals = [];
  const n = ohlc.length;

  // Calcul SL/TP dynamique basÃ© sur le prix
  function buildSignal(dir, figure, slPct = CONFIG.SL_PCT) {
    const tpPct  = slPct * CONFIG.RATIO;
    const entry  = price;
    const sl     = dir === 'LONG'
      ? +(entry * (1 - slPct / 100)).toFixed(pairInfo.decimals)
      : +(entry * (1 + slPct / 100)).toFixed(pairInfo.decimals);
    const tp     = dir === 'LONG'
      ? +(entry * (1 + tpPct / 100)).toFixed(pairInfo.decimals)
      : +(entry * (1 - tpPct / 100)).toFixed(pairInfo.decimals);
    const riskUSD = CONFIG.TRADE_AMOUNT_USD;
    const gainUSD = +(CONFIG.TRADE_AMOUNT_USD * CONFIG.RATIO).toFixed(2);

    return {
      id         : `${pairInfo.apiname}_${figure.replace(/ /g,'_')}_${Date.now()}`,
      pair       : pairInfo.apiname,
      wsname     : pairInfo.wsname,
      figure,
      dir,
      entry,
      sl,
      tp,
      slPct,
      tpPct,
      riskUSD,
      gainUSD,
      vol24USD   : pairInfo.vol24USD,
      detectedAt : new Date().toISOString(),
      tf         : `${CONFIG.OHLC_INTERVAL}m`,
    };
  }

  // â”€â”€ Double Bottom (LONG) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  const lows20 = ohlc.slice(-20).map(x => x.l);
  const bot1   = Math.min(...lows20.slice(0, 10));
  const bot2   = Math.min(...lows20.slice(10));
  const botAvg = (bot1 + bot2) / 2;
  if (Math.abs(bot1 - bot2) / botAvg < 0.009 && price > botAvg * 1.004) {
    signals.push(buildSignal('LONG', 'Double Bottom', 0.6));
  }

  // â”€â”€ Double Top (SHORT) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  const highs20 = ohlc.slice(-20).map(x => x.h);
  const top1    = Math.max(...highs20.slice(0, 10));
  const top2    = Math.max(...highs20.slice(10));
  const topAvg  = (top1 + top2) / 2;
  if (Math.abs(top1 - top2) / topAvg < 0.009 && price < topAvg * 0.996) {
    signals.push(buildSignal('SHORT', 'Double Top', 0.6));
  }

  // â”€â”€ Triangle Ascendant (LONG) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  const lastH15 = ohlc.slice(-15).map(x => x.h);
  const lastC15 = ohlc.slice(-15).map(x => x.c);
  const res     = Math.max(...lastH15);
  const nearRes = lastH15.filter(h => Math.abs(h - res) / res < 0.005).length;
  const upClose = lastC15[lastC15.length - 1] > lastC15[0];
  if (nearRes >= 3 && upClose && price < res * 1.003) {
    signals.push(buildSignal('LONG', 'Triangle Ascendant', 0.5));
  }

  // â”€â”€ Triangle Descendant (SHORT) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  const lastL15 = ohlc.slice(-15).map(x => x.l);
  const sup     = Math.min(...lastL15);
  const nearSup = lastL15.filter(l => Math.abs(l - sup) / sup < 0.005).length;
  const dnClose = lastC15[lastC15.length - 1] < lastC15[0];
  if (nearSup >= 3 && dnClose && price > sup * 0.997) {
    signals.push(buildSignal('SHORT', 'Triangle Descendant', 0.5));
  }

  // â”€â”€ Flag Haussier (LONG) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  const prevBodies = ohlc.slice(-15, -10).map(x => x.c - x.o);
  const currBodies = ohlc.slice(-5).map(x => x.c - x.o);
  const prevAvg    = prevBodies.reduce((a, b) => a + b, 0) / prevBodies.length;
  const currAvg    = currBodies.reduce((a, b) => a + b, 0) / currBodies.length;
  if (prevAvg > 0 && currAvg < 0 && currAvg > -prevAvg * 0.6) {
    signals.push(buildSignal('LONG', 'Flag Haussier', 0.45));
  }

  // â”€â”€ Flag Baissier (SHORT) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  if (prevAvg < 0 && currAvg > 0 && currAvg < -prevAvg * 0.6) {
    signals.push(buildSignal('SHORT', 'Flag Baissier', 0.45));
  }

  // â”€â”€ Marubozu Haussier (LONG) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  const bodies10  = ohlc.slice(-10).map(x => Math.abs(x.c - x.o));
  const avgBody   = bodies10.reduce((a, b) => a + b, 0) / bodies10.length;
  const lastCandle = ohlc[n - 1];
  const lastBody  = Math.abs(lastCandle.c - lastCandle.o);
  const lastWick  = lastCandle.h - lastCandle.l;
  if (lastBody > avgBody * 2.5 && lastCandle.c > lastCandle.o &&
      lastBody / lastWick > 0.85) {
    signals.push(buildSignal('LONG', 'Marubozu Haussier', 0.35));
  }

  // â”€â”€ Marubozu Baissier (SHORT) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  if (lastBody > avgBody * 2.5 && lastCandle.c < lastCandle.o &&
      lastBody / lastWick > 0.85) {
    signals.push(buildSignal('SHORT', 'Marubozu Baissier', 0.35));
  }

  // â”€â”€ Breakout Volume (LONG/SHORT) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  const vols10 = ohlc.slice(-10).map(x => x.v);
  const avgVol = vols10.reduce((a, b) => a + b, 0) / vols10.length;
  if (lastCandle.v > avgVol * 2.5) {
    const dir = lastCandle.c > lastCandle.o ? 'LONG' : 'SHORT';
    signals.push(buildSignal(dir, 'Breakout Volume', 0.4));
  }

  // â”€â”€ Harami Haussier (LONG) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  const prev = ohlc[n - 2];
  const curr = ohlc[n - 1];
  const prevBodySize = Math.abs(prev.c - prev.o);
  const currBodySize = Math.abs(curr.c - curr.o);
  if (prev.c < prev.o && curr.c > curr.o &&
      currBodySize < prevBodySize * 0.5 &&
      curr.o > prev.c && curr.c < prev.o) {
    signals.push(buildSignal('LONG', 'Harami Haussier', 0.4));
  }

  // â”€â”€ Harami Baissier (SHORT) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  if (prev.c > prev.o && curr.c < curr.o &&
      currBodySize < prevBodySize * 0.5 &&
      curr.o < prev.c && curr.c > prev.o) {
    signals.push(buildSignal('SHORT', 'Harami Baissier', 0.4));
  }

  return signals;
}

// â”€â”€â”€ 5. Scan d'une paire (avec retry) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function scanPair(pairInfo, retries = 2) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const ohlc   = await fetchOHLC(pairInfo.apiname);
      const price  = ohlc.length > 0 ? ohlc[ohlc.length - 1].c : pairInfo.price;
      state.priceCache[pairInfo.apiname] = price;
      const signals = detectFigures(ohlc, price, pairInfo);
      return signals;
    } catch (err) {
      if (attempt === retries) {
        addError(`${pairInfo.apiname}: ${err.message}`);
        return [];
      }
      await new Promise(r => setTimeout(r, 500 * (attempt + 1)));
    }
  }
  return [];
}

// â”€â”€â”€ 6. Scan complet du marchÃ© Kraken â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function fullMarketScan() {
  if (state.running) {
    log('Scan dÃ©jÃ  en cours, skip', 'warn');
    return;
  }
  state.running = true;
  const startTime = Date.now();
  log(`=== Scan #${++state.scanCount} dÃ©marrÃ© â€” ${state.pairs.length} paires ===`);

  try {
    // Scan toutes les paires en parallÃ¨le (batches de 10 pour Ã©viter rate limit)
    const BATCH = 10;
    const allSignals = [];

    for (let i = 0; i < state.pairs.length; i += BATCH) {
      const batch   = state.pairs.slice(i, i + BATCH);
      const results = await Promise.allSettled(batch.map(p => scanPair(p)));

      results.forEach((res, idx) => {
        if (res.status === 'fulfilled') {
          allSignals.push(...res.value);
        } else {
          addError(`${batch[idx].apiname}: ${res.reason?.message}`);
        }
      });

      // Petite pause entre batches (respect rate limit Kraken: 1 req/s par IP)
      if (i + BATCH < state.pairs.length) {
        await new Promise(r => setTimeout(r, 1200));
      }
    }

    // Tri par volume dÃ©croissant (les paires les plus liquides en premier)
    allSignals.sort((a, b) => (b.vol24USD || 0) - (a.vol24USD || 0));

    state.signals     = allSignals;
    state.lastScan    = new Date().toISOString();
    state.scanDuration = Date.now() - startTime;

    // Ajout Ã  l'historique (max 200 signaux)
    state.history.unshift(...allSignals);
    if (state.history.length > 200) state.history.splice(200);

    log(`=== Scan #${state.scanCount} terminÃ© en ${state.scanDuration}ms â€” ${allSignals.length} signal(s) dÃ©tectÃ©(s) ===`);

  } catch (err) {
    addError(`Scan global: ${err.message}`);
    log(`Erreur scan: ${err.message}`, 'error');
  } finally {
    state.running = false;
  }
}

// â”€â”€â”€ 7. Initialisation â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function init() {
  log('Initialisation du scanner Krakenâ€¦');
  try {
    const allPairs = await fetchActivePairs();
    state.pairs    = await filterByVolume(allPairs);
    log(`Scanner prÃªt â€” ${state.pairs.length} paires actives`);

    // Premier scan immÃ©diat
    await fullMarketScan();

    // Scan automatique toutes les 60 secondes
    setInterval(fullMarketScan, CONFIG.SCAN_INTERVAL_MS);
    log(`Scan automatique toutes les ${CONFIG.SCAN_INTERVAL_MS / 1000}s activÃ©`);

  } catch (err) {
    log(`Erreur init: ${err.message}`, 'error');
    addError(`Init: ${err.message}`);
    // Retry dans 30 secondes si l'init Ã©choue
    setTimeout(init, 30_000);
  }
}

// â”€â”€â”€ 8. Routes Express â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
/**
 * Ã€ ajouter dans ton server.js:
 *
 *   const krakenScanner = require('./kraken-scanner');
 *   app.use('/api/kraken', krakenScanner.router);
 */
let _express;
try { _express = require('express'); } catch(e) { _express = null; }

const router = _express ? _express.Router() : null;

if (router) {
  // CORS â€” autorise ton frontend Netlify
  router.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
  });

  // GET /api/kraken/status â€” Ã©tat du scanner
  router.get('/status', (req, res) => {
    res.json({
      ok           : true,
      scanCount    : state.scanCount,
      lastScan     : state.lastScan,
      scanDuration : state.scanDuration,
      pairsActive  : state.pairs.length,
      signalsActive: state.signals.length,
      running      : state.running,
      errors       : state.errors.slice(0, 5),
    });
  });

  // GET /api/kraken/signals â€” signaux du dernier scan
  router.get('/signals', (req, res) => {
    const { dir, figure, limit = 50 } = req.query;
    let signals = [...state.signals];
    if (dir)    signals = signals.filter(s => s.dir === dir.toUpperCase());
    if (figure) signals = signals.filter(s => s.figure.toLowerCase().includes(figure.toLowerCase()));
    res.json({
      ok          : true,
      lastScan    : state.lastScan,
      total       : signals.length,
      signals     : signals.slice(0, parseInt(limit)),
    });
  });

  // GET /api/kraken/signals/live â€” SSE (Server-Sent Events) pour le frontend
  router.get('/signals/live', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('Access-Control-Allow-Origin', '*');

    const send = () => {
      const data = JSON.stringify({
        lastScan    : state.lastScan,
        signals     : state.signals,
        pairsActive : state.pairs.length,
        scanCount   : state.scanCount,
      });
      res.write(`data: ${data}\n\n`);
    };

    send();
    const interval = setInterval(send, 10_000);
    req.on('close', () => clearInterval(interval));
  });

  // GET /api/kraken/pairs â€” liste des paires scannÃ©es
  router.get('/pairs', (req, res) => {
    res.json({
      ok    : true,
      total : state.pairs.length,
      pairs : state.pairs.map(p => ({
        apiname  : p.apiname,
        wsname   : p.wsname,
        price    : state.priceCache[p.apiname],
        vol24USD : Math.round(p.vol24USD),
      })),
    });
  });

  // GET /api/kraken/history â€” historique des signaux
  router.get('/history', (req, res) => {
    const { limit = 100 } = req.query;
    res.json({
      ok      : true,
      total   : state.history.length,
      history : state.history.slice(0, parseInt(limit)),
    });
  });

  // POST /api/kraken/scan â€” force un scan immÃ©diat
  router.post('/scan', async (req, res) => {
    if (state.running) {
      return res.json({ ok: false, msg: 'Scan dÃ©jÃ  en cours' });
    }
    res.json({ ok: true, msg: 'Scan lancÃ©' });
    fullMarketScan();
  });
}

// â”€â”€â”€ Export â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
module.exports = { router, init, state, fullMarketScan };

// Auto-dÃ©marrage si lancÃ© directement
if (require.main === module) {
  init();
}
