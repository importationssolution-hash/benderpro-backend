
    if (Number(user.leverage) > MAX_LEVERAGE) {
      console.log(`Attention: levier configure (${user.leverage}x) depasse la limite de securite â€” plafonne a ${MAX_LEVERAGE}x`);
    }
    if (Number(user.tradeAmount) > MAX_TRADE_AMOUNT_USD) {
      console.log(`Attention: montant configure (${user.tradeAmount}$) depasse la limite de securite â€” plafonne a ${MAX_TRADE_AMOUNT_USD}$`);
    }

    for (const symbol of FUTURES_SYMBOLS) {
      try {
        const prices = await fetchFuturesCandles(symbol);
        if (prices.length < 20) continue;
        const sig = getSignal(prices);
        const price = prices[prices.length - 1];

        console.log(symbol, '-', sig.signal, sig.confidence + '%', 'Â· levier:', safeLeverage + 'x');

        if (sig.signal === 'WAIT' || sig.confidence <= 65) continue;
        if (availableMargin < safeTradeAmount) {
          console.log('Marge insuffisante pour', symbol);
          continue;
        }

        const side = sig.signal === 'BUY' ? 'buy' : 'sell';
        // Taille de position = (montant * levier) / prix
        const positionSize = (safeTradeAmount * safeLeverage) / price;

        console.log(`ORDRE REEL FUTURES ${side.toUpperCase()}: ${symbol} Â· taille ${positionSize.toFixed(4)} Â· levier ${safeLeverage}x`);

        const orderParams = {
          orderType: 'mkt',
          symbol: symbol,
          side: side,
          size: positionSize.toFixed(4)
        };

        const orderRes = await futuresRequest('/derivatives/api/v3/sendorder', user.apiKey, user.apiSecret, 'POST', orderParams);

        if (orderRes.result === 'success' && orderRes.sendStatus?.status === 'placed') {
          const orderId = orderRes.sendStatus.order_id;
          console.log('Ordre futures execute:', orderId);

          const commissionUSD = safeTradeAmount * COMMISSION_RATE;
          await new FuturesTrade({
            email: user.email,
            symbol,
            side,
            size: positionSize,
            leverage: safeLeverage,
            price: price.toFixed(2),
            commissionUSD,
            walletDestination: BENDER_WALLET,
            orderId
          }).save();

          console.log('Trade futures enregistre pour', user.email);
        } else {
          console.log('Echec ordre futures:', JSON.stringify(orderRes.sendStatus || orderRes.error));
        }

      } catch (e) {
        console.log('Erreur', symbol, ':', e.message);
      }
    }

  } catch (e) {
    console.log('Erreur utilisateur futures', user.email, ':', e.message);
  }
}

// â”€â”€ CYCLE PRINCIPAL â”€â”€
async function runFuturesCycle() {
  console.log('=== Bender Pro Futures - Cycle de trading ===');
  console.log('Heure:', new Date().toISOString());
  console.log('Limites de securite â€” montant max:', MAX_TRADE_AMOUNT_USD, 'USD Â· levier max:', MAX_LEVERAGE + 'x');

  const users = await FuturesUser.find({ active: true, apiKey: { $exists: true } });
  console.log('Utilisateurs Futures actifs:', users.length);

  for (const user of users) {
    await tradeUserFutures(user);
  }

  console.log('=== Cycle Futures termine ===');
}

// â”€â”€ MAIN â”€â”€
async function main() {
  if (!MONGODB_URI) {
    console.log('MONGODB_URI manquant !');
    process.exit(1);
  }

  await mongoose.connect(MONGODB_URI);
  console.log('MongoDB connecte (module Futures) !');

  const runOnce = process.argv.includes('--once');

  if (runOnce) {
    await runFuturesCycle();
    process.exit(0);
  } else {
    await runFuturesCycle();
    setInterval(() => {
      runFuturesCycle().catch(err => console.error('Erreur cycle futures:', err.message));
    }, 60 * 1000);
  }
}

if (require.main === module) {
  main().catch(err => {
    console.error('Erreur fatale futures:', err.message);
    process.exit(1);
  });
}

module.exports = { FuturesUser, FuturesTrade, runFuturesCycle, MAX_TRADE_AMOUNT_USD, MAX_LEVERAGE };
