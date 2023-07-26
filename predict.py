import ccxt
import random


def predict_function(topic, estimated_time):
    """Given a topic, let's predict
    Topic object looks like:

    {
        "name":"ETH-USDT",
        "address":"0x54b5ebeed85f4178c6cb98dd185067991d058d55",
        "symbol":"ETH-USDT",
        "blocks_per_epoch":"60",
        "blocks_per_subscription":"86400",
        "last_submited_epoch":0,
        "pair":"eth-usdt",
        "base":"eth",
        "quote":"usdt",
        "source":"kraken",
        "timeframe":"5m"
    }

    """
    print(
        f" We were asked to predict {topic['name']} (contract: {topic['address']}) value at estimated timestamp: {estimated_time}"
    )
    predicted_confidence = None
    predicted_value = None

    try:
        predicted_value = bool(random.getrandbits(1))
        predicted_confidence = random.randint(1, 100)

        """ Or do something fancy, like:
        
        exchange_class = getattr(ccxt, topic["source"])
        exchange_ccxt = exchange_class()
        candles = exchange_ccxt.fetch_ohlcv(topic['pair'], topic['timeframe'])
        #if price is not moving, let's not predict anything
        if candles[0][1] != candles[1][1]:
            #just follow the trend.  True (up) is last close price > previous close price, False (down) otherwise
            predicted_confidence = random.randint(1,100)
            predicted_value = True if candles[0][1]>candles[1][1] else False
            print(f"Predicting {predicted_value} with a confidence of {predicted_confidence}")
        
        """
    except Exception as e:
        print(e)
        pass
    return (predicted_value, predicted_confidence)
