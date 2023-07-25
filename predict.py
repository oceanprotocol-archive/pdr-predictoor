import ccxt
import random

def predict_function(topic,contract_address, estimated_time):
    """ We need to """
    print(f" We were asked to predict {topic} (contract: {contract_address}) value at estimated timestamp: {estimated_time}")
    kraken = ccxt.kraken()
    predicted_confidence = None
    predicted_value = None
    topic = topic.replace("-", "/")
    try:
        candles = kraken.fetch_ohlcv(topic, "5m")
        #if price is not moving, let's not predict anything
        if candles[0][1] != candles[1][1]:
            #just follow the trend.  True (up) is last close price > previous close price, False (down) otherwise
            predicted_confidence = random.randint(1,100)
            predicted_value = True if candles[0][1]>candles[1][1] else False
            print(f"Predicting {predicted_value} with a confidence of {predicted_confidence}")
    except Exception as e:
        print(e)
        pass
    return(predicted_value,predicted_confidence)
