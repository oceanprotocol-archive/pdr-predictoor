import ccxt
import pandas as pd
import random

from pdr_predictoors.models.richard1 import RichardModel1

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

def predict_function_rb(topic,contract_address, estimated_time):
    print(f" We were asked to predict {topic} (contract: {contract_address}) value at estimated timestamp: {estimated_time}")

    exchange_id = 'binance'
    pair='BTC/TUSD'
    timeframe='5m'
    exchange_class = getattr(ccxt, exchange_id)
    exchange_ccxt = exchange_class({
        'timeout': 30000
    })

    columns_short = [
        "timestamp",
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]

    candles = exchange_ccxt.fetch_ohlcv(pair, "5m")

    main_pd = pd.DataFrame(columns=columns_short)
    for ohl in candles:
            ohlc= {
                    'timestamp':int(ohl[0]/1000),
                    'open':float(ohl[1]),
                    'close':float(ohl[4]),
                    'low':float(ohl[3]),
                    'high':float(ohl[2]),
                    'volume':float(ohl[5]),
            }
            main_pd.loc[ohlc["timestamp"]]=ohlc
            main_pd["datetime"] = pd.to_datetime(main_pd["timestamp"], unit="s", utc=True)

    model = RichardModel1(exchange_id,pair,timeframe)

    model.unpickle_model("/home/richard/code/ocean/pdr-predictoor/pdr_predictoors/models/trained_models")

    predicted_value, main_pd_features = model.predict(main_pd.drop(['datetime'], axis=1))
    predicted_confidence = 0.5
    predicted_value = bool(predicted_value)
    print(f"Predicting {predicted_value} with a confidence of {predicted_confidence}")

    return(predicted_value,predicted_confidence)
