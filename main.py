import ccxt
import csv
import numpy as np
import pandas as pd
import time
import os

from pdr_predictoors.models.richard1 import RichardModel1
from pdr_predictoors.utils.process import process_block
from pdr_predictoors.utils.contract import w3


# TODO - check for all envs
assert os.environ.get("RPC_URL",None), "You must set RPC_URL environment variable"
assert os.environ.get("SUBGRAPH_URL",None), "You must set SUBGRAPH_URL environment variable"

avergage_time_between_blocks = 0
last_block_time=0

exchange_id = 'binance'
pair='BTC/TUSD'
timeframe='5m'
exchange_class = getattr(ccxt, exchange_id)
exchange_ccxt = exchange_class({
    'timeout': 30000
})

models = [
    # OceanModel(exchange_id,pair,timeframe),
    RichardModel1(exchange_id,pair,timeframe)
]

def log_loop(blockno, model, main_pd):
    global avergage_time_between_blocks,last_block_time
    block = w3.eth.get_block(blockno, full_transactions=False)
    if block:
        if last_block_time>0:
            avergage_time_between_blocks = (avergage_time_between_blocks + (block["timestamp"] - last_block_time))/2
        last_block_time = block["timestamp"]
    prediction = process_block(block,avergage_time_between_blocks,model,main_pd)
    if prediction is not None:
        return prediction

def main():
    print("Starting main loop...")

    ts_now=int( time.time() )
    results_csv_name='./results/'+exchange_id+"_"+models[0].pair+"_"+models[0].timeframe+"_"+str(ts_now)+".csv"

    columns_short = [
        "timestamp",
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]

    columns_models = []
    for model in models:
        model.unpickle_model("./pdr_predictoors/models/trained_models")
        columns_models.append(model.model_name) # prediction column.  0 or 1

    all_columns=columns_short+columns_models

    #write csv header for results
    size = 0
    try:
        files_stats=os.stat(results_csv_name)
        size = files_stats.st_size
    except:
        pass
    if size==0:
        with open(results_csv_name, 'a') as f:
            writer = csv.writer(f)
            writer.writerow(all_columns)

    #read initial set of candles
    candles = exchange_ccxt.fetch_ohlcv(pair, "5m")
    #load past data
    main_pd = pd.DataFrame(columns=all_columns)
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

    lastblock =0
    last_finalized_timestamp = 0
    while True:

        candles = exchange_ccxt.fetch_ohlcv(pair, "5m")

        #update last two candles
        for ohl in candles[-2:]:
            t = int(ohl[0]/1000)
            main_pd.loc[t,['datetime']]=pd.to_datetime(t, unit="s", utc=True)
            main_pd.loc[t,['open']]=float(ohl[1])
            main_pd.loc[t,['close']]=float(ohl[4])
            main_pd.loc[t,['low']]=float(ohl[3])
            main_pd.loc[t,['high']]=float(ohl[2])
            main_pd.loc[t,['volume']]=float(ohl[5])

        timestamp = main_pd.index.values[-2]

        block = w3.eth.block_number
        if block>lastblock:
            lastblock = block

            # #we have a new candle
            if last_finalized_timestamp<timestamp:
                last_finalized_timestamp = timestamp

                should_write = False
                for model in models:
                    prediction = main_pd.iloc[-2][model.model_name]
                    if not np.isnan(prediction):
                        should_write = True

                if should_write:
                    with open(results_csv_name, 'a') as f:
                        writer = csv.writer(f)
                        row = [
                            main_pd.index.values[-2],
                            main_pd.iloc[-2]['datetime'],
                            main_pd.iloc[-2]['open'],
                            main_pd.iloc[-2]["high"],
                            main_pd.iloc[-2]["low"],
                            main_pd.iloc[-2]["close"],
                            main_pd.iloc[-2]["volume"],
                        ]
                        for model in models:
                            row.append(main_pd.iloc[-2][model.model_name])
                        writer.writerow(row)

            for model in models:
                index = main_pd.index.values[-1]
                current_prediction = main_pd.iloc[-1][model.model_name]
                if np.isnan(current_prediction):
                    prediction = log_loop(block, model, main_pd.drop(columns_models+['datetime'], axis=1))
                    if prediction is not None:
                        main_pd.loc[index,[model.model_name]]=float(prediction)

            print(main_pd.loc[:, ~main_pd.columns.isin(['volume','open','high','low'])].tail(15))

        else:
            time.sleep(1)



if __name__ == '__main__':
    main()