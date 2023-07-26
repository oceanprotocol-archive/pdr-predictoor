First cut: it may work, with errors

## Flow
- reads from subgraph list of template3 contracts, this gets list of all template3 deployed contracts
- for every contract, monitors when epoch is changing
- once we can predict a value, we call predict_function in prd_predictoors/predictions/predict.py. See below


## How to run

For full flow see [README](https://github.com/oceanprotocol/pdr-trueval/blob/main/README_local_full_flow.md)

```bash
export RPC_URL=http://127.0.0.1:8545
export SUBGRAPH_URL="http://172.15.0.15:8000/subgraphs/name/oceanprotocol/ocean-subgraph"
export PRIVATE_KEY="xxx"
export STAKE_AMOUNT=1
```
where:
  - STAKE_AMOUNT  = maximum stake (in decimal) for each prediction.  See predicted_confidence below


Install requirements if needed
```bash
pip install -r requirements.txt
```

Start the predictoor:
```bash
python3 main.py
```

## Additional ENV variables used to filter:

 - PAIR_FILTER = if we do want to act upon only same pair, like  "BTC/USDT,ETH/USDT"
 - TIMEFRAME_FILTER = if we do want to act upon only same timeframes, like  "5m,15m"
 - SOURCE_FILTER = if we do want to act upon only same sources, like  "binance,kraken"
 - OWNER_ADDRS = if we do want to act upon only same publishers, like  "0x123,0x124"

## Fork and customize
  The actual prediction code is in [predict.py](https://github.com/oceanprotocol/pdr-predictoors/blob/main/predict.py).

  We call predict_function with 2 args:
   - topic:  this is pair object
   - estimated_time:  estimated timestamp of block that we are going to predict.   This is informal, blockchain mining time is not accurate

  Function returns two variables:
   - predicted_value:  boolean, up/down
   - predicted_confidence:   int, 1 -> 100%. This sets the stake (STAKE_AMOUNT * predicted_confidence/100) that you are willing to put in your prediction.


  You need to change the function code and do some of your stuff. Now, it's just doing some random predictions

## BLOCKS_TILL_EPOCH_END
  If we want to predict the value for epoch E, we need to do it in epoch E - 2 (latest.  Of course, we could predictor values for a distant epoch in the future if we want to)
  And to do so, our tx needs to be confirmed in the last block of epoch (otherwise, it's going to be part of next epoch and our prediction tx will revert)

  But, for every prediction, there are some steps involved, each one taking it's toll on duration:
    - the actual prediction code
    - time to generate the tx
    - time until your pending tx in mempool is picked by miner
    - time until your tx is confirmed in a block

  You can control how early to predict, taking the above in consideration, using env BLOCKS_TILL_EPOCH_END.
  It's translation is:  With how many blocks in advanced untill epoch end do we start the prediction process.
  The default value is 5, which leaves us enough time.  (Ie: if block generation duration is 12 sec, we have 60 seconds to do our job)

## TO DO
  - [ ]  - improve payouts collect flow
  - [ ]  - check for balances
  - [ ]  - improve approve/allowence flow
