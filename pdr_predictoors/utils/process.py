import threading
import os
from pdr_utils.subgraph import get_all_interesting_prediction_contracts
from pdr_predictoors.utils.contract import PredictorContract
from pdr_predictoors.utils.threads import NewPrediction

""" Get all intresting topics that we can predict.  Like ETH-USDT, BTC-USDT """
topics = []
def process_block(block,avergage_time_between_blocks,model,main_pd):
    global topics
    """ Process each contract and see if we need to submit """
    if not topics:
        topics = get_all_interesting_prediction_contracts()
    print(f"Got new block: {block['number']} with {len(topics)} topics")
    for address in topics:
        topic = topics[address]
        predictor_contract = PredictorContract(address)
        epoch = predictor_contract.get_current_epoch()
        blocks_per_epoch = predictor_contract.get_blocksPerEpoch()
        blocks_till_epoch_end=epoch*blocks_per_epoch+blocks_per_epoch-block['number']
        print(f"\t{topic['name']} (at address {topic['address']} is at epoch {epoch}, blocks_per_epoch: {blocks_per_epoch}, blocks_till_epoch_end: {blocks_till_epoch_end}")
        if epoch > topic['last_submited_epoch'] and blocks_till_epoch_end<=int(os.getenv("BLOCKS_TILL_EPOCH_END",20)):
            """ Let's make a prediction & claim rewards"""
            thr = NewPrediction(topic,predictor_contract,block["number"],avergage_time_between_blocks,epoch,blocks_per_epoch,model,main_pd)
            predicted_value = thr.run()
            address=thr.values['contract_address'].lower()
            new_epoch = thr.values['last_submited_epoch']
            topics[address]["last_submited_epoch"]=new_epoch
            return predicted_value


