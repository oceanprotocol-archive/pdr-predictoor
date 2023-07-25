import time
import os
import time
import os
import threading
from datetime import datetime, timedelta, timezone
from threading import Thread

from predict import predict_function
from pdr_utils.subgraph import get_all_interesting_prediction_contracts
from pdr_utils.contract import PredictorContract, Web3Config



avergage_time_between_blocks = 0
last_block_time=0
topics = []

# TODO - check for all envs
assert os.environ.get("RPC_URL",None), "You must set RPC_URL environment variable"
assert os.environ.get("SUBGRAPH_URL",None), "You must set SUBGRAPH_URL environment variable"
web3_config = Web3Config(os.environ.get("RPC_URL"),os.environ.get("PRIVATE_KEY"))
owner = web3_config.owner



class NewPrediction(Thread):
    def __init__(self,topic,predictor_contract,current_block_num,avergage_time_between_blocks,epoch,blocks_per_epoch):
        # set a default value
        self.values = { "last_submited_epoch": epoch,
                      "contract_address": predictor_contract.contract_address   
                      }
        self.topic = topic
        self.epoch = epoch
        self.predictor_contract = predictor_contract
        self.current_block_num = current_block_num
        self.avergage_time_between_blocks = avergage_time_between_blocks
        self.blocks_per_epoch = blocks_per_epoch

    def run(self):
        soonest_block = (self.epoch+2)*self.blocks_per_epoch
        now = datetime.now(timezone.utc).timestamp()
        estimated_time = now + (soonest_block - self.current_block_num)* self.avergage_time_between_blocks
        (predicted_value,predicted_confidence) = predict_function(self.topic['name'],self.topic['address'],estimated_time)
        if predicted_value is not None and predicted_confidence>0:
            """ We have a prediction, let's submit it"""
            stake_amount = os.getenv("STAKE_AMOUNT",1)*predicted_confidence/100
            print(f"Contract:{self.predictor_contract.contract_address} - Submiting prediction for slot:{soonest_block}")
            self.predictor_contract.submit_prediction(predicted_value,stake_amount,soonest_block)
        else:
            print(f"We do not submit, prediction function returned ({predicted_value}, {predicted_confidence})")
        """ claim payouts if needed """
        trueValSubmitTimeoutBlock = self.predictor_contract.get_trueValSubmitTimeoutBlock()
        blocks_per_epoch = self.predictor_contract.get_blocksPerEpoch()
        slot = self.epoch*blocks_per_epoch - trueValSubmitTimeoutBlock-1 
        print(f"Contract:{self.predictor_contract.contract_address} - Claiming revenue for slot:{slot}")
        self.predictor_contract.payout(slot)
            


def process_block(block,avergage_time_between_blocks):
    global topics
    """ Process each contract and see if we need to submit """
    if not topics:
        topics = get_all_interesting_prediction_contracts()
    print(f"Got new block: {block['number']} with {len(topics)} topics")
    for address in topics:
        topic = topics[address]
        predictor_contract = PredictorContract(web3_config,address)
        epoch = predictor_contract.get_current_epoch()
        blocks_per_epoch = predictor_contract.get_blocksPerEpoch()
        blocks_till_epoch_end=epoch*blocks_per_epoch+blocks_per_epoch-block['number']
        print(f"\t{topic['name']} (at address {topic['address']} is at epoch {epoch}, blocks_per_epoch: {blocks_per_epoch}, blocks_till_epoch_end: {blocks_till_epoch_end}")
        if epoch > topic['last_submited_epoch'] and blocks_till_epoch_end<=int(os.getenv("BLOCKS_TILL_EPOCH_END",5)):
            """ Let's make a prediction & claim rewards"""
            thr = NewPrediction(topic,predictor_contract,block["number"],avergage_time_between_blocks,epoch,blocks_per_epoch)
            thr.run()
            address=thr.values['contract_address'].lower()
            new_epoch = thr.values['last_submited_epoch']
            topics[address]["last_submited_epoch"]=new_epoch


def log_loop(blockno):
    global avergage_time_between_blocks,last_block_time
    block = web3_config.w3.eth.get_block(blockno, full_transactions=False)
    if block:
        if last_block_time>0:
            avergage_time_between_blocks = (avergage_time_between_blocks + (block["timestamp"] - last_block_time))/2
        last_block_time = block["timestamp"]
    process_block(block,avergage_time_between_blocks)
        

def main():
    print("Starting main loop...")
    lastblock =0
    while True:
        block = web3_config.w3.eth.block_number
        if block>lastblock:
            lastblock = block
            log_loop(block)
        else:
            time.sleep(1)



if __name__ == '__main__':
    main()