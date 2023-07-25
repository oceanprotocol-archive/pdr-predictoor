import time
import os
import threading
from datetime import datetime, timedelta, timezone
from threading import Thread
from pdr_predictoors.predictions.predict import predict_function, predict_function_rb

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
            
