
import glob
import json
import os
from eth_account import Account
from eth_account.signers.local import LocalAccount
from pathlib import Path
from web3 import Web3, HTTPProvider, WebsocketProvider
from web3.middleware import construct_sign_and_send_raw_middleware
from os.path import expanduser
home = expanduser("~")

rpc_url = os.environ.get("RPC_URL")
assert rpc_url is not None, "You must set RPC_URL environment variable"
private_key = os.environ.get("PRIVATE_KEY")
assert private_key is not None, "You must set PRIVATE_KEY environment variable"
assert private_key.startswith("0x"), "Private key must start with 0x hex prefix"

# instantiate Web3 instance
w3 = Web3(Web3.HTTPProvider(rpc_url))
account: LocalAccount = Account.from_key(private_key)
owner = account.address
w3.middleware_onion.add(construct_sign_and_send_raw_middleware(account))




class Token:
    def __init__(self, address):
        self.contract_address = w3.to_checksum_address(address)
        self.contract_instance = w3.eth.contract(address=w3.to_checksum_address(address), abi=get_contract_abi('ERC20Template3'))

    def allowance(self,account,spender):
        return self.contract_instance.functions.allowance(account,spender).call()

    def balanceOf(self,account):
        return self.contract_instance.functions.balanceOf(account).call()
    
    def approve(self,spender,amount):
        gasPrice = w3.eth.gas_price
        #print(f"Approving {amount} for {spender} on contract {self.contract_address}")
        try:
            tx = self.contract_instance.functions.approve(spender,amount).transact({"from":owner,"gasPrice":gasPrice})
            receipt = w3.eth.wait_for_transaction_receipt(tx)
            return receipt
        except:
            return None


class PredictorContract:
    def __init__(self, address):
        self.contract_address = w3.to_checksum_address(address)
        self.contract_instance = w3.eth.contract(address=w3.to_checksum_address(address), abi=get_contract_abi('ERC20Template3'))
    
    def soonest_block_to_predict(self,block):
        return self.contract_instance.functions.soonestBlockToPredict(block).call()
    def get_current_epoch(self):
        return self.contract_instance.functions.curEpoch().call()
    
    def submit_prediction(self,predicted_value,stake_amount,prediction_block):
        """ Sumbits a prediction"""
        stake_token = Token(self.contract_instance.functions.stakeToken().call())
        # TO DO - check allowence
        amount_wei = w3.to_wei(str(stake_amount),'ether')
        stake_token.approve(self.contract_address,amount_wei)
        gasPrice = w3.eth.gas_price
        try:
            tx = self.contract_instance.functions.submitPredval(predicted_value,amount_wei,prediction_block).transact({"from":owner,"gasPrice":gasPrice})
            print(f"Submitted prediction, txhash: {tx.hex()}")
            receipt = w3.eth.wait_for_transaction_receipt(tx)
            return receipt
        except Exception as e:
            print(e)
            return None
    
    def get_trueValSubmitTimeoutBlock(self):
        return self.contract_instance.functions.trueValSubmitTimeoutBlock().call()
    
    def get_blocksPerEpoch(self):
        return self.contract_instance.functions.blocksPerEpoch().call()
    
    def get_prediction(self,slot):
        return self.contract_instance.functions.getPrediction(slot).call({"from":owner})
    
    def payout(self,slot):
        """ Claims the payout for a slot"""
        gasPrice = w3.eth.gas_price
        try:
            tx = self.contract_instance.functions.payout(slot,owner).transact({"from":owner,"gasPrice":gasPrice})
            print(f"Submitted payout, txhash: {tx.hex()}")
            receipt = w3.eth.wait_for_transaction_receipt(tx)
            return receipt
        except Exception as e:
            print(e)
            return None

def get_contract_abi(contract_name):
    """Returns the abi for a contract name."""
    path = get_contract_filename(contract_name)

    if not path.exists():
        raise TypeError("Contract name does not exist in artifacts.")

    with open(path) as f:
        data = json.load(f)
        return data['abi']

def get_contract_filename(contract_name):
    """Returns abi for a contract."""
    contract_basename = f"{contract_name}.json"

    # first, try to find locally
    address_filename = os.getenv("ADDRESS_FILE")
    path = None
    if address_filename:
        address_dir = os.path.dirname(address_filename)
        root_dir = os.path.join(address_dir, "..")
        paths = Path(root_dir).rglob(contract_basename)
        paths = [str(path) for path in paths]
        if paths:
            assert len(paths) == 1, "had duplicates for {contract_basename}"
            path = paths[0]
            path = Path(path).expanduser().resolve()
            assert (
                path.exists()
            ), f"Found path = '{path}' via glob, yet path.exists() is False"
            return path
    # didn't find locally, so use use artifacts lib
    #path = os.path.join(os.path.dirname(artifacts.__file__), "", contract_basename)
    #path = Path(path).expanduser().resolve()
    #if not path.exists():
    #    raise TypeError(f"Contract '{contract_name}' not found in artifacts.")
    return path

