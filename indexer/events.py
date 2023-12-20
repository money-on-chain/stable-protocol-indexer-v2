import datetime
from collections import OrderedDict
from web3 import Web3

from .logger import log


def sanitize_address(address):
    return Web3.to_checksum_address(address.replace("0x000000000000000000000000", "0x"))


class BaseEvent:

    name = 'Name'
    precision = 10 ** 18

    def __init__(self, options, connection_helper, filter_contracts_addresses, block_info):

        self.options = options
        self.connection_helper = connection_helper
        self.filter_contracts_addresses = filter_contracts_addresses
        self.block_info = block_info

    def parse_event(self, parsed_receipt, decoded_event):
        fields = dict()
        for field in decoded_event:
            fields[field['name']] = field['value']

        return dict(**parsed_receipt, **fields)

    def status_tx(self, parse_receipt):

        if self.block_info["last_block"] - parse_receipt['blockNumber'] > self.block_info['confirm_blocks']:
            status = 'confirmed'
            confirmation_time = self.block_info['block_ts']
        else:
            status = 'confirming'
            confirmation_time = None

        return status, confirmation_time

    def confirming_percent(self, parse_receipt):

        if self.block_info["last_block"] - parse_receipt['blockNumber'] > self.block_info['confirm_blocks']:
            status = 'confirmed'
            confirmation_time = self.block_info['block_ts']
            confirming_percent = 100
        else:
            status = 'confirming'
            confirmation_time = None
            confirming_percent = (self.block_info['last_block'] - parse_receipt["blockNumber"]) * 10

        return status, confirmation_time, confirming_percent


class EventMocLiqTPRedeemed(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection transaction
        liq_tp_redeemed = self.connection_helper.mongo_collection('LiqTPRedeemed')

        tx_hash = parsed['hash']

        d_tx = dict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = parsed["blockNumber"]
        d_tx["tp_"] = sanitize_address(parsed["tp_"]).lower()
        d_tx["sender_"] = sanitize_address(parsed["sender_"]).lower()
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"]).lower()
        d_tx["qTP_"] = parsed_receipt["qTP_"]
        d_tx["qAC_"] = parsed_receipt["qAC_"]

        post_id = liq_tp_redeemed.find_one_and_update(
            {"tx_hash": d_tx["transactionHash"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Event :: Liq TP Redeemed :: qTP: {0} qAC: {1}".format(d_tx["qTP_"], d_tx["qAC_"]))
        log.info(d_tx)

        return parsed


class EventMocSuccessFeeDistributed(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection transaction
        success_fee = self.connection_helper.mongo_collection('SuccessFeeDistributed')

        tx_hash = parsed['hash']

        d_tx = dict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = parsed["blockNumber"]
        d_tx["mocGain_"] = parsed_receipt["mocGain_"]
        d_tx["tpGain_"] = parsed_receipt["tpGain_"]

        post_id = success_fee.find_one_and_update(
            {"tx_hash": d_tx["transactionHash"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Event :: Success Fee Distributed ")
        log.info(d_tx)

        return parsed


class EventMocSettlementExecuted(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection transaction
        settlement_executed = self.connection_helper.mongo_collection('SettlementExecuted')

        tx_hash = parsed['hash']

        d_tx = dict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = parsed["blockNumber"]

        post_id = settlement_executed.find_one_and_update(
            {"tx_hash": d_tx["transactionHash"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Event :: Settlement Executed ")
        log.info(d_tx)

        return parsed


class EventMocTCInterestPayment(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection transaction
        tc_interest_payment = self.connection_helper.mongo_collection('TCInterestPayment')

        tx_hash = parsed['hash']

        d_tx = dict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = parsed["blockNumber"]
        d_tx["interestAmount_"] = parsed_receipt["interestAmount_"]

        post_id = tc_interest_payment.find_one_and_update(
            {"tx_hash": d_tx["transactionHash"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Event :: TC Interest Payment :: Amount: {0} ".format(d_tx["interestAmount_"]))
        log.info(d_tx)

        return parsed


class EventMocTPemaUpdated(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection transaction
        tp_ema_updated = self.connection_helper.mongo_collection('TPemaUpdated')

        tx_hash = parsed['hash']

        d_tx = dict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = parsed["blockNumber"]
        d_tx["i_"] = parsed_receipt["i_"]
        d_tx["oldTPema_"] = parsed_receipt["oldTPema_"]
        d_tx["newTPema_"] = parsed_receipt["newTPema_"]

        post_id = tp_ema_updated.find_one_and_update(
            {"tx_hash": d_tx["transactionHash"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Event :: TP Ema Updated :: i_: {0} oldTPema_: {1} newTPema_: {2}".format(d_tx["i_"], d_tx["oldTPema_"], d_tx["newTPema_"]))
        log.info(d_tx)

        return parsed


class EventMocQueueOperationError(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection transaction
        operation_queue_status = self.connection_helper.mongo_collection('OperationQueueStatus')

        tx_hash = parsed['hash']

        # STATUS:
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed

        d_tx = dict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = parsed["blockNumber"]
        d_tx["operId_"] = parsed_receipt["operId_"]
        d_tx["errorCode_"] = parsed_receipt["errorCode_"]
        d_tx["msg_"] = parsed_receipt["msg_"]
        d_tx["status"] = -1

        post_id = operation_queue_status.find_one_and_update(
            {"operId_": d_tx["operId_"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Event :: Update Status Operation ID :: {0}".format(d_tx["operId_"]))
        log.info(d_tx)

        return parsed


class EventMocQueueUnhandledError(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):
        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection transaction
        operation_queue_status = self.connection_helper.mongo_collection('OperationQueueStatus')

        tx_hash = parsed['hash']

        # STATUS:
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed

        d_tx = dict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = parsed["blockNumber"]
        d_tx["operId_"] = parsed_receipt["operId_"]
        d_tx["reason_"] = parsed_receipt["reason_"]
        d_tx["status"] = -2

        post_id = operation_queue_status.find_one_and_update(
            {"operId_": d_tx["operId_"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Event :: Update Status Operation ID :: {0}".format(d_tx["operId_"]))
        log.info(d_tx)

        return parsed


class EventMocQueueOperationQueued(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):
        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection transaction
        operation_queue_status = self.connection_helper.mongo_collection('OperationQueueStatus')

        tx_hash = parsed['hash']

        # STATUS:
        #
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed

        # Operation Type:
        #
        # 0 none
        # 1 mintTC
        # 2 redeemTC
        # 3 mintTP
        # 4 redeemTP
        # 5 mintTCandTP
        # 6 redeemTCandTP
        # 7 swapTCforTP
        # 8 swapTPforTC
        # 9 swapTPforTP

        d_tx = dict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = parsed["blockNumber"]
        d_tx["operId_"] = parsed_receipt["operId_"]
        d_tx["bucket_"] = sanitize_address(parsed["bucket_"]).lower()
        d_tx["operType_"] = parsed_receipt["operType_"]
        d_tx["status"] = 0

        post_id = operation_queue_status.find_one_and_update(
            {"operId_": d_tx["operId_"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Event :: Update Status Operation ID :: {0}".format(d_tx["operId_"]))
        log.info(d_tx)

        return parsed


class EventMocQueueOperationExecuted(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):
        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection transaction
        operation_queue_status = self.connection_helper.mongo_collection('OperationQueueStatus')

        tx_hash = parsed['hash']

        # STATUS:
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed

        d_tx = dict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = parsed["blockNumber"]
        d_tx["operId_"] = parsed_receipt["operId_"]
        d_tx["executor"] = sanitize_address(parsed["executor"]).lower()
        d_tx["status"] = 1

        post_id = operation_queue_status.find_one_and_update(
            {"operId_": d_tx["operId_"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Event :: Update Status Operation ID :: {0}".format(d_tx["operId_"]))
        log.info(d_tx)

        return parsed


class EventMocQueueTCMinted(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # status of tx
        status, confirmation_time, confirming_percent = self.confirming_percent(parsed)

        # get collection transaction
        collection_tx = self.connection_helper.mongo_collection('Transaction')

        tx_hash = parsed['hash']

        d_tx = OrderedDict()
        d_tx["blockNumber"] = parsed_receipt["blockNumber"]
        d_tx["event"] = 'TCMinted'
        d_tx["transactionHash"] = tx_hash
        d_tx["processLogs"] = True
        d_tx["createdAt"] = parsed_receipt['createdAt']
        d_tx["gas"] = parsed['gas']
        d_tx["gasPrice"] = str(parsed['gasPrice'])
        d_tx["gasUsed"] = parsed['gasUsed']
        d_tx["confirmationTime"] = confirmation_time
        d_tx['confirmingPercent'] = confirming_percent
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["status"] = status
        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qTC_"] = str(parsed["qTC_"])
        d_tx["qAC_"] = str(parsed["qAC_"])
        d_tx["qACfee_"] = str(parsed["qACfee_"])
        d_tx["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_tx["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_tx["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_tx["vendor_"] = sanitize_address(parsed["vendor_"])
        d_tx["operId_"] = str(parsed["operId_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} - Sender: [{1}] Recipient: [{2}] TC: [{3}] qAC: [{4}] qACfee: [{5}] qFeeToken: [{6}] Tx Hash: {7}".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qTC_"],
            d_tx["qAC_"],
            d_tx["qACfee_"],
            d_tx["qFeeToken_"],
            tx_hash))

        return parsed


class EventMocQueueTCRedeemed(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # status of tx
        status, confirmation_time, confirming_percent = self.confirming_percent(parsed)

        # get collection transaction
        collection_tx = self.connection_helper.mongo_collection('Transaction')

        tx_hash = parsed['hash']

        d_tx = OrderedDict()
        d_tx["blockNumber"] = parsed_receipt["blockNumber"]
        d_tx["event"] = 'TCRedeemed'
        d_tx["transactionHash"] = tx_hash
        d_tx["processLogs"] = True
        d_tx["createdAt"] = parsed_receipt['createdAt']
        d_tx["gas"] = parsed['gas']
        d_tx["gasPrice"] = str(parsed['gasPrice'])
        d_tx["gasUsed"] = parsed['gasUsed']
        d_tx["confirmationTime"] = confirmation_time
        d_tx['confirmingPercent'] = confirming_percent
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["status"] = status
        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qTC_"] = str(parsed["qTC_"])
        d_tx["qAC_"] = str(parsed["qAC_"])
        d_tx["qACfee_"] = str(parsed["qACfee_"])
        d_tx["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_tx["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_tx["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_tx["vendor_"] = sanitize_address(parsed["vendor_"])
        d_tx["operId_"] = str(parsed["operId_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} - Sender: [{1}] Recipient: [{2}]  qTC: [{3}] qAC: [{4}] qACfee: [{5}] qFeeToken: [{6}] Tx Hash: [{7}]".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qTC_"],
            d_tx["qAC_"],
            d_tx["qACfee_"],
            d_tx["qFeeToken_"],
            tx_hash))

        return parsed


class EventMocQueueTPMinted(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # status of tx
        status, confirmation_time, confirming_percent = self.confirming_percent(parsed)

        # get collection transaction
        collection_tx = self.connection_helper.mongo_collection('Transaction')

        tx_hash = parsed['hash']

        d_tx = OrderedDict()
        d_tx["blockNumber"] = parsed_receipt["blockNumber"]
        d_tx["event"] = 'TPMinted'
        d_tx["transactionHash"] = tx_hash
        d_tx["processLogs"] = True
        d_tx["createdAt"] = parsed_receipt['createdAt']
        d_tx["gas"] = parsed['gas']
        d_tx["gasPrice"] = str(parsed['gasPrice'])
        d_tx["gasUsed"] = parsed['gasUsed']
        d_tx["confirmationTime"] = confirmation_time
        d_tx['confirmingPercent'] = confirming_percent
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["status"] = status
        d_tx["tp"] = sanitize_address(parsed["tp"])
        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qTP_"] = str(parsed["qTP_"])
        d_tx["qAC_"] = str(parsed["qAC_"])
        d_tx["qACfee_"] = str(parsed["qACfee_"])
        d_tx["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_tx["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_tx["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_tx["vendor_"] = sanitize_address(parsed["vendor_"])
        d_tx["operId_"] = str(parsed["operId_"])
                        
        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} - Sender: [{1}] Recipient: [{2}] qTP: [{3}] qAC: [{4}] qACfee: [{5}] qFeeToken: [{6}] Tx Hash: [{7}]".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qTP_"],
            d_tx["qAC_"],
            d_tx["qACfee_"],
            d_tx["qFeeToken_"],
            tx_hash))

        return parsed


class EventMocQueueTPRedeemed(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # status of tx
        status, confirmation_time, confirming_percent = self.confirming_percent(parsed)

        # get collection transaction
        collection_tx = self.connection_helper.mongo_collection('Transaction')

        tx_hash = parsed['hash']

        d_tx = OrderedDict()
        d_tx["blockNumber"] = parsed_receipt["blockNumber"]
        d_tx["event"] = 'TPRedeemed'
        d_tx["transactionHash"] = tx_hash
        d_tx["processLogs"] = True
        d_tx["createdAt"] = parsed_receipt['createdAt']
        d_tx["gas"] = parsed['gas']
        d_tx["gasPrice"] = str(parsed['gasPrice'])
        d_tx["gasUsed"] = parsed['gasUsed']
        d_tx["confirmationTime"] = confirmation_time
        d_tx['confirmingPercent'] = confirming_percent
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["status"] = status
        d_tx["tp_"] = sanitize_address(parsed["tp_"])
        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qTP_"] = str(parsed["qTP_"])
        d_tx["qAC_"] = str(parsed["qAC_"])
        d_tx["qACfee_"] = str(parsed["qACfee_"])
        d_tx["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_tx["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_tx["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_tx["vendor_"] = sanitize_address(parsed["vendor_"])
        d_tx["operId_"] = str(parsed["operId_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} - Sender: [{1}] Recipient: [{2}]  qTP: [{3}] qAC: [{4}] qACfee: [{5}] qFeeToken: [{6}] Tx Hash: [{7}]".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qTP_"],
            d_tx["qAC_"],
            d_tx["qACfee_"],
            d_tx["qFeeToken_"],
            tx_hash))

        return parsed


class EventMocQueueTPSwappedForTP(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # status of tx
        status, confirmation_time, confirming_percent = self.confirming_percent(parsed)

        # get collection transaction
        collection_tx = self.connection_helper.mongo_collection('Transaction')

        tx_hash = parsed['hash']

        d_tx = OrderedDict()
        d_tx["blockNumber"] = parsed_receipt["blockNumber"]
        d_tx["event"] = parsed_receipt['eventName']
        d_tx["transactionHash"] = tx_hash
        d_tx["processLogs"] = True
        d_tx["createdAt"] = parsed_receipt['createdAt']
        d_tx["gas"] = parsed['gas']
        d_tx["gasPrice"] = str(parsed['gasPrice'])
        d_tx["gasUsed"] = parsed['gasUsed']
        d_tx["confirmationTime"] = confirmation_time
        d_tx['confirmingPercent'] = confirming_percent
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["status"] = status
        d_tx["tpFrom_"] = sanitize_address(parsed["tpFrom_"])
        d_tx["tpTo_"] = sanitize_address(parsed["tpTo_"])
        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qTPfrom_"] = str(parsed["qTPfrom_"])
        d_tx["qTPto_"] = str(parsed["qTPto_"])
        d_tx["qACfee_"] = str(parsed["qACfee_"])
        d_tx["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_tx["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_tx["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_tx["vendor_"] = sanitize_address(parsed["vendor_"])
        d_tx["operId_"] = str(parsed["operId_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} - Sender: [{1}] Recipient: [{2}] qTPfrom_: [{3}] qTPto_: [{4}] qACfee_: [{5}] qACfee_: [{6}] Tx Hash: [{7}]".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qTPfrom_"],
            d_tx["qTPto_"],
            d_tx["qACfee_"],
            d_tx["qFeeToken_"],
            tx_hash))

        return parsed


class EventMocQueueTPSwappedForTC(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # status of tx
        status, confirmation_time, confirming_percent = self.confirming_percent(parsed)

        # get collection transaction
        collection_tx = self.connection_helper.mongo_collection('Transaction')

        tx_hash = parsed['hash']

        d_tx = OrderedDict()
        d_tx["blockNumber"] = parsed_receipt["blockNumber"]
        d_tx["event"] = parsed_receipt['eventName']
        d_tx["transactionHash"] = tx_hash
        d_tx["processLogs"] = True
        d_tx["createdAt"] = parsed_receipt['createdAt']
        d_tx["gas"] = parsed['gas']
        d_tx["gasPrice"] = str(parsed['gasPrice'])
        d_tx["gasUsed"] = parsed['gasUsed']
        d_tx["confirmationTime"] = confirmation_time
        d_tx['confirmingPercent'] = confirming_percent
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["status"] = status
        d_tx["tp_"] = sanitize_address(parsed["tp_"])
        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qTC_"] = str(parsed["qTC_"])
        d_tx["qTP_"] = str(parsed["qTP_"])
        d_tx["qACfee_"] = str(parsed["qACfee_"])
        d_tx["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_tx["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_tx["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_tx["vendor_"] = sanitize_address(parsed["vendor_"])
        d_tx["operId_"] = str(parsed["operId_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} - Sender: [{1}] Recipient: [{2}] qTP: [{3}] qTC: [{4}] qACfee: [{5}] qFeeToken: [{6}] Tx Hash: [{6}]".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qTP_"],
            d_tx["qTC_"],
            d_tx["qACfee_"],
            d_tx["qFeeToken_"],
            tx_hash))

        return parsed


class EventMocQueueTCSwappedForTP(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # status of tx
        status, confirmation_time, confirming_percent = self.confirming_percent(parsed)

        # get collection transaction
        collection_tx = self.connection_helper.mongo_collection('Transaction')

        tx_hash = parsed['hash']

        d_tx = OrderedDict()
        d_tx["blockNumber"] = parsed_receipt["blockNumber"]
        d_tx["event"] = parsed_receipt['eventName']
        d_tx["transactionHash"] = tx_hash
        d_tx["processLogs"] = True
        d_tx["createdAt"] = parsed_receipt['createdAt']
        d_tx["gas"] = parsed['gas']
        d_tx["gasPrice"] = str(parsed['gasPrice'])
        d_tx["gasUsed"] = parsed['gasUsed']
        d_tx["confirmationTime"] = confirmation_time
        d_tx['confirmingPercent'] = confirming_percent
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["status"] = status
        d_tx["tp_"] = sanitize_address(parsed["tp_"])
        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qTC_"] = str(parsed["qTC_"])
        d_tx["qTP_"] = str(parsed["qTP_"])
        d_tx["qACfee_"] = str(parsed["qACfee_"])
        d_tx["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_tx["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_tx["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_tx["vendor_"] = sanitize_address(parsed["vendor_"])
        d_tx["operId_"] = str(parsed["operId_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} - Sender: [{1}] Recipient: [{2}] qTC: [{3}] qTP: [{4}] qACfee: [{5}] qACfee: [{6}] Tx Hash: [{7}]".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qTC_"],
            d_tx["qTP_"],
            d_tx["qACfee_"],
            d_tx["qFeeToken_"],
            tx_hash))

        return parsed


class EventMocQueueTCandTPRedeemed(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):
        parsed = self.parse_event(parsed_receipt, decoded_event)

        # status of tx
        status, confirmation_time, confirming_percent = self.confirming_percent(parsed)

        # get collection transaction
        collection_tx = self.connection_helper.mongo_collection('Transaction')

        tx_hash = parsed['hash']

        d_tx = OrderedDict()
        d_tx["blockNumber"] = parsed_receipt["blockNumber"]
        d_tx["event"] = parsed_receipt['eventName']
        d_tx["transactionHash"] = tx_hash
        d_tx["processLogs"] = True
        d_tx["createdAt"] = parsed_receipt['createdAt']
        d_tx["gas"] = parsed['gas']
        d_tx["gasPrice"] = str(parsed['gasPrice'])
        d_tx["gasUsed"] = parsed['gasUsed']
        d_tx["confirmationTime"] = confirmation_time
        d_tx['confirmingPercent'] = confirming_percent
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["status"] = status
        d_tx["tp_"] = sanitize_address(parsed["tp_"])
        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qTC_"] = str(parsed["qTC_"])
        d_tx["qTP_"] = str(parsed["qTP_"])
        d_tx["qAC_"] = str(parsed["qAC_"])
        d_tx["qACfee_"] = str(parsed["qACfee_"])
        d_tx["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_tx["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_tx["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_tx["vendor_"] = sanitize_address(parsed["vendor_"])
        d_tx["operId_"] = str(parsed["operId_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} - Sender: [{1}] Recipient: [{2}]  qTC: [{3}] qTP: [{4}] qAC: [{5}] qACfee: [{6}] qFeeToken: [{7}] Tx Hash: [{8}]".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qTC_"],
            d_tx["qTP_"],
            d_tx["qAC_"],
            d_tx["qACfee_"],
            d_tx["qFeeToken_"],
            tx_hash))

        return parsed


class EventMocQueueTCandTPMinted(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):
        parsed = self.parse_event(parsed_receipt, decoded_event)

        # status of tx
        status, confirmation_time, confirming_percent = self.confirming_percent(parsed)

        # get collection transaction
        collection_tx = self.connection_helper.mongo_collection('Transaction')

        tx_hash = parsed['hash']

        d_tx = OrderedDict()
        d_tx["blockNumber"] = parsed_receipt["blockNumber"]
        d_tx["event"] = parsed_receipt['eventName']
        d_tx["transactionHash"] = tx_hash
        d_tx["processLogs"] = True
        d_tx["createdAt"] = parsed_receipt['createdAt']
        d_tx["gas"] = parsed['gas']
        d_tx["gasPrice"] = str(parsed['gasPrice'])
        d_tx["gasUsed"] = parsed['gasUsed']
        d_tx["confirmationTime"] = confirmation_time
        d_tx['confirmingPercent'] = confirming_percent
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["status"] = status
        d_tx["tp_"] = sanitize_address(parsed["tp_"])
        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qTC_"] = str(parsed["qTC_"])
        d_tx["qTP_"] = str(parsed["qTP_"])
        d_tx["qAC_"] = str(parsed["qAC_"])
        d_tx["qACfee_"] = str(parsed["qACfee_"])
        d_tx["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_tx["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_tx["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_tx["vendor_"] = sanitize_address(parsed["vendor_"])
        d_tx["operId_"] = str(parsed["operId_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} - Sender: [{1}] Recipient: [{2}] qTC: [{2}] qTP: [{3}] qAC: [{4}] qACfee: [{5}] qFeeToken: [{6}] Tx Hash: [{7}]".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qTC_"],
            d_tx["qTP_"],
            d_tx["qAC_"],
            d_tx["qACfee_"],
            d_tx["qFeeToken_"],
            tx_hash))

        return parsed


class EventTokenTransfer(BaseEvent):

    def __init__(self, options, connection_helper, filter_contracts_addresses, block_info, token_involved):

        self.options = options
        self.connection_helper = connection_helper
        self.filter_contracts_addresses = filter_contracts_addresses
        self.block_info = block_info
        self.token_involved = token_involved

        super().__init__(options, connection_helper, filter_contracts_addresses, block_info)

    # def parse_event(self, parsed_receipt, decoded_event):
    #
    #     # decode event to support write in mongo
    #     parsed_receipt['from'] = decoded_event['from'].lower()
    #     parsed_receipt['to'] = decoded_event['to'].lower()
    #     parsed_receipt['value'] = str(decoded_event['value'])
    #
    #     return parsed_receipt

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        address_from_contract = '0x0000000000000000000000000000000000000000'
        address_not_allowed = [str.lower(address_from_contract), self.filter_contracts_addresses]

        if sanitize_address(parsed['from']) in address_not_allowed or \
                sanitize_address(parsed['to']) in address_not_allowed:
            # skip transfers to our contracts
            return parsed

        # status of tx
        status, confirmation_time, confirming_percent = self.confirming_percent(parsed)

        # get collection transaction
        collection_tx = self.connection_helper.mongo_collection('Transaction')

        tx_hash = parsed['hash']

        d_tx = OrderedDict()
        d_tx["blockNumber"] = parsed_receipt["blockNumber"]
        d_tx["event"] = parsed_receipt['eventName']
        d_tx["transactionHash"] = tx_hash
        d_tx["processLogs"] = True
        d_tx["createdAt"] = parsed_receipt['createdAt']
        d_tx["gas"] = parsed['gas']
        d_tx["gasPrice"] = str(parsed['gasPrice'])
        d_tx["gasUsed"] = parsed['gasUsed']
        d_tx["confirmationTime"] = confirmation_time
        d_tx['confirmingPercent'] = confirming_percent
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["status"] = status

        d_tx["token"] = self.token_involved
        d_tx["from_"] = sanitize_address(parsed["from"]).lower()
        d_tx["to_"] = sanitize_address(parsed["to"]).lower()
        d_tx["value_"] = str(parsed["value"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)

        log.info("Tx {0} - Token: [{1}] From: [{2}] To: [{3}] Value: [{4}] Tx Hash: [{5}]".format(
            d_tx["event"],
            d_tx["token"],
            d_tx["from_"],
            d_tx["to_"],
            d_tx["value_"],
            tx_hash))

        return parsed


class EventFastBtcBridgeNewBitcoinTransfer(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection transaction
        collection_bridge = self.connection_helper.mongo_collection('FastBtcBridge')

        tx_hash = parsed['hash']

        d_tx = dict()
        d_tx["transactionHash"] = tx_hash
        d_tx["transactionHashLastUpdated"] = tx_hash
        d_tx["blockNumber"] = parsed["blockNumber"]
        d_tx["type"] = 'PEG_OUT'
        d_tx["transferId"] = str(parsed["transferId"])
        d_tx["btcAddress"] = parsed["btcAddress"]
        d_tx["nonce"] = parsed["nonce"]
        d_tx["amountSatoshi"] = str(parsed["amountSatoshi"])
        d_tx["feeSatoshi"] = str(parsed["feeSatoshi"])
        d_tx["rskAddress"] = sanitize_address(parsed["rskAddress"])
        d_tx["status"] = 0
        d_tx["timestamp"] = parsed["timestamp"]
        d_tx["updated"] = parsed["timestamp"]
        d_tx["processLogs"] = True

        post_id = collection_bridge.find_one_and_update(
            {"transferId": d_tx["transferId"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("EVENT::NewBitcoinTransfer::{0}".format(d_tx["transferId"]))
        log.info(d_tx)

        return parsed


class EventFastBtcBridgeBitcoinTransferStatusUpdated(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection transaction
        collection_bridge = self.connection_helper.mongo_collection('FastBtcBridge')

        d_tx = dict()
        d_tx["transactionHashLastUpdated"] = parsed["hash"]
        d_tx["status"] = parsed["newStatus"]
        d_tx["transferId"] = str(parsed["transferId"])
        d_tx["updated"] = parsed["timestamp"]

        post_id = collection_bridge.find_one_and_update(
            {"transferId": d_tx["transferId"]},
            {"$set": d_tx},
            upsert=False)
        d_tx['post_id'] = post_id

        log.info("EVENT::BitcoinTransferStatusUpdated::{0}".format(d_tx["transferId"]))
        log.info(d_tx)

        return parsed
