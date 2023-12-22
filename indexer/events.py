import datetime
from collections import OrderedDict
from web3 import Web3

from .logger import log


def sanitize_address(address):
    return Web3.to_checksum_address(address.replace("0x000000000000000000000000", "0x"))


class BaseEvent:

    name = 'Name'
    precision = 10 ** 18

    def __init__(self, options, connection_helper, contracts_loaded, filter_contracts_addresses, block_info):

        self.options = options
        self.connection_helper = connection_helper
        self.contracts_loaded = contracts_loaded
        self.filter_contracts_addresses = filter_contracts_addresses
        self.block_info = block_info

    def parse_event(self, parsed_receipt, decoded_event):
        fields = dict()
        for field in decoded_event:
            fields[field['name']] = field['value']

        return dict(**parsed_receipt, **fields)


class EventMocLiqTPRedeemed(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_Moc_LiqTPRedeemed')

        tx_hash = parsed['hash']

        d_event = dict()
        d_event["hash"] = tx_hash
        d_event["blockNumber"] = parsed["blockNumber"]
        d_event["tp_"] = sanitize_address(parsed["tp_"]).lower()
        d_event["sender_"] = sanitize_address(parsed["sender_"]).lower()
        d_event["recipient_"] = sanitize_address(parsed["recipient_"]).lower()
        d_event["qTP_"] = parsed_receipt["qTP_"]
        d_event["qAC_"] = parsed_receipt["qAC_"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        insert_id = collection.find_one_and_update(
            {"hash": d_event["hash"]},
            {"$set": d_event},
            upsert=True)
        d_event['insert_id'] = insert_id

        log.info("Event :: Moc_LiqTPRedeemed :: qTP: {0} qAC: {1}".format(d_event["qTP_"], d_event["qAC_"]))
        log.info(d_event)

        return d_event, parsed


class EventMocSuccessFeeDistributed(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection SuccessFeeDistributed
        collection = self.connection_helper.mongo_collection('event_Moc_SuccessFeeDistributed')

        tx_hash = parsed['hash']

        d_event = dict()
        d_event["hash"] = tx_hash
        d_event["blockNumber"] = parsed["blockNumber"]
        d_event["mocGain_"] = parsed_receipt["mocGain_"]
        d_event["tpGain_"] = parsed_receipt["tpGain_"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        insert_id = collection.find_one_and_update(
            {"hash": d_event["hash"]},
            {"$set": d_event},
            upsert=True)
        d_event['insert_id'] = insert_id

        log.info("Event :: Success Fee Distributed ")
        log.info(d_event)

        return d_event, parsed


class EventMocSettlementExecuted(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_Moc_SettlementExecuted')

        tx_hash = parsed['hash']

        d_event = dict()
        d_event["hash"] = tx_hash
        d_event["blockNumber"] = parsed["blockNumber"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        insert_id = collection.find_one_and_update(
            {"hash": d_event["hash"]},
            {"$set": d_event},
            upsert=True)
        d_event['insert_id'] = insert_id

        log.info("Event :: Settlement Executed ")
        log.info(d_event)

        return d_event, parsed


class EventMocTCInterestPayment(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_Moc_TCInterestPayment')

        tx_hash = parsed['hash']

        d_event = dict()
        d_event["hash"] = tx_hash
        d_event["blockNumber"] = parsed["blockNumber"]
        d_event["interestAmount_"] = parsed_receipt["interestAmount_"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        insert_id = collection.find_one_and_update(
            {"hash": d_event["hash"]},
            {"$set": d_event},
            upsert=True)
        d_event['insert_id'] = insert_id

        log.info("Event :: TC Interest Payment :: Amount: {0} ".format(d_event["interestAmount_"]))
        log.info(d_event)

        return d_event, parsed


class EventMocTPemaUpdated(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_Moc_TPemaUpdated')

        tx_hash = parsed['hash']

        d_event = dict()
        d_event["hash"] = tx_hash
        d_event["blockNumber"] = parsed["blockNumber"]
        d_event["i_"] = parsed_receipt["i_"]
        d_event["oldTPema_"] = parsed_receipt["oldTPema_"]
        d_event["newTPema_"] = parsed_receipt["newTPema_"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        insert_id = collection.find_one_and_update(
            {"hash": d_event["hash"]},
            {"$set": d_event},
            upsert=True)
        d_event['insert_id'] = insert_id

        log.info("Event :: TP Ema Updated :: i_: {0} oldTPema_: {1} newTPema_: {2}".format(d_event["i_"], d_event["oldTPema_"], d_event["newTPema_"]))
        log.info(d_event)

        return d_event, parsed


class EventMocQueueOperationError(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection OperationQueueStatus
        collection = self.connection_helper.mongo_collection('event_MocQueue_OperationError')

        tx_hash = parsed['hash']

        # STATUS:
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_event = dict()
        d_event["hash"] = tx_hash
        d_event["blockNumber"] = parsed["blockNumber"]
        d_event["operId_"] = parsed_receipt["operId_"]
        d_event["errorCode_"] = parsed_receipt["errorCode_"]
        d_event["msg_"] = parsed_receipt["msg_"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        insert_id = collection.find_one_and_update(
            {"hash": d_event["hash"]},
            {"$set": d_event},
            upsert=True)
        d_event['insert_id'] = insert_id

        log.info("Event :: OperationError :: operId_: {0}".format(d_event["operId_"]))
        log.info(d_event)

        # change status in collection operations
        collection = self.connection_helper.mongo_collection('operations')

        d_oper = OrderedDict()
        d_oper["blockNumber"] = parsed_receipt["blockNumber"]
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = parsed['gasUsed']
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["reason_"] = d_event["reason_"]
        d_oper["status"] = -1  # Queue

        insert_id = collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)
        d_oper['insert_id'] = insert_id

        return d_oper, parsed


class EventMocQueueUnhandledError(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):
        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_UnhandledError')

        tx_hash = parsed['hash']

        # STATUS:
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_event = dict()
        d_event["hash"] = tx_hash
        d_event["blockNumber"] = parsed["blockNumber"]
        d_event["operId_"] = parsed_receipt["operId_"]
        d_event["reason_"] = parsed_receipt["reason_"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        insert_id = collection.find_one_and_update(
            {"hash": d_event["hash"]},
            {"$set": d_event},
            upsert=True)
        d_event['insert_id'] = insert_id

        log.info("Event :: MocQueue_UnhandledError :: operId_: {0}".format(d_event["operId_"]))
        log.info(d_event)

        # change status in collection operations
        collection = self.connection_helper.mongo_collection('operations')

        d_oper = OrderedDict()
        d_oper["blockNumber"] = parsed_receipt["blockNumber"]
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = parsed['gasUsed']
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["reason_"] = d_event["reason_"]
        d_oper["status"] = -2  # Queue

        insert_id = collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)
        d_oper['insert_id'] = insert_id

        return d_oper, parsed


class EventMocQueueOperationQueued(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):
        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_OperationQueued')

        tx_hash = parsed['hash']

        # STATUS:
        #
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

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

        d_event = dict()
        d_event["hash"] = tx_hash
        d_event["blockNumber"] = parsed["blockNumber"]
        d_event["operId_"] = parsed_receipt["operId_"]
        d_event["bucket_"] = sanitize_address(parsed["bucket_"]).lower()
        d_event["operType_"] = parsed_receipt["operType_"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        insert_id = collection.find_one_and_update(
            {"hash": d_event["hash"]},
            {"$set": d_event},
            upsert=True)
        d_event['insert_id'] = insert_id

        log.info("Event :: MocQueue_OperationQueued :: operId_: {0}".format(d_event["operId_"]))
        log.info(d_event)

        # write to collection operations as queue operation
        collection = self.connection_helper.mongo_collection('operations')

        operation = None
        d_params = dict()
        if parsed_receipt["operType_"] == 'mintTC':
            operation = 'TCMint'
            raw_params = self.contracts_loaded["MocQueue"].sc.functions.operationsMintTC(d_event["operId_"]).call()
            d_params['qTC'] = raw_params['qTC']
            d_params['qACmax'] = raw_params['qACmax']
            d_params['sender'] = raw_params['sender']
            d_params['recipient'] = raw_params['recipient']
            d_params['vendor'] = raw_params['vendor']
        elif parsed_receipt["operType_"] == 'redeemTC':
            operation = 'TCRedeem'
            raw_params = self.contracts_loaded["MocQueue"].sc.functions.operationsRedeemTC(d_event["operId_"]).call()
            d_params['qTC'] = raw_params['qTC']
            d_params['qACmin'] = raw_params['qACmin']
            d_params['sender'] = raw_params['sender']
            d_params['recipient'] = raw_params['recipient']
            d_params['vendor'] = raw_params['vendor']
        elif parsed_receipt["operType_"] == 'mintTP':
            operation = 'TPMint'
            raw_params = self.contracts_loaded["MocQueue"].sc.functions.operationsMintTP(d_event["operId_"]).call()
            d_params['tp'] = raw_params['tp']
            d_params['qTP'] = raw_params['qTP']
            d_params['qACmax'] = raw_params['qACmax']
            d_params['sender'] = raw_params['sender']
            d_params['recipient'] = raw_params['recipient']
            d_params['vendor'] = raw_params['vendor']
        elif parsed_receipt["operType_"] == 'redeemTP':
            operation = 'TPRedeem'
            raw_params = self.contracts_loaded["MocQueue"].sc.functions.operationsRedeemTP(d_event["operId_"]).call()
            d_params['tp'] = raw_params['tp']
            d_params['qTP'] = raw_params['qTP']
            d_params['qACmin'] = raw_params['qACmin']
            d_params['sender'] = raw_params['sender']
            d_params['recipient'] = raw_params['recipient']
            d_params['vendor'] = raw_params['vendor']
        elif parsed_receipt["operType_"] == 'mintTCandTP':
            operation = 'TCandTPMint'
            raw_params = self.contracts_loaded["MocQueue"].sc.functions.operationsMintTCandTP(d_event["operId_"]).call()
            d_params['tp'] = raw_params['tp']
            d_params['qTP'] = raw_params['qTP']
            d_params['qACmax'] = raw_params['qACmax']
            d_params['sender'] = raw_params['sender']
            d_params['recipient'] = raw_params['recipient']
            d_params['vendor'] = raw_params['vendor']
        elif parsed_receipt["operType_"] == 'redeemTCandTP':
            operation = 'TCandTPRedeem'
            raw_params = self.contracts_loaded["MocQueue"].sc.functions.operationsRedeemTCandTP(d_event["operId_"]).call()
            d_params['tp'] = raw_params['tp']
            d_params['qTC'] = raw_params['qTC']
            d_params['qTP'] = raw_params['qTP']
            d_params['qACmin'] = raw_params['qACmin']
            d_params['sender'] = raw_params['sender']
            d_params['recipient'] = raw_params['recipient']
            d_params['vendor'] = raw_params['vendor']
        elif parsed_receipt["operType_"] == 'swapTCforTP':
            operation = 'TCSwapForTP'
            raw_params = self.contracts_loaded["MocQueue"].sc.functions.operationsSwapTCforTP(d_event["operId_"]).call()
            d_params['tp'] = raw_params['tp']
            d_params['qTC'] = raw_params['qTC']
            d_params['qTPmin'] = raw_params['qTPmin']
            d_params['qACmax'] = raw_params['qACmax']
            d_params['sender'] = raw_params['sender']
            d_params['recipient'] = raw_params['recipient']
            d_params['vendor'] = raw_params['vendor']
        elif parsed_receipt["operType_"] == 'swapTPforTC':
            operation = 'TPSwapForTC'
            raw_params = self.contracts_loaded["MocQueue"].sc.functions.operationsSwapTPforTC(d_event["operId_"]).call()
            d_params['tp'] = raw_params['tp']
            d_params['qTP'] = raw_params['qTP']
            d_params['qTCmin'] = raw_params['qTCmin']
            d_params['qACmax'] = raw_params['qACmax']
            d_params['sender'] = raw_params['sender']
            d_params['recipient'] = raw_params['recipient']
            d_params['vendor'] = raw_params['vendor']
        elif parsed_receipt["operType_"] == 'swapTPforTP':
            operation = 'TPSwapForTP'
            raw_params = self.contracts_loaded["MocQueue"].sc.functions.operationsSwapTPforTP(d_event["operId_"]).call()
            d_params['tpFrom'] = raw_params['tpFrom']
            d_params['tpTo'] = raw_params['tpTo']
            d_params['qTP'] = raw_params['qTP']
            d_params['qTPmin'] = raw_params['qTPmin']
            d_params['qACmax'] = raw_params['qACmax']
            d_params['sender'] = raw_params['sender']
            d_params['recipient'] = raw_params['recipient']
            d_params['vendor'] = raw_params['vendor']

        d_oper = OrderedDict()
        d_oper["blockNumber"] = parsed_receipt["blockNumber"]
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_params['hash'] = tx_hash
        d_params['blockNumber'] = parsed_receipt["blockNumber"]
        d_oper["params"] = d_params
        d_oper["operation"] = operation
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = parsed['gasUsed']
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 0  # Queue

        insert_id = collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)
        d_oper['insert_id'] = insert_id

        return d_oper, parsed


class EventMocQueueOperationExecuted(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection OperationQueueStatus
        collection = self.connection_helper.mongo_collection('event_MocQueue_OperationExecuted')

        tx_hash = parsed['hash']

        # STATUS:
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_event = dict()
        d_event["hash"] = tx_hash
        d_event["blockNumber"] = parsed["blockNumber"]
        d_event["operId_"] = parsed_receipt["operId_"]
        d_event["executor"] = sanitize_address(parsed["executor"]).lower()
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        insert_id = collection.find_one_and_update(
            {"hash": d_event["hash"]},
            {"$set": d_event},
            upsert=True)
        d_event["insert_id"] = insert_id

        log.info("Event :: MocQueue_OperationExecuted :: operId_: {0}".format(d_event["operId_"]))
        log.info(d_event)

        return d_event, parsed


class EventMocQueueTCMinted(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_TCMinted')

        tx_hash = parsed['hash']

        d_event = OrderedDict()
        d_event["blockNumber"] = parsed_receipt["blockNumber"]
        d_event["hash"] = tx_hash
        d_event["sender_"] = sanitize_address(parsed["sender_"])
        d_event["recipient_"] = sanitize_address(parsed["recipient_"])
        d_event["qTC_"] = str(parsed["qTC_"])
        d_event["qAC_"] = str(parsed["qAC_"])
        d_event["qACfee_"] = str(parsed["qACfee_"])
        d_event["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_event["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_event["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_event["vendor_"] = sanitize_address(parsed["vendor_"])
        d_event["operId_"] = str(parsed["operId_"])
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        insert_id = collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_event},
            upsert=True)
        d_event['insert_id'] = insert_id

        # write to collection operations
        collection = self.connection_helper.mongo_collection('operations')

        # STATUS:
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_oper = OrderedDict()
        d_oper["blockNumber"] = parsed_receipt["blockNumber"]
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["executed"] = d_event
        d_oper["operation"] = 'TCMint'
        d_oper["createdAt"] = parsed_receipt['createdAt']
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = parsed['gasUsed']
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1  # Executed

        insert_id = collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)
        d_oper['insert_id'] = insert_id

        log.info("Event MocQueue {0} - Tx Hash: {1}".format(d_oper["event"], tx_hash))

        return d_oper


class EventMocQueueTCRedeemed(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_TCRedeemed')

        tx_hash = parsed['hash']

        d_event = OrderedDict()
        d_event["blockNumber"] = parsed_receipt["blockNumber"]
        d_event["hash"] = tx_hash
        d_event["sender_"] = sanitize_address(parsed["sender_"])
        d_event["recipient_"] = sanitize_address(parsed["recipient_"])
        d_event["qTC_"] = str(parsed["qTC_"])
        d_event["qAC_"] = str(parsed["qAC_"])
        d_event["qACfee_"] = str(parsed["qACfee_"])
        d_event["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_event["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_event["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_event["vendor_"] = sanitize_address(parsed["vendor_"])
        d_event["operId_"] = str(parsed["operId_"])
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        insert_id = collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_event},
            upsert=True)
        d_event['insert_id'] = insert_id

        # write to collection operations
        collection = self.connection_helper.mongo_collection('operations')

        # STATUS:
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_oper = OrderedDict()
        d_oper["blockNumber"] = parsed_receipt["blockNumber"]
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["executed"] = d_event
        d_oper["operation"] = 'TCRedeem'
        d_oper["createdAt"] = parsed_receipt['createdAt']
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = parsed['gasUsed']
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1  # Executed

        insert_id = collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)
        d_oper['insert_id'] = insert_id

        log.info("Event MocQueue {0} - Tx Hash: {1}".format(d_oper["event"], tx_hash))

        return d_oper


class EventMocQueueTPMinted(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_TPMinted')

        tx_hash = parsed['hash']

        d_event = OrderedDict()
        d_event["blockNumber"] = parsed_receipt["blockNumber"]
        d_event["hash"] = tx_hash
        d_event["tp"] = sanitize_address(parsed["tp"])
        d_event["sender_"] = sanitize_address(parsed["sender_"])
        d_event["recipient_"] = sanitize_address(parsed["recipient_"])
        d_event["qTP_"] = str(parsed["qTP_"])
        d_event["qAC_"] = str(parsed["qAC_"])
        d_event["qACfee_"] = str(parsed["qACfee_"])
        d_event["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_event["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_event["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_event["vendor_"] = sanitize_address(parsed["vendor_"])
        d_event["operId_"] = str(parsed["operId_"])
        d_event["lastUpdatedAt"] = datetime.datetime.now()
                        
        insert_id = collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_event},
            upsert=True)
        d_event['insert_id'] = insert_id

        collection = self.connection_helper.mongo_collection('operations')

        # STATUS:
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_oper = OrderedDict()
        d_oper["blockNumber"] = parsed_receipt["blockNumber"]
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["executed"] = d_event
        d_oper["operation"] = 'TPMint'
        d_oper["createdAt"] = parsed_receipt['createdAt']
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = parsed['gasUsed']
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1  # Executed

        insert_id = collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)
        d_oper['insert_id'] = insert_id

        log.info("Event MocQueue {0} - Tx Hash: {1}".format(d_oper["event"], tx_hash))

        return d_oper


class EventMocQueueTPRedeemed(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_TPRedeemed')

        tx_hash = parsed['hash']

        d_event = OrderedDict()
        d_event["blockNumber"] = parsed_receipt["blockNumber"]
        d_event["hash"] = tx_hash
        d_event["tp_"] = sanitize_address(parsed["tp_"])
        d_event["sender_"] = sanitize_address(parsed["sender_"])
        d_event["recipient_"] = sanitize_address(parsed["recipient_"])
        d_event["qTP_"] = str(parsed["qTP_"])
        d_event["qAC_"] = str(parsed["qAC_"])
        d_event["qACfee_"] = str(parsed["qACfee_"])
        d_event["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_event["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_event["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_event["vendor_"] = sanitize_address(parsed["vendor_"])
        d_event["operId_"] = str(parsed["operId_"])
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        insert_id = collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_event},
            upsert=True)
        d_event['insert_id'] = insert_id

        collection = self.connection_helper.mongo_collection('operations')

        # STATUS:
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_oper = OrderedDict()
        d_oper["blockNumber"] = parsed_receipt["blockNumber"]
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["executed"] = d_event
        d_oper["operation"] = 'TPRedeem'
        d_oper["createdAt"] = parsed_receipt['createdAt']
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = parsed['gasUsed']
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1  # Executed

        insert_id = collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)
        d_oper['insert_id'] = insert_id

        log.info("Event MocQueue {0} - Tx Hash: {1}".format(d_oper["event"], tx_hash))

        return d_oper


class EventMocQueueTPSwappedForTP(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_TPSwappedForTP')

        tx_hash = parsed['hash']

        d_event = OrderedDict()
        d_event["blockNumber"] = parsed_receipt["blockNumber"]
        d_event["hash"] = tx_hash
        d_event["tpFrom_"] = sanitize_address(parsed["tpFrom_"])
        d_event["tpTo_"] = sanitize_address(parsed["tpTo_"])
        d_event["sender_"] = sanitize_address(parsed["sender_"])
        d_event["recipient_"] = sanitize_address(parsed["recipient_"])
        d_event["qTPfrom_"] = str(parsed["qTPfrom_"])
        d_event["qTPto_"] = str(parsed["qTPto_"])
        d_event["qACfee_"] = str(parsed["qACfee_"])
        d_event["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_event["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_event["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_event["vendor_"] = sanitize_address(parsed["vendor_"])
        d_event["operId_"] = str(parsed["operId_"])
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        insert_id = collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_event},
            upsert=True)
        d_event['insert_id'] = insert_id

        collection = self.connection_helper.mongo_collection('operations')

        # STATUS:
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_oper = OrderedDict()
        d_oper["blockNumber"] = parsed_receipt["blockNumber"]
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["executed"] = d_event
        d_oper["operation"] = 'TPSwapForTP'
        d_oper["createdAt"] = parsed_receipt['createdAt']
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = parsed['gasUsed']
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1  # Executed

        insert_id = collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)
        d_oper['insert_id'] = insert_id

        log.info("Event MocQueue {0} - Tx Hash: {1}".format(d_oper["event"], tx_hash))

        return d_oper


class EventMocQueueTPSwappedForTC(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_TPSwappedForTC')

        tx_hash = parsed['hash']

        d_event = OrderedDict()
        d_event["blockNumber"] = parsed_receipt["blockNumber"]
        d_event["hash"] = tx_hash
        d_event["tp_"] = sanitize_address(parsed["tp_"])
        d_event["sender_"] = sanitize_address(parsed["sender_"])
        d_event["recipient_"] = sanitize_address(parsed["recipient_"])
        d_event["qTC_"] = str(parsed["qTC_"])
        d_event["qTP_"] = str(parsed["qTP_"])
        d_event["qACfee_"] = str(parsed["qACfee_"])
        d_event["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_event["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_event["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_event["vendor_"] = sanitize_address(parsed["vendor_"])
        d_event["operId_"] = str(parsed["operId_"])
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        insert_id = collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_event},
            upsert=True)
        d_event['insert_id'] = insert_id

        collection = self.connection_helper.mongo_collection('operations')

        # STATUS:
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_oper = OrderedDict()
        d_oper["blockNumber"] = parsed_receipt["blockNumber"]
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["executed"] = d_event
        d_oper["operation"] = 'TPSwapForTC'
        d_oper["createdAt"] = parsed_receipt['createdAt']
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = parsed['gasUsed']
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1  # Executed

        insert_id = collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)
        d_oper['insert_id'] = insert_id

        log.info("Event MocQueue {0} - Tx Hash: {1}".format(d_oper["event"], tx_hash))

        return d_oper


class EventMocQueueTCSwappedForTP(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_TCSwappedForTP')

        tx_hash = parsed['hash']

        d_event = OrderedDict()
        d_event["blockNumber"] = parsed_receipt["blockNumber"]
        d_event["hash"] = tx_hash
        d_event["tp_"] = sanitize_address(parsed["tp_"])
        d_event["sender_"] = sanitize_address(parsed["sender_"])
        d_event["recipient_"] = sanitize_address(parsed["recipient_"])
        d_event["qTC_"] = str(parsed["qTC_"])
        d_event["qTP_"] = str(parsed["qTP_"])
        d_event["qACfee_"] = str(parsed["qACfee_"])
        d_event["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_event["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_event["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_event["vendor_"] = sanitize_address(parsed["vendor_"])
        d_event["operId_"] = str(parsed["operId_"])
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        insert_id = collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_event},
            upsert=True)
        d_event['insert_id'] = insert_id

        collection = self.connection_helper.mongo_collection('operations')

        # STATUS:
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_oper = OrderedDict()
        d_oper["blockNumber"] = parsed_receipt["blockNumber"]
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["executed"] = d_event
        d_oper["operation"] = 'TCSwapForTP'
        d_oper["createdAt"] = parsed_receipt['createdAt']
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = parsed['gasUsed']
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1  # Executed

        insert_id = collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)
        d_oper['insert_id'] = insert_id

        log.info("Event MocQueue {0} - Tx Hash: {1}".format(d_oper["event"], tx_hash))

        return d_oper


class EventMocQueueTCandTPRedeemed(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):
        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_TCandTPRedeemed')

        tx_hash = parsed['hash']

        d_event = OrderedDict()
        d_event["blockNumber"] = parsed_receipt["blockNumber"]
        d_event["hash"] = tx_hash
        d_event["tp_"] = sanitize_address(parsed["tp_"])
        d_event["sender_"] = sanitize_address(parsed["sender_"])
        d_event["recipient_"] = sanitize_address(parsed["recipient_"])
        d_event["qTC_"] = str(parsed["qTC_"])
        d_event["qTP_"] = str(parsed["qTP_"])
        d_event["qAC_"] = str(parsed["qAC_"])
        d_event["qACfee_"] = str(parsed["qACfee_"])
        d_event["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_event["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_event["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_event["vendor_"] = sanitize_address(parsed["vendor_"])
        d_event["operId_"] = str(parsed["operId_"])
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        insert_id = collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_event},
            upsert=True)
        d_event['insert_id'] = insert_id

        collection = self.connection_helper.mongo_collection('operations')

        # STATUS:
        # -3 Stale Transaction
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_oper = OrderedDict()
        d_oper["blockNumber"] = parsed_receipt["blockNumber"]
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["executed"] = d_event
        d_oper["operation"] = 'TCandTPRedeem'
        d_oper["createdAt"] = parsed_receipt['createdAt']
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = parsed['gasUsed']
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1  # Executed

        insert_id = collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)
        d_oper['insert_id'] = insert_id

        log.info("Event MocQueue {0} - Tx Hash: {1}".format(d_oper["event"], tx_hash))

        return d_oper


class EventMocQueueTCandTPMinted(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):
        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_TCandTPMinted')

        tx_hash = parsed['hash']

        d_event = OrderedDict()
        d_event["blockNumber"] = parsed_receipt["blockNumber"]
        d_event["hash"] = tx_hash
        d_event["tp_"] = sanitize_address(parsed["tp_"])
        d_event["sender_"] = sanitize_address(parsed["sender_"])
        d_event["recipient_"] = sanitize_address(parsed["recipient_"])
        d_event["qTC_"] = str(parsed["qTC_"])
        d_event["qTP_"] = str(parsed["qTP_"])
        d_event["qAC_"] = str(parsed["qAC_"])
        d_event["qACfee_"] = str(parsed["qACfee_"])
        d_event["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_event["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_event["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_event["vendor_"] = sanitize_address(parsed["vendor_"])
        d_event["operId_"] = str(parsed["operId_"])
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        insert_id = collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_event},
            upsert=True)
        d_event['insert_id'] = insert_id

        collection = self.connection_helper.mongo_collection('operations')

        # STATUS:
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_oper = OrderedDict()
        d_oper["blockNumber"] = parsed_receipt["blockNumber"]
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["executed"] = d_event
        d_oper["operation"] = 'TCandTPMint'
        d_oper["createdAt"] = parsed_receipt['createdAt']
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = parsed['gasUsed']
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1  # Executed

        insert_id = collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)
        d_oper['insert_id'] = insert_id

        log.info("Event MocQueue {0} - Tx Hash: {1}".format(d_oper["event"], tx_hash))

        return d_oper


class EventTokenTransfer(BaseEvent):

    def __init__(self, options, connection_helper, contracts_loaded, filter_contracts_addresses, block_info, token_involved):

        self.options = options
        self.connection_helper = connection_helper
        self.contracts_loaded = contracts_loaded
        self.filter_contracts_addresses = filter_contracts_addresses
        self.block_info = block_info
        self.token_involved = token_involved

        super().__init__(options, connection_helper, contracts_loaded, filter_contracts_addresses, block_info)

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

        # get collection
        collection = self.connection_helper.mongo_collection('operations')

        tx_hash = parsed['hash']

        d_oper = OrderedDict()
        d_oper["blockNumber"] = parsed_receipt["blockNumber"]
        d_oper["operation"] = 'Transfer'
        d_oper["hash"] = tx_hash
        d_oper["createdAt"] = parsed_receipt['createdAt']
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = parsed['gasUsed']
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parsed_receipt['gasUsed'] * Web3.from_wei(parsed_receipt["gasPrice"], 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1
        d_oper["token"] = self.token_involved
        d_oper["from_"] = sanitize_address(parsed["from"]).lower()
        d_oper["to_"] = sanitize_address(parsed["to"]).lower()
        d_oper["value_"] = str(parsed["value"])

        insert_id = collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_oper},
            upsert=True)
        d_oper['insert_id'] = insert_id

        log.info("Tx {0} - Token: [{1}] From: [{2}] To: [{3}] Value: [{4}] Tx Hash: [{5}]".format(
            'Transfer',
            d_oper["token"],
            d_oper["from_"],
            d_oper["to_"],
            d_oper["value_"],
            tx_hash))

        return d_oper, parsed


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

        post_id = collection_bridge.find_one_and_update(
            {"transferId": d_tx["transferId"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("EVENT::NewBitcoinTransfer::{0}".format(d_tx["transferId"]))
        log.info(d_tx)

        return d_tx, parsed


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

        return d_tx, parsed
