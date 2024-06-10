import datetime
from collections import OrderedDict

from eth_typing import HexStr
from web3 import Web3

from .logger import log


def sanitize_address(address):
    # not allow empty addresses
    if address == "0x0000000000000000000000000000000000000000":
        return None

    return Web3.to_checksum_address(address.replace("0x000000000000000000000000", "0x"))


def oper_id_to_int(oper_id):

    if str(oper_id).startswith("0x"):
        return Web3.to_int(hexstr=HexStr(oper_id))
    else:
        return int(oper_id)


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
        d_event["blockNumber"] = int(parsed["blockNumber"])
        d_event["tp_"] = sanitize_address(parsed["tp_"])
        d_event['tpIndex_'] = self.options["addresses"]["TP"].index(d_event["tp_"])
        d_event["sender_"] = sanitize_address(parsed["sender_"]).lower()
        d_event["recipient_"] = sanitize_address(parsed["recipient_"]).lower()
        d_event["qTP_"] = parsed["qTP_"]
        d_event["qAC_"] = parsed["qAC_"]
        d_event["createdAt"] = parsed["createdAt"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        collection.find_one_and_update(
            {"hash": d_event["hash"]},
            {"$set": d_event},
            upsert=True)

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
        d_event["blockNumber"] = int(parsed["blockNumber"])
        d_event["mocGain_"] = str(parsed["mocGain_"])
        d_event["tpGain_"] = str(parsed["tpGain_"])
        d_event["createdAt"] = parsed["createdAt"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        collection.find_one_and_update(
            {"hash": d_event["hash"]},
            {"$set": d_event},
            upsert=True)

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
        d_event["blockNumber"] = int(parsed["blockNumber"])
        d_event["createdAt"] = parsed["createdAt"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        collection.find_one_and_update(
            {"hash": d_event["hash"]},
            {"$set": d_event},
            upsert=True)

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
        d_event["blockNumber"] = int(parsed["blockNumber"])
        d_event["interestAmount_"] = str(parsed["interestAmount_"])
        d_event["createdAt"] = parsed["createdAt"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        collection.find_one_and_update(
            {"hash": d_event["hash"]},
            {"$set": d_event},
            upsert=True)

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
        d_event["blockNumber"] = int(parsed["blockNumber"])
        d_event["i_"] = oper_id_to_int(parsed["i_"])
        d_event["oldTPema_"] = str(parsed["oldTPema_"])
        d_event["newTPema_"] = str(parsed["newTPema_"])
        d_event["createdAt"] = parsed["createdAt"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        collection.find_one_and_update(
            {"hash": d_event["hash"]},
            {"$set": d_event},
            upsert=True)

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
        # -4 Revert
        # -3 Stale Transaction
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        # Error Code:
        # LOW_COVERAGE: 0x79121201
        # INSUFFICIENT_QAC_SENT: 0x0b63f1a7
        # INSUFFICIENT_TC_TO_REDEEM: 0xa5db715d
        # INSUFFICIENT_TP_TO_MINT: 0xc39b739f
        # INSUFFICIENT_TP_TO_REDEEM: 0x3fe8c5eb
        # INSUFFICIENT_QTP_SENT: 0xf4063b46
        # QAC_NEEDED_MUST_BE_GREATER_ZERO: 0xf3e39b5d
        # QAC_BELOW_MINIMUM: 0x54cde313
        # QTP_BELOW_MINIMUM: 0x9cb8fd64
        # QTC_BELOW_MINIMUM: 0xf577bef5
        # INVALID_FLUX_CAPACITOR_OPERATION: 0x1f69fa6a
        # TRANSFER_FAILED: 0x90b8ec18

        d_event = dict()
        d_event["hash"] = tx_hash
        d_event["blockNumber"] = int(parsed["blockNumber"])
        d_event["operId_"] = oper_id_to_int(parsed["operId_"])
        d_event["errorCode_"] = parsed["errorCode_"]
        d_event["msg_"] = parsed["msg_"]
        d_event["createdAt"] = parsed["createdAt"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        collection.find_one_and_update(
            {"hash": d_event["hash"]},
            {"$set": d_event},
            upsert=True)

        log.info("Event :: OperationError :: operId_: {0}".format(d_event["operId_"]))
        log.info(d_event)

        # change status in collection operations
        collection = self.connection_helper.mongo_collection('operations')

        d_oper = OrderedDict()
        d_oper["blockNumber"] = int(parsed["blockNumber"])
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = oper_id_to_int(d_event["operId_"])
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = int(parsed['gasUsed'])
        gas_fee = parsed['gasUsed'] * Web3.from_wei(int(parsed["gasPrice"]), 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["errorCode_"] = d_event["errorCode_"]
        d_oper["msg_"] = d_event["msg_"]
        d_oper["status"] = -1  # Error
        d_oper["createdAt"] = parsed["createdAt"]
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        d_oper["confirmationTime"] = None
        d_oper["last_block_indexed"] = int(parsed["blockNumber"])

        # Issue: ROC-990: Operation limited by the flux capacitor should stay in the queue and not fail
        # msg: Max flux capacitor operation reached
        # Constant: MAX_FLUX_CAPACITOR_REACHED
        if d_oper["errorCode_"] == "0x0db483ca":
            # skip if is a problem with flux capacitor stay on the queue, so set queue status
            log.warning("Event :: OperationError :: operId_: {0} Skipping... Fluxcapacitor limitation not failing".format(d_event["operId_"]))
            d_oper["status"] = 0

        operation = collection.find_one({"operId_": d_oper["operId_"]})
        if operation:
            if operation['status'] >= 1:
                # if executed don't update
                log.warning("Event :: OperationError :: operId_: {0} Skipping writting to database is already in status 1".format(d_event["operId_"]))
                return d_oper, parsed

        collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)

        return d_oper, parsed


class EventMocQueueUnhandledError(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):
        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_UnhandledError')

        tx_hash = parsed['hash']

        # STATUS:
        # -4 Revert
        # -3 Stale Transaction
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_event = dict()
        d_event["hash"] = tx_hash
        d_event["blockNumber"] = int(parsed["blockNumber"])
        d_event["operId_"] = oper_id_to_int(parsed["operId_"])
        d_event["reason_"] = parsed["reason_"]
        d_event["createdAt"] = parsed["createdAt"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        collection.find_one_and_update(
            {"hash": d_event["hash"]},
            {"$set": d_event},
            upsert=True)

        log.info("Event :: MocQueue_UnhandledError :: operId_: {0}".format(d_event["operId_"]))
        log.info(d_event)

        # change status in collection operations
        collection = self.connection_helper.mongo_collection('operations')

        d_oper = OrderedDict()
        d_oper["blockNumber"] = int(parsed["blockNumber"])
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = oper_id_to_int(d_event["operId_"])
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = int(parsed['gasUsed'])
        gas_fee = parsed['gasUsed'] * Web3.from_wei(int(parsed["gasPrice"]), 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["reason_"] = d_event["reason_"]
        d_oper["status"] = -2  # Error
        d_oper["createdAt"] = parsed["createdAt"]
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        d_oper["confirmationTime"] = None
        d_oper["last_block_indexed"] = int(parsed["blockNumber"])

        operation = collection.find_one({"operId_": d_oper["operId_"]})
        if operation:
            if operation['status'] >= 1:
                # if executed don't update
                log.warning("Event :: MocQueue_UnhandledError :: Skipping writting to database is already in status 1")
                return d_oper, parsed

        collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)

        return d_oper, parsed


class EventMocQueueOperationQueued(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):
        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_OperationQueued')

        tx_hash = parsed['hash']

        # STATUS:
        # -4 Revert
        # -3 Stale Transaction
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
        d_event["blockNumber"] = int(parsed["blockNumber"])
        d_event["operId_"] = oper_id_to_int(parsed["operId_"])
        d_event["bucket_"] = sanitize_address(parsed["bucket_"])
        d_event["operType_"] = int(parsed["operType_"])
        d_event["createdAt"] = parsed["createdAt"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        collection.find_one_and_update(
            {"hash": d_event["hash"]},
            {"$set": d_event},
            upsert=True)

        log.info("Event :: MocQueue_OperationQueued :: operId_: {0}".format(d_event["operId_"]))
        log.info(d_event)

        # write to collection operations as queue operation
        collection = self.connection_helper.mongo_collection('operations')

        operation = None
        d_params = dict()
        if d_event["operType_"] == 1:
            operation = 'TCMint'
            raw_params = self.contracts_loaded["MocQueue"].sc.functions.operationsMintTC(d_event["operId_"]).call()
            d_params['qTC'] = str(raw_params[0])
            d_params['qACmax'] = str(raw_params[1])
            d_params['sender'] = sanitize_address(raw_params[2])
            d_params['recipient'] = sanitize_address(raw_params[3])
            d_params['vendor'] = sanitize_address(raw_params[4])
        elif d_event["operType_"] == 2:
            operation = 'TCRedeem'
            raw_params = self.contracts_loaded["MocQueue"].sc.functions.operationsRedeemTC(d_event["operId_"]).call()
            d_params['qTC'] = str(raw_params[0])
            d_params['qACmin'] = str(raw_params[1])
            d_params['sender'] = sanitize_address(raw_params[2])
            d_params['recipient'] = sanitize_address(raw_params[3])
            d_params['vendor'] = sanitize_address(raw_params[4])
        elif d_event["operType_"] == 3:
            operation = 'TPMint'
            raw_params = self.contracts_loaded["MocQueue"].sc.functions.operationsMintTP(d_event["operId_"]).call()
            d_params['tp'] = sanitize_address(raw_params[0])
            if d_params['tp']:
                d_params['tpIndex'] = self.options["addresses"]["TP"].index(d_params['tp'])
            else:
                # by default the first one
                d_params['tpIndex'] = 0
            d_params['qTP'] = str(raw_params[1])
            d_params['qACmax'] = str(raw_params[2])
            d_params['sender'] = sanitize_address(raw_params[3])
            d_params['recipient'] = sanitize_address(raw_params[4])
            d_params['vendor'] = sanitize_address(raw_params[5])
        elif d_event["operType_"] == 4:
            operation = 'TPRedeem'
            raw_params = self.contracts_loaded["MocQueue"].sc.functions.operationsRedeemTP(d_event["operId_"]).call()
            d_params['tp'] = sanitize_address(raw_params[0])
            if d_params['tp']:
                d_params['tpIndex'] = self.options["addresses"]["TP"].index(d_params['tp'])
            else:
                # by default the first one
                d_params['tpIndex'] = 0
            d_params['qTP'] = str(raw_params[1])
            d_params['qACmin'] = str(raw_params[2])
            d_params['sender'] = sanitize_address(raw_params[3])
            d_params['recipient'] = sanitize_address(raw_params[4])
            d_params['vendor'] = sanitize_address(raw_params[5])
        elif d_event["operType_"] == 5:
            operation = 'TCandTPMint'
            raw_params = self.contracts_loaded["MocQueue"].sc.functions.operationsMintTCandTP(d_event["operId_"]).call()
            d_params['tp'] = sanitize_address(raw_params[0])
            if d_params['tp']:
                d_params['tpIndex'] = self.options["addresses"]["TP"].index(d_params['tp'])
            else:
                # by default the first one
                d_params['tpIndex'] = 0
            d_params['qTP'] = str(raw_params[1])
            d_params['qACmax'] = str(raw_params[2])
            d_params['sender'] = sanitize_address(raw_params[3])
            d_params['recipient'] = sanitize_address(raw_params[4])
            d_params['vendor'] = sanitize_address(raw_params[5])
        elif d_event["operType_"] == 6:
            operation = 'TCandTPRedeem'
            raw_params = self.contracts_loaded["MocQueue"].sc.functions.operationsRedeemTCandTP(d_event["operId_"]).call()
            d_params['tp'] = sanitize_address(raw_params[0])
            if d_params['tp']:
                d_params['tpIndex'] = self.options["addresses"]["TP"].index(d_params['tp'])
            else:
                # by default the first one
                d_params['tpIndex'] = 0
            d_params['qTC'] = str(raw_params[1])
            d_params['qTP'] = str(raw_params[2])
            d_params['qACmin'] = str(raw_params[3])
            d_params['sender'] = sanitize_address(raw_params[4])
            d_params['recipient'] = sanitize_address(raw_params[5])
            d_params['vendor'] = sanitize_address(raw_params[6])
        elif d_event["operType_"] == 7:
            operation = 'TCSwapForTP'
            raw_params = self.contracts_loaded["MocQueue"].sc.functions.operationsSwapTCforTP(d_event["operId_"]).call()
            d_params['tp'] = sanitize_address(raw_params[0])
            if d_params['tp']:
                d_params['tpIndex'] = self.options["addresses"]["TP"].index(d_params['tp'])
            else:
                # by default the first one
                d_params['tpIndex'] = 0
            d_params['qTC'] = str(raw_params[1])
            d_params['qTPmin'] = str(raw_params[2])
            d_params['qACmax'] = str(raw_params[3])
            d_params['sender'] = sanitize_address(raw_params[4])
            d_params['recipient'] = sanitize_address(raw_params[5])
            d_params['vendor'] = sanitize_address(raw_params[6])
        elif d_event["operType_"] == 8:
            operation = 'TPSwapForTC'
            raw_params = self.contracts_loaded["MocQueue"].sc.functions.operationsSwapTPforTC(d_event["operId_"]).call()
            d_params['tp'] = sanitize_address(raw_params[0])
            if d_params['tp']:
                d_params['tpIndex'] = self.options["addresses"]["TP"].index(d_params['tp'])
            else:
                # by default the first one
                d_params['tpIndex'] = 0
            d_params['qTP'] = str(raw_params[1])
            d_params['qTCmin'] = str(raw_params[2])
            d_params['qACmax'] = str(raw_params[3])
            d_params['sender'] = sanitize_address(raw_params[4])
            d_params['recipient'] = sanitize_address(raw_params[5])
            d_params['vendor'] = sanitize_address(raw_params[6])
        elif d_event["operType_"] == 9:
            operation = 'TPSwapForTP'
            raw_params = self.contracts_loaded["MocQueue"].sc.functions.operationsSwapTPforTP(d_event["operId_"]).call()
            d_params['tpFrom'] = sanitize_address(raw_params[0])
            if d_params['tpFrom']:
                d_params['tpFromIndex'] = self.options["addresses"]["TP"].index(d_params['tpFrom'])
            else:
                d_params['tpFromIndex'] = 0
            d_params['tpTo'] = sanitize_address(raw_params[1])
            if d_params['tpTo']:
                d_params['tpToIndex'] = self.options["addresses"]["TP"].index(d_params['tpTo'])
            else:
                d_params['tpToIndex'] = 0
            d_params['qTP'] = str(raw_params[2])
            d_params['qTPmin'] = str(raw_params[3])
            d_params['qACmax'] = str(raw_params[4])
            d_params['sender'] = sanitize_address(raw_params[5])
            d_params['recipient'] = sanitize_address(raw_params[6])
            d_params['vendor'] = sanitize_address(raw_params[7])

        d_oper = OrderedDict()
        d_oper["blockNumber"] = int(parsed["blockNumber"])
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = oper_id_to_int(d_event["operId_"])
        d_params['hash'] = tx_hash
        d_params['blockNumber'] = int(parsed["blockNumber"])
        d_params["createdAt"] = parsed["createdAt"]
        d_params["lastUpdatedAt"] = datetime.datetime.now()
        d_oper["params"] = d_params
        d_oper["operation"] = operation
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = int(parsed['gasUsed'])
        gas_fee = d_oper['gasUsed'] * Web3.from_wei(int(parsed["gasPrice"]), 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 0  # Queue
        d_oper["createdAt"] = parsed["createdAt"]
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        d_oper["confirmationTime"] = None
        d_oper["last_block_indexed"] = int(parsed["blockNumber"])

        operation = collection.find_one({"operId_": d_oper["operId_"]})
        if operation:
            if operation['status'] >= 1:
                # if executed don't update
                log.warning("Event :: MocQueue_OperationQueued :: Skipping writting to database is already in status 1")
                return d_oper, parsed

        collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)

        return d_oper, parsed


class EventMocQueueOperationExecuted(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection OperationQueueStatus
        collection = self.connection_helper.mongo_collection('event_MocQueue_OperationExecuted')

        tx_hash = parsed['hash']

        # STATUS:
        # -4 Revert
        # -3 Stale Transaction
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_event = dict()
        d_event["hash"] = tx_hash
        d_event["blockNumber"] = int(parsed["blockNumber"])
        d_event["operId_"] = oper_id_to_int(parsed["operId_"]) #int(parsed["operId_"].split('0x')[1])
        d_event["executor"] = sanitize_address(parsed["executor"])
        d_event["createdAt"] = parsed["createdAt"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        collection.find_one_and_update(
            {"hash": d_event["hash"]},
            {"$set": d_event},
            upsert=True)

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
        d_event["blockNumber"] = int(parsed["blockNumber"])
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
        d_event["operId_"] = oper_id_to_int(parsed["operId_"])
        d_event["createdAt"] = parsed["createdAt"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_event},
            upsert=True)

        # write to collection operations
        collection = self.connection_helper.mongo_collection('operations')

        # STATUS:
        # -4 Revert
        # -3 Stale Transaction
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_oper = OrderedDict()
        d_oper["blockNumber"] = int(parsed["blockNumber"])
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["executed"] = d_event
        d_oper["operation"] = 'TCMint'
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = int(parsed['gasUsed'])
        gas_fee = parsed["gasUsed"] * Web3.from_wei(int(parsed["gasPrice"]), 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1  # Executed
        d_oper["createdAt"] = parsed["createdAt"]
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        d_oper["confirmationTime"] = None
        d_oper["last_block_indexed"] = int(parsed["blockNumber"])

        collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)

        log.info("Event MocQueue {0} - Tx Hash: {1}".format(d_oper["operation"], tx_hash))

        return d_oper


class EventMocQueueTCRedeemed(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_TCRedeemed')

        tx_hash = parsed['hash']

        d_event = OrderedDict()
        d_event["blockNumber"] = int(parsed["blockNumber"])
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
        d_event["operId_"] = oper_id_to_int(parsed["operId_"])
        d_event["createdAt"] = parsed["createdAt"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_event},
            upsert=True)

        # write to collection operations
        collection = self.connection_helper.mongo_collection('operations')

        # STATUS:
        # -4 Revert
        # -3 Stale Transaction
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_oper = OrderedDict()
        d_oper["blockNumber"] = int(parsed["blockNumber"])
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["executed"] = d_event
        d_oper["operation"] = 'TCRedeem'
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = int(parsed['gasUsed'])
        gas_fee = parsed['gasUsed'] * Web3.from_wei(int(parsed["gasPrice"]), 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1  # Executed
        d_oper["createdAt"] = parsed["createdAt"]
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        d_oper["confirmationTime"] = None
        d_oper["last_block_indexed"] = int(parsed["blockNumber"])

        collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)

        log.info("Event MocQueue {0} - Tx Hash: {1}".format(d_oper["operation"], tx_hash))

        return d_oper


class EventMocQueueTPMinted(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_TPMinted')

        tx_hash = parsed['hash']

        d_event = OrderedDict()
        d_event["blockNumber"] = int(parsed["blockNumber"])
        d_event["hash"] = tx_hash
        d_event["tp"] = sanitize_address(parsed["tp"])
        d_event['tpIndex_'] = self.options["addresses"]["TP"].index(d_event["tp"])
        d_event["sender_"] = sanitize_address(parsed["sender_"])
        d_event["recipient_"] = sanitize_address(parsed["recipient_"])
        d_event["qTP_"] = str(parsed["qTP_"])
        d_event["qAC_"] = str(parsed["qAC_"])
        d_event["qACfee_"] = str(parsed["qACfee_"])
        d_event["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_event["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_event["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_event["vendor_"] = sanitize_address(parsed["vendor_"])
        d_event["operId_"] = oper_id_to_int(parsed["operId_"])
        d_event["createdAt"] = parsed["createdAt"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()
                        
        collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_event},
            upsert=True)

        collection = self.connection_helper.mongo_collection('operations')

        # STATUS:
        # -4 Revert
        # -3 Stale Transaction
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_oper = OrderedDict()
        d_oper["blockNumber"] = int(parsed["blockNumber"])
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["executed"] = d_event
        d_oper["operation"] = 'TPMint'
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = int(parsed['gasUsed'])
        gas_fee = parsed['gasUsed'] * Web3.from_wei(int(parsed["gasPrice"]), 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1  # Executed
        d_oper["createdAt"] = parsed["createdAt"]
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        d_oper["confirmationTime"] = None
        d_oper["last_block_indexed"] = int(parsed["blockNumber"])

        collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)

        log.info("Event MocQueue {0} - Tx Hash: {1}".format(d_oper["operation"], tx_hash))

        return d_oper


class EventMocQueueTPRedeemed(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_TPRedeemed')

        tx_hash = parsed['hash']

        d_event = OrderedDict()
        d_event["blockNumber"] = int(parsed["blockNumber"])
        d_event["hash"] = tx_hash
        d_event["tp_"] = sanitize_address(parsed["tp_"])
        d_event['tpIndex_'] = self.options["addresses"]["TP"].index(d_event["tp_"])
        d_event["sender_"] = sanitize_address(parsed["sender_"])
        d_event["recipient_"] = sanitize_address(parsed["recipient_"])
        d_event["qTP_"] = str(parsed["qTP_"])
        d_event["qAC_"] = str(parsed["qAC_"])
        d_event["qACfee_"] = str(parsed["qACfee_"])
        d_event["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_event["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_event["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_event["vendor_"] = sanitize_address(parsed["vendor_"])
        d_event["operId_"] = oper_id_to_int(parsed["operId_"])
        d_event["createdAt"] = parsed["createdAt"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_event},
            upsert=True)

        collection = self.connection_helper.mongo_collection('operations')

        # STATUS:
        # -4 Revert
        # -3 Stale Transaction
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_oper = OrderedDict()
        d_oper["blockNumber"] = int(parsed["blockNumber"])
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["executed"] = d_event
        d_oper["operation"] = 'TPRedeem'
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = int(parsed['gasUsed'])
        gas_fee = parsed['gasUsed'] * Web3.from_wei(int(parsed["gasPrice"]), 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1  # Executed
        d_oper["createdAt"] = parsed["createdAt"]
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        d_oper["confirmationTime"] = None
        d_oper["last_block_indexed"] = int(parsed["blockNumber"])

        collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)

        log.info("Event MocQueue {0} - Tx Hash: {1}".format(d_oper["operation"], tx_hash))

        return d_oper


class EventMocQueueTPSwappedForTP(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_TPSwappedForTP')

        tx_hash = parsed['hash']

        d_event = OrderedDict()
        d_event["blockNumber"] = int(parsed["blockNumber"])
        d_event["hash"] = tx_hash
        d_event["tpFrom_"] = sanitize_address(parsed["tpFrom_"])
        d_event['tpFromIndex_'] = self.options["addresses"]["TP"].index(d_event["tpFrom_"])
        d_event["tpTo_"] = sanitize_address(parsed["tpTo_"])
        d_event['tpToIndex_'] = self.options["addresses"]["TP"].index(d_event["tpTo_"])
        d_event["sender_"] = sanitize_address(parsed["sender_"])
        d_event["recipient_"] = sanitize_address(parsed["recipient_"])
        d_event["qTPfrom_"] = str(parsed["qTPfrom_"])
        d_event["qTPto_"] = str(parsed["qTPto_"])
        d_event["qACfee_"] = str(parsed["qACfee_"])
        d_event["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_event["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_event["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_event["vendor_"] = sanitize_address(parsed["vendor_"])
        d_event["operId_"] = oper_id_to_int(parsed["operId_"])
        d_event["createdAt"] = parsed["createdAt"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_event},
            upsert=True)

        collection = self.connection_helper.mongo_collection('operations')

        # STATUS:
        # -4 Revert
        # -3 Stale Transaction
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_oper = OrderedDict()
        d_oper["blockNumber"] = int(parsed["blockNumber"])
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["executed"] = d_event
        d_oper["operation"] = 'TPSwapForTP'
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = int(parsed['gasUsed'])
        gas_fee = parsed['gasUsed'] * Web3.from_wei(int(parsed["gasPrice"]), 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1  # Executed
        d_oper["createdAt"] = parsed["createdAt"]
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        d_oper["confirmationTime"] = None
        d_oper["last_block_indexed"] = int(parsed["blockNumber"])

        collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)

        log.info("Event MocQueue {0} - Tx Hash: {1}".format(d_oper["operation"], tx_hash))

        return d_oper


class EventMocQueueTPSwappedForTC(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_TPSwappedForTC')

        tx_hash = parsed['hash']

        d_event = OrderedDict()
        d_event["blockNumber"] = int(parsed["blockNumber"])
        d_event["hash"] = tx_hash
        d_event["tp_"] = sanitize_address(parsed["tp_"])
        d_event['tpIndex_'] = self.options["addresses"]["TP"].index(d_event["tp_"])
        d_event["sender_"] = sanitize_address(parsed["sender_"])
        d_event["recipient_"] = sanitize_address(parsed["recipient_"])
        d_event["qTC_"] = str(parsed["qTC_"])
        d_event["qTP_"] = str(parsed["qTP_"])
        d_event["qACfee_"] = str(parsed["qACfee_"])
        d_event["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_event["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_event["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_event["vendor_"] = sanitize_address(parsed["vendor_"])
        d_event["operId_"] = oper_id_to_int(parsed["operId_"])
        d_event["createdAt"] = parsed["createdAt"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_event},
            upsert=True)

        collection = self.connection_helper.mongo_collection('operations')

        # STATUS:
        # -4 Revert
        # -3 Stale Transaction
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_oper = OrderedDict()
        d_oper["blockNumber"] = int(parsed["blockNumber"])
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["executed"] = d_event
        d_oper["operation"] = 'TPSwapForTC'
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = int(parsed['gasUsed'])
        gas_fee = parsed['gasUsed'] * Web3.from_wei(int(parsed["gasPrice"]), 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1  # Executed
        d_oper["createdAt"] = parsed["createdAt"]
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        d_oper["confirmationTime"] = None
        d_oper["last_block_indexed"] = int(parsed["blockNumber"])

        collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)

        log.info("Event MocQueue {0} - Tx Hash: {1}".format(d_oper["operation"], tx_hash))

        return d_oper


class EventMocQueueTCSwappedForTP(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):

        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_TCSwappedForTP')

        tx_hash = parsed['hash']

        d_event = OrderedDict()
        d_event["blockNumber"] = int(parsed["blockNumber"])
        d_event["hash"] = tx_hash
        d_event["tp_"] = sanitize_address(parsed["tp_"])
        d_event['tpIndex_'] = self.options["addresses"]["TP"].index(d_event["tp_"])
        d_event["sender_"] = sanitize_address(parsed["sender_"])
        d_event["recipient_"] = sanitize_address(parsed["recipient_"])
        d_event["qTC_"] = str(parsed["qTC_"])
        d_event["qTP_"] = str(parsed["qTP_"])
        d_event["qACfee_"] = str(parsed["qACfee_"])
        d_event["qFeeToken_"] = str(parsed["qFeeToken_"])
        d_event["qACVendorMarkup_"] = str(parsed["qACVendorMarkup_"])
        d_event["qFeeTokenVendorMarkup_"] = str(parsed["qFeeTokenVendorMarkup_"])
        d_event["vendor_"] = sanitize_address(parsed["vendor_"])
        d_event["operId_"] = oper_id_to_int(parsed["operId_"])
        d_event["createdAt"] = parsed["createdAt"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_event},
            upsert=True)

        collection = self.connection_helper.mongo_collection('operations')

        # STATUS:
        # -4 Revert
        # -3 Stale Transaction
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_oper = OrderedDict()
        d_oper["blockNumber"] = int(parsed["blockNumber"])
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["executed"] = d_event
        d_oper["operation"] = 'TCSwapForTP'
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = int(parsed['gasUsed'])
        gas_fee = parsed['gasUsed'] * Web3.from_wei(int(parsed["gasPrice"]), 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1  # Executed
        d_oper["createdAt"] = parsed["createdAt"]
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        d_oper["confirmationTime"] = None
        d_oper["last_block_indexed"] = int(parsed["blockNumber"])

        collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)

        log.info("Event MocQueue {0} - Tx Hash: {1}".format(d_oper["operation"], tx_hash))

        return d_oper


class EventMocQueueTCandTPRedeemed(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):
        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_TCandTPRedeemed')

        tx_hash = parsed['hash']

        d_event = OrderedDict()
        d_event["blockNumber"] = int(parsed["blockNumber"])
        d_event["hash"] = tx_hash
        d_event["tp_"] = sanitize_address(parsed["tp_"])
        d_event['tpIndex_'] = self.options["addresses"]["TP"].index(d_event["tp_"])
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
        d_event["operId_"] = oper_id_to_int(parsed["operId_"])
        d_event["createdAt"] = parsed["createdAt"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_event},
            upsert=True)

        collection = self.connection_helper.mongo_collection('operations')

        # STATUS:
        # -4 Revert
        # -3 Stale Transaction
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_oper = OrderedDict()
        d_oper["blockNumber"] = int(parsed["blockNumber"])
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["executed"] = d_event
        d_oper["operation"] = 'TCandTPRedeem'
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = int(parsed['gasUsed'])
        gas_fee = parsed['gasUsed'] * Web3.from_wei(int(parsed["gasPrice"]), 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1  # Executed
        d_oper["createdAt"] = parsed["createdAt"]
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        d_oper["confirmationTime"] = None
        d_oper["last_block_indexed"] = int(parsed["blockNumber"])

        collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)

        log.info("Event MocQueue {0} - Tx Hash: {1}".format(d_oper["operation"], tx_hash))

        return d_oper


class EventMocQueueTCandTPMinted(BaseEvent):

    def parse_event_and_save(self, parsed_receipt, decoded_event):
        parsed = self.parse_event(parsed_receipt, decoded_event)

        # get collection
        collection = self.connection_helper.mongo_collection('event_MocQueue_TCandTPMinted')

        tx_hash = parsed['hash']

        d_event = OrderedDict()
        d_event["blockNumber"] = int(parsed["blockNumber"])
        d_event["hash"] = tx_hash
        d_event["tp_"] = sanitize_address(parsed["tp_"])
        d_event['tpIndex_'] = self.options["addresses"]["TP"].index(d_event["tp_"])
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
        d_event["operId_"] = oper_id_to_int(parsed["operId_"])
        d_event["createdAt"] = parsed["createdAt"]
        d_event["lastUpdatedAt"] = datetime.datetime.now()

        collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_event},
            upsert=True)

        collection = self.connection_helper.mongo_collection('operations')

        # STATUS:
        # -4 Revert
        # -3 Stale Transaction
        # -2 Error Unhandled
        # -1 Error
        #  0 Queue
        #  1 Executed
        #  2 Confirmed > 10 blocks

        d_oper = OrderedDict()
        d_oper["blockNumber"] = int(parsed["blockNumber"])
        d_oper["hash"] = tx_hash
        d_oper["operId_"] = d_event["operId_"]
        d_oper["executed"] = d_event
        d_oper["operation"] = 'TCandTPMint'
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = int(parsed['gasUsed'])
        gas_fee = parsed['gasUsed'] * Web3.from_wei(int(parsed["gasPrice"]), 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1  # Executed
        d_oper["createdAt"] = parsed["createdAt"]
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        d_oper["confirmationTime"] = None
        d_oper["last_block_indexed"] = int(parsed["blockNumber"])

        collection.find_one_and_update(
            {"operId_": d_oper["operId_"]},
            {"$set": d_oper},
            upsert=True)

        log.info("Event MocQueue {0} - Tx Hash: {1}".format(d_oper["operation"], tx_hash))

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
        address_not_allowed = [str.lower(address_from_contract)] + self.filter_contracts_addresses

        if sanitize_address(parsed['from']).lower() in address_not_allowed or \
                sanitize_address(parsed['to']).lower() in address_not_allowed:
            # skip transfers to our contracts
            return parsed

        # get collection
        collection = self.connection_helper.mongo_collection('operations')

        tx_hash = parsed['hash']

        d_oper = OrderedDict()
        d_oper["blockNumber"] = int(parsed["blockNumber"])
        d_oper["operation"] = 'Transfer'
        d_oper["hash"] = tx_hash
        d_oper["gas"] = parsed['gas']
        d_oper["gasPrice"] = str(parsed['gasPrice'])
        d_oper["gasUsed"] = int(parsed['gasUsed'])
        gas_fee = parsed['gasUsed'] * Web3.from_wei(int(parsed['gasPrice']), 'ether')
        d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_oper["status"] = 1
        d_params = dict()
        d_params['hash'] = tx_hash
        d_params['blockNumber'] = int(parsed["blockNumber"])
        d_params["createdAt"] = parsed["createdAt"]
        d_params["lastUpdatedAt"] = datetime.datetime.now()
        d_params["token"] = self.token_involved
        d_params["sender"] = sanitize_address(parsed["from"])
        d_params["recipient"] = sanitize_address(parsed["to"])
        d_params["amount"] = str(parsed["value"])
        d_oper["params"] = d_params
        d_oper["createdAt"] = parsed["createdAt"]
        d_oper["lastUpdatedAt"] = datetime.datetime.now()
        d_oper["confirmationTime"] = None
        d_oper["last_block_indexed"] = int(parsed["blockNumber"])

        collection.find_one_and_update(
            {"hash": tx_hash},
            {"$set": d_oper},
            upsert=True)

        log.info("Tx {0} - Token: [{1}] From: [{2}] To: [{3}] Value: [{4}] Tx Hash: [{5}]".format(
            'Transfer',
            d_params["token"],
            d_params["sender"],
            d_params["recipient"],
            d_params["amount"],
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

        collection_bridge.find_one_and_update(
            {"transferId": d_tx["transferId"]},
            {"$set": d_tx},
            upsert=True)

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

        collection_bridge.find_one_and_update(
            {"transferId": d_tx["transferId"]},
            {"$set": d_tx},
            upsert=False)

        log.info("EVENT::BitcoinTransferStatusUpdated::{0}".format(d_tx["transferId"]))
        log.info(d_tx)

        return d_tx, parsed
