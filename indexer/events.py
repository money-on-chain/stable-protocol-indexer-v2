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


class EventMocCABagTCMinted(BaseEvent):

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

        # only save if recipient is not the wrapper as address
        recipient = sanitize_address(parsed["recipient_"]).lower()
        ca_wrapper = self.options['addresses']['MocCAWrapper'].lower()
        if recipient != ca_wrapper:
            d_tx["address"] = recipient

        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qTC_"] = str(parsed["qTC_"])
        d_tx["qAC_"] = str(parsed["qAC_"])
        d_tx["qACfee_"] = str(parsed["qACfee_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} - Sender: [{1}] Recipient: [{2}] TC: [{3}] qAC: [{4}] qACfee: [{5}] Tx Hash: {6}".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qTC_"],
            d_tx["qAC_"],
            d_tx["qACfee_"],
            tx_hash))

        return parsed


class EventMocCABagTCMintedWithWrapper(BaseEvent):

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
        d_tx["processWrapper"] = True
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

        recipient = sanitize_address(parsed["recipient_"]).lower()
        d_tx["address"] = recipient

        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qAsset_"] = str(parsed["qAsset_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} Wrapper - Sender: [{1}] Recipient: [{2}] qAsset: [{3}] Tx Hash: {4}".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qAsset_"],
            tx_hash))

        return parsed


class EventMocCABagTCRedeemed(BaseEvent):

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

        # only save if recipient is not the wrapper as address
        recipient = sanitize_address(parsed["recipient_"]).lower()
        ca_wrapper = self.options['addresses']['MocCAWrapper'].lower()
        if recipient != ca_wrapper:
            d_tx["address"] = recipient

        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qTC_"] = str(parsed["qTC_"])
        d_tx["qAC_"] = str(parsed["qAC_"])
        d_tx["qACfee_"] = str(parsed["qACfee_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} - Sender: [{1}] Recipient: [{2}]  qTC: [{3}] qAC: [{4}] qACfee: [{5}] Tx Hash: [{6}]".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qTC_"],
            d_tx["qAC_"],
            d_tx["qACfee_"],
            tx_hash))

        return parsed


class EventMocCABagTCRedeemedWithWrapper(BaseEvent):

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
        d_tx["processWrapper"] = True
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

        # only save if recipient is not the wrapper as address
        recipient = sanitize_address(parsed["recipient_"]).lower()
        d_tx["address"] = recipient

        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qAsset_"] = str(parsed["qAsset_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} Wrapper - Sender: [{1}] Recipient: [{2}]  qAsset: [{3}] Tx Hash: [{4}]".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qAsset_"],
            tx_hash))

        return parsed


class EventMocCABagTPMinted(BaseEvent):

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

        # only save if recipient is not the wrapper as address
        recipient = sanitize_address(parsed["recipient_"]).lower()
        ca_wrapper = self.options['addresses']['MocCAWrapper'].lower()
        if recipient != ca_wrapper:
            d_tx["address"] = recipient

        d_tx["i_"] = sanitize_address(parsed["i_"])
        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qTP_"] = str(parsed["qTP_"])
        d_tx["qAC_"] = str(parsed["qAC_"])
        d_tx["qACfee_"] = str(parsed["qACfee_"])
                        
        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} - Sender: [{1}] Recipient: [{2}] qTP: [{3}] qAC: [{4}] qACfee: [{5}] Tx Hash: [{6}]".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qTP_"],
            d_tx["qAC_"],
            d_tx["qACfee_"],
            tx_hash))

        return parsed


class EventMocCABagTPMintedWithWrapper(BaseEvent):

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
        d_tx["processWrapper"] = True
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

        # only save if recipient is not the wrapper as address
        recipient = sanitize_address(parsed["recipient_"]).lower()
        d_tx["address"] = recipient

        d_tx["i_"] = sanitize_address(parsed["i_"])
        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["asset_"] = sanitize_address(parsed["asset_"])
        d_tx["qAsset_"] = str(parsed["qAsset_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} Wrapper - Sender: [{1}] Recipient: [{2}] qAsset_: [{3}] Tx Hash: [{4}]".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qAsset_"],
            tx_hash))

        return parsed


class EventMocCABagTPRedeemed(BaseEvent):

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

        # only save if recipient is not the wrapper as address
        recipient = sanitize_address(parsed["recipient_"]).lower()
        ca_wrapper = self.options['addresses']['MocCAWrapper'].lower()
        if recipient != ca_wrapper:
            d_tx["address"] = recipient

        d_tx["i_"] = sanitize_address(parsed["i_"])
        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qTP_"] = str(parsed["qTP_"])
        d_tx["qAC_"] = str(parsed["qAC_"])
        d_tx["qACfee_"] = str(parsed["qACfee_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} - Sender: [{1}] Recipient: [{2}]  qTP: [{3}] qAC: [{4}] qACfee: [{5}] Tx Hash: [{6}]".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qTP_"],
            d_tx["qAC_"],
            d_tx["qACfee_"],
            tx_hash))

        return parsed


class EventMocCABagTPRedeemedWithWrapper(BaseEvent):

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
        d_tx["processWrapper"] = True
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

        recipient = sanitize_address(parsed["recipient_"]).lower()
        d_tx["address"] = recipient

        d_tx["i_"] = sanitize_address(parsed["i_"])
        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["asset_"] = sanitize_address(parsed["asset_"])
        d_tx["qAsset_"] = str(parsed["qAsset_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} Wrapper - Sender: [{1}] Recipient: [{2}]  qAsset_: [{3}] Tx Hash: [{4}]".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qAsset_"],
            tx_hash))

        return parsed


class EventMocCABagTPSwappedForTP(BaseEvent):

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

        # only save if recipient is not the wrapper as address
        recipient = sanitize_address(parsed["recipient_"]).lower()
        ca_wrapper = self.options['addresses']['MocCAWrapper'].lower()
        if recipient != ca_wrapper:
            d_tx["address"] = recipient

        d_tx["iFrom_"] = sanitize_address(parsed["iFrom_"])
        d_tx["iTo_"] = sanitize_address(parsed["iTo_"])
        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qTPfrom_"] = str(parsed["qTPfrom_"])
        d_tx["qTPto_"] = str(parsed["qTPto_"])
        d_tx["qACfee_"] = str(parsed["qACfee_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} - Sender: [{1}] Recipient: [{2}] qTPfrom_: [{3}] qTPto_: [{4}] qACfee_: [{5}] Tx Hash: [{6}]".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qTPfrom_"],
            d_tx["qTPto_"],
            d_tx["qACfee_"],
            tx_hash))

        return parsed


class EventMocCABagTPSwappedForTC(BaseEvent):

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

        # only save if recipient is not the wrapper as address
        recipient = sanitize_address(parsed["recipient_"]).lower()
        ca_wrapper = self.options['addresses']['MocCAWrapper'].lower()
        if recipient != ca_wrapper:
            d_tx["address"] = recipient

        d_tx["i_"] = sanitize_address(parsed["i_"])
        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qTP_"] = str(parsed["qTP_"])
        d_tx["qTC_"] = str(parsed["qTC_"])
        d_tx["qACfee_"] = str(parsed["qACfee_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} - Sender: [{1}] Recipient: [{2}] qTP: [{3}] qTC: [{4}] qACfee: [{5}] Tx Hash: [{6}]".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qTP_"],
            d_tx["qTC_"],
            d_tx["qACfee_"],
            tx_hash))

        return parsed


class EventMocCABagTCSwappedForTP(BaseEvent):

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

        # only save if recipient is not the wrapper as address
        recipient = sanitize_address(parsed["recipient_"]).lower()
        ca_wrapper = self.options['addresses']['MocCAWrapper'].lower()
        if recipient != ca_wrapper:
            d_tx["address"] = recipient

        d_tx["i_"] = sanitize_address(parsed["i_"])
        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qTC_"] = str(parsed["qTC_"])
        d_tx["qTP_"] = str(parsed["qTP_"])
        d_tx["qACfee_"] = str(parsed["qACfee_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} - Sender: [{1}] Recipient: [{2}] qTC: [{3}] qTP: [{4}] qACfee: [{5}] Tx Hash: [{6}]".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qTC_"],
            d_tx["qTP_"],
            d_tx["qACfee_"],
            tx_hash))

        return parsed


class EventMocCABagTCandTPRedeemed(BaseEvent):

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

        # only save if recipient is not the wrapper as address
        recipient = sanitize_address(parsed["recipient_"]).lower()
        ca_wrapper = self.options['addresses']['MocCAWrapper'].lower()
        if recipient != ca_wrapper:
            d_tx["address"] = recipient

        d_tx["i_"] = sanitize_address(parsed["i_"])
        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qTC_"] = str(parsed["qTC_"])
        d_tx["qTP_"] = str(parsed["qTP_"])
        d_tx["qAC_"] = str(parsed["qAC_"])
        d_tx["qACfee_"] = str(parsed["qACfee_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} - Sender: [{1}] Recipient: [{2}]  qTC: [{3}] qTP: [{4}] qAC: [{5}] qACfee: [{6}] Tx Hash: [{7}]".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qTC_"],
            d_tx["qTP_"],
            d_tx["qAC_"],
            d_tx["qACfee_"],
            tx_hash))

        return parsed


class EventMocCABagTCandTPMinted(BaseEvent):

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

        # only save if recipient is not the wrapper as address
        recipient = sanitize_address(parsed["recipient_"]).lower()
        ca_wrapper = self.options['addresses']['MocCAWrapper'].lower()
        if recipient != ca_wrapper:
            d_tx["address"] = recipient

        d_tx["i_"] = sanitize_address(parsed["i_"])
        d_tx["sender_"] = sanitize_address(parsed["sender_"])
        d_tx["recipient_"] = sanitize_address(parsed["recipient_"])
        d_tx["qTC_"] = str(parsed["qTC_"])
        d_tx["qTP_"] = str(parsed["qTP_"])
        d_tx["qAC_"] = str(parsed["qAC_"])
        d_tx["qACfee_"] = str(parsed["qACfee_"])

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} - Sender: [{1}] Recipient: [{2}] qTC: [{2}] qTP: [{3}] qAC: [{4}] qACfee: [{5}] Tx Hash: [{6}]".format(
            d_tx["event"],
            d_tx["sender_"],
            d_tx["recipient_"],
            d_tx["qTC_"],
            d_tx["qTP_"],
            d_tx["qAC_"],
            d_tx["qACfee_"],
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
