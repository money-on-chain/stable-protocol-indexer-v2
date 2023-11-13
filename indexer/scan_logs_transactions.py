import time
import datetime
from collections import OrderedDict

from .logger import log
from .events import EventMocTCMinted, \
    EventMocTCRedeemed, \
    EventMocTPMinted, \
    EventMocTPRedeemed, \
    EventMocTPSwappedForTP, \
    EventMocTPSwappedForTC, \
    EventMocTCSwappedForTP, \
    EventMocTCandTPRedeemed, \
    EventMocTCandTPMinted, \
    EventTokenTransfer, \
    EventFastBtcBridgeNewBitcoinTransfer, \
    EventFastBtcBridgeBitcoinTransferStatusUpdated, \
    EventMocTCMintedWithWrapper, \
    EventMocTCRedeemedWithWrapper, \
    EventMocTPMintedWithWrapper, \
    EventMocTPRedeemedWithWrapper


from .base.decoder import LogDecoder, UnknownEvent


class ScanLogsTransactions:

    def __init__(self,
                 options,
                 connection_helper,
                 contracts_loaded,
                 contracts_addresses,
                 filter_contracts_addresses):
        self.options = options
        self.connection_helper = connection_helper
        self.contracts_loaded = contracts_loaded
        self.contracts_addresses = contracts_addresses
        self.filter_contracts_addresses = filter_contracts_addresses
        self.confirm_blocks = self.options['scan_logs']['confirm_blocks']

        # init log decoder
        self.contracts_log_decoder = self.init_log_decoder()

        # update block info
        self.last_block = connection_helper.connection_manager.block_number
        self.block_ts = connection_helper.connection_manager.block_timestamp(self.last_block)
        self.block_info = dict(
            last_block=self.last_block,
            block_ts=self.block_ts,
            confirm_blocks=self.confirm_blocks
        )

        self.map_events_contracts = self.map_events()

    def init_log_decoder(self):

        contracts_log_decoder = dict()
        contracts_log_decoder[self.contracts_addresses['Moc'].lower()] = LogDecoder(
            self.contracts_loaded['Moc'].sc
        )

        # Only load with collateral == bag
        if self.options['collateral'] == "bag":
            contracts_log_decoder[self.contracts_addresses['MocWrapper'].lower()] = LogDecoder(
                self.contracts_loaded['MocWrapper'].sc
            )

        contracts_log_decoder[self.contracts_addresses['TC'].lower()] = LogDecoder(
            self.contracts_loaded['TC'].sc
        )

        i = 0
        for t_pegged in self.options['addresses']['TP']:
            contracts_log_decoder[t_pegged.lower()] = LogDecoder(
                self.contracts_loaded['TP'][i].sc
            )
            i += 1

        i = 0
        for c_asset in self.options['addresses']['CA']:
            contracts_log_decoder[c_asset.lower()] = LogDecoder(
                self.contracts_loaded['CA'][i].sc
            )
            i += 1

        if 'TG' in self.options['addresses']:
            contracts_log_decoder[self.contracts_addresses['TG'].lower()] = LogDecoder(
                self.contracts_loaded['TG'].sc
            )

        contracts_log_decoder[self.options['addresses']['FastBtcBridge'].lower()] = LogDecoder(
            self.contracts_loaded['FastBtcBridge'].sc
        )

        return contracts_log_decoder

    def update_info_last_block(self):

        collection_moc_indexer = self.connection_helper.mongo_collection('moc_indexer')
        indexer = collection_moc_indexer.find_one(sort=[("updatedAt", -1)])
        if indexer:
            if 'last_block_number' in indexer:
                self.last_block = indexer['last_block_number']
                self.block_ts = indexer['last_block_ts']
                self.block_info = dict(
                    last_block=self.last_block,
                    block_ts=self.block_ts,
                    confirm_blocks=self.confirm_blocks
                )

    def map_events(self):

        d_event = dict()
        d_event[self.contracts_addresses["Moc"].lower()] = {
            "TCMinted": EventMocTCMinted(
                self.options,
                self.connection_helper,
                self.filter_contracts_addresses,
                self.block_info),
            "TCRedeemed": EventMocTCRedeemed(
                self.options,
                self.connection_helper,
                self.filter_contracts_addresses,
                self.block_info),
            "TPMinted": EventMocTPMinted(
                self.options,
                self.connection_helper,
                self.filter_contracts_addresses,
                self.block_info),
            "TPRedeemed": EventMocTPRedeemed(
                self.options,
                self.connection_helper,
                self.filter_contracts_addresses,
                self.block_info),
            "TPSwappedForTP": EventMocTPSwappedForTP(
                self.options,
                self.connection_helper,
                self.filter_contracts_addresses,
                self.block_info),
            "TPSwappedForTC": EventMocTPSwappedForTC(
                self.options,
                self.connection_helper,
                self.filter_contracts_addresses,
                self.block_info),
            "TCSwappedForTP": EventMocTCSwappedForTP(
                self.options,
                self.connection_helper,
                self.filter_contracts_addresses,
                self.block_info),
            "TCandTPRedeemed": EventMocTCandTPRedeemed(
                self.options,
                self.connection_helper,
                self.filter_contracts_addresses,
                self.block_info),
            "TCandTPMinted": EventMocTCandTPMinted(
                self.options,
                self.connection_helper,
                self.filter_contracts_addresses,
                self.block_info),
        }

        # Only load with collateral == bag
        if self.options['collateral'] == "bag":
            d_event[self.contracts_addresses["MocWrapper"].lower()] = {
                "TCMintedWithWrapper": EventMocTCMintedWithWrapper(
                    self.options,
                    self.connection_helper,
                    self.filter_contracts_addresses,
                    self.block_info),
                "TCRedeemedWithWrapper": EventMocTCRedeemedWithWrapper(
                    self.options,
                    self.connection_helper,
                    self.filter_contracts_addresses,
                    self.block_info),
                "TPMintedWithWrapper": EventMocTPMintedWithWrapper(
                    self.options,
                    self.connection_helper,
                    self.filter_contracts_addresses,
                    self.block_info),
                "TPRedeemedWithWrapper": EventMocTPRedeemedWithWrapper(
                    self.options,
                    self.connection_helper,
                    self.filter_contracts_addresses,
                    self.block_info)
            }

        d_event[self.contracts_addresses["TC"].lower()] = {
            "Transfer": EventTokenTransfer(
                self.options,
                self.connection_helper,
                self.filter_contracts_addresses,
                self.block_info,
                'TC')
        }

        i = 0
        for t_pegged in self.options['addresses']['TP']:
            d_event[t_pegged.lower()] = {
                "Transfer": EventTokenTransfer(
                    self.options,
                    self.connection_helper,
                    self.filter_contracts_addresses,
                    self.block_info,
                    'TP_{0}'.format(i))
            }
            i += 1

        i = 0
        for c_asset in self.options['addresses']['CA']:
            d_event[c_asset.lower()] = {
                "Transfer": EventTokenTransfer(
                    self.options,
                    self.connection_helper,
                    self.filter_contracts_addresses,
                    self.block_info,
                    'CA_{0}'.format(i))
            }
            i += 1

        if 'TG' in self.options['addresses']:
            d_event[self.contracts_addresses["TG"].lower()] = {
                "Transfer": EventTokenTransfer(
                    self.options,
                    self.connection_helper,
                    self.filter_contracts_addresses,
                    self.block_info,
                    'TG')
            }

        d_event[self.options['addresses']['FastBtcBridge'].lower()] = {
            "NewBitcoinTransfer": EventFastBtcBridgeNewBitcoinTransfer(
                self.options,
                self.connection_helper,
                self.filter_contracts_addresses,
                self.block_info),
            "BitcoinTransferStatusUpdated": EventFastBtcBridgeBitcoinTransferStatusUpdated(
                self.options,
                self.connection_helper,
                self.filter_contracts_addresses,
                self.block_info)
        }

        return d_event

    def on_init(self):
        pass

    def parse_tx_receipt(self, tx_receipt, event_name, log_index=1):

        parse_info = dict()
        parse_info['blockNumber'] = tx_receipt['blockNumber']
        parse_info['hash'] = tx_receipt['hash']
        parse_info['gas'] = tx_receipt['gas']
        parse_info['gasPrice'] = int(tx_receipt['gasPrice'])
        parse_info['gasUsed'] = tx_receipt['gasUsed']
        parse_info['timestamp'] = tx_receipt['timestamp']
        parse_info['createdAt'] = tx_receipt['createdAt']
        parse_info['eventName'] = event_name
        parse_info['logIndex'] = log_index

        return parse_info

    def process_logs(self, raw_tx):

        if raw_tx["status"] == 0:
            # reverted by EVM

            collection_tx = self.connection_helper.mongo_collection('Transaction')

            d_tx = OrderedDict()
            d_tx["hash"] = raw_tx["hash"]
            d_tx["blockNumber"] = raw_tx["blockNumber"]
            d_tx["address"] = raw_tx["from"]
            d_tx["event"] = 'ERROR'
            d_tx["gas"] = raw_tx["gas"]
            d_tx["gasPrice"] = str(raw_tx["gasPrice"])
            d_tx["confirmations"] = self.connection_helper.connection_manager.block_number - raw_tx['blockNumber']
            d_tx["timestamp"] = raw_tx["timestamp"]
            d_tx["createdAt"] = raw_tx["createdAt"]
            d_tx["lastUpdatedAt"] = datetime.datetime.now()

            post_id = collection_tx.find_one_and_update(
                {"transactionHash": d_tx['hash'],
                 "event": d_tx["event"]},
                {"$set": d_tx},
                upsert=True)
            d_tx['post_id'] = post_id

            log.info("Tx (REVERT) {0} From: [{1}] Tx Hash: {2}".format(
                d_tx["event"],
                d_tx["address"],
                raw_tx['hash'],))

            return

        if raw_tx["logs"]:
            for tx_log in raw_tx["logs"]:
                log_address = str.lower(tx_log['address'])
                if log_address in self.contracts_log_decoder:
                    try:
                        decoded_event = self.contracts_log_decoder[log_address].decode_log(tx_log)
                    except UnknownEvent:
                        log.error("Skipping. Not known event in ABI. Contract address: {0} Info: {1}".format(
                            log_address, tx_log))
                        continue
                    if decoded_event['name'] in self.map_events_contracts[log_address]:
                        log_index = tx_log['logIndex']
                        parsed_receipt = self.parse_tx_receipt(raw_tx, decoded_event['name'], log_index=log_index)
                        parsed_event = self.map_events_contracts[log_address][decoded_event['name']]\
                            .parse_event_and_save(
                                parsed_receipt,
                                decoded_event['data']
                            )
                    else:
                        log.warning("Event name not recognized. Event: {0}".format(decoded_event['name']))

    def scan_events_txs(self, task=None):

        start_time = time.time()

        # update block information
        self.update_info_last_block()

        collection_raw_transactions = self.connection_helper.mongo_collection('raw_transactions')
        raw_txs = collection_raw_transactions.find({"processed": False}, sort=[("blockNumber", 1)])

        count = 0
        if raw_txs:
            for raw_tx in raw_txs:
                # update block information
                self.update_info_last_block()

                count += 1
                self.process_logs(raw_tx)

                collection_raw_transactions.find_one_and_update(
                    {"hash": raw_tx["hash"], "blockNumber": raw_tx["blockNumber"]},
                    {"$set": {"processed": True}},
                    upsert=False)

        duration = time.time() - start_time
        log.info("[2. Scan Events Txs] Processed: [{0}] Done! [{1} seconds]".format(count, duration))

    def scan_events_not_processed_txs(self, task=None):
        """ Trying to reindex when there is a problem with events"""

        start_time = time.time()

        collection_transactions = self.connection_helper.mongo_collection('Transaction')

        collection_raw_transactions = self.connection_helper.mongo_collection('raw_transactions')

        collection_moc_indexer = self.connection_helper.mongo_collection('moc_indexer')
        moc_index = collection_moc_indexer.find_one(sort=[("updatedAt", -1)])

        # we need to query tx with processLogs=None and in the last 24hs
        only_last_tx = datetime.datetime.now() - datetime.timedelta(minutes=1440)
        txs = collection_transactions.find({
            "processLogs": None,
            "createdAt": {"$gte": only_last_tx}}, sort=[("createdAt", 1)])

        count = 0
        if txs:
            for tx in txs:
                # no process when no status
                if 'status' not in tx:
                    continue

                # only status confirmed and confirming
                if tx["status"] not in ["confirmed", "confirming"]:
                    continue

                raw_tx = collection_raw_transactions.find_one({"hash": tx["transactionHash"]})

                if not raw_tx:
                    log.info("[8. Scan Blocks not processed] Not exist RAW Tx for hash: {0}".format(tx["transactionHash"]))
                    continue

                dt_older_than = moc_index["last_block_ts"] - datetime.timedelta(minutes=5)
                if tx["createdAt"] >= dt_older_than:
                    continue

                log.info("[8. Scan Blocks not processed] Reindexing with hash: {0}".format(tx["transactionHash"]))

                # update block information
                self.update_info_last_block()

                count += 1
                self.process_logs(raw_tx)

        duration = time.time() - start_time
        log.info("[8. Scan Blocks not processed] Done! Processed: [{0}] [{1} seconds]".format(count, duration))

    def on_task(self, task=None):
        self.scan_events_txs(task=task)

    def on_task_not_processed(self, task=None):
        self.scan_events_not_processed_txs(task=task)

