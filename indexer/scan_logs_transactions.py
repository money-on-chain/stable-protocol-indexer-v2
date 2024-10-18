import time
import datetime
from collections import OrderedDict
from web3 import Web3

from .logger import log
from .events import EventMocQueueTCMinted, \
    EventMocQueueTCRedeemed, \
    EventMocQueueTPMinted, \
    EventMocQueueTPRedeemed, \
    EventMocQueueTPSwappedForTP, \
    EventMocQueueTPSwappedForTC, \
    EventMocQueueTCSwappedForTP, \
    EventMocQueueTCandTPRedeemed, \
    EventMocQueueTCandTPMinted, \
    EventTokenTransfer, \
    EventFastBtcBridgeNewBitcoinTransfer, \
    EventFastBtcBridgeBitcoinTransferStatusUpdated, \
    EventMocQueueOperationError, \
    EventMocQueueUnhandledError, \
    EventMocQueueOperationQueued, \
    EventMocQueueOperationExecuted, \
    EventMocLiqTPRedeemed, \
    EventMocSuccessFeeDistributed, \
    EventMocSettlementExecuted, \
    EventMocTCInterestPayment, \
    EventMocTPemaUpdated, \
    EventOMOCIncentiveV2ClaimOK, \
    EventOMOCVestingFactoryVestingCreated, \
    EventOMOCDelayMachinePaymentCancel, \
    EventOMOCDelayMachinePaymentDeposit, \
    EventOMOCDelayMachinePaymentWithdraw, \
    EventOMOCSupportersAddStake, \
    EventOMOCSupportersCancelEarnings, \
    EventOMOCSupportersPayEarnings, \
    EventOMOCSupportersWithdraw, \
    EventOMOCSupportersWithdrawStake, \
    EventOMOCVotingMachineVoteEvent


from .base.decoder import LogDecoder, UnknownEvent


class ScanLogsTransactions:
    precision = 10 ** 18

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

        contracts_log_decoder[self.contracts_addresses['MocQueue'].lower()] = LogDecoder(
            self.contracts_loaded['MocQueue'].sc
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

        if 'FeeToken' in self.options['addresses']:
            contracts_log_decoder[self.contracts_addresses['FeeToken'].lower()] = LogDecoder(
                self.contracts_loaded['FeeToken'].sc
            )

        contracts_log_decoder[self.options['addresses']['FastBtcBridge'].lower()] = LogDecoder(
            self.contracts_loaded['FastBtcBridge'].sc
        )

        if 'IncentiveV2' in self.contracts_loaded:
            contracts_log_decoder[self.contracts_addresses['IncentiveV2'].lower()] = LogDecoder(
                self.contracts_loaded['IncentiveV2'].sc
            )

        contracts_log_decoder[self.contracts_addresses['VestingFactory'].lower()] = LogDecoder(
            self.contracts_loaded['VestingFactory'].sc
        )

        contracts_log_decoder[self.contracts_addresses['DelayMachine'].lower()] = LogDecoder(
            self.contracts_loaded['DelayMachine'].sc
        )

        contracts_log_decoder[self.contracts_addresses['Supporters'].lower()] = LogDecoder(
            self.contracts_loaded['Supporters'].sc
        )

        contracts_log_decoder[self.contracts_addresses['VotingMachine'].lower()] = LogDecoder(
            self.contracts_loaded['VotingMachine'].sc
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
            "LiqTPRedeemed": EventMocLiqTPRedeemed(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "SuccessFeeDistributed": EventMocSuccessFeeDistributed(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "SettlementExecuted": EventMocSettlementExecuted(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "TCInterestPayment": EventMocTCInterestPayment(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "TPemaUpdated": EventMocTPemaUpdated(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info)
        }

        d_event[self.contracts_addresses["MocQueue"].lower()] = {
            "OperationError": EventMocQueueOperationError(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "UnhandledError": EventMocQueueUnhandledError(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "OperationQueued": EventMocQueueOperationQueued(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "OperationExecuted": EventMocQueueOperationExecuted(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "TCMinted": EventMocQueueTCMinted(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "TCRedeemed": EventMocQueueTCRedeemed(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "TPMinted": EventMocQueueTPMinted(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "TPRedeemed": EventMocQueueTPRedeemed(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "TPSwappedForTP": EventMocQueueTPSwappedForTP(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "TPSwappedForTC": EventMocQueueTPSwappedForTC(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "TCSwappedForTP": EventMocQueueTCSwappedForTP(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "TCandTPRedeemed": EventMocQueueTCandTPRedeemed(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "TCandTPMinted": EventMocQueueTCandTPMinted(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
        }

        d_event[self.contracts_addresses["TC"].lower()] = {
            "Transfer": EventTokenTransfer(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
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
                    self.contracts_loaded,
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
                    self.contracts_loaded,
                    self.filter_contracts_addresses,
                    self.block_info,
                    'CA_{0}'.format(i))
            }
            i += 1

        if 'FeeToken' in self.options['addresses']:
            d_event[self.contracts_addresses["FeeToken"].lower()] = {
                "Transfer": EventTokenTransfer(
                    self.options,
                    self.connection_helper,
                    self.contracts_loaded,
                    self.filter_contracts_addresses,
                    self.block_info,
                    'FeeToken')
            }

        d_event[self.options['addresses']['FastBtcBridge'].lower()] = {
            "NewBitcoinTransfer": EventFastBtcBridgeNewBitcoinTransfer(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "BitcoinTransferStatusUpdated": EventFastBtcBridgeBitcoinTransferStatusUpdated(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info)
        }

        if 'IncentiveV2' in self.contracts_loaded:
            d_event[self.contracts_addresses['IncentiveV2'].lower()] = {
                "ClaimOK": EventOMOCIncentiveV2ClaimOK(
                    self.options,
                    self.connection_helper,
                    self.contracts_loaded,
                    self.filter_contracts_addresses,
                    self.block_info)
            }

        d_event[self.contracts_addresses['VestingFactory'].lower()] = {
            "VestingCreated": EventOMOCVestingFactoryVestingCreated(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info)
        }

        d_event[self.contracts_addresses['DelayMachine'].lower()] = {
            "PaymentCancel": EventOMOCDelayMachinePaymentCancel(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "PaymentDeposit": EventOMOCDelayMachinePaymentDeposit(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "PaymentWithdraw": EventOMOCDelayMachinePaymentWithdraw(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info)
        }

        d_event[self.contracts_addresses['Supporters'].lower()] = {
            "AddStake": EventOMOCSupportersAddStake(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "CancelEarnings": EventOMOCSupportersCancelEarnings(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "PayEarnings": EventOMOCSupportersPayEarnings(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "Withdraw": EventOMOCSupportersWithdraw(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info),
            "WithdrawStake": EventOMOCSupportersWithdrawStake(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
                self.filter_contracts_addresses,
                self.block_info)
        }

        d_event[self.contracts_addresses['VotingMachine'].lower()] = {
            "VoteEvent": EventOMOCVotingMachineVoteEvent(
                self.options,
                self.connection_helper,
                self.contracts_loaded,
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

            collection_tx = self.connection_helper.mongo_collection('operations')

            d_oper = OrderedDict()
            d_oper["blockNumber"] = raw_tx["blockNumber"]
            d_oper["hash"] = raw_tx["hash"]
            d_oper["operId_"] = None
            d_params = dict()
            d_params['hash'] = raw_tx["hash"]
            d_params['blockNumber'] = int(raw_tx["blockNumber"])
            d_params["createdAt"] = raw_tx["createdAt"]
            d_params["lastUpdatedAt"] = datetime.datetime.now()
            d_params['sender'] = raw_tx["from"]
            d_params["recipient"] = raw_tx["from"]
            d_oper["params"] = d_params
            d_oper["operation"] = 'ERROR'
            d_oper["gas"] = raw_tx["gas"]
            d_oper["gasPrice"] = str(raw_tx["gasPrice"])
            d_oper["gasUsed"] = int(raw_tx['gasUsed'])
            gas_fee = d_oper['gasUsed'] * Web3.from_wei(int(raw_tx["gasPrice"]), 'ether')
            d_oper["gasFeeRBTC"] = str(int(gas_fee * self.precision))
            d_oper["status"] = -4  # Revert
            d_oper["createdAt"] = raw_tx["createdAt"]
            d_oper["lastUpdatedAt"] = datetime.datetime.now()
            d_oper['from'] = raw_tx["from"]
            d_oper["to"] = raw_tx["to"]

            try:
                d_oper["contract"] = list(self.contracts_addresses.keys())[list(self.contracts_addresses.values()).index(d_oper["to"].lower())]
            except KeyError:
                d_oper["contract"] = ''

            if d_oper["contract"] not in ['Moc', 'MocQueue', 'TC', 'TP', 'CA', 'FeeToken']:
                log.info("Tx (REVERT) contract is not from Stable Protocol. Tx Hash: {0}".format(raw_tx['hash']))
                return

            collection_tx.find_one_and_update(
                {"hash": d_oper['hash']},
                {"$set": d_oper},
                upsert=True)

            log.info("Tx (REVERT) Tx Hash: {0}".format(raw_tx['hash']))

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

    def on_task(self, task=None):
        self.scan_events_txs(task=task)
