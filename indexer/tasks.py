from pymongo import ASCENDING, DESCENDING
import os
import json

from .base.main import ConnectionHelperMongo
from .base.token import ERC20Token
from .tasks_manager import TasksManager
from .logger import log
from .contracts import Multicall2, Moc, FastBtcBridge, MocQueue, \
    OMOCDelayMachine, OMOCIncentiveV2, OMOCSupporters, OMOCVestingFactory, \
    OMOCVotingMachine, OMOCIRegistry
from .scan_raw_transactions import ScanRawTxs
from .scan_logs_transactions import ScanLogsTransactions
from .scan_transactions_status import ScanTxStatus

__VERSION__ = '4.2.3'

log.info("Starting Protocol Indexer version {0}".format(__VERSION__))


def read_omoc_json_file(filename=None):
    """ Read Json File """

    if not filename:
        filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'omoc.json')

    with open(filename) as f:
        options = json.load(f)

    return options


class StableIndexerTasks(TasksManager):

    def __init__(self, config):

        TasksManager.__init__(self)

        self.config = config
        self.connection_helper = ConnectionHelperMongo(config)

        self.contracts_loaded = dict()
        self.contracts_addresses = dict()
        self.filter_contracts_addresses = dict()

        # load contracts
        self.load_contracts()

        # Add tasks
        self.schedule_tasks()

    def load_contracts(self):
        """ Get contract address to use later """

        log.info("Loading contracts...")

        self.contracts_loaded["Multicall2"] = Multicall2(
            self.connection_helper.connection_manager,
            contract_address=self.config['addresses']['Multicall2'])
        #self.contracts_addresses['Multicall2'] = self.contracts_loaded["Multicall2"].address().lower()

        # Moc
        self.contracts_loaded["Moc"] = Moc(
            self.connection_helper.connection_manager,
            self.config,
            contract_address=self.config['addresses']['Moc'])
        self.contracts_addresses['Moc'] = self.contracts_loaded["Moc"].address().lower()

        # MocQueue
        self.contracts_loaded["MocQueue"] = MocQueue(
            self.connection_helper.connection_manager,
            self.config,
            contract_address=self.config['addresses']['MocQueue'])
        self.contracts_addresses['MocQueue'] = self.contracts_loaded["MocQueue"].address().lower()

        # Token TC
        self.contracts_loaded["TC"] = ERC20Token(
            self.connection_helper.connection_manager,
            contract_address=self.config['addresses']['TC'])
        self.contracts_addresses['TC'] = self.contracts_loaded["TC"].address().lower()

        # TP Tokens load
        self.contracts_loaded["TP"] = list()
        self.contracts_addresses['TP'] = list()
        for t_pegged in self.config['addresses']['TP']:
            self.contracts_loaded["TP"].append(
                ERC20Token(
                    self.connection_helper.connection_manager,
                    contract_address=t_pegged)
            )
            self.contracts_addresses['TP'].append(t_pegged.lower())

        # CA Tokens load
        self.contracts_loaded["CA"] = list()
        self.contracts_addresses['CA'] = list()
        for c_asset in self.config['addresses']['CA']:
            self.contracts_loaded["CA"].append(
                ERC20Token(
                    self.connection_helper.connection_manager,
                    contract_address=c_asset)
            )
            self.contracts_addresses['CA'].append(c_asset.lower())

        # Token FeeToken
        if 'FeeToken' in self.config['addresses']:
            self.contracts_loaded["FeeToken"] = ERC20Token(
                self.connection_helper.connection_manager,
                contract_address=self.config['addresses']['FeeToken'])
            self.contracts_addresses['FeeToken'] = self.contracts_loaded["FeeToken"].address().lower()

        # OMOC

        omoc = read_omoc_json_file()

        # IRegistry
        self.contracts_loaded["IRegistry"] = OMOCIRegistry(
            self.connection_helper.connection_manager,
            self.config,
            contract_address=self.config['addresses']['IRegistry'])
        self.contracts_addresses['IRegistry'] = self.contracts_loaded["IRegistry"].address().lower()

        # Getting addresses from Registry
        self.contracts_addresses['DelayMachine'] = self.contracts_loaded["IRegistry"].sc.functions.getAddress(
            omoc['RegistryConstants']['MOC_DELAY_MACHINE']).call().lower()
        self.contracts_addresses['Supporters'] = self.contracts_loaded["IRegistry"].sc.functions.getAddress(
            omoc['RegistryConstants']['SUPPORTERS_ADDR']).call().lower()
        self.contracts_addresses['VestingFactory'] = self.contracts_loaded["IRegistry"].sc.functions.getAddress(
            omoc['RegistryConstants']['MOC_VESTING_MACHINE']).call().lower()
        self.contracts_addresses['VotingMachine'] = self.contracts_loaded["IRegistry"].sc.functions.getAddress(
            omoc['RegistryConstants']['MOC_VOTING_MACHINE']).call().lower()
        self.contracts_addresses['StakingMachine'] = self.contracts_loaded["IRegistry"].sc.functions.getAddress(
            omoc['RegistryConstants']['MOC_STAKING_MACHINE']).call().lower()

        # IncentiveV2
        if self.config['addresses']['IncentiveV2']:
            self.contracts_loaded["IncentiveV2"] = OMOCIncentiveV2(
                self.connection_helper.connection_manager,
                self.config,
                contract_address=self.config['addresses']['IncentiveV2'])
            self.contracts_addresses['IncentiveV2'] = self.contracts_loaded["IncentiveV2"].address().lower()

        # DelayMachine
        self.contracts_loaded["DelayMachine"] = OMOCDelayMachine(
            self.connection_helper.connection_manager,
            self.config,
            contract_address=self.contracts_addresses['DelayMachine'])

        # Supporters
        self.contracts_loaded["Supporters"] = OMOCSupporters(
            self.connection_helper.connection_manager,
            self.config,
            contract_address=self.contracts_addresses['Supporters'])

        # VestingFactory
        self.contracts_loaded["VestingFactory"] = OMOCVestingFactory(
            self.connection_helper.connection_manager,
            self.config,
            contract_address=self.contracts_addresses['VestingFactory'])

        # VotingMachine
        self.contracts_loaded["VotingMachine"] = OMOCVotingMachine(
            self.connection_helper.connection_manager,
            self.config,
            contract_address=self.contracts_addresses['VotingMachine'])
        self.contracts_addresses['VotingMachine'] = self.contracts_loaded["VotingMachine"].address().lower()

        # FastBTCBridge
        self.contracts_loaded["FastBtcBridge"] = FastBtcBridge(
            self.connection_helper.connection_manager,
            contract_address=self.config['addresses']['FastBtcBridge'])
        self.contracts_addresses['FastBtcBridge'] = self.config['addresses']['FastBtcBridge']

        self.filter_contracts_addresses = []
        for k, v in self.contracts_addresses.items():
            if isinstance(v, list):
                for v2 in v:
                    self.filter_contracts_addresses.append(v2.lower())
            elif isinstance(v, str):
                self.filter_contracts_addresses.append(v.lower())
            else:
                raise Exception("Filter address not recognize!")

    def create_mongo_index(self):

        # Operations collection
        index_map = [('operId_', DESCENDING)]
        self.connection_helper.create_index('operations', index_map, unique=False)

        index_map = [('createdAt', DESCENDING)]
        self.connection_helper.create_index('operations', index_map, unique=False)

    def schedule_tasks(self):

        log.info("Starting adding indexer tasks...")

        # set max workers
        self.max_workers = 1

        log.info("Creating mongo collection index...")
        self.create_mongo_index()

        # 1. Scan Raw Transactions
        if 'scan_raw_transactions' in self.config['tasks']:
            log.info("Jobs add: 1. Scan Raw Transactions")
            interval = self.config['tasks']['scan_raw_transactions']['interval']
            scan_raw_txs = ScanRawTxs(self.config, self.connection_helper, self.filter_contracts_addresses)
            self.add_task(scan_raw_txs.on_task,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='1. Scan Raw Transactions')

        # 2. Scan Logs Txs
        if 'scan_logs' in self.config['tasks']:
            log.info("Jobs add: 2. Scan Logs Transactions")
            interval = self.config['tasks']['scan_logs']['interval']
            scan_logs_txs = ScanLogsTransactions(
                self.config,
                self.connection_helper,
                self.contracts_loaded,
                self.contracts_addresses,
                self.filter_contracts_addresses)
            self.add_task(scan_logs_txs.on_task,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='2. Scan Logs Transactions')

        # 3. Scan TX Status
        if 'scan_tx_status' in self.config['tasks']:
            log.info("Jobs add: 3. Scan Transactions Status")
            interval = self.config['tasks']['scan_tx_status']['interval']
            scan_tx_status = ScanTxStatus(self.config, self.connection_helper)
            self.add_task(scan_tx_status.on_task,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='3. Scan Transactions Status')

        # 4. Scan Raw Transactions Confirming
        if 'scan_raw_transactions_confirming' in self.config['tasks']:
            log.info("Jobs add: 4. Scan Raw Transactions Confirming")
            interval = self.config['tasks']['scan_raw_transactions_confirming']['interval']
            scan_raw_txs_confirming = ScanRawTxs(self.config, self.connection_helper, self.filter_contracts_addresses)
            self.add_task(scan_raw_txs_confirming.on_task_confirming,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='4. Scan Raw Transactions Confirming')

        # 5. Scan Raw Transactions History
        if 'scan_raw_transactions_history' in self.config['tasks']:
            log.info("Jobs add: 5. Scan Raw Transactions History")
            interval = self.config['tasks']['scan_raw_transactions_history']['interval']
            scan_raw_txs_history = ScanRawTxs(self.config, self.connection_helper,
                                                 self.filter_contracts_addresses)
            self.add_task(scan_raw_txs_history.on_task_history,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='5. Scan Raw Transactions History')

        # Set max tasks
        self.max_tasks = len(self.tasks)
