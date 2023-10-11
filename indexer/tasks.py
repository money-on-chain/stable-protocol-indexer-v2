
from .base.main import ConnectionHelperMongo
from .base.token import ERC20Token
from .tasks_manager import TasksManager
from .logger import log
from .contracts import Multicall2, MocCAWrapper, MocCABag, FastBtcBridge
from .scan_raw_transactions import ScanRawTxs
from .scan_logs_transactions import ScanLogsTransactions
from .scan_transactions_status import ScanTxStatus

__VERSION__ = '4.0.4'

log.info("Starting Protocol Indexer version {0}".format(__VERSION__))


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

        # MocCAWrapper
        self.contracts_loaded["MocCAWrapper"] = MocCAWrapper(
            self.connection_helper.connection_manager,
            contract_address=self.config['addresses']['MocCAWrapper'])
        self.contracts_addresses['MocCAWrapper'] = self.contracts_loaded["MocCAWrapper"].address().lower()

        # MocCABag
        self.contracts_loaded["MocCABag"] = MocCABag(
            self.connection_helper.connection_manager,
            contract_address=self.config['addresses']['MocCABag'])
        self.contracts_addresses['MocCABag'] = self.contracts_loaded["MocCABag"].address().lower()

        # Token TC
        self.contracts_loaded["TC"] = ERC20Token(
            self.connection_helper.connection_manager,
            contract_address=self.config['addresses']['TC'])
        self.contracts_addresses['TC'] = self.contracts_loaded["TC"].address().lower()

        # Token TP_0
        self.contracts_loaded["TP_0"] = ERC20Token(
            self.connection_helper.connection_manager,
            contract_address=self.config['addresses']['TP_0'])
        self.contracts_addresses['TP_0'] = self.contracts_loaded["TP_0"].address().lower()

        # Token TP_1
        if 'TP_1' in self.config['addresses']:
            self.contracts_loaded["TP_1"] = ERC20Token(
                self.connection_helper.connection_manager,
                contract_address=self.config['addresses']['TP_1'])
            self.contracts_addresses['TP_1'] = self.contracts_loaded["TP_1"].address().lower()

        # Token CA_0
        self.contracts_loaded["CA_0"] = ERC20Token(
            self.connection_helper.connection_manager,
            contract_address=self.config['addresses']['CA_0'])
        self.contracts_addresses['CA_0'] = self.contracts_loaded["CA_0"].address().lower()

        # Token CA_1
        if 'CA_1' in self.config['addresses']:
            self.contracts_loaded["CA_1"] = ERC20Token(
                self.connection_helper.connection_manager,
                contract_address=self.config['addresses']['CA_1'])
            self.contracts_addresses['CA_1'] = self.contracts_loaded["CA_1"].address().lower()

        # Token TG
        if 'TG' in self.config['addresses']:
            self.contracts_loaded["TG"] = ERC20Token(
                self.connection_helper.connection_manager,
                contract_address=self.config['addresses']['TG'])
            self.contracts_addresses['TG'] = self.contracts_loaded["TG"].address().lower()

        # FastBTCBridge
        self.contracts_loaded["FastBtcBridge"] = FastBtcBridge(
            self.connection_helper.connection_manager,
            contract_address=self.config['addresses']['FastBtcBridge'])
        self.contracts_addresses['FastBtcBridge'] = self.config['addresses']['FastBtcBridge']

        self.filter_contracts_addresses = [v.lower() for k, v in self.contracts_addresses.items()]

    def schedule_tasks(self):

        log.info("Starting adding indexer tasks...")

        # set max workers
        self.max_workers = 1

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

        # 4. Scan events not processed
        if 'scan_logs_not_processed' in self.config['tasks']:
            log.info("Jobs add: 4. Scan logs not processed")
            interval = self.config['tasks']['scan_logs_not_processed']['interval']
            scan_logs_not_processed = ScanLogsTransactions(
                self.config,
                self.connection_helper,
                self.contracts_loaded,
                self.contracts_addresses,
                self.filter_contracts_addresses)
            self.add_task(scan_logs_not_processed.on_task_not_processed,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='4. Scan Logs not processed')

        # 5. Scan Raw Transactions Confirming
        if 'scan_raw_transactions_confirming' in self.config['tasks']:
            log.info("Jobs add: 5. Scan Raw Transactions Confirming")
            interval = self.config['tasks']['scan_raw_transactions_confirming']['interval']
            scan_raw_txs_confirming = ScanRawTxs(self.config, self.connection_helper, self.filter_contracts_addresses)
            self.add_task(scan_raw_txs_confirming.on_task_confirming,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='5. Scan Raw Transactions Confirming')

        # Set max tasks
        self.max_tasks = len(self.tasks)
