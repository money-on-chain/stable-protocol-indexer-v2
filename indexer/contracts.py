"""
                    GNU AFFERO GENERAL PUBLIC LICENSE
                       Version 3, 19 November 2007

 Copyright (C) 2007 Free Software Foundation, Inc. <https://fsf.org/>
 Everyone is permitted to copy and distribute verbatim copies
 of this license document, but changing it is not allowed.

 THIS IS A PART OF MONEY ON CHAIN PACKAGE
 by Martin Mulone (martin.mulone@moneyonchain.com)

"""

import os
import logging
from web3.types import BlockIdentifier

from .base.contracts import Contract


class Multicall2(Contract):

    log = logging.getLogger()
    precision = 10 ** 18

    contract_name = 'Multicall2'
    contract_abi = Contract.content_abi_file(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), 'abi/Multicall2.abi'))

    def __init__(self, connection_manager, contract_address=None, contract_abi=None, contract_bin=None):

        super().__init__(connection_manager,
                         contract_address=contract_address,
                         contract_abi=contract_abi,
                         contract_bin=contract_bin)

        # finally load the contract
        self.load_contract()

    def aggregate_multiple(self, call_list, require_success=False, block_identifier: BlockIdentifier = 'latest'):

        list_aggregate = list()
        if not isinstance(call_list, list):
            raise Exception("list_aggregate must be a list")

        for aggregate_tuple in call_list:
            if not isinstance(aggregate_tuple, (tuple, list)):
                raise Exception("The list must contains tuple or list of parameters: "
                                "(contract_address, encode_input, decode_output, format output)")

            if len(aggregate_tuple) != 4:
                raise Exception("The list must contains tuple or list of parameters: "
                                "(contract_address, function, input_parameters, format output). "
                                "Example: (moc_state_address, moc_state.sc.getBitcoinPrice, None, None)")

            if aggregate_tuple[2]:
                list_aggregate.append((aggregate_tuple[0], aggregate_tuple[1].encode_input(*aggregate_tuple[2])))
            else:
                list_aggregate.append((aggregate_tuple[0], aggregate_tuple[1].encode_input()))

        results = self.sc.tryBlockAndAggregate(require_success, list_aggregate, block_identifier=block_identifier)

        # decode results
        count = 0
        decoded_results = list()
        validity = True
        d_validity = dict()
        l_validity_results = list()
        for result in results[2]:
            fn = call_list[count][1]
            format_result = call_list[count][3]
            decoded_result = fn.decode_output(result[1])
            if format_result:
                decoded_result = format_result(decoded_result)

            decoded_results.append(decoded_result)

            # Results validity
            if validity and not result[0]:
                validity = False

            l_validity_results.append(result[0])
            count += 1

        if count == 0:
            # no results so not valid
            d_validity['valid'] = False
        else:
            d_validity['valid'] = validity

        d_validity['results'] = l_validity_results

        # return tuple (BlockNumber, List of results, Validation)
        return results[0], decoded_results, d_validity


class Moc(Contract):

    log = logging.getLogger()
    precision = 10 ** 18

    contract_name = 'Moc'
    contract_abi = Contract.content_abi_file(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), 'abi/flipmoney/Moc.abi'))

    def __init__(self, connection_manager, config, contract_address=None, contract_abi=None, contract_bin=None):

        self.contract_abi = Contract.content_abi_file(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                'abi/{0}/Moc.abi'.format(config['app_project'])
            )
        )

        super().__init__(connection_manager,
                         contract_address=contract_address,
                         contract_abi=contract_abi,
                         contract_bin=contract_bin)

        # finally load the contract
        self.load_contract()


class MocQueue(Contract):

    log = logging.getLogger()
    precision = 10 ** 18

    contract_name = 'MocQueue'
    contract_abi = Contract.content_abi_file(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), 'abi/flipmoney/MocQueue.abi'))

    def __init__(self, connection_manager, config, contract_address=None, contract_abi=None, contract_bin=None):

        self.contract_abi = Contract.content_abi_file(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                'abi/{0}/MocQueue.abi'.format(config['app_project'])
            )
        )

        super().__init__(connection_manager,
                         contract_address=contract_address,
                         contract_abi=contract_abi,
                         contract_bin=contract_bin)

        # finally load the contract
        self.load_contract()


class FastBtcBridge(Contract):

    log = logging.getLogger()
    precision = 10 ** 18

    contract_name = 'FastBtcBridge'
    contract_abi = Contract.content_abi_file(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), 'abi/FastBtcBridge.abi'))

    def __init__(self, connection_manager, contract_address=None, contract_abi=None, contract_bin=None):

        super().__init__(connection_manager,
                         contract_address=contract_address,
                         contract_abi=contract_abi,
                         contract_bin=contract_bin)

        # finally load the contract
        self.load_contract()
