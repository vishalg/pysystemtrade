"""
this:

- gets capital from the database (earmarked with a strategy name)
- runs a backtest using that capital level, and mongodb data
- gets the final positions and position buffers
- writes these into a table (earmarked with a strategy name)


"""
import datetime
from syscore.constants import arg_not_supplied
from syscore.exceptions import missingData

from sysdata.config.configdata import Config
from sysdata.data_blob import dataBlob

from sysobjects.production.optimal_positions import (
    bufferedOptimalPositions,
)
from sysobjects.production.tradeable_object import instrumentStrategy

from sysproduction.data.currency_data import dataCurrency
from sysproduction.data.capital import dataCapital
from sysproduction.data.contracts import dataContracts
from sysproduction.data.optimal_positions import dataOptimalPositions
from sysproduction.data.sim_data import get_sim_data_object_for_production

#from sysproduction.data.backtest import store_backtest_state
from sysproduction.reporting.reporting_functions import parse_report_results, output_report, pandas_display_for_reports
from sysproduction.reporting.strategies_report import get_output_for_system_object
from sysproduction.data.reports import dataReports

from syslogging.logger import *

from systems.provided.futures_chapter15.basesystem import futures_system
from systems.basesystem import System


class runSystemClassic(object):
    def __init__(
        self,
        data: dataBlob,
        strategy_name: str,
        backtest_config_filename=arg_not_supplied,
    ):
        if backtest_config_filename is arg_not_supplied:
            raise Exception("Need to supply config filename")

        self.data = data
        self.strategy_name = strategy_name
        self.backtest_config_filename = backtest_config_filename

    ## DO NOT CHANGE THE NAME OF THIS FUNCTION
    def run_backtest(self):
        strategy_name = self.strategy_name
        data = self.data

        base_currency, notional_trading_capital = self._get_currency_and_capital()

        system = self.system_method(
            notional_trading_capital=notional_trading_capital,
            base_currency=base_currency,
        )

        function_to_call_on_update = self.function_to_call_on_update
        function_to_call_on_update(
            data=data, strategy_name=strategy_name, system=system
        )

        # Pickling backtest leads to OOM errors. So instead create and send the report rightnow
        # Also disable strategy report in private_control_config
        #store_backtest_state(data, system, strategy_name=strategy_name)
        report_results = get_output_for_system_object(data=data, strategy_name=strategy_name, system=system)

        pandas_display_for_reports()
        parsed_report = parse_report_results(data=data, report_results=report_results)

        data_reports = dataReports(data)
        all_report_configs = data_reports.get_report_configs_to_run()
        strategy_report_config = all_report_configs.get("strategy_report")

        output_report(parsed_report=parsed_report, report_config=strategy_report_config, data=data)


    ## MODIFY THIS WHEN INHERITING FOR A DIFFERENT STRATEGY
    ## ARGUMENTS MUST BE: data: dataBlob, strategy_name: str, system: System
    @property
    def function_to_call_on_update(self):
        return updated_buffered_positions

    def _get_currency_and_capital(self):
        data = self.data
        strategy_name = self.strategy_name

        capital_data = dataCapital(data)
        try:
            notional_trading_capital = capital_data.get_current_capital_for_strategy(
                strategy_name
            )
        except missingData:
            # critical log will send email
            error_msg = (
                "Capital data is missing for %s: can't run backtest" % strategy_name
            )
            data.log.critical(error_msg)
            raise Exception(error_msg)

        currency_data = dataCurrency(data)
        base_currency = currency_data.get_base_currency()

        self.data.log.debug(
            "Using capital of %s %.2f" % (base_currency, notional_trading_capital)
        )

        return base_currency, notional_trading_capital

    # DO NOT CHANGE THE NAME OF THIS FUNCTION; IT IS HARDCODED INTO CONFIGURATION FILES
    # BECAUSE IT IS ALSO USED TO LOAD BACKTESTS
    def system_method(
        self,
        notional_trading_capital: float = arg_not_supplied,
        base_currency: str = arg_not_supplied,
    ) -> System:
        data = self.data
        backtest_config_filename = self.backtest_config_filename

        system = production_classic_futures_system(
            data,
            backtest_config_filename,
            log=data.log,
            notional_trading_capital=notional_trading_capital,
            base_currency=base_currency,
        )

        return system


def production_classic_futures_system(
    data: dataBlob,
    config_filename: str,
    log=get_logger("futures_system"),
    notional_trading_capital: float = arg_not_supplied,
    base_currency: str = arg_not_supplied,
) -> System:
    sim_data = get_sim_data_object_for_production(data)
    config = Config(config_filename)

    # Overwrite capital and base currency
    if notional_trading_capital is not arg_not_supplied:
        config.notional_trading_capital = notional_trading_capital

    if base_currency is not arg_not_supplied:
        config.base_currency = base_currency

    system = futures_system(data=sim_data, config=config)
    system._log = log

    return system


def updated_buffered_positions(data: dataBlob, strategy_name: str, system: System):
    log = data.log

    data_optimal_positions = dataOptimalPositions(data)

    list_of_instruments = system.get_instrument_list()
    for instrument_code in list_of_instruments:
        lower_buffer, upper_buffer = get_position_buffers_from_system(
            system, instrument_code
        )
        position_entry = construct_position_entry(
            data=data,
            system=system,
            instrument_code=instrument_code,
            lower_position=lower_buffer,
            upper_position=upper_buffer,
        )
        instrument_strategy = instrumentStrategy(
            instrument_code=instrument_code, strategy_name=strategy_name
        )
        data_optimal_positions.update_optimal_position_for_instrument_strategy(
            instrument_strategy=instrument_strategy, position_entry=position_entry
        )
        log.debug(
            "New buffered positions %.3f %.3f"
            % (position_entry.lower_position, position_entry.upper_position),
            instrument_code=instrument_code,
        )


def get_position_buffers_from_system(system: System, instrument_code: str):
    buffers = system.portfolio.get_buffers_for_position(
        instrument_code
    )  # get the upper and lower edges of the buffer
    lower_buffer = buffers.iloc[-1].bot_pos
    upper_buffer = buffers.iloc[-1].top_pos

    return lower_buffer, upper_buffer


def construct_position_entry(
    data: dataBlob,
    system: System,
    instrument_code: str,
    lower_position: float,
    upper_position: float,
) -> bufferedOptimalPositions:
    diag_contracts = dataContracts(data)
    reference_price = system.rawdata.get_daily_prices(instrument_code).iloc[-1]
    reference_contract = diag_contracts.get_priced_contract_id(instrument_code)
    position_entry = bufferedOptimalPositions(
        date=datetime.datetime.now(),
        lower_position=lower_position,
        upper_position=upper_position,
        reference_price=reference_price,
        reference_contract=reference_contract,
    )

    return position_entry
