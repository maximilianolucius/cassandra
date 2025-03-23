import os
import pandas as pd
import json
import logging
import time
from time import sleep
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import pytz
import yfinance as yf
from pathlib import Path

from api.dwx_client import dwx_client

# -----------------
# Cuenta demo para evaluacion de Viridis Cassandra V1 (Python, WorldTime, Yahho Finance).
# 65900
# d8bdsvg
# Servidor TraderviewDemo-Europe
# C:\Users\maxim\AppData\Roaming\MetaQuotes\Terminal\E647CE9DE0A6700F25735E9AF6F6CBBE
# -----------------


#65788
#jqf7kkn
#C:\Users\maxim\AppData\Roaming\MetaQuotes\Terminal\8542FEA8EC2B4DB48DA3F3FB71BED373

#Cuenta de pruebas:
#65897
#zntz4rq
#C:\Users\maxim\AppData\Roaming\MetaQuotes\Terminal\33F10EB7DA1E64855A7E700316574D86


# Constants --------------------------------------------------------------------------------
COMMENT_PATTERN = "{}__Cassandra__V{}"
VERSION = "1.0"
ON_CASSANDRA_MAGIC_NUMBER = 202502241
TD_CASSANDRA_MAGIC_NUMBER = 202502242

SYMBOL = 'SPXm'
TD_LOT_SIZE = 1.0
ON_LOT_SIZE = 1.0

TD_OPEN_HOUR = 11
TD_OPEN_MINUTE = 30
TD_CLOSE_HOUR = 15
TD_CLOSE_MINUTE = 30

ON_OPEN_HOUR = 15
ON_OPEN_MINUTE = 50
ON_CLOSE_HOUR = 9
ON_CLOSE_MINUTE = 30

TICKER_SYMBOL = "^GSPC"  # Yahoo Finance ticker for the S&P500 index

FIBONACCI_BASE_PRICE = 3594.52
FIBONACCI_TOP_PRICE = 4808.93;      # Fibonacci top price
INITIAL_STATE_VALUE = 14;           # Initial state value
INITIAL_STATE_DATE = datetime(2025, 1, 1, 0, 0, 0) # Initial state date
INHIBIT_STATES = [3,5,6,7,9]        # Inhibit state list.

TIME_TOLERANCE_WINDOW = 1800  # seconds
CONTINUE_TRADING = True

ORDERS_DIR = Path('./orders')
ORDERS_DIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
class TickProcessor:
    def __init__(self, mt4_files_dir, sleep_delay=0.005, max_retry_seconds=10, verbose=True):
        self.strategies = []
        self.mt4_files_dir = mt4_files_dir
        self.master_orders_collector = {}
        self.last_open_time = datetime.now(tz=ZoneInfo("UTC"))
        self.last_modification_time = datetime.now(tz=ZoneInfo("UTC"))

        # Initialize trading client
        self.dwx = dwx_client(self, mt4_files_dir, sleep_delay, max_retry_seconds, verbose=verbose)
        sleep(1)
        self.dwx.start()

        logger.info("Account info: %s", self.dwx.account_info)

        sleep(1)
        # subscribe to tick data:
        self.dwx.subscribe_symbols(['EURUSD', SYMBOL])

        sleep(2)
        if len(self.dwx.open_orders):
            logger.info("Existing open orders: %s", self.dwx.open_orders)
        else:
            logger.info("No open orders.")
    
    def set_strategy(self, strategy):
        strategy.set_dwx(self.dwx)
        strategy.initialize()
        self.strategies.append(strategy)

    def on_tick(self, symbol, bid, ask):
        for strategy in self.strategies:
            strategy.on_tick(symbol, bid, ask)

    def on_message(self, message):
        for strategy in self.strategies:
            strategy.on_message(message)

    def on_order_event(self):
        for strategy in self.strategies:
            strategy.on_order_event()


def get_nyc_yesterday_milestone_date():
    current_datetime = datetime.now()
    # Calculate yesterday's date
    yesterday = current_datetime - timedelta(days=1)

    # Check if yesterday was Saturday or Sunday
    yesterday_weekday = yesterday.weekday()  # Monday is 0, Sunday is 6
    if yesterday_weekday in [5, 6]:    # If yesterday was Saturday (5) or Sunday (6), move to Friday (4)
        adjusted_date = yesterday - timedelta(days=(yesterday_weekday - 4))
    else:
        adjusted_date = yesterday
    ny_tz = pytz.timezone('America/New_York')
    adjusted_date_deadline = adjusted_date.replace(hour=15, minute=30, second=0, microsecond=0)
    nyc_yesterday_deadline = ny_tz.localize(adjusted_date_deadline)

    return nyc_yesterday_deadline


class ONCassandraTickProcessor:
    def __init__(self, dwx=None,
                 nbi=FIBONACCI_BASE_PRICE, nti=FIBONACCI_TOP_PRICE, Zin=INITIAL_STATE_VALUE, Vstr=INHIBIT_STATES, Zdin=INITIAL_STATE_DATE,
             ):
        self.master_orders_collector = {}
        self.last_open_time = datetime.now(tz=ZoneInfo("UTC"))
        self.last_modification_time = datetime.now(tz=ZoneInfo("UTC"))

        # Initialize trading client
        self.dwx = dwx

        self.nbi = nbi   # Fibonacci base price
        self.nti = nti   # Fibonacci top price
        self.Zin = Zin   # Initial state value
        self.Zdin = Zdin # Initial state date
        self.Vstr = Vstr # Inhibit state list.
        self.Z = None
        self.V = Vstr
        self.Zs = []
        self.Zdts = []
        self.Zs_close = []

        # Additional state tracking
        self._prev_ref_price = 0.0

        self.order_id = None
        self.lots = ON_LOT_SIZE
        self.executed_order_time = None
        self.order_sent = False
        self.check_order_closed = False
        self.order_close_dt = None
        self.last_order_close_trial_dt = None

        # Set up order timing and state
        self._setup_order_timing()

        # Global variables for Fibonacci levels
        self.fibo_base = 0.0
        self.fibo_38 = 0.0
        self.fibo_50 = 0.0
        self.fibo_61 = 0.0
        self.fibo_top = 0.0
        self.fibo_138 = 0.0
        self.fibo_150 = 0.0
        self.fibo_161 = 0.0
        self.fibo_200 = 0.0
        self._calculate_fibonacci_levels(self.nbi, self.nti)
        self.timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") # timestamp mark to file names.
        self.save_fibo_levels_to_csv()



        # Previous reference price
        self.prev_ref_price = 0.0
        self._prev_ref_price = 0.0

        # Internal state variables
        self.last_candle_time = None                # Equivalent to datetime in MQL4
        self.open_date = None                       # Equivalent to datetime in MQL4

        self.last_trading_times_update = time.time() #  Set the timer to trigger every 60 minutes (3600 seconds)

        self.update_state()
        self.save_state_data_to_csv()

        print("ON __init__")

    def initialize(self):
        if self.dwx is None:
            raise Exception("dwx should be initializated!")
        self.last_trading_times_update = time.time() #  Set the timer to trigger every 60 minutes (3600 seconds)
        self._scan_market_for_order()

        print("I'm HERE")

    def save_fibo_levels_to_csv(self):
        """Save Fibonacci levels to CSV with datetime stamp"""
        fibo_data = {
            'fibo_base': [self.fibo_base],
            'fibo_38': [self.fibo_38],
            'fibo_50': [self.fibo_50],
            'fibo_61': [self.fibo_61],
            'fibo_top': [self.fibo_top],
            'fibo_138': [self.fibo_138],
            'fibo_150': [self.fibo_150],
            'fibo_161': [self.fibo_161],
            'fibo_200': [self.fibo_200]
        }

        df = pd.DataFrame(fibo_data)
        filename = f"fibo_levels_{self.timestamp_str}.csv"
        df.to_csv(filename, index=False)
        print(f"Saved Fibonacci levels to {filename}")

    def save_state_data_to_csv(self):
        """Save state arrays to CSV with datetime stamp"""
        if len(self.Zs) != len(self.Zdts) or len(self.Zs) != len(self.Zs_close):
            raise ValueError("State arrays must be of equal length")

        state_data = {
            'Z': self.Zs,
            'Zdt': self.Zdts,
            'Z_close': self.Zs_close
        }

        df = pd.DataFrame(state_data)
        filename = f"state_data_{self.timestamp_str}.csv"
        df.to_csv(filename, index=False)
        print(f"Saved state data to {filename}")


    def _calculate_fibonacci_levels(self, nbi, nti):
        if nti <= nbi:
            raise ValueError("Error: 'nti' must be greater than 'nbi' for valid Fibonacci calculation.")
        # Global variables for Fibonacci levels
        size_ = nti - nbi
        self.fibo_base = nbi
        self.fibo_38 = nbi + 0.382 * size_
        self.fibo_50 = nbi + 0.50 * size_
        self.fibo_61 = nbi + 0.618 * size_
        self.fibo_top = nti
        self.fibo_138 = nbi + 1.382 * size_
        self.fibo_150 = nbi + 1.50 * size_
        self.fibo_161 = nbi + 1.618 * size_
        self.fibo_200 = nbi + 2.0 * size_

    def update_state(self, verbose=False):
        """
        Update the state (Z) based on the reference price and the Fibonacci levels.
        The state changes when the ref_price crosses a Fibonacci level.

        self.Zs = []
        self.Zdts = []
        self.Zs_close = []
        """
        if len(self.Zs) == 0:
            start_date = "2025-01-01"
        else:
            start_date=(self.Zdts[-1] - timedelta(days=3)).strftime("%Y-%m-%d") #@TODO Achicar considerando sab, dom
        end_date = datetime.now().strftime("%Y-%m-%d")
        # Download the data; note that Yahoo may limit intraday historical data availability.
        try:
            data = yf.download(TICKER_SYMBOL, start=start_date, end=end_date, interval="1h")
        except Exception as e:
            print(f'Yahoo issue: {e}')
            return
        df = data.reset_index()
        df.columns = ['Datetime', 'Close', 'High', 'Low', 'Open', 'Volume']

        if df.shape[0] == 0:
            return
        df_dict = df.to_dict(orient='records')

        nyc_tz = pytz.timezone('America/New_York')
        for item in df_dict:
            item['Datetime'] = item['Datetime'].to_pydatetime().astimezone(nyc_tz)
        if len(self.Zs):
            for idx, Zdt in enumerate(self.Zdts):
                if Zdt >=  df_dict[0]['Datetime']:
                    break
            assert idx > 0
            self.Zs = self.Zs[: idx]
            self.Zdts = self.Zdts[:idx]
            self.Zs_close = self.Zs_close[:idx]
        if len(self.Zs):
            Z_close_prev = self.Zs_close[-1]
            Z = self.Zs[-1]
        else:
            Z_close_prev, Z = 0.0, 0
        for item in df_dict:
            Z = self._compute_state(ref_price=item['Close'], prev_ref_price=Z_close_prev, Z=Z)
            Z_close_prev=item['Close']
            self.Zs.append(Z)
            self.Zdts.append(item['Datetime'])
            self.Zs_close.append(item['Close'])

    def _compute_state(self, ref_price: float, prev_ref_price: float, Z: int, verbose=False):
        """
        Update the state (Z) based on the reference price and the Fibonacci levels.
        The state changes when the ref_price crosses a Fibonacci level.
        """
        # Check conditions for downward transitions
        if ref_price < self.fibo_base and prev_ref_price >= self.fibo_base:
            Z = 1
        elif ref_price < self.fibo_38 and prev_ref_price >= self.fibo_38:
            Z = 3
        elif ref_price < self.fibo_50 and prev_ref_price >= self.fibo_50:
            Z = 5
        elif ref_price < self.fibo_61 and prev_ref_price >= self.fibo_61:
            Z = 7
        elif ref_price < self.fibo_top and prev_ref_price >= self.fibo_top:
            Z = 9
        elif ref_price < self.fibo_138 and prev_ref_price >= self.fibo_138:
            Z = 11
        elif ref_price < self.fibo_161 and prev_ref_price >= self.fibo_161:
            Z = 13
        elif ref_price < self.fibo_200 and prev_ref_price >= self.fibo_200:
            Z = 15

        # Check conditions for upward transitions
        if ref_price > self.fibo_200 and prev_ref_price <= self.fibo_200:
            Z = 16
        elif ref_price > self.fibo_161 and prev_ref_price <= self.fibo_161:
            Z = 14
        elif ref_price > self.fibo_138 and prev_ref_price <= self.fibo_138:
            Z = 12
        elif ref_price > self.fibo_top and prev_ref_price <= self.fibo_top:
            Z = 10
        elif ref_price > self.fibo_61 and prev_ref_price <= self.fibo_61:
            Z = 8
        elif ref_price > self.fibo_50 and prev_ref_price <= self.fibo_50:
            Z = 6
        elif ref_price > self.fibo_38 and prev_ref_price <= self.fibo_38:
            Z = 4
        elif ref_price > self.fibo_base and prev_ref_price <= self.fibo_base:
            Z = 2

        # Verbose logging if enabled
        if verbose:
            print(
                f"Reference Price: {ref_price}   Previous Ref Price: {prev_ref_price}   Updated State Z: {Z}")

        return Z

    def set_dwx(self, dwx):
        # Set trading client
        self.dwx = dwx

    def _setup_order_timing(self):
        now_ny = datetime.now(tz=ZoneInfo("America/New_York"))
        if self.order_id is None : # Update only if there is no market order.
            self.open_time = now_ny.replace(hour=ON_OPEN_HOUR, minute=ON_OPEN_MINUTE, second=0, microsecond=0)
            self.close_time = now_ny.replace(hour=ON_CLOSE_HOUR, minute=ON_CLOSE_MINUTE, second=0, microsecond=0) + timedelta(days=1)

            # If the open time has already passed today, schedule for the next day
            if datetime.now(tz=ZoneInfo("America/New_York")) >= self.open_time:
                self.open_time += timedelta(days=1)
                self.close_time += timedelta(days=1)

            # Adjust for weekends (assuming trading on Monday if order falls on Saturday/Sunday)
            if self.open_time.weekday() >= 5:
                days_to_add = (7 - self.open_time.weekday())
                self.open_time += timedelta(days=days_to_add)
                self.close_time += timedelta(days=days_to_add)
            if self.close_time.weekday() >= 5:
                days_to_add = (7 - self.close_time.weekday())
                self.close_time += timedelta(days=days_to_add)
        # ----------------------------------------------------------------------------------------------------

    def _scan_market_for_order(self):
        # Unificarla !!
        td_comment = COMMENT_PATTERN.format('ON', VERSION)
        for order_id, order in self.dwx.open_orders.items():
            if order.get('comment') == td_comment:
                self.order_id = order_id
                if self.order_sent:
                    self.order_sent = False
                self.lots = order.get('lots', ON_LOT_SIZE)
                try:
                    dt = datetime.strptime(order['open_time'], '%Y.%m.%d %H:%M:%S')
                except Exception as e:
                    logger.error("Error parsing open_time for order %s: %s", order_id, e)
                    continue

                order_file = ORDERS_DIR / f'{order_id}_on.json'
                if order_file.is_file():
                    try:
                        with order_file.open('r') as fp:
                            order_loaded = json.load(fp)
                        date_format = "%Y-%m-%dT%H:%M:%S%z"
                        self.executed_order_time = datetime.strptime(order_loaded.get('open_time_extended'), date_format)

                    except Exception as e:
                        logger.error("Error reading order file %s: %s", order_file, e)
                else:
                    self.executed_order_time = datetime( # Error de conversion
                        dt.year, dt.month, dt.day, ON_OPEN_HOUR, dt.minute, dt.second,
                        tzinfo=ZoneInfo("America/New_York")
                    )
                    order['open_time_extended'] = self.executed_order_time.isoformat()
                    order['order_id'] = order_id
                    try:
                        with order_file.open('w') as fp:
                            json.dump(order, fp, indent=4)
                    except Exception as e:
                        logger.error("Error writing order file %s: %s", order_file, e)

                # Update close time based on execution time
                dt_ref = self.executed_order_time
                self.close_time = datetime(dt_ref.year, dt_ref.month, dt_ref.day, ON_CLOSE_HOUR, ON_CLOSE_MINUTE,
                                              tzinfo=ZoneInfo("America/New_York"))  + timedelta(days=1)
                if self.close_time.weekday() >= 5:
                    days_to_add = (7 - self.close_time.weekday())
                    self.close_time += timedelta(days=days_to_add)

                if datetime.now(tz=ZoneInfo("America/New_York")) > self.close_time:
                    self.check_order_closed = True
                    self.order_close_dt = self.close_time
                    self.last_order_close_trial_dt = self.close_time

                logger.info("TD Cassandra order found: %s", order)

    def on_tick(self, symbol, bid, ask):
        now = datetime.now(tz=ZoneInfo("America/New_York"))
        # Replace or define TD_PositionOpened appropriately
        td_position_opened = self.order_id is not None

        if self.open_time <= now < (self.open_time + timedelta(seconds=TIME_TOLERANCE_WINDOW)) \
                and not td_position_opened and not self.order_sent and CONTINUE_TRADING \
                and self.Z not in self.V:
            # Assuming self.dwx_slave is equivalent to self.dwx or set up separately
            self.dwx.open_order(
                symbol=SYMBOL,  # ensure symbol is provided correctly
                order_type='buy',
                lots=ON_LOT_SIZE,
                magic=ON_CASSANDRA_MAGIC_NUMBER,
                comment=COMMENT_PATTERN.format('ON', VERSION)
            )
            self.order_sent = True
        elif now >= self.close_time and td_position_opened and not self.check_order_closed:
            self.dwx.close_order(
                ticket=self.order_id,
                lots=TD_LOT_SIZE
            )
            self.check_order_closed = True
            self.order_close_dt = now
            self.last_order_close_trial_dt = now

            logger.info("[TDBot] Buy Time: %s, Sell Time: %s", self.executed_order_time, now)

        if self.order_id and self.check_order_closed:
            if now - self.last_order_close_trial_dt > timedelta(seconds=10):
                self.last_order_close_trial_dt = now
                if now - self.order_close_dt < timedelta(minutes=4):
                    self.dwx.close_order(
                        ticket=self.order_id,
                        lots=TD_LOT_SIZE
                    )
                else:
                    if self.dwx.open_orders.get(self.order_id, {}).get('pnl', 0) > 0:
                        self.dwx.close_order(
                            ticket=self.order_id,
                            lots=TD_LOT_SIZE
                        )

        if time.time() - self.last_trading_times_update > 2 * 3600:
            self.last_trading_times_update = time.time()
            self.update_state() # Ejecutar cada 2 horas.
            # Update self.Z
            # datetime FiboCriticalTimeInNYZone = todayNY + 16 * 3600; // 16:00 New York time
            nyc_yesterday_milestone_date = get_nyc_yesterday_milestone_date()
            dt_Z_mapper = {dt: Z for dt, Z in zip(self.Zdts, self.Zs)}
            self.Z = dt_Z_mapper[nyc_yesterday_milestone_date]
            logger.info("%s | %s", 'INFO', f'New Z: {self.Z}')
            self.save_state_data_to_csv()


    def on_message(self, message):
        msg_type = message.get('type')
        if msg_type == 'ERROR':
            logger.error("%s | %s | %s", msg_type, message.get('error_type'), message.get('description'))
        elif msg_type == 'INFO':
            logger.info("%s | %s", msg_type, message.get('message'))

            if 'Successfully closed order' in message.get('message', '') and str(self.order_id) in message.get('message', ''):
                self.check_order_closed = False
                self.order_sent = False
                self.order_id = None
                self._setup_order_timing()

    def on_order_event(self):
        logger.info("on_order_event. Open orders count: %d", len(self.dwx.open_orders))
        self._scan_market_for_order()

class TDCassandraTickProcessor:
    def __init__(self, dwx=None):
        self.master_orders_collector = {}
        self.last_open_time = datetime.now(tz=ZoneInfo("UTC"))
        self.last_modification_time = datetime.now(tz=ZoneInfo("UTC"))

        # Initialize trading client
        self.dwx = dwx

        # Set up order timing and state
        self._setup_order_timing()

        self.order_id = None
        self.lots = TD_LOT_SIZE
        self.executed_order_time = None
        self.order_sent = False
        self.check_order_closed = False
        self.order_close_dt = None
        self.last_order_close_trial_dt = None

    def set_dwx(self, dwx):
        # Set trading client
        self.dwx = dwx

    def initialize(self):
        if self.dwx is None:
            raise Exception("dwx should be initializated!")
        self._scan_market_for_order()

    def _setup_order_timing(self):
        now_ny = datetime.now(tz=ZoneInfo("America/New_York"))
        self.open_time = now_ny.replace(hour=TD_OPEN_HOUR, minute=TD_OPEN_MINUTE, second=0, microsecond=0)
        self.close_time = now_ny.replace(hour=TD_CLOSE_HOUR, minute=TD_CLOSE_MINUTE, second=0, microsecond=0)

        # If the open time has already passed today, schedule for the next day
        if datetime.now(tz=ZoneInfo("America/New_York")) >= self.open_time:
            self.open_time += timedelta(days=1)
            self.close_time += timedelta(days=1)

        # Adjust for weekends (assuming trading on Monday if order falls on Saturday/Sunday)
        if self.open_time.weekday() >= 5:
            days_to_add = (7 - self.open_time.weekday())
            self.open_time += timedelta(days=days_to_add)
            self.close_time += timedelta(days=days_to_add)

    def _scan_market_for_order(self):
        td_comment = COMMENT_PATTERN.format('TD', VERSION)
        for order_id, order in self.dwx.open_orders.items():
            if order.get('comment') == td_comment:
                self.order_id = order_id
                if self.order_sent:
                    self.order_sent = False
                self.lots = order.get('lots', TD_LOT_SIZE)
                try:
                    dt = datetime.strptime(order['open_time'], '%Y.%m.%d %H:%M:%S')
                except Exception as e:
                    logger.error("Error parsing open_time for order %s: %s", order_id, e)
                    continue

                order_file = ORDERS_DIR / f'{order_id}_td.json'
                if order_file.is_file():
                    try:
                        with order_file.open('r') as fp:
                            order_loaded = json.load(fp)
                        date_format = "%Y-%m-%dT%H:%M:%S%z"
                        self.executed_order_time = datetime.strptime(order_loaded.get('open_time_extended'), date_format)

                    except Exception as e:
                        logger.error("Error reading order file %s: %s", order_file, e)
                else:
                    self.executed_order_time = datetime(
                        dt.year, dt.month, dt.day, TD_OPEN_HOUR, dt.minute, dt.second,
                        tzinfo=ZoneInfo("America/New_York")
                    )
                    order['open_time_extended'] = self.executed_order_time.isoformat()
                    order['order_id'] = order_id
                    try:
                        with order_file.open('w') as fp:
                            json.dump(order, fp, indent=4)
                    except Exception as e:
                        logger.error("Error writing order file %s: %s", order_file, e)

                # Update close time based on execution time
                dt_ref = self.executed_order_time
                self.close_time = datetime(dt_ref.year, dt_ref.month, dt_ref.day,
                                              TD_CLOSE_HOUR, TD_CLOSE_MINUTE,
                                              tzinfo=ZoneInfo("America/New_York"))
                if datetime.now(tz=ZoneInfo("America/New_York")) > self.close_time:
                    self.check_order_closed = True
                    self.order_close_dt = self.close_time
                    self.last_order_close_trial_dt = self.close_time

                logger.info("TD Cassandra order found: %s", order)

    def on_tick(self, symbol, bid, ask):
        now = datetime.now(tz=ZoneInfo("America/New_York"))
        # Replace or define TD_PositionOpened appropriately
        td_position_opened = self.order_id is not None

        if self.open_time <= now < (self.open_time + timedelta(seconds=TIME_TOLERANCE_WINDOW)) \
                and not td_position_opened and not self.order_sent:
            # Assuming self.dwx_slave is equivalent to self.dwx or set up separately
            self.dwx.open_order(
                symbol=SYMBOL,  # ensure symbol is provided correctly
                order_type='buy',
                lots=TD_LOT_SIZE,
                magic=TD_CASSANDRA_MAGIC_NUMBER,
                comment=COMMENT_PATTERN.format('TD', VERSION)
            )
            self.order_sent = True
        elif now >= self.close_time and td_position_opened and not self.check_order_closed:
            self.dwx.close_order(
                ticket=self.order_id,
                lots=TD_LOT_SIZE
            )
            self.check_order_closed = True
            self.order_close_dt = now
            self.last_order_close_trial_dt = now

            logger.info("[TDBot] Buy Time: %s, Sell Time: %s", self.executed_order_time, now)

        if self.order_id and self.check_order_closed:
            if now - self.last_order_close_trial_dt > timedelta(seconds=10):
                self.last_order_close_trial_dt = now
                if now - self.order_close_dt < timedelta(minutes=4):
                    self.dwx.close_order(
                        ticket=self.order_id,
                        lots=TD_LOT_SIZE
                    )
                else:
                    if self.dwx.open_orders.get(self.order_id, {}).get('pnl', 0) > 0:
                        self.dwx.close_order(
                            ticket=self.order_id,
                            lots=TD_LOT_SIZE
                        )

    def on_message(self, message):
        msg_type = message.get('type')
        if msg_type == 'ERROR':
            logger.error("%s | %s | %s", msg_type, message.get('error_type'), message.get('description'))
        elif msg_type == 'INFO':
            logger.info("%s | %s", msg_type, message.get('message'))

            if 'Successfully closed order' in message.get('message', '') and str(self.order_id) in message.get('message', ''):
                self.check_order_closed = False
                self.order_sent = False
                self.order_id = None
                self._setup_order_timing()

    def on_order_event(self):
        logger.info("on_order_event. Open orders count: %d", len(self.dwx.open_orders))
        self._scan_market_for_order()

# ------------------------------------------------------------------------------
MT4_FILES_DIR = r'C:\Users\maxim\AppData\Roaming\MetaQuotes\Terminal\E647CE9DE0A6700F25735E9AF6F6CBBE\MQL4\Files'

if __name__ == "__main__":
    processor = TickProcessor(MT4_FILES_DIR)

    td_strat = TDCassandraTickProcessor()
    on_strat = ONCassandraTickProcessor()

    processor.set_strategy(td_strat)
    processor.set_strategy(on_strat)

    while processor.dwx.ACTIVE:
        sleep(1)
