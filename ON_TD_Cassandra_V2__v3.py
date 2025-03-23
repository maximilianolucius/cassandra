import os
import pandas as pd
import json
import logging
import time
from time import sleep
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pytz
import yfinance as yf
from pathlib import Path
from api.dwx_client import dwx_client
from functools import lru_cache
from threading import Timer

# -----------------
# Cuenta demo para evaluacion de Viridis Cassandra V2 (Python, WorldTime, Yahho Finance).
# 65901
# ncu4zet
# Servidor TraderviewDemo-Europe
# C:\Users\maxim\AppData\Roaming\MetaQuotes\Terminal\CBF2F6AE4D36F7FF1F027A9DADBE3E5A
# -----------------


#65788
#jqf7kkn
#C:\Users\maxim\AppData\Roaming\MetaQuotes\Terminal\8542FEA8EC2B4DB48DA3F3FB71BED373

#Cuenta de pruebas:
#65897
#zntz4rq
#C:\Users\maxim\AppData\Roaming\MetaQuotes\Terminal\33F10EB7DA1E64855A7E700316574D86


# Constants
COMMENT_PATTERN = "{}__Cassandra__V{}"
VERSION = "2.0"
ON_CASSANDRA_MAGIC_NUMBER = 202502251
TD_CASSANDRA_MAGIC_NUMBER = 202502252

SYMBOL = 'SPXm'
TD_LOT_SIZE = 1.0
ON_LOT_SIZE = 1.0

TD_OPEN_HOUR, TD_OPEN_MINUTE = 11, 30
TD_CLOSE_HOUR, TD_CLOSE_MINUTE = 15, 30

ON_OPEN_HOUR, ON_OPEN_MINUTE = 15, 50
ON_CLOSE_HOUR, ON_CLOSE_MINUTE = 9, 30

TICKER_SYMBOL = "^GSPC"  # Yahoo Finance ticker for the S&P500 index

FIBONACCI_BASE_PRICE = 3594.52
FIBONACCI_TOP_PRICE = 4808.93
INITIAL_STATE_VALUE = 14
INITIAL_STATE_DATE = datetime(2025, 1, 1, 0, 0, 0)
INHIBIT_STATES = [-5, -3, 3, 4, 5, 6, 7, 9]

TIME_TOLERANCE_WINDOW = 1800  # seconds

ORDERS_DIR = Path('./orders')
ORDERS_DIR.mkdir(exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_nyc_yesterday_milestone_date():
    """Get yesterday's closing time in NYC market with weekend adjustment."""
    current_datetime = datetime.now()
    yesterday = current_datetime - timedelta(days=1)

    # Adjust for weekends
    yesterday_weekday = yesterday.weekday()
    if yesterday_weekday in [5, 6]:  # Saturday or Sunday
        adjusted_date = yesterday - timedelta(days=(yesterday_weekday - 4))
    else:
        adjusted_date = yesterday

    ny_tz = pytz.timezone('America/New_York')
    adjusted_date_deadline = adjusted_date.replace(hour=15, minute=30, second=0, microsecond=0)
    return ny_tz.localize(adjusted_date_deadline)


class Strategy:
    """Base strategy class for trading strategies."""

    def __init__(self, dwx=None):
        self.dwx = dwx

    def set_dwx(self, dwx):
        self.dwx = dwx

    def initialize(self):
        if self.dwx is None:
            raise ValueError("Trading client (dwx) must be initialized first")

    def on_tick(self, symbol, bid, ask):
        pass

    def on_message(self, message):
        pass

    def on_order_event(self):
        pass


class TickProcessor:
    """Main processor class for handling tick data and routing to strategies."""

    def __init__(self, mt4_files_dir, sleep_delay=0.005, max_retry_seconds=10, verbose=True):
        self.strategies = []
        self.mt4_files_dir = mt4_files_dir

        # Initialize trading client
        self.dwx = dwx_client(self, mt4_files_dir, sleep_delay, max_retry_seconds, verbose=verbose)
        sleep(1)
        self.dwx.start()

        logger.info(f"Account info: {self.dwx.account_info}")

        # Subscribe to tick data
        sleep(1)
        self.dwx.subscribe_symbols(['EURUSD', SYMBOL])

        # Log existing orders
        sleep(2)
        if self.dwx.open_orders:
            logger.info(f"Existing open orders: {self.dwx.open_orders}")
        else:
            logger.info("No open orders.")

    def set_strategy(self, strategy):
        """Add a strategy to the processor."""
        strategy.set_dwx(self.dwx)
        strategy.initialize()
        self.strategies.append(strategy)

    def on_tick(self, symbol, bid, ask):
        """Route tick data to all registered strategies."""
        for strategy in self.strategies:
            strategy.on_tick(symbol, bid, ask)

    def on_message(self, message):
        """Route messages to all registered strategies."""
        for strategy in self.strategies:
            strategy.on_message(message)

    def on_order_event(self):
        """Route order events to all registered strategies."""
        for strategy in self.strategies:
            strategy.on_order_event()


class CassandraTickProcessor(Strategy):
    """Base Cassandra trading strategy class."""

    def __init__(self, dwx=None):
        super().__init__(dwx)
        self.master_orders_collector = {}
        self.order_id = None
        self.lots = TD_LOT_SIZE
        self.executed_order_time = None
        self.order_sent = False
        self.check_order_closed = False
        self.order_close_dt = None
        self.last_order_close_trial_dt = None

        # Default to TD settings, will be overridden in subclasses
        self.open_hour = TD_OPEN_HOUR
        self.open_minute = TD_OPEN_MINUTE
        self.close_hour = TD_CLOSE_HOUR
        self.close_minute = TD_CLOSE_MINUTE
        self.comment = COMMENT_PATTERN.format('TD', VERSION)
        self.magic_number = TD_CASSANDRA_MAGIC_NUMBER

        self.open_time = None
        self.close_time = None

    def initialize(self):
        """Initialize the strategy and look for existing orders."""
        super().initialize()
        self._setup_order_timing()
        self._scan_market_for_order()

    def _setup_order_timing(self):
        """Set up order timing based on strategy parameters."""
        if self.order_id is not None:
            return  # Don't update if there's an active order

        now_ny = datetime.now(tz=ZoneInfo("America/New_York"))
        self.open_time = now_ny.replace(hour=self.open_hour, minute=self.open_minute, second=0, microsecond=0)
        self.close_time = now_ny.replace(hour=self.close_hour, minute=self.close_minute, second=0, microsecond=0)

        # If the open time has already passed today, schedule for the next day
        if now_ny >= self.open_time:
            self.open_time += timedelta(days=1)
            self.close_time += timedelta(days=1)

        # Adjust for weekends
        if self.open_time.weekday() >= 5:  # Weekend
            days_to_add = (7 - self.open_time.weekday()) + (0 if self.open_time.weekday() == 6 else 1)
            self.open_time += timedelta(days=days_to_add)
            self.close_time += timedelta(days=days_to_add)

    def _scan_market_for_order(self):
        """Scan for existing orders in the market."""
        for order_id, order in self.dwx.open_orders.items():
            if order.get('comment') == self.comment:
                self.order_id = order_id
                self.order_sent = False
                self.lots = order.get('lots', self.lots)

                try:
                    dt = datetime.strptime(order['open_time'], '%Y.%m.%d %H:%M:%S')
                except Exception as e:
                    logger.error(f"Error parsing open_time for order {order_id}: {e}")
                    continue

                order_file = ORDERS_DIR / f'{order_id}_cassandra.json'

                if order_file.is_file():
                    try:
                        with order_file.open('r') as fp:
                            order_loaded = json.load(fp)
                        date_format = "%Y-%m-%dT%H:%M:%S%z"
                        self.executed_order_time = datetime.strptime(order_loaded.get('open_time_extended'),
                                                                     date_format)
                    except Exception as e:
                        logger.error(f"Error reading order file {order_file}: {e}")
                else:
                    self.executed_order_time = datetime(
                        dt.year, dt.month, dt.day, self.open_hour, dt.minute, dt.second,
                        tzinfo=ZoneInfo("America/New_York")
                    )
                    order['open_time_extended'] = self.executed_order_time.isoformat()
                    order['order_id'] = order_id

                    try:
                        with order_file.open('w') as fp:
                            json.dump(order, fp, indent=4)
                    except Exception as e:
                        logger.error(f"Error writing order file {order_file}: {e}")

                # Update close time based on execution time
                dt_ref = self.executed_order_time
                self.close_time = datetime(
                    dt_ref.year, dt_ref.month, dt_ref.day,
                    self.close_hour, self.close_minute,
                    tzinfo=ZoneInfo("America/New_York")
                )

                if datetime.now(tz=ZoneInfo("America/New_York")) > self.close_time:
                    self.check_order_closed = True
                    self.order_close_dt = self.close_time
                    self.last_order_close_trial_dt = self.close_time

                logger.info(f"{self.comment} order found ({order_id}): {order}")

    def valid_state(self):
        """Check if current state is valid for trading."""
        return True

    def on_tick(self, symbol, bid, ask):
        """Process tick data for trading decisions."""
        now = datetime.now(tz=ZoneInfo("America/New_York"))
        td_position_opened = self.order_id is not None

        # Check if it's time to open an order
        if (self.open_time <= now < (self.open_time + timedelta(seconds=TIME_TOLERANCE_WINDOW))
                and not td_position_opened and not self.order_sent and self.valid_state()):
            self.dwx.open_order(
                symbol=SYMBOL,
                order_type='buy',
                lots=self.lots,
                magic=self.magic_number,
                comment=self.comment
            )
            self.order_sent = True

        # Check if it's time to close an order
        elif now >= self.close_time and td_position_opened and not self.check_order_closed:
            self.dwx.close_order(
                ticket=self.order_id,
                lots=self.lots
            )
            self.check_order_closed = True
            self.order_close_dt = now
            self.last_order_close_trial_dt = now
            logger.info(f"[{self.comment}] Buy Time: {self.executed_order_time}, Sell Time: {now}")

        # Retry closing order if needed
        if self.order_id and self.check_order_closed:
            if now - self.last_order_close_trial_dt > timedelta(seconds=10):
                self.last_order_close_trial_dt = now

                if now - self.order_close_dt < timedelta(minutes=4):
                    self.dwx.close_order(ticket=self.order_id, lots=self.lots)
                else:
                    # Close only if profitable after 4 minutes
                    if self.dwx.open_orders.get(self.order_id, {}).get('pnl', 0) > 0:
                        self.dwx.close_order(ticket=self.order_id, lots=self.lots)

    def on_message(self, message):
        """Process messages from the trading client."""
        msg_type = message.get('type')

        if msg_type == 'ERROR':
            logger.error(f"{msg_type} | {message.get('error_type')} | {message.get('description')}")
        elif msg_type == 'INFO':
            logger.info(f"{msg_type} | {message.get('message')}")

            # Check if our order was closed
            if ('Successfully closed order' in message.get('message', '') and
                    str(self.order_id) in message.get('message', '')):
                self.check_order_closed = False
                self.order_sent = False
                self.order_id = None
                self._setup_order_timing()

    def on_order_event(self):
        """Handle order events."""
        logger.info(f"on_order_event. Open orders count: {len(self.dwx.open_orders)}")
        self._scan_market_for_order()


class TDCassandraTickProcessor(CassandraTickProcessor):
    """TD (Trading Day) Cassandra strategy implementation."""

    def __init__(self, dwx=None):
        super().__init__(dwx)

        # Override with TD-specific settings
        self.open_hour = TD_OPEN_HOUR
        self.open_minute = TD_OPEN_MINUTE
        self.close_hour = TD_CLOSE_HOUR
        self.close_minute = TD_CLOSE_MINUTE
        self.lots = TD_LOT_SIZE
        self.comment = COMMENT_PATTERN.format('TD', VERSION)
        self.magic_number = TD_CASSANDRA_MAGIC_NUMBER


class ONCassandraTickProcessor(CassandraTickProcessor):
    """ON (Overnight) Cassandra strategy implementation with Fibonacci levels."""

    def __init__(self, dwx=None,
                 nbi=FIBONACCI_BASE_PRICE,
                 nti=FIBONACCI_TOP_PRICE,
                 Zin=INITIAL_STATE_VALUE,
                 Vstr=INHIBIT_STATES,
                 Zdin=INITIAL_STATE_DATE):
        super().__init__(dwx)

        # Override with ON-specific settings
        self.open_hour = ON_OPEN_HOUR
        self.open_minute = ON_OPEN_MINUTE
        self.close_hour = ON_CLOSE_HOUR
        self.close_minute = ON_CLOSE_MINUTE
        self.lots = ON_LOT_SIZE
        self.comment = COMMENT_PATTERN.format('ON', VERSION)
        self.magic_number = ON_CASSANDRA_MAGIC_NUMBER

        # Fibonacci parameters
        self.nbi = nbi  # Fibonacci base price
        self.nti = nti  # Fibonacci top price
        self.Zin = Zin  # Initial state value
        self.Zdin = Zdin  # Initial state date
        self.Vstr = Vstr  # Inhibit state list
        self.Z = None
        self.V = Vstr
        self.Zs = []
        self.Zdts = []
        self.Zs_close = []

        # Global variables for Fibonacci levels
        self.fibo_levels = {}
        self._calculate_fibonacci_levels()

        # Create timestamp for file naming
        self.timestamp_str = datetime.now(tz=ZoneInfo("UTC")).strftime("%Y%m%d_%H%M%S")

        # Save initial data
        self.save_fibo_levels_to_csv()

        # State tracking
        self.last_trading_times_update = time.time()  # For periodic updates
        self.update_state()
        self.save_state_data_to_csv()

    def initialize(self):
        """Initialize the ON strategy."""
        self.last_trading_times_update = time.time()
        super().initialize()

    def _setup_order_timing(self):
        """Set up order timing for ON strategy."""
        super()._setup_order_timing()

        if self.order_id is None:  # Only update if no active order
            self.close_time += timedelta(days=1)

            # Adjust for weekends
            if self.close_time.weekday() >= 5:
                days_to_add = (7 - self.close_time.weekday()) + (0 if self.close_time.weekday() == 6 else 1)
                self.close_time += timedelta(days=days_to_add)

    def _calculate_fibonacci_levels(self):
        """Calculate all Fibonacci levels based on the base and top prices."""
        # Input validation
        if self.nti <= self.nbi:
            raise ValueError("'nti' must be greater than 'nbi' for Fibonacci calculations")

        # Calculate price range
        size_ = self.nti - self.nbi

        # Store all Fibonacci levels in a dictionary
        self.fibo_levels = {
            'fibo_base': self.nbi,
            'fibo_38': self.nbi + 0.382 * size_,
            'fibo_50': self.nbi + 0.50 * size_,
            'fibo_61': self.nbi + 0.618 * size_,
            'fibo_top': self.nti,
            'fibo_138': self.nbi + 1.382 * size_,
            'fibo_150': self.nbi + 1.50 * size_,
            'fibo_161': self.nbi + 1.618 * size_,
            'fibo_200': self.nbi + 2.0 * size_,
            'fibo_n200': self.nbi - 1.0 * size_,
            'fibo_n161': self.nbi - 0.618 * size_,
            'fibo_n138': self.nbi - 0.382 * size_
        }

        # Also set individual attributes for backward compatibility
        for key, value in self.fibo_levels.items():
            setattr(self, key, value)

    def save_fibo_levels_to_csv(self):
        """Save Fibonacci levels to CSV with timestamp."""
        fibo_data = {
            'price_name': list(self.fibo_levels.keys()) + ['base_price', 'top_price'],
            'price_values': list(self.fibo_levels.values()) + [self.nbi, self.nti]
        }

        df = pd.DataFrame(fibo_data)
        filename = f"fibo_levels_{self.timestamp_str}.csv"
        df.to_csv(filename, index=False)
        logger.info(f"Saved Fibonacci levels to {filename}")

    def save_state_data_to_csv(self):
        """Save state arrays to CSV with timestamp."""
        if not self.Zs or len(self.Zs) != len(self.Zdts) or len(self.Zs) != len(self.Zs_close):
            logger.warning("State arrays are empty or of unequal length - skipping CSV export")
            return

        state_data = {
            'Z': self.Zs,
            'Zdt': self.Zdts,
            'Z_close': self.Zs_close
        }

        df = pd.DataFrame(state_data)
        filename = f"state_data_{self.timestamp_str}.csv"
        df.to_csv(filename, index=False)
        logger.info(f"Saved state data to {filename}")

    def update_state(self, verbose=False):
        """Update state based on market data."""
        # Determine date range for the data fetch
        if len(self.Zs) == 0:
            start_date = "2025-01-01"
        else:
            # Get last date and go back 3 days to ensure overlap
            start_date = (self.Zdts[-1] - timedelta(days=3)).strftime("%Y-%m-%d")

        end_date = datetime.now().strftime("%Y-%m-%d")

        # Download the data
        try:
            data = yf.download(TICKER_SYMBOL, start=start_date, end=end_date, interval="1h")
            if data.empty:
                logger.warning("No data returned from Yahoo Finance")
                return
        except Exception as e:
            logger.error(f'Yahoo Finance API error: {e}')
            return

        # Process the data
        df = data.reset_index()
        df.columns = ['Datetime', 'Close', 'High', 'Low', 'Open', 'Volume']
        df_dict = df.to_dict(orient='records')

        # Convert times to NYC timezone
        nyc_tz = pytz.timezone('America/New_York')
        for item in df_dict:
            item['Datetime'] = item['Datetime'].to_pydatetime().astimezone(nyc_tz)

        # Handle overlap with existing data
        if len(self.Zs):
            # Find where to start in the existing data (avoid duplicates)
            for idx, Zdt in enumerate(self.Zdts):
                if Zdt >= df_dict[0]['Datetime']:
                    break
            else:
                idx = 0

            # Truncate existing data to avoid duplicates
            if idx > 0:
                self.Zs = self.Zs[:idx]
                self.Zdts = self.Zdts[:idx]
                self.Zs_close = self.Zs_close[:idx]

        # Initialize state tracking variables
        if len(self.Zs):
            Z_close_prev = self.Zs_close[-1]
            Z = self.Zs[-1]
        else:
            Z_close_prev, Z = 0.0, 0

        # Process each data point
        for item in df_dict:
            Z = self._compute_state(ref_price=item['Close'], prev_ref_price=Z_close_prev, Z=Z)
            Z_close_prev = item['Close']
            self.Zs.append(Z)
            self.Zdts.append(item['Datetime'])
            self.Zs_close.append(item['Close'])

    def _compute_state(self, ref_price: float, prev_ref_price: float, Z: int, verbose=False):
        """Compute state based on price crossing Fibonacci levels."""
        # Use dictionary of transition conditions to simplify code
        bearish_transitions = [
            (self.fibo_n200, -5),
            (self.fibo_n161, -3),
            (self.fibo_n138, -1),
            (self.fibo_base, 1),
            (self.fibo_38, 3),
            (self.fibo_50, 5),
            (self.fibo_61, 7),
            (self.fibo_top, 9),
            (self.fibo_138, 11),
            (self.fibo_161, 13),
            (self.fibo_200, 15)
        ]

        bullish_transitions = [
            (self.fibo_200, 16),
            (self.fibo_161, 14),
            (self.fibo_138, 12),
            (self.fibo_top, 10),
            (self.fibo_61, 8),
            (self.fibo_50, 6),
            (self.fibo_38, 4),
            (self.fibo_base, 2),
            (self.fibo_n138, 0),
            (self.fibo_n161, -2),
            (self.fibo_n200, -4)
        ]

        # Check bearish transitions (crossing down)
        for level, new_state in bearish_transitions:
            if ref_price < level and prev_ref_price >= level:
                Z = new_state
                break

        # Check bullish transitions (crossing up)
        for level, new_state in bullish_transitions:
            if ref_price > level and prev_ref_price <= level:
                Z = new_state
                break

        if verbose:
            logger.debug(f"Price: {ref_price:.2f} Prev: {prev_ref_price:.2f} State: {Z}")

        return Z

    def _scan_market_for_order(self):
        super()._scan_market_for_order()
        if self.order_id is None : # Update only if there is no market order.
            self.close_time += timedelta(days=1)
            # Adjust for weekends (assuming trading on Monday if order falls on Saturday/Sunday)
            if self.close_time.weekday() >= 5:
                days_to_add = (7 - self.close_time.weekday())
                self.close_time += timedelta(days=days_to_add)

    def valid_state(self):
        """Check if current state allows for trading."""
        return self.Z not in self.V

    def on_tick(self, symbol, bid, ask):
        """Process tick data for ON strategy."""
        # Call parent implementation
        super().on_tick(symbol, bid, ask)

        # Check if it's time to update state (every 2 hours)
        current_time = time.time()
        if current_time - self.last_trading_times_update > 2 * 3600:
            self.last_trading_times_update = current_time

            # Update state
            self.update_state()

            # Get yesterday's state at market close
            nyc_yesterday_milestone_date = get_nyc_yesterday_milestone_date()

            # Find the closest state to the milestone date
            dt_Z_pairs = list(zip(self.Zdts, self.Zs))
            dt_Z_pairs.sort(key=lambda x: abs((x[0] - nyc_yesterday_milestone_date).total_seconds()))

            if dt_Z_pairs:
                self.Z = dt_Z_pairs[0][1]
                logger.info(f"New Z state: {self.Z}")
                self.save_state_data_to_csv()
            else:
                logger.warning("No state data available to update Z")


def main():
    """Main function to start the trading system."""
    # MT4 files directory path
    MT4_FILES_DIR = r'C:\Users\maxim\AppData\Roaming\MetaQuotes\Terminal\33F10EB7DA1E64855A7E700316574D86\MQL4\Files'

    # Create tick processor
    processor = TickProcessor(MT4_FILES_DIR)

    # Create strategies
    td_strat = TDCassandraTickProcessor()
    on_strat = ONCassandraTickProcessor()

    # Register strategies
    processor.set_strategy(td_strat)
    processor.set_strategy(on_strat)

    # Main loop
    try:
        logger.info("Starting Cassandra trading system...")
        while processor.dwx.ACTIVE:
            sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down Cassandra trading system...")
    finally:
        processor.dwx.ACTIVE = False
        logger.info("Cassandra trading system stopped")


if __name__ == "__main__":
    main()