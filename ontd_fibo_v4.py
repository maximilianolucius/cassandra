import copy

import pandas as pd
from datetime import datetime, time
import pytz
import json


DEBUG = False
DEBUG_SESSION_LEVEL = True

data = pd.read_csv('./data/2022.csv', sep=';', parse_dates=['Date'], dayfirst=True)
data['Date'] = pd.to_datetime(data['Date'], utc=True)
data=data.loc[:, ['Date', 'open', 'high', 'low', 'close']]
data.rename(columns={'Date': 'time'}, inplace=True)

data_collector = []
for idx, row in data.iterrows():
    data_collector.append(row.to_dict())
    ts = row['time']
    candle_time = row['time'].astimezone('Europe/Madrid')
    candle_hour = candle_time.time()

    # if candle_hour.hour == 22:
    #     r = row.to_dict()
    #     r['open'] = r['close']
    #     r['time'] += pd.Timedelta(hours=1)
    #     data_collector.append(r)

data = pd.DataFrame(data_collector)
data.head(10)


if DEBUG:
    data = data.iloc[:100]



# data = data.iloc[:160]
# data.tail(10)



ny_timezone = pytz.timezone('America/New_York')


aware_datetime = ny_timezone.localize(datetime(2023, 12, 26, 10, 5))
print(aware_datetime)  # Output: 2023-12-26 10:05:00-05:00


utc_timezone = pytz.timezone('UTC')
utc_datetime = aware_datetime.astimezone(utc_timezone)
print("UTC Time:", utc_datetime)


# #### ON bot code
class ONTradingBot:
    def __init__(
            self,
            market_timezone='UTC', # 'Europe/Madrid',
            item='spxm',
            size=1,
            contract_size=1, # Debe ser 10.
            nbi: float = 3594.52,
            nti: float = 4808.93,
            Z: int = 9, # 14                  <---<<<
            V: list = [3,5,6,7,9], # Vector de inhibidores
            verbose: bool = True,
    ):
        """
        Initializes the ONTradingBot.

        :param market_timezone: The timezone of the market (default is Europe/Madrid).
        :param item: The trading instrument (default 'spxm').
        :param size: The size (number of contracts) per order (default 1).
        :param contract_size: Multiplying factor for notional size (default 10).
        """

        help(pytz.timezone)

        self.market_timezone = pytz.timezone(market_timezone)
        self.size = size
        self.contract_size = contract_size
        self.item = item

        self.nbi = nbi
        self.nti = nti
        self.fibo_base = 0.0
        self.fibo_38 = 0.0
        self.fibo_50 = 0.0
        self.fibo_61 = 0.0
        self.fibo_top = 0.0
        self.fibo_138 = 0.0
        self.fibo_150 = 0.0
        self.fibo_161 = 0.0
        self.fibo_200 = 0.0
        self._prev_ref_price = 0.0

        self.Z = Z # current zone (hourley updated)
        self.V = V  # Vector de inhibidores.

        # Commission and fees (adjust as necessary)
        self.commission = 3.0
        self.taxes = 0.0
        self.swap = 0.0

        # Internal state
        self.position_open = False
        self.current_order = None
        self.orders = []
        self.ticket_counter = 1
        self.last_candle_time = None
        self.open_date = None  # Will store the date on which we open a position

        # Initialize buy and sell times based on current date/time
        self.buy_time = None
        self.sell_time = None
        self.set_trading_times(datetime.now(self.market_timezone), verbose=True)

        self.verbose = verbose
        self.prev_session_last_candle_zone = Z
        self.current_session_last_candle_zone = Z
        self.session_log = {}
        self.session_log_collector = []
        self.status_collector = []

        self._calc_fibo_levels()


    def _calc_fibo_levels(self):
        size_ = self.nti - self.nbi
        self.fibo_base = self.nbi
        self.fibo_38 = self.nbi + 0.382 * size_
        self.fibo_50 = self.nbi + 0.50 * size_
        self.fibo_61 = self.nbi + 0.618 * size_
        self.fibo_top = self.nti
        self.fibo_138 = self.nbi + 1.382 * size_
        self.fibo_150 = self.nbi + 1.50 * size_
        self.fibo_161 = self.nbi + 1.618 * size_
        self.fibo_200 = self.nbi + 2.0 * size_

        self._prev_ref_price = 10000

        if self.verbose:
            # Print the calculated Fibonacci levels to the terminal.
            print("Calculated Fibonacci Levels:")
            print(f" Base:   {self.fibo_base}")
            print(f" 38.2%:  {self.fibo_38}")
            print(f" 50.0%:  {self.fibo_50}")
            print(f" 61.8%:  {self.fibo_61}")
            print(f" Top:    {self.fibo_top}")
            print(f"138.2%:  {self.fibo_138}")
            print(f"150.0%:  {self.fibo_150}")
            print(f"161.8%:  {self.fibo_161}")
            print(f"200.0%:  {self.fibo_200}")


    def update_state(self, ref_price: float):
        if ref_price < self.fibo_base and self._prev_ref_price >= self.fibo_base:
            self.Z = 1
        elif ref_price < self.fibo_38 and self._prev_ref_price >= self.fibo_38:
            self.Z = 3
        elif ref_price < self.fibo_50 and self._prev_ref_price >= self.fibo_50:
            self.Z = 5
        elif ref_price < self.fibo_61 and self._prev_ref_price >= self.fibo_61:
            self.Z = 7
        elif ref_price < self.fibo_top and self._prev_ref_price >= self.fibo_top:
            self.Z = 9
        elif ref_price < self.fibo_138 and self._prev_ref_price >= self.fibo_138:
            self.Z = 11
        elif ref_price < self.fibo_161 and self._prev_ref_price >= self.fibo_161:
            self.Z = 13
        elif ref_price < self.fibo_161 and self._prev_ref_price >= self.fibo_161:
            self.Z = 13
        elif ref_price < self.fibo_200 and self._prev_ref_price >= self.fibo_200:
            self.Z = 15

        if ref_price > self.fibo_200 and self._prev_ref_price <= self.fibo_200:
            self.Z = 16
        elif ref_price > self.fibo_161 and self._prev_ref_price <= self.fibo_161:
            self.Z = 14
        elif ref_price > self.fibo_138 and self._prev_ref_price <= self.fibo_138:
            self.Z = 12
        elif ref_price > self.fibo_top and self._prev_ref_price <= self.fibo_top:
            self.Z = 10
        elif ref_price > self.fibo_61 and self._prev_ref_price <= self.fibo_61:
            self.Z = 8
        elif ref_price > self.fibo_50 and self._prev_ref_price <= self.fibo_50:
            self.Z = 6
        elif ref_price > self.fibo_38 and self._prev_ref_price <= self.fibo_38:
            self.Z = 4
        elif ref_price > self.fibo_base and self._prev_ref_price <= self.fibo_base:
            self.Z = 2

        # Verbose logging if enabled
        if self.verbose and False:
            print(f"Reference Price: {ref_price}")
            print(f"Previous Hourly Open Price: {self._prev_ref_price}")
            print(f"Updated State Z: {self.Z}")

            # Collect all Fibonacci levels in a dictionary for convenience
            fibo_levels = {
                "fibo_base": self.fibo_base,
                "fibo_38": self.fibo_38,
                "fibo_50": self.fibo_50,
                "fibo_61": self.fibo_61,
                "fibo_top": self.fibo_top,
                "fibo_138": self.fibo_138,
                "fibo_150": getattr(self, 'fibo_150', None),  # Optional, if needed
                "fibo_161": self.fibo_161,
                "fibo_200": self.fibo_200
            }

            # Calculate the distance of each Fibonacci level from the reference price
            distances = {level: abs(price - ref_price) for level, price in fibo_levels.items() if price is not None}

            # Sort Fibonacci levels by closeness to the reference price
            nearest_levels = sorted(distances.items(), key=lambda x: x[1])[:2]

            print("Nearest Fibonacci Levels:")
            for level, distance in nearest_levels:
                print(f" {level}: {fibo_levels[level]} (Distance: {distance})")

        self._prev_ref_price = ref_price


    def generate_ticket(self):
        """
        Generates a unique ticket number for each order.
        """
        ticket = self.ticket_counter
        self.ticket_counter += 1
        return ticket

    def set_trading_times(self, current_time, verbose=False):
        """
        Determines the buy and sell times for the ON strategy based on DST.

        Winter:  Buy at 21:45, Sell at 14:05 (next day)
        Summer:  Buy at 20:45, Sell at 13:05 (next day)
        """
        ts = current_time.astimezone(self.market_timezone)
        ny_timezone = pytz.timezone('America/New_York')
        aware_datetime = ny_timezone.localize(datetime(ts.year, ts.month, ts.day, 10, 5))
        aware_datetime = ny_timezone.localize(datetime(ts.year, ts.month, ts.day, 9, 0))
        self.sell_time = aware_datetime.astimezone(self.market_timezone).time()
        aware_datetime = ny_timezone.localize(datetime(ts.year, ts.month, ts.day, 15, 55))
        aware_datetime = ny_timezone.localize(datetime(ts.year, ts.month, ts.day, 16, 0))
        self.buy_time = aware_datetime.astimezone(self.market_timezone).time()

        if verbose:
            print(f"[ONTradingBot] Buy Time: {self.buy_time}, Sell Time: {self.sell_time}")

        return

    def update_fibo_zone(self, candle):
        ts = candle['time'].astimezone(self.market_timezone)
        self.update_state(candle['close'])

        self.status_collector.append({
            'Time': candle['time'],
            'Status': self.Z
        })


        ny_timezone = pytz.timezone('America/New_York')
        aware_datetime = ny_timezone.localize(datetime(ts.year, ts.month, ts.day, 16, 0))

        if ts == aware_datetime:
            # update last session
            self.prev_session_last_candle_zone = self.current_session_last_candle_zone
            self.current_session_last_candle_zone = self.Z

            self.session_log['Date'] = ts.strftime('%Y-%m-%d')
            self.session_log['K_Close'] = self.Z
            self.session_log_collector.append(copy.deepcopy(self.session_log))

            if self.verbose:
                print(f'ts: {ts}  open: {candle["open"]}  prev_session_last_candle_zone: {self.prev_session_last_candle_zone}  current_session_last_candle_zone: {self.current_session_last_candle_zone}')

        return


    def receive_5M_candle(self, candle):
        """
        Receives an hourly candle and decides whether to open or close the position.

        For ON strategy:
        - If not in a position, buy at self.buy_time.
        - If in a position, close (sell) the next day at self.sell_time.
        """
        candle_time = candle['time'].astimezone(self.market_timezone)
        candle_hour = candle_time.time()

        # Update DST-based schedule if the day changed (optional, in case DST toggles mid-year)
        self.set_trading_times(candle_time)


        # ----- Session level debug -----
        if DEBUG_SESSION_LEVEL:
            if candle_hour == self.buy_time:
                self.session_log['ON_Close'] = candle['close']
            if candle_hour == self.sell_time:
                self.session_log['ON_Open'] = candle['close']


        # ----- BUY LOGIC -----
        # If no position is open, check if it's the exact buy_time

        # print(f'self.position_open: {self.position_open}; candle_hour: {candle_hour}; self.buy_time: {self.buy_time}, {candle_time.date()} {self.open_date} {candle_hour}, self.sell_time: {self.sell_time}')
        if (not self.position_open) and (candle_hour == self.buy_time) and not (self.current_session_last_candle_zone in self.V): # prev_session_last_candle_zone
            self.execute_order('BUY', candle['close'], candle_time, self.current_session_last_candle_zone)
            self.open_date = candle_time.date()
            self.position_open = True

        # ----- SELL LOGIC -----
        # If a position is open, we only sell if:
        # 1) It's a different day than the open day (the "next day")
        # 2) The candle hour is the self.sell_time
        elif self.position_open:
            # Check if the date is strictly greater and the time is the sell_time
            if (candle_time.date() > self.open_date) and (candle_hour == self.sell_time):
                self.execute_order('STOP', candle['open'], candle_time, None)
                self.position_open = False
                self.open_date = None



        if candle_hour.minute == 0: # Only hourly candles.
            self.update_fibo_zone(candle)


    def receive_tick(self, tick):
        """
        Receives a tick (optional). Currently just prints the tick data.
        """
        tick_time = tick['time'].astimezone(self.market_timezone)
        print(f"[ONTradingBot] Tick @ {tick_time}: Price={tick['price']}; Z: {self.prev_session_last_candle_zone}")
        # In a more complex setup, you could use tick data for partial fills, slippage, etc.

    def execute_order(self, order_type, price, time, k):
        """
        Executes a buy or sell order and stores the order details.

        :param order_type: 'BUY' or 'SELL'
        :param price: Execution price
        :param time: Execution datetime
        """
        if order_type == 'BUY':
            # Create new order
            order = {
                'Ticket': self.generate_ticket(),
                'Open Time': time,
                'Type': order_type,
                'Size': self.size,
                'Item': self.item,
                'Price': price,
                'S/L': None,
                'T/P': None,
                'Close Time': None,
                'Close Price': None,
                'Commission': 0.0,
                'Taxes': 0.0,
                'Swap': 0.0,
                'raw_profit': None,
                'Profit': None,
                'K': k
            }
            self.current_order = order
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - [ONTradingBot] Executing BUY @ {price}; Z: {self.prev_session_last_candle_zone}")
        elif order_type == 'STOP' and self.current_order is not None:
            # Close existing order
            open_price = self.current_order['Price']
            # Calculate raw profit
            profit_raw = (price - open_price) * self.size * self.contract_size
            # Deduct commission per contract * size
            total_commission = self.commission * self.size
            # Update order fields
            self.current_order.update({
                'Close Time': time,
                'Close Price': price,
                'Profit': profit_raw - total_commission,
                'Commission': total_commission,
                'Taxes': self.taxes,
                'Swap': self.swap,
                'raw_profit': profit_raw
            })
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - [ONTradingBot] Executing SELL @ {price}")
            print(f"         Profit: {self.current_order['Profit']:.2f}")
            self.orders.append(self.current_order)
            self.current_order = None

    def run(self, candles, ticks):
        """
        Processes lists of candles and ticks in chronological order, simulating live operation.
        """
        candles_sorted = sorted(candles, key=lambda x: x['time'])
        ticks_sorted = sorted(ticks, key=lambda x: x['time'])

        i, j = 0, 0
        while i < len(candles_sorted) or j < len(ticks_sorted):
            next_candle_time = candles_sorted[i]['time'] if i < len(candles_sorted) else None
            next_tick_time = ticks_sorted[j]['time'] if j < len(ticks_sorted) else None

            # Decide which event (candle or tick) happens next in time
            if next_candle_time and (not next_tick_time or next_candle_time <= next_tick_time):
                self.receive_hourly_candle(candles_sorted[i])
                i += 1
            elif next_tick_time:
                self.receive_tick(ticks_sorted[j])
                j += 1

    def get_orders(self):
        """
        Returns a list of all closed orders.
        """
        return self.orders

    def save_orders_to_json(self, filename):
        """
        Saves all executed (closed) orders to a JSON file.
        """
        try:
            with open(filename, 'w') as f:
                orders_serializable = []
                for order in self.orders:
                    serializable_order = {}
                    for key, value in order.items():
                        # Convert datetime to ISO format for JSON
                        if isinstance(value, datetime):
                            serializable_order[key] = value.isoformat()
                        else:
                            serializable_order[key] = value
                    orders_serializable.append(serializable_order)

                json.dump(orders_serializable, f, indent=4)
            print(f"[ONTradingBot] All orders have been saved to {filename}")
        except Exception as e:
            print(f"[ONTradingBot] Error saving orders: {e}")



bot = ONTradingBot()


for idx, row in data.iterrows():
    bot.receive_5M_candle(row)

# Retrieve and print all orders
orders = bot.get_orders()
print("\nAll Executed Orders:")
total_profit = 0
for order in orders:
    # print(order)
    total_profit += order['Profit']
print(total_profit)

session_log_collector = bot.session_log_collector



bot.save_orders_to_json('executed_orders.json')
pd.DataFrame(orders).to_csv('order_ON.csv', index=False)


pd.DataFrame(orders).to_csv('ON-2022.csv', index=False)

len(orders)

slog_df = pd.DataFrame(session_log_collector).loc[:, ['Date', 'K_Close', 'ON_Open', 'ON_Close']]

orders_df = pd.DataFrame(orders)
orders_df['Open Time'] = pd.to_datetime(orders_df['Open Time'])
orders_df['Date'] = orders_df['Open Time'].dt.strftime('%Y-%m-%d')


df = slog_df.merge(orders_df, on='Date', how='outer')
df.to_csv('ON-2022-debug_log.csv', index=False)



print(slog_df.head(20))

status_df = pd.DataFrame(bot.status_collector)
status_df.to_csv('ON-2022-status.csv', index=False)

# 2025.01.18 06:25:10.806	CurrentTimeAETest SPXm,M5: Current time at initialization: 1737158099
#2025.01.18 06:30:47.838	CurrentTimeAETest SPXm,M5: Current time at initialization: 1737210647.000000

import time

# Get the current time in seconds since the Unix epoch
current_time_seconds = time.time()

# Print the result
print(current_time_seconds)
print((current_time_seconds - 1737210647.000000)/3600)
1737158099
1737210395
