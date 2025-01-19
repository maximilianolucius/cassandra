#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import datetime
import pytz
import json


# #### Load the dataset
data = pd.read_csv('./data/2022.csv', sep=';', parse_dates=['Date'], dayfirst=True)
data['Date'] = pd.to_datetime(data['Date'], utc=True)

# data.set_index('time', inplace=True)
data=data.loc[:, ['Date', 'open', 'high', 'low', 'close']]
data.rename(columns={'Date': 'time'}, inplace=True)
data.head(12)


# #### TD bot code
class TDTradingBot:
    def __init__(self, market_timezone='Europe/Madrid', item='spxm', size=1, contract_size=10, verbose=False):
        """
        Initializes the TDTradingBot.

        :param market_timezone: The timezone of the market (default is Europe/Madrid).
        :param item: The trading instrument.
        :param size: The size of each order.
        """

        self.verbose = verbose
        self.market_timezone = pytz.timezone(market_timezone)
        self.buy_time = None
        self.close_time = None
        self.position_open = False
        self.last_candle_time = None
        self.set_trading_times(datetime.datetime.now(self.market_timezone))

        # Order tracking
        self.orders = []  # List to store all orders
        self.current_order = None  # Dictionary to store the current open order
        self.ticket_counter = 1  # Simple counter for generating unique tickets
        self.contract_size = contract_size
        self.item = item  # Trading instrument
        self.size = size  # Order size

        # Fee assumptions (can be parameterized)
        self.commission = 3.0
        self.taxes = 0.0  # Assume no taxes for simplicity
        self.swap = 0.0  # Assume no swap for simplicity

    def generate_ticket(self):
        """
        Generates a unique ticket number for each order.

        :return: Unique ticket number as an integer.
        """
        ticket = self.ticket_counter
        self.ticket_counter += 1
        return ticket

    def set_trading_times(self, current_time, verbose=False):
        """
        Sets the buy and sell times based on whether it's summer or winter time.

        :param current_time: Current datetime with timezone.
        """

        ts = current_time.astimezone(self.market_timezone)
        ny_timezone = pytz.timezone('America/New_York')
        aware_datetime = ny_timezone.localize(datetime(ts.year, ts.month, ts.day, 9+2+1, 0))
        self.buy_time = aware_datetime.astimezone(self.market_timezone).time()
        aware_datetime = ny_timezone.localize(datetime(ts.year, ts.month, ts.day, 9+7+1, 55))
        self.sell_time = aware_datetime.astimezone(self.market_timezone).time()

        if verbose:
            print(f"[TDTradingBot] Buy Time: {self.buy_time}, Sell Time: {self.sell_time}")

        return


    def receive_hourly_candle(self, candle):
        """
        Receives an hourly candle and decides whether to buy or sell.

        :param candle: A dictionary containing candle data with 'time' and 'close' prices.
                       Example: {'time': datetime_object, 'close': price}
        """
        candle_time = candle['time'].astimezone(self.market_timezone)
        candle_hour = candle_time.time()
        self.last_candle_time = candle_time

        # Update trading times in case of DST change
        self.set_trading_times(candle_time)

        # Check if it's time to buy
        if candle_hour == self.buy_time and not self.position_open:
            self.execute_order('BUY', candle['close'], candle_time)
            self.position_open = True

        # Check if it's time to sell
        elif candle_hour == self.close_time and self.position_open:
            self.execute_order('STOP', candle['close'], candle_time)
            self.position_open = False

    def receive_tick(self, tick):
        """
        Receives a tick and can be used to handle order execution at tick level.
        Currently, it just prints the tick. This can be expanded based on requirements.

        :param tick: A dictionary containing tick data with 'time' and 'price'.
                     Example: {'time': datetime_object, 'price': price}
        """
        tick_time = tick['time'].astimezone(self.market_timezone)
        # Placeholder for tick processing logic
        print(f"Tick received at {tick_time}: Price={tick['price']}")
        # Implement tick-level order handling if needed

    def execute_order(self, order_type, price, time):
        """
        Executes a buy or sell order and records it in the orders list.

        :param order_type: 'BUY' or 'SELL'.
        :param price: The price at which the order is executed.
        :param time: The datetime when the order is executed.
        """
        if order_type == 'BUY':
            # Create a new order
            order = {
                'Ticket': self.generate_ticket(),
                'Open Time': time,
                'Type': order_type,
                'Size': self.size,
                'Item': self.item,
                'Price': price,
                'S/L': None,  # Can be set based on strategy
                'T/P': None,  # Can be set based on strategy
                'Close Time': None,
                'Close Price': None,
                'Commission': self.commission,
                'Taxes': self.taxes,
                'Swap': self.swap,
                'Profit': None
            }
            self.current_order = order
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Executing BUY order: {order}")
        elif order_type == 'STOP' and self.current_order is not None:
            # Close the current order
            open_price = self.current_order['Price']
            profit_raw = (price - open_price) * self.size if self.current_order['Type'] == 'BUY' else (
                          open_price - price) * self.size
            self.current_order.update({
                'Close Time': time,
                'Close Price': price,
                'Profit': profit_raw - self.commission * self.size,
                'Commission': self.commission * self.size,
                'Taxes': self.taxes,
                'Swap': self.swap
            })
            self.orders.append(self.current_order)

            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {self.market_timezone} - Executing CLOSE order: {self.current_order}")

    def run(self, candles, ticks):
        """
        Simulates running the bot with streams of candles and ticks.
        This is a simple simulation where candles and ticks are processed in order.

        :param candles: List of candle dictionaries.
        :param ticks: List of tick dictionaries.
        """
        # Sort candles and ticks by time
        candles_sorted = sorted(candles, key=lambda x: x['time'])
        ticks_sorted = sorted(ticks, key=lambda x: x['time'])

        i, j = 0, 0
        while i < len(candles_sorted) or j < len(ticks_sorted):
            next_candle_time = candles_sorted[i]['time'] if i < len(candles_sorted) else None
            next_tick_time = ticks_sorted[j]['time'] if j < len(ticks_sorted) else None

            if next_candle_time and (not next_tick_time or next_candle_time <= next_tick_time):
                self.receive_hourly_candle(candles_sorted[i])
                i += 1
            elif next_tick_time:
                self.receive_tick(ticks_sorted[j])
                j += 1

    def get_orders(self):
        """
        Returns the list of all executed orders.

        :return: List of order dictionaries.
        """
        return self.orders

    def save_orders_to_json(self, filename):
        """
        Saves all executed orders to a JSON file.

        :param filename: The name of the JSON file to save orders.
        """
        try:
            with open(filename, 'w') as f:
                # Convert datetime objects to ISO format strings
                orders_serializable = []
                for order in self.orders:
                    serializable_order = {}
                    for key, value in order.items():
                        if isinstance(value, datetime.datetime):
                            serializable_order[key] = value.isoformat()
                        else:
                            serializable_order[key] = value
                    orders_serializable.append(serializable_order)

                json.dump(orders_serializable, f, indent=4)
            print(f"All orders have been saved to {filename}")
        except Exception as e:
            print(f"An error occurred while saving orders to JSON: {e}")
            


# #### Run the dataset

# In[ ]:


bot = TDTradingBot()

for idx, row in data.iterrows():
    bot.receive_hourly_candle(row)

# Retrieve and print all orders
orders = bot.get_orders()
print("\nAll Executed Orders:")
for order in orders:
    print(order)


# #### Save orders to json file
bot.save_orders_to_json('executed_orders.json')

pd.DataFrame(orders).to_csv('2022_orders_td.csv', index=False)




