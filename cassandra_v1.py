import numpy as np
import pandas as pd
from pandas import Timestamp


class FibonacciRetracement:
    def __init__(self, back_window_length: int):
        self.back_window_length = back_window_length

        # Initialize data as an empty DataFrame with Timestamp as index
        self.data = pd.DataFrame(columns=['Adj Close'])

        # Initialize additional members with null values (None)
        self.current_datetime = None
        self.support_price = None
        self.resistance_price = None
        self.support_datetime = None
        self.resistance_datetime = None
        self.fibo38 = None
        self.fibo68 = None

        self.fibonacci_retracement_collector = []

    def go(self, adj_close: np.float64, timestamp: pd.Timestamp):
        # Add the new value with the timestamp as index
        self.data.loc[timestamp] = adj_close
        self.current_datetime = timestamp

        if self.data.loc[timestamp].shape[0] < self.back_window_length:
            return

        if self.support_price is not None and self.resistance_price is not None:
            if adj_close > self.resistance_price: # Update resistance level
                self.resistance_price = adj_close
                self.resistance_datetime = timestamp
                self._compute_fibonacci_retracement()
                self._log() # Log new entry.
            elif adj_close < self.support_price:
                self.support_price = adj_close
                self.resistance_datetime = timestamp
                self._compute_fibonacci_retracement()
                self._log()  # Log new entry.
            else:
                #
                pass
        else:
            self.support_price = self.data['Adj Close'].min()
            self.resistance_price = self.data['Adj Close'].max()
            self.support_datetime = self.data['Adj Close'].idxmin()
            self.resistance_datetime = self.data['Adj Close'].idxmax()

            self._compute_fibonacci_retracement()

    def _compute_fibonacci_retracement(self):
        if self.support_datetime < self.resistance_datetime:
            self.fibo38 = self.support_price + (self.resistance_price - self.support_price) * 0.382
            self.fibo68 = self.support_price + (self.resistance_price - self.support_price) * 0.683
        else:
            self.fibo38 = self.resistance_price - (self.resistance_price - self.support_price) * 0.382
            self.fibo68 = self.resistance_price - (self.resistance_price - self.support_price) * 0.683

    def _log(self):
        data = {
            'support_price': self.support_price,
            'resistance_price': self.resistance_price,
            'support_datetime': self.support_datetime,
            'resistance_datetime': self.resistance_datetime,
            'fibo38': self.fibo38,
            'fibo68': self.fibo68,
            'trend': self._get_trend_label()
        }
        self.fibonacci_retracement_collector.append(data)
        return

    def _get_trend_label(self):
        if self.support_datetime < self.resistance_datetime:
            return 'Alcista'
        return 'Bajista'

    def write_log(self, fname: str='fibonacci_retracement_log.csv'):
        pd.DataFrame(self.fibonacci_retracement_collector).to_csv(fname, index=False)



# Example usage:
fibonacci = FibonacciRetracement(back_window_length=5)
fibonacci.go(np.float64(359.69000244140625), Timestamp('1990-01-02 00:00:00+0000', tz='UTC'))
print(fibonacci.data)
print(fibonacci.current_datetime)
