# Data manipulation and analysis
import pandas as pd
import numpy as np

# Plotting libraries
import plotly.graph_objects as go
import holoviews as hv
import datashader as ds
from holoviews.operation.datashader import datashade, rasterize
from holoviews import opts
from bokeh.plotting import show
import pyqtgraph as pg
from pyqtgraph.Qt import QtWidgets
from pyqtgraph import AxisItem

# System and utilities
import subprocess
import os
import datetime
# Load the Bokeh extension for Holoviews
hv.extension('bokeh')

PRICE_WIDTH = 2
TRADE_SIZE = 6

class NanoDatetimeAxisItem(AxisItem):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def tickStrings(self, values, scale, spacing):
        return [
            datetime.datetime.fromtimestamp(v, tz=datetime.timezone.utc).strftime('%H:%M:%S.%f')[:-3]
            for v in values
        ]
def check_file_exists(file_path):
    """Check if a file exists at the given path.
    
    Args:
        file_path (str): Path to the file to check
        
    Returns:
        bool: True if file exists, False otherwise
    """
    exists = os.path.exists(file_path)
    print(f'File {file_path} exists: {exists}')
    return exists

def handle_duplicate_timestamps(df, ts_col):
    """Handles duplicate timestamps by adding incremental offsets.
    
    Args:
        df (pd.DataFrame): DataFrame containing timestamp column
        ts_col (str): Name of timestamp column to process
        
    Returns:
        pd.DataFrame: DataFrame with unique timestamps
    """
    # Create a copy to avoid modifying original
    df = df.copy()
    
    # Find groups of duplicate timestamps
    duplicate_groups = df[df[ts_col].duplicated(keep=False)].groupby(ts_col)
    
    # Add incremental offset to duplicates within each group
    for _, group in duplicate_groups:
        indices = group.index[1:] # Skip first occurrence
        df.loc[indices, ts_col] += np.arange(1, len(indices) + 1)
        
    return df

def handle_trade_df(trade_df):
    # Extract data from the 'result' field for each row
    try:
        trade_df['data'] = [eval(i) for i in trade_df['data']]
    except:
        pass
    results = [i['result'] for i in trade_df['data']]
    
    # Create new columns directly from the extracted data
    trade_df['trade_price'] = [float(r['price']) for r in results]
    trade_df['trade_side'] = [r['side'] for r in results] 
    trade_df['trade_size'] = [float(r['amount']) for r in results]
    trade_df['exchange_ts_ns'] = [int(float(r['create_time_ms'])) * 1_000_000 for r in results]
    
    # Calculate latency metrics
    trade_df['data_latency_ns'] = trade_df['timestamp'] - trade_df['exchange_ts_ns']
    trade_df['data_latency_ms'] = trade_df['data_latency_ns'] / 1_000_000
    
    # Handle duplicate timestamps and set index
    trade_df = handle_duplicate_timestamps(trade_df, 'exchange_ts_ns')
    trade_df.set_index(pd.to_datetime(trade_df['timestamp'].to_list(), unit='ns'), inplace=True)
    
    return trade_df

def handle_book_ticker_df(book_ticker_df):
    # Extract data from the 'result' field for each row
    try:
        book_ticker_df['data'] = [eval(i) for i in book_ticker_df['data']]
    except:
        pass
    results = [i['result'] for i in book_ticker_df['data']]
    
    # Create new columns directly from the extracted data
    book_ticker_df['bid_price'] = [float(r['b']) for r in results]
    book_ticker_df['ask_price'] = [float(r['a']) for r in results]
    book_ticker_df['exchange_ts_ns'] = [int(float(r['t'])) * 1_000_000 for r in results]
    
    # Calculate latency metrics
    book_ticker_df['data_latency_ns'] = book_ticker_df['timestamp'] - book_ticker_df['exchange_ts_ns']
    book_ticker_df['data_latency_ms'] = book_ticker_df['data_latency_ns'] / 1_000_000
    
    # Handle duplicate timestamps and set index
    book_ticker_df = handle_duplicate_timestamps(book_ticker_df, 'exchange_ts_ns')
    book_ticker_df.set_index(pd.to_datetime(book_ticker_df['timestamp'].to_list(), unit='ns'), inplace=True)
    
    return book_ticker_df

def plot(trade_df, book_ticker_df):
    import sys

    # Sort and fix timestamps
    def make_monotonic(index):
        x = index.astype('int64')
        for i in range(1, len(x)):
            if x[i] <= x[i - 1]:
                x[i] = x[i - 1] + 1
        return pd.to_datetime(x)
    pg.setConfigOption('background', 'w') 
    trade_df = trade_df.sort_index()
    book_ticker_df = book_ticker_df.sort_index()

    book_ticker_df.index = make_monotonic(book_ticker_df.index)
    trade_df.index = make_monotonic(trade_df.index)
    book_ticker_df.index  = pd.to_datetime(book_ticker_df.index.to_list(), unit='ns', utc=True)
    trade_df.index = pd.to_datetime(trade_df.index.to_list(), unit='ns', utc=True)
    # Convert index to seconds for plotting
    x_price = book_ticker_df.index
    y_bid = book_ticker_df['bid_price'].values
    y_ask = book_ticker_df['ask_price'].values

    x_trades = trade_df.index
    y_trades = trade_df['trade_price'].values
    colors = [
    (0, 255, 0, 150) if side == 'buy' else (255, 0, 0, 150)
    for side in trade_df['trade_side']
        ]

    # PyQtGraph application
    app = QtWidgets.QApplication([])

    win = pg.GraphicsLayoutWidget(title="BTCUSDT Tick Viewer")
    win.resize(1600, 800)
    win.setWindowTitle('BTCUSDT Tick Viewer - PyQtGraph')

    plot = win.addPlot(title="Bid/Ask Price + Trades", row=0, col=0)
    plot.showGrid(x=True, y=True)

    # Bid / Ask lines
    plot.plot(x_price, y_bid, pen=pg.mkPen('b', width=PRICE_WIDTH), name="Bid")
    plot.plot(x_price, y_ask, pen=pg.mkPen('orange', width=PRICE_WIDTH), name="Ask")

    # Trades as scatter
    scatter = pg.ScatterPlotItem(x=x_trades, y=y_trades, brush=colors, size=TRADE_SIZE, pen=None)
    plot.addItem(scatter)

    # Enable pan/zoom
    plot.setMouseEnabled(x=True, y=True)

    # Show
    win.show()
    sys.exit(app.exec_())

def plot_multi(symbol_data_dict):
    import sys

    pg.setConfigOption('background', 'w')
    pg.setConfigOption('foreground', 'k')

    app = QtWidgets.QApplication([])
    win = pg.GraphicsLayoutWidget(title="Multi-Symbol Tick Viewer")
    win.resize(1600, 900)
    win.setWindowTitle('Multi-Symbol Tick Viewer - PyQtGraph')

    first_plot = None

    def make_monotonic(index):
        x = index.astype('int64')
        for i in range(1, len(x)):
            if x[i] <= x[i - 1]:
                x[i] = x[i - 1] + 1
        return x

    for idx, (symbol, (trade_df, book_ticker_df)) in enumerate(symbol_data_dict.items()):
        trade_df = trade_df.sort_index()
        book_ticker_df = book_ticker_df.sort_index()

        book_ticker_df.index = make_monotonic(book_ticker_df.index)
        trade_df.index = make_monotonic(trade_df.index)
        book_ticker_df.index  = book_ticker_df.index.astype('int64') / 1e9
        trade_df.index = trade_df.index.astype('int64') / 1e9
        bbo_index = book_ticker_df.index
        bbo_bid = book_ticker_df['bid_price'].values
        bbo_ask = book_ticker_df['ask_price'].values

        trade_index = trade_df.index
        trade_price = trade_df['trade_price'].values
        colors = [
            (0, 150, 0, 160) if side == 'buy' else (200, 0, 0, 160)
            for side in trade_df['trade_side']
        ]
        axis = NanoDatetimeAxisItem(orientation='bottom')
        # plot = win.addPlot(title="...", row=idx, col=0, axisItems={'bottom': axis})
        plot = win.addPlot(title=f"{symbol} Bid/Ask + Trades", row=idx, col=0, axisItems={'bottom': axis})
        plot.showGrid(x=True, y=True)

        if first_plot:
            plot.setXLink(first_plot)  # share X axis
        else:
            first_plot = plot

        plot.plot(bbo_index, bbo_bid, pen=pg.mkPen('blue', width=PRICE_WIDTH), name="Bid")
        plot.plot(bbo_index, bbo_ask, pen=pg.mkPen('orange', width=PRICE_WIDTH), name="Ask")

        scatter = pg.ScatterPlotItem(x=trade_index, y=trade_price, brush=colors, size=TRADE_SIZE, pen=None)
        plot.addItem(scatter)
        

        plot.setMouseEnabled(x=True, y=True)

    win.show()
    sys.exit(app.exec_())

def download_data(exchange: str, symbols: list[str], date: str) -> None:
    """
    Downloads trade and book ticker data files from a remote server using rsync.
    
    Args:
        exchange (str): Name of the exchange (e.g. 'gateio')
        symbols (list[str]): List of trading pairs to download data for
        date (str): Date string in YYYYMMDD format
        
    The function:
    1. Creates local directory structure if needed
    2. Builds list of trade and book ticker filenames to download
    3. Downloads files that don't already exist locally using rsync
    4. Handles download errors gracefully
    """
    # Setup paths
    remote_host = 'root@172.104.74.179'
    remote_dir = f'/home/md_recorder_py/data/{exchange}/{date}/'
    local_dir = f'./data/{exchange}/{date}/'
    os.makedirs(local_dir, exist_ok=True)

    # Build list of files to download
    remote_filenames = []
    for symbol in symbols:
        filenames = [
            f"spot.trades_v2.{symbol}_{date}.parquet",
            f"spot.book_ticker.{symbol}_{date}.parquet"
        ]
        remote_filenames.extend(filenames)

    # Download files that don't exist locally
    for filename in remote_filenames:
        local_path = os.path.join(local_dir, filename)
        
        if check_file_exists(local_path):
            print(f"File {filename} already exists in {local_dir}")
            continue
            
        rsync_command = [
            'rsync',
            '-avz',
            f'{remote_host}:{remote_dir}{filename}',
            local_dir
        ]
        
        try:
            subprocess.run(rsync_command, check=True)
            print(f"Successfully downloaded {filename}")
        except subprocess.CalledProcessError as e:
            print(f"Error downloading {filename}: {e}")
   

def preprocess_data(exchange: str, symbols: list[str], date: str) -> dict[str, tuple[pd.DataFrame, pd.DataFrame]]:
    """
    Preprocesses trade and book ticker data for multiple trading symbols.
    
    Args:
        exchange (str): Name of the exchange (e.g. 'gateio')
        symbols (list[str]): List of trading pairs to process
        date (str): Date string in YYYYMMDD format
        
    Returns:
        dict: Dictionary mapping symbols to tuples of (trade_df, book_ticker_df)
              where each dataframe contains processed market data
    """
    data_dir = f'./data/{exchange}/{date}/'
    symbol_data = {}
    
    for symbol in symbols:
        # Construct filenames for trade and book ticker data
        trade_file = f"spot.trades_v2.{symbol}_{date}.parquet"
        book_file = f"spot.book_ticker.{symbol}_{date}.parquet"
        
        # Load raw data from parquet files
        trade_df = pd.read_parquet(f'{data_dir}{trade_file}')
        book_ticker_df = pd.read_parquet(f'{data_dir}{book_file}')
        
        # Process the raw data
        trade_df = handle_trade_df(trade_df)
        book_ticker_df = handle_book_ticker_df(book_ticker_df)
        
        # Store processed dataframes
        symbol_data[symbol] = (trade_df, book_ticker_df)
        
    return symbol_data
    
def main(exchange: str, symbols: list[str], date: str) -> None:
    """
    Main function to process and visualize trading data for multiple symbols.

    Args:
        exchange (str): Name of the exchange (e.g. 'gateio')
        symbols (list[str]): List of trading pairs to analyze (e.g. ['BTC_USDT'])
        date (str): Date string in YYYYMMDD format (e.g. '20250418')

    The function:
    1. Preprocesses trade and book ticker data for each symbol
    2. Creates interactive visualizations of the data
    """
    # Get processed trade and book ticker data for all symbols
    download_data(exchange, symbols, date)
    symbol_data_dict = preprocess_data(exchange, symbols, date)
    
    # Generate interactive plots for the data
    plot_multi(symbol_data_dict)

if __name__ == "__main__":
    exchange = "gateio"
    symbols = ["BTC_USDT", "BTC_USDC", "USDC_USDT"]
    date = "20250418"
    main(exchange, symbols, date)








