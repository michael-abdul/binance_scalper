# backtest/run.py
from backtest.engine import run_backtest
from backtest.report import save_csv, print_summary
from pathlib import Path

SYMBOLS = [
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
    "ADAUSDT","DOGEUSDT","AVAXUSDT","LINKUSDT","MATICUSDT",
]

if __name__ == "__main__":
    Path("results").mkdir(exist_ok=True)

    all_trades = run_backtest(SYMBOLS, data_dir="data", interval="1m")

    for symbol, trades in all_trades.items():
        save_csv(trades, f"results/{symbol}.csv")

    print_summary(all_trades)