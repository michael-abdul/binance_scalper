# backtest/report.py
import csv
from backtest.engine import Trade


def save_csv(trades: list[Trade], path: str) -> None:
    if not trades:
        return
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol","side","entry_px","exit_px",
                    "qty","pnl","outcome","entry_ts","exit_ts"])
        for t in trades:
            w.writerow([t.symbol, t.side, t.entry_px, t.exit_px,
                        t.qty, round(t.pnl, 4), t.outcome,
                        t.entry_ts, t.exit_ts])


def print_summary(all_trades: dict[str, list[Trade]]) -> None:
    print("\n" + "="*60)
    print(f"{'SYMBOL':<12} {'TRADES':>7} {'WIN%':>7} "
          f"{'PNL':>10} {'MAX_DD':>10}")
    print("="*60)

    for symbol, trades in all_trades.items():
        if not trades:
            print(f"{symbol:<12} {'—':>7}")
            continue

        wins    = sum(1 for t in trades if t.pnl > 0)
        pnl     = sum(t.pnl for t in trades)
        winrate = wins / len(trades) * 100

        # Max drawdown
        equity, peak, max_dd = 10_000.0, 10_000.0, 0.0
        for t in trades:
            equity += t.pnl
            peak    = max(peak, equity)
            max_dd  = max(max_dd, peak - equity)

        print(f"{symbol:<12} {len(trades):>7} {winrate:>6.1f}% "
              f"{pnl:>10.2f} {max_dd:>10.2f}")

    print("="*60)