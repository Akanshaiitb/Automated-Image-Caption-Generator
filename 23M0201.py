import csv
import argparse
import os
from queue import Queue

def process_trades(trades):
    opening_trades = {}
    paired_trades = []
    total_pnl = 0

    for trade in trades:
        time, symbol, side, price, quantity = trade
        price = float(price)
        quantity = int(quantity)

        if side == 'B':
            # Opening trade
            opening_trades.setdefault(symbol, Queue()).put((time, quantity, price))
        else:
            # Closing trade
            if symbol in opening_trades:
                while quantity > 0 and not opening_trades[symbol].empty():
                    open_time, open_quantity, open_price = opening_trades[symbol].get()

                    paired_quantity = min(open_quantity, quantity)
                    pnl = round(paired_quantity * (price - open_price), 2)

                    paired_trades.append((open_time, time, symbol, paired_quantity, pnl, 'B', 'S', open_price, price))

                    total_pnl += pnl
                    quantity -= paired_quantity

                    if open_quantity > paired_quantity:
                        opening_trades[symbol].put((open_time, open_quantity - paired_quantity, open_price))

                if opening_trades[symbol].empty():
                    del opening_trades[symbol]

                # Check for excess quantity and create a new opening trade on the opposite side
                if quantity > 0:
                    opening_side = 'S' if side == 'B' else 'B'
                    opening_trades.setdefault(symbol, Queue()).put((time, quantity, price))

    return paired_trades, total_pnl

def main(path):
    with open(path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        trades = [row for row in reader]

    paired_trades, total_pnl = process_trades(trades)

    # Print paired trades
    for trade in paired_trades:
        print(','.join(map(str, trade)))

    # Print total PNL
    print(f"{total_pnl:.2f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('path', type=str)
    args = parser.parse_args()
    path = args.path

    if os.path.exists(path):
        print(f'Path: {path}')
    else:
        print(f'The specified path does not exist: {path}')

    main(path)