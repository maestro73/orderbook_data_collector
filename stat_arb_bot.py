#!/usr/bin/env python
# Imports
import pandas as pd
import datetime
import time

BITMEX_SYMBOL = 'BTC/USD.BX.PC'
OKEX_SYMBOL = 'BTC/USD.OK.PC'
BITMEX = 0
OKEX = 1
BITMEX_MAKER_FEES = -0.00025
BITMEX_TAKER_FEES = 0.00075
OKEX_MAKER_FEES = 0.0002
OKEX_TAKER_FEES = 0.0005
MIN_RATE = 1.001


def trade(exchange, buy_ask, sell_bid, buy_balance, sell_balance):
    buy_amount = buy_ask[0] * buy_ask[1]
    sell_amount = sell_bid[0] * sell_bid[1]
    trade_balance = min(buy_amount, sell_amount, buy_balance)
    trade_quantity = trade_balance / sell_bid[0]

    if exchange == BITMEX:  # buy at Bitmex and sell at Okex
        taker_fees = BITMEX_TAKER_FEES
        maker_fees = OKEX_MAKER_FEES
    else:   # buy at Okex and sell at Bitmex
        taker_fees = OKEX_TAKER_FEES
        maker_fees = BITMEX_MAKER_FEES

    pay_premium = buy_ask[0] * trade_quantity * (1 + taker_fees)
    buy_balance -= pay_premium
    receive_premium = sell_bid[0] * trade_quantity * (1 - maker_fees)
    sell_balance += receive_premium
    trade_profit = receive_premium - pay_premium

    print('Buy@ ' + ('Bitmex ' if not exchange else 'Okex ') + str(buy_ask[0]) + ' and Sell@' + ('Okex ' if not exchange else 'Bitmex ') + str(sell_bid[0]) +
          ' with quantity@' + str(trade_quantity) + ' at profit@' + str(trade_profit))

    return trade_profit, buy_balance, sell_balance


def main(date, profit, bitmex_balance, okex_balance):
    datestr = str(date.year) + ('0' if date.month < 10 else '') + str(date.month) + \
              ('0' if date.day < 10 else '') + str(date.day)

    data_Bidmex = pd.read_csv('Bitmex/Depth_' + datestr + '_btc_usd.csv')
    data_Okex = pd.read_csv('Okex/Depth_' + datestr + '_btc_usd.csv')

    # Concatenate data from two exchanges and sort by date.
    data = pd.concat([data_Bidmex, data_Okex])
    data['DateTime'] = data['DateTime'].apply(lambda _: datetime.datetime.strptime(_, '%Y-%m-%d %H:%M:%S.%f'))
    data = data.sort_values(by=['DateTime'])

    data_Bitmex_highest_bid = [None, 0]
    data_Bitmex_lowest_ask = [None, 0]
    data_Okex_highest_bid = [None, 0]
    data_Okex_lowest_ask = [None, 0]

    length = len(data)
    trade_count = 0
    pre_profit = profit
    for i in range(0, length):
        row = data.iloc[i, :]
        if row['Symbol'] == BITMEX_SYMBOL:
            data_Bitmex_highest_bid = [row['BidsPrice1'], row['BidsQuantity1']]
            data_Bitmex_lowest_ask = [row['AsksPrice1'], row['AsksQuantity1']]
        else:
            data_Okex_highest_bid = [row['BidsPrice1'], row['BidsQuantity1']]
            data_Okex_lowest_ask = [row['AsksPrice1'], row['AsksQuantity1']]

        if data_Bitmex_highest_bid[0] and data_Bitmex_lowest_ask[0] and \
                data_Okex_highest_bid[0] and data_Okex_lowest_ask[0]:

            # buy in Okex at lowest ask and sell in Bitmex at highest bid
            if data_Bitmex_highest_bid[0] > data_Okex_lowest_ask[0] and \
                    (data_Bitmex_highest_bid[0] / data_Okex_lowest_ask[0]) >= MIN_RATE:
                try:
                    if okex_balance > data_Okex_lowest_ask[0]:    # minimum trade size
                        temp = trade(1, data_Okex_lowest_ask, data_Bitmex_highest_bid, okex_balance, bitmex_balance)
                        profit += temp[0]
                        okex_balance = temp[1]
                        bitmex_balance = temp[2]
                        trade_count += 1
                except Exception as e:
                    print(e)

            # buy in Bitmex at lowest ask and sell in Okex at highest bid
            if data_Bitmex_lowest_ask[0] < data_Okex_highest_bid[0] and \
                    (data_Okex_highest_bid[0] / data_Bitmex_lowest_ask[0]) >= MIN_RATE:
                try:
                    if bitmex_balance > data_Bitmex_lowest_ask[0]:  # minimum trade size
                        temp = trade(0, data_Bitmex_lowest_ask, data_Okex_highest_bid, bitmex_balance, okex_balance)
                        profit += temp[0]
                        bitmex_balance = temp[1]
                        okex_balance = temp[2]
                        trade_count += 1
                except Exception as e:
                    print(e)

    print(date, str(trade_count) + ' trades',
          'average profit for each trade is ' + str((profit - pre_profit) / trade_count))

    return profit, bitmex_balance, okex_balance


if __name__ == "__main__":
    start_date = datetime.datetime(2020, 1, 1)
    total_profit = 0
    bitmex_balance = 500000
    okex_balance = 500000
    for i in range(0, 31):
        start_time = time.time()
        date = start_date + datetime.timedelta(days=i)
        daily_trades = main(date, total_profit, bitmex_balance, okex_balance)
        total_profit += daily_trades[0]
        bitmex_balance = daily_trades[1]
        okex_balance = daily_trades[2]
        print("--- %s minutes ---" % ((time.time() - start_time) / 60.0))
    print('Total profit: ' + str(total_profit))
