import os
import pendulum
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor


def getPercentage(prev, curr):
    return str((prev-curr)/prev*100) + "%"


def get_ticker_data(file, symbol):
    lines = file.readlines()
    first_line = pendulum.parse(lines[0].rstrip())
    last_line = pendulum.parse(lines[-1])
    ticker = yf.Ticker(symbol)
    ticker_data = ticker.history(
        start=last_line, end=first_line, interval="1h")
    return ticker_data


def get_data_file(file_name, symbol, rows):
    with open(file_name, "r") as file:
        ticker_data = get_ticker_data(file, symbol)
        curr_price = ticker_data['High']

        rows.append([ticker_data.index[0], symbol, 0])

        for i, date in enumerate(ticker_data.index[1:], start=1):
            rows.append([date, symbol, getPercentage(
                curr_price[i-1], curr_price[i])])


def write_data_to_file(rows):
    header = ['dateTime', 'symbol', 'change']
    data = pd.DataFrame(rows, columns=header)
    data.to_csv(os.environ.pop('DESTINATION_FILE'))


def main():
    os.environ['BITCOIN_DATES'] = os.path.realpath('bitcoin_dates.txt')
    os.environ['GOOGLE_DATES'] = os.path.realpath('google_dates.txt')
    os.environ['AMAZON_DATES'] = os.path.realpath('amazon_dates.txt')
    os.environ['DESTINATION_FILE'] = './data.csv'

    rows = []
    stocks = {'BTC-USD': 'BITCOIN_DATES',
              'GOOG': 'GOOGLE_DATES', 'AMZN': 'AMAZON_DATES'}

    with ThreadPoolExecutor() as executor:
        for index, stock in enumerate(stocks, start=1):
            print("create and start thread ", index)
            executor.submit(get_data_file, os.environ.pop(
                stocks[stock]), stock, rows)

    write_data_to_file(rows)


if __name__ == "__main__":
    main()
