#!python3
"""
Created on Sun Aug 23 15:25:12 2020

Pulls current stock fundamental ratios from Finviz and stores in a specified
database.

    Ratios pulled from Finviz:
       Market_cap
       P/B
       P/E
       Forward_P/E
       PEG
       Debt_Eq
       EPS_ttm
       Dividend_pct
       ROE
       ROI
       EPS_QoQ
       Insider_own

@author: HM
"""

import contextlib
import datetime
import os
import pandas as pd
import requests
import sqlite3
import time

from sqlite3 import Error
from bs4 import BeautifulSoup


class Stock:
    sectors = ['financials', 'consumer staples', 'consumer discretionary',
               'utilities', 'basic materials', 'industrials', 'tech',
               'telecom', 'healthcare', 'energy', 'real estate']
    market_caps = ['small', 'mid', 'large']
    types = ['growth', 'value']
    stocks = []

    def __init__(self, ticker, sector, market_cap, type_):
        self.ticker = ticker.upper()
        if sector.lower() in Stock.sectors:
            self.sector = sector.title()
        else:
            raise ValueError('Not eligible sector: {}'.format(sector))
        if market_cap.lower() in Stock.market_caps:
            self.market_cap = market_cap.title()
        else:
            raise ValueError('Not eligible market cap: {}'.format(market_cap))
        if type_.lower() in Stock.types:
            self.type_ = type_.title()
        else:
            raise ValueError('Not eligible stock type: {}'.format(type_))

        Stock.stocks.append(self)


def fundamental_metric(soup, metric):
    return soup.find(text=metric).find_next(class_='snapshot-td2').text


def get_fundamental_data(dataframe, stock_list):
    for symbol in stock_list:
        try:
            header = {'User-Agent': 'my-app/0.0.1'}
            url = ("https://www.finviz.com/quote.ashx?t=" + symbol.ticker)
            soup = BeautifulSoup(requests.get(url, headers=header)
                                 .content, 'lxml')
            for column in dataframe.columns:
                if column == 'Ticker':
                    dataframe.loc[symbol.ticker, column] = symbol.ticker
                elif column == 'Sector':
                    dataframe.loc[symbol.ticker, column] = symbol.sector
                elif column == 'Market Cap':
                    dataframe.loc[symbol.ticker, column] = symbol.market_cap
                elif column == 'Type':
                    dataframe.loc[symbol.ticker, column] = symbol.type_
                elif column == 'Date':
                    dataframe.loc[symbol.ticker, column] = datetime.datetime.today(
                    ).strftime('%Y-%m-%d')
                else:
                    dataframe.loc[symbol.ticker, column] = fundamental_metric(
                        soup, column)
            time.sleep(5)
        except AttributeError:
            print(symbol.ticker, 'not found')
    return dataframe


def db_insert(stock_data):
    try:
        with contextlib.closing(sqlite3.connect(
                'Stock_Fundamentals_database.db')) as conn:
            with conn:
                with contextlib.closing(conn.cursor()) as c:
                    c.execute("""CREATE TABLE IF NOT EXISTS Stock_fundamentals(
                            index_key text PRIMARY KEY,
                            ticker string,
                            sector string,
                            market_cap string,
                            type string,
                            date timestamp,
                            PB float,
                            PE float,
                            Forward_PE float,
                            PEG float,
                            Debt_Eq float,
                            EPS_ttm float,
                            Dividend_pct float,
                            ROE float,
                            ROI float,
                            EPS_QoQ,
                            Insider_own float,
                            UNIQUE(index_key)
                            )"""
                              )
                    conn.commit()

                    for index, stock in stock_data.iterrows():
                        c.execute("INSERT OR REPLACE INTO Stock_fundamentals VALUES ("
                                  ":index_key, :ticker, :sector, :market_cap, :type, :date, :PB, :PE, :Forward_PE, "
                                  ":PEG, :Debt_Eq, :EPS_ttm, :Dividend_pct, :ROE, :ROI, :EPS_QoQ, :Insider_own)",
                                  {'index_key': str(index),
                                   'ticker': stock['Ticker'],
                                   'sector': stock['Sector'],
                                   'market_cap': stock['Market Cap'],
                                   'type': stock['Type'],
                                   'date': stock['Date'],
                                   'PB': stock['P/B'],
                                   'PE': stock['P/E'],
                                   'Forward_PE': stock['Forward P/E'],
                                   'PEG': stock['PEG'],
                                   'Debt_Eq': stock['Debt/Eq'],
                                   'EPS_ttm': stock['EPS (ttm)'],
                                   'Dividend_pct': stock['Dividend %'],
                                   'ROE': stock['ROE'],
                                   'ROI': stock['ROI'],
                                   'EPS_QoQ': stock['EPS Q/Q'],
                                   'Insider_own': stock['Insider Own']}
                                  )
                        conn.commit()
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


def get_db_stocks():
    with contextlib.closing(sqlite3.connect(
            'Stock_Fundamentals_database.db')) as conn:
        with conn:
            with contextlib.closing(conn.cursor()) as c:
                stocks = c.execute(
                    "SELECT * FROM Stock_fundamentals").fetchall()
                conn.commit()
                return stocks


def df_format(columns_list, stock_list):
    df_reformat = get_fundamental_data(pd.DataFrame(columns=columns_list), stock_list)
    df_reformat.reset_index(drop=True, inplace=True)
    df_reformat['Index'] = df_reformat['Ticker'] + ' ' + df_reformat['Date']
    df_reformat.set_index('Index', inplace=True)

    for column in df_reformat.columns:
        df_reformat[column] = df_reformat[column].str.replace('%', '')
    return df_reformat


# Function to store data in CSV file as well
def csv_file(columns, file_path):
    os.chdir(file_path)

    csv_df = pd.DataFrame(get_db_stocks())
    csv_df.set_index(0, inplace=True)
    csv_df.columns = columns
    csv_df.to_csv('Stock_fundamentals.csv')


# Add/remove stocks of interest below
amzn = Stock('amzn', 'consumer discretionary', 'large', 'growth')
goog = Stock('goog', 'telecom', 'large', 'growth')
aapl = Stock('aapl', 'tech', 'large', 'value')
ally = Stock('ally', 'financials', 'mid', 'value')
brkb = Stock('brk-b', 'financials', 'large', 'value')

metric_list = ['Ticker', 'Sector', 'Market Cap', 'Type', 'Date', 'P/B', 'P/E', 'Forward P/E', 'PEG', 'Debt/Eq',
               'EPS (ttm)', 'Dividend %', 'ROE', 'ROI', 'EPS Q/Q', 'Insider Own']

print('Starting Stock Fundamentals Database program....')

df = df_format(metric_list, Stock.stocks)

db_insert(df)

file_path = r'C:\Users\Primary user\Documents\PyCharmProjects\Stock-Fundamentals-Database'

csv_file(metric_list, file_path)
