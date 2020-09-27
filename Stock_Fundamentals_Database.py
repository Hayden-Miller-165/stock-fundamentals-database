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

import pandas as pd, requests, time, datetime, contextlib, sqlite3, os
from bs4 import BeautifulSoup
from sqlite3 import Error

def fundamental_metric(soup, metric):
    return soup.find(text=metric).find_next(class_='snapshot-td2').text

def get_fundamental_data(df, stock_list):
    for symbol in stock_list:
        try:
            header = {'User-Agent': 'my-app/0.0.1'}
            url = ("https://www.finviz.com/quote.ashx?t=" + symbol.ticker)
            soup = BeautifulSoup(requests.get(url,headers=header)
                .content, 'lxml') 
            for column in df.columns:
                if column == 'Ticker':
                    df.loc[symbol.ticker, column] = symbol.ticker
                elif column == 'Sector':
                    df.loc[symbol.ticker, column] = symbol.sector
                elif column == 'Market Cap':
                    df.loc[symbol.ticker, column] = symbol.market_cap
                elif column == 'Type':
                    df.loc[symbol.ticker, column] = symbol.type_
                elif column == 'Date':
                    df.loc[symbol.ticker, column] = datetime.datetime.today(
                            ).strftime('%Y-%m-%d')
                else:
                    df.loc[symbol.ticker, column] = fundamental_metric(
                            soup, column)
            time.sleep(5)
        except AttributeError:
            print(symbol.ticker, 'not found')
    return df

def db_insert(df):
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
                    
                    for index, stock in df.iterrows():
                        c.execute("INSERT OR REPLACE INTO Stock_fundamentals VALUES" +\
                                  "(:index_key, :ticker, :sector," +\
                                  ":market_cap, :type, :date, :PB, :PE," +\
                                  ":Forward_PE, :PEG, :Debt_Eq, :EPS_ttm," +\
                                  ":Dividend_pct, :ROE, :ROI, :EPS_QoQ," +\
                                  ":Insider_own)", 
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
    df = get_fundamental_data(pd.DataFrame(columns=columns_list), stock_list)
    df.reset_index(drop=True, inplace=True)
    df['Index'] = df['Ticker'] + ' ' + df['Date']
    df.set_index('Index', inplace=True)
    
    for column in df.columns:
        df[column] = df[column].str.replace('%', '')
    return df
       
# Function to store data in CSV file as well     
def csv_file(columns):
    os.chdir('INSERT DATABASE PATH HERE')
    
    csv_df = pd.DataFrame(get_db_stocks())
    csv_df.set_index(0, inplace=True)
    csv_df.columns = columns
    csv_df.to_csv('Stock_fundamentals.csv')
            
class stock:
    sectors = ['financials', 'consumer staples', 'consumer discretionary', 
               'utilities', 'basic materials', 'industrials', 'tech',
               'telecom', 'healthcare', 'energy', 'real estate']
    market_caps = ['small', 'mid', 'large']
    types = ['growth', 'value']
    stocks = []
    
    def __init__(self, ticker, sector, market_cap, type_):
        self.ticker = ticker.upper()
        if sector.lower() in stock.sectors:
            self.sector = sector.title()
        else:
            raise ValueError('Not eligible sector: {}'.format(sector))
        if market_cap.lower() in stock.market_caps:
            self.market_cap = market_cap.title()
        else:
            raise ValueError('Not eligible market cap: {}'.format(market_cap))
        if type_.lower() in stock.types:
            self.type_ = type_.title()
        else:
            raise ValueError('Not eligible stock type: {}'.format(type_))

        stock.stocks.append(self)

# Add/remove stocks of interest below
amzn = stock('amzn', 'consumer discretionary', 'large', 'growth')
goog = stock('goog', 'telecom', 'large', 'growth')
aapl = stock('aapl', 'tech', 'large', 'value')
ally = stock('ally', 'financials', 'mid', 'value')
brkb = stock('brk-b', 'financials', 'large', 'value')

metric_list = ['Ticker','Sector','Market Cap','Type','Date','P/B','P/E',
          'Forward P/E','PEG','Debt/Eq','EPS (ttm)','Dividend %','ROE','ROI',
          'EPS Q/Q','Insider Own']

print('Starting Stock Fundamentals Database program....')

df = df_format(metric_list, stock.stocks)

db_insert(df)

csv_file(metric_list)