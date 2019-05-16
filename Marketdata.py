import requests
import pandas as pd
from pandas.compat import StringIO
import numpy as np
import sqlite3


def get_Tickers(conn,file="PairTrading.csv",header=None):
	cur = conn.cursor()
	cur.execute('''CREATE TABLE IF NOT EXISTS Tickers
        (id INTEGER PRIMARY KEY, Ticker CHAR(20) NOT NULL UNIQUE)''')
	tickers=list()
	Tickers = pd.read_csv("PairTrading.csv",header=header)
	if header==None:
	    Tickers.columns = ['ticker1', 'ticker2']
	#print(Tickers['ticker1'])
	for column in Tickers.columns:
		for symbol in Tickers[column]:
			if symbol in tickers: continue
			tickers.append(symbol)
			cur.execute('''INSERT OR IGNORE INTO Tickers (Ticker)
	            VALUES ( ? )''', (symbol,))
	conn.commit()
	cur.close()
	print("Ticker table completed")
	return tickers

def get_Pairs(conn,file="PairTrading.csv",header=None):
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS Pairs
        (id INTEGER PRIMARY KEY, Ticker1 CHAR(20) NOT NULL,
        Ticker2 CHAR(20) NOT NULL, PriceRatio FLOAT,
        ProfitableRatio FLOAT, TotalProfit FLOAT,
        ProfitableTrades INTEGER, LossTrades INTEGER,
        TotalTrades INTEGER, UNIQUE(Ticker1,Ticker2))''')
    pairs=list()
    Tickers = pd.read_csv("PairTrading.csv",header=header)
    if header==None:
        Tickers.columns = ['ticker1', 'ticker2']
    for index in range(Tickers.shape[0]):
        #if Tickers.iloc[index,0]=="Ticker1": continue
        pairs.append(np.array([Tickers.iloc[index,0],Tickers.iloc[index,1]]))
        cur.execute('''INSERT OR IGNORE INTO Pairs ( Ticker1, Ticker2)
            VALUES (  ?, ? )''', (Tickers.iloc[index,0],Tickers.iloc[index,1] ))
    conn.commit()
    cur.close()
    print("Pair table completed")
    return Tickers



def get_eod_data(symbol="AAPL", api_token="5ba84ea974ab42.45160048", startdate="2008-01-01",enddate="2018-12-31",session=None):
    if session is None:
        session = requests.Session()
        url ="https://eodhistoricaldata.com/api/eod/%s.US?" % symbol
        params = {
        'from': startdate,
        'to': enddate,
        'api_token': api_token
        }
        r = session.get(url, params=params)
        if r.status_code == requests.codes.ok:
            df = pd.read_csv(StringIO(r.text), skipfooter=1, parse_dates=[0], index_col=0,engine='python')
            return df[["Open","Close","Adjusted_close"]]
        else:
            raise Exception(r.status_code, r.reason, url)

def get_prices(conn,tickers=["AAPL"],startdate="2008-01-01",enddate="2018-12-31"):
	cur = conn.cursor()
	cur.execute('''CREATE TABLE IF NOT EXISTS Prices
    (Date CHAR(20) NOT NULL, adjClose FLOAT, Symbol CHAR(20) NOT NULL,UNIQUE (Symbol, Date))''')
	Prices=dict()
	for symbol in tickers:
		df = get_eod_data(symbol=symbol,startdate=startdate,enddate=enddate)
		#print(df)
		for index in range(df.shape[0]):
			date=str(df.index[index]).split()[0]
			open=df.iloc[index,0]
			close=df.iloc[index,1]
			adjclose=df.iloc[index,2]
			if adjclose==0: continue
			Prices[date]=[open, close,adjclose]
			cur.execute('''INSERT OR IGNORE INTO Prices ( Date, adjClose, Symbol)
                VALUES (  ?, ?, ? )''', (date, adjclose, symbol))
		print(symbol,"Prices Done!")
	conn.commit()
	cur.close()
	print("Price table completed")
	return Prices

def get_testprices(conn,tickers=["AAPL"],startdate="2019-01-01",enddate="2019-05-03"):
	cur = conn.cursor()
	cur.execute('''CREATE TABLE IF NOT EXISTS TestPrices
    (Date CHAR(20) NOT NULL,Open FLOAT, Close FLOAT, Symbol CHAR(20) NOT NULL,UNIQUE (Symbol, Date))''')
	TestPrices=dict()
	for symbol in tickers:
		df = get_eod_data(symbol=symbol,startdate=startdate,enddate=enddate)
		#print(df)
		for index in range(df.shape[0]):
			date=str(df.index[index]).split()[0]
			open=df.iloc[index,0]
			close=df.iloc[index,1]
			adjclose=df.iloc[index,2]
			if open==0: continue
			if close==0: continue
			TestPrices[date]=[open, close,adjclose]
			cur.execute('''INSERT OR IGNORE INTO TestPrices ( Date, Open, Close, Symbol)
                VALUES (  ?, ?, ?, ? )''', (date, open, close, symbol))
		print(symbol,"TestPrices Done!")
	conn.commit()
	cur.close()
	print("TestPrice table completed")
	return TestPrices
