import Marketdata
import PairPriceRatio
import PairTestPrices
import Trades
import PairProfit
import pandas as pd
import sqlite3

print("Start Pairtrading Program\n......\n")

conn = sqlite3.connect('PairTrading.db')

tickers=Marketdata.get_Tickers(conn,file="PairTrading.csv",header=0)
Pairs=Marketdata.get_Pairs(conn,file="PairTrading.csv",header=0)

Prices=Marketdata.get_prices(conn,tickers=tickers,startdate="2018-01-01",enddate="2018-12-31")
TestPrices=Marketdata.get_testprices(conn,tickers=tickers,startdate="2019-01-01",enddate="2019-05-15")

PairPriceRatiodict=PairPriceRatio.pairpriceratio(conn)
PairPriceRatioDF=pd.DataFrame(list(PairPriceRatiodict.items()),columns=["Ticker Pairs", "Stdev"])

Tickerbook=PairTestPrices.pairtestprices(conn)

Tickerbook=Trades.get_trade(conn,PairPriceRatiodict,Tickerbook)

backtest=PairProfit.pairprofit(conn)
backtestdict=backtest[0]
backtestDF=backtest[1]
print("Pairtrading Program Completed\n")

while True:
    choice=int(input('''\nPlease enter 1 to pull PairPriceRatio table.
Please enter 2 to pull PairTestPrices table.
Please enter 3 to pull Backtest Result table.
Please enter 4 to enter a simulated Pairtrade.
Please enter 5 to quit the program.\n\n'''))
    if choice==1:
        print(PairPriceRatioDF)
    if choice==2:
        while True:
            a=input('''Please choose pair from following pairs:\n(type 'quit' to quit, or press enter to continue)\n''')
            if a=='quit' : break
            print(Pairs)
            ticker1=input("Ticker1:")
            ticker2=input("Ticker2:")
            try:
                trades=Tickerbook[(ticker1,ticker2)]
            except:
                print("Pairs don't exist. Please try again.")
                continue
            tradeDF=pd.DataFrame()
            for trade in trades:
                tradeDF=tradeDF.append(trade,ignore_index=True)
            print(tradeDF)
    if choice==3:
        print(backtestDF)
    if choice==4:
        while True:
            a=input('''Please choose pair from following pairs:\n(type 'quit' to return, or press enter to continue)\n''')
            if a=='quit' : break
            print(Pairs)
            ticker1=input("Ticker1:")
            ticker2=input("Ticker2:")
            pair=(ticker1,ticker2)
            try:
                sigma=PairPriceRatiodict[pair]
                trades=Tickerbook[pair]
            except:
                print("Pairs don't exist. Please try again.")
                continue
            close1=trades[len(trades)-1]["Ticker1Close"]
            close2=trades[len(trades)-1]["Ticker2Close"]
            print("Ticker1 Yestoday's close price:",close1)
            print("Ticker2 Yestoday's close price:",close2)
            open1=float(input("Please enter Ticker1 today's open price:"))
            open2=float(input("Please enter Ticker2 today's open price:"))
            Tclose1=float(input("Please enter Ticker1 today's close price:"))
            Tclose2=float(input("Please enter Ticker2 today's close price:"))
            k=int(input("Please enter k:"))
            Trades.EnterPairTrade(pair,open1,open2,close1,close2,k,sigma,Tclose1,Tclose2)
    if choice==5:
        conn.close()
        print("Program terminated.")
        break
