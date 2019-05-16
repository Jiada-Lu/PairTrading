import sqlite3
import pandas as pd

def pairprofit(conn):
    cur = conn.cursor()
    cur.execute(''' SELECT Pairs.id, Pairs.Ticker1, Pairs.Ticker2,
    sum(CASE WHEN Trades.Profit>0 THEN 1 ELSE 0 END) AS ProfitableTrades,
    sum(CASE WHEN Trades.Profit<0 THEN 1 ELSE 0 END) AS LossTrades,
    sum(CASE WHEN Trades.Profit<>0 THEN 1 ELSE 0 END) AS TotalTrades,
    ROUND((sum(CASE WHEN Trades.Profit>0 THEN 1 ELSE 0 END)+0.0)/sum(CASE WHEN Trades.Profit<0 THEN 1 ELSE 0 END),2)
    AS ProfitableRatio,
    sum(Trades.Profit) AS TotalProfit
    FROM Pairs, Trades
    WHERE Pairs.id=Trades.PairID
    GROUP BY Pairs.id, Pairs.Ticker1, Pairs.Ticker2;
    ''')
    backtestdict=dict()
    backtest=list()
    for row in cur:
        pair=(row[1],row[2])
        backtestdict[pair]=[row[0],row[3],row[4],row[5],row[6],row[7]]
        backtest.append([pair,row[0],row[3],row[4],row[5],row[6],row[7]])
        #print(PairResult[pair])
    for Pair in backtest:
        cur.execute('''UPDATE OR REPLACE Pairs SET ProfitableRatio=?
        ,TotalProfit=? ,ProfitableTrades=?,LossTrades=? ,TotalTrades=?
        WHERE Ticker1=? AND Ticker2=? ''', (Pair[5],Pair[6],Pair[2],Pair[3],Pair[4],Pair[0][0], Pair[0][1]))
    backtestDF=pd.DataFrame(backtest)
    backtestDF.columns=["Pair","PairID","ProfitableTrades","LossTrades","TotalTrades","ProfitableRatio", "TotalProfit"]
    conn.commit()
    cur.close()
    return backtestdict,backtestDF
