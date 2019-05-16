import sqlite3

#conn = sqlite3.connect('PairTrading.sqlite')

def pairtestprices(conn):
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS PairTestPrices
        (Ticker1 CHAR(20) NOT NULL,Ticker2 CHAR(20) NOT NULL ,
       Ticker1Open FLOAT, Ticker1Close FLOAT, Ticker2Open FLOAT,
        Ticker2Close FLOAT, PriceDate CHAR(20) NOT NULL, PairID INTEGER,UNIQUE(Ticker1,Ticker2,PriceDate))''')

    cur.execute('''SELECT Pairs.Ticker1 AS Ticker1, Pairs.Ticker2 AS Ticker2,
    T1.Open AS Ticker1Open, T1.Close AS Ticker1Close,
    T2.Open AS Ticker2Open, T2.Close AS Ticker2Close,
    T2.Date AS PriceDate, Pairs.id AS PairID
    FROM TestPrices T1, TestPrices T2, Pairs
    WHERE T1.Symbol=Pairs.Ticker1 and T2.Symbol=Pairs.Ticker2
    and T1.Date=T2.Date
    GROUP BY Pairs.Ticker1, Pairs.Ticker2, T1.Date
    ORDER BY Pairs.Ticker1, Pairs.Ticker2, T1.Date;
    ''')
    Tickerbook=dict()
    Trades=list()
    for row in cur:
        Trade=dict()
        Trade["Ticker1"]=row[0]
        Trade["Ticker2"]=row[1]
        Pair=(Trade["Ticker1"],Trade["Ticker2"])
        if Pair not in Tickerbook.keys(): Tickerbook[Pair]=list()
        Trade["Ticker1Open"]=row[2]
        Trade["Ticker1Close"]=row[3]
        Trade["Ticker2Open"]=row[4]
        Trade["Ticker2Close"]=row[5]
        Trade["TradeDate"]=row[6]
        Trade["PairID"]=row[7]
        Tickerbook[Pair].append(Trade)
        Trades.append(Trade)
    for Trade in Trades:
        cur.execute('''INSERT OR IGNORE INTO PairTestPrices
         (Ticker1, Ticker2, Ticker1Open, Ticker1Close, Ticker2Open, Ticker2Close, PriceDate, PairID)
            VALUES (  ?, ?, ?, ?, ?, ?, ? ,? )'''
            ,(Trade["Ticker1"],Trade["Ticker2"], Trade["Ticker1Open"],Trade["Ticker1Close"],Trade["Ticker2Open"],Trade["Ticker2Close"],Trade["TradeDate"],Trade["PairID"]))
    conn.commit()
    cur.close()
    return Tickerbook
