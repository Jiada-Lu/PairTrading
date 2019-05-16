import PairPriceRatio
import PairTestPrices
import sqlite3

#conn = sqlite3.connect('PairTrading.sqlite')
def get_trade(conn, PairPriceRatiodict,Tickerbook,k=1):
    #Tickerbook=PairTestPrices.pairtestprices(conn)
    #PairPriceRatiodict=PairPriceRatio.pairpriceratio(conn)
    cur=conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS Trades
        (TradeID INTEGER PRIMARY KEY, Ticker1 CHAR(20) NOT NULL,
        Ticker2 CHAR(20) NOT NULL , TradeDate CHAR(20) NOT NULL,
        Profit FLOAT,PairID INTEGER NOT NULL,UNIQUE(PairID,TradeDate))''')
    for pair,tickertrades in Tickerbook.items():
        i=0
        sigma=PairPriceRatiodict[pair]
        for trade in tickertrades:
            cur.execute('''INSERT OR IGNORE INTO Trades (Ticker1, Ticker2, TradeDate,PairID)
            VALUES (  ?, ?, ?, ? )''', (trade["Ticker1"], trade["Ticker2"], trade["TradeDate"],trade["PairID"]))
            if i==0:
                Profit=0
            if i>0 and abs(tickertrades[i-1]["Ticker1Close"]/tickertrades[i-1]["Ticker2Close"]-trade["Ticker1Open"]/trade["Ticker2Open"])>k*sigma:
                N1=-10000
                N2=int(-N1*trade["Ticker1Open"]/trade["Ticker2Open"])
                Profit=N1*(trade["Ticker1Close"]-trade["Ticker1Open"])+N2*(trade["Ticker2Close"]-trade["Ticker2Open"])
            if i>0 and abs(tickertrades[i-1]["Ticker1Close"]/tickertrades[i-1]["Ticker2Close"]-trade["Ticker1Open"]/trade["Ticker2Open"])<k*sigma:
                N1=10000
                N2=int(-N1*trade["Ticker1Open"]/trade["Ticker2Open"])
                Profit=N1*(trade["Ticker1Close"]-trade["Ticker1Open"])+N2*(trade["Ticker2Close"]-trade["Ticker2Open"])
            Tickerbook[pair][i]["Profit"]=Profit
            i=i+1
    conn.commit()

    for pair,tickertrades in Tickerbook.items():
        for trade in tickertrades:
            cur.execute('''UPDATE OR REPLACE Trades SET Profit=? WHERE PairID=? AND TradeDate=? '''
            , (trade["Profit"],trade["PairID"],trade["TradeDate"]))
    conn.commit()
    cur.close()
    return Tickerbook

def EnterPairTrade(pair,open1,open2,close1,close2,k,sigma,Tclose1=None,Tclose2=None):
    if abs(close1/close2-open1/open2)>k*sigma:
        N1=-10000
        N2=int(-N1*open1/open2)
        print("Your strategy:")
        print("Sell",abs(N1),pair[0],"stocks; Buy",abs(N2),pair[1],"stocks")
        if Tclose1!=None and Tclose2!=None:
            profit=N1*(Tclose1-open1)+N2*(Tclose2-open2)
            print("Your profit:",profit)
    if abs(close1/close2-open1/open2)<k*sigma:
        N1=10000
        N2=int(-N1*open1/open2)
        print("Your strategy:")
        print("Buy",abs(N1),pair[0],"stocks; Sell",abs(N2),pair[1],"stocks")
        if Tclose1!=None and Tclose2!=None:
            profit=N1*(Tclose1-open1)+N2*(Tclose2-open2)
            print("Your profit:",profit)
    if abs(close1/close2-open1/open2)==k*sigma: print("Do nothing.")
    return None
