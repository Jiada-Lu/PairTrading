import sqlite3
import math

class StdevFunc:
    def __init__(self):
        self.M = 0.0
        self.S = 0.0
        self.k = 1
    def step(self, value):
            if value is None:
                return
            tM = self.M
            self.M += (value - tM) / self.k
            self.S += (value - tM) * (value - self.M)
            self.k += 1
    def finalize(self):
            if self.k < 3:
                return None
            return math.sqrt(self.S / (self.k-2))

#conn = sqlite3.connect('PairTrading.sqlite')
def pairpriceratio(conn):
    conn.create_aggregate("stdev",1,StdevFunc)
    cur = conn.cursor()

    cur.execute(''' SELECT Pairs.id, Pairs.Ticker1, Pairs.Ticker2,
    stdev(p1.adjClose/p2.adjClose) AS PriceRatio
    FROM Pairs
    JOIN  Prices p1, Prices p2
    ON p1.Symbol=Pairs.Ticker1 and p2.Symbol=Pairs.Ticker2
    and p1.Date=p2.Date
    GROUP BY Pairs.Id, Pairs.Ticker1, Pairs.Ticker2
    ORDER BY Pairs.Id, Pairs.Ticker1, Pairs.Ticker2;
    ''')
    #cur.execute(''' SELECT Pairs.id, Pairs.Ticker1, Pairs.Ticker2,
    #stdev(p1.Close/p2.Close) AS PriceRatio
    #FROM Prices p1, Prices p2, Pairs
    #WHERE p1.Symbol=Pairs.Ticker1 and p2.Symbol=Pairs.Ticker2
    #and p1.Date=p2.Date
    #GROUP BY Pairs.Id, Pairs.Ticker1, Pairs.Ticker2
    #ORDER BY Pairs.Id, Pairs.Ticker1, Pairs.Ticker2;
    #''')

    #PairPriceRatiotable=list()
    PairPriceRatiodict=dict()
    for row in cur:
        #PairPriceRatiotable.append(row)
        pair=(row[1],row[2])
        PairPriceRatiodict[pair]=row[3]
    for pair in PairPriceRatiodict:
        cur.execute('''UPDATE OR REPLACE Pairs SET  PriceRatio=? WHERE Ticker1=?
            AND Ticker2=? ''', (PairPriceRatiodict[pair],pair[0], pair[1]))
    conn.commit()
    cur.close()
    return PairPriceRatiodict
