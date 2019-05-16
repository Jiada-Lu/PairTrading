import requests
import pandas as pd
from pandas.compat import StringIO
import numpy as np
import sqlite3
import Marketdata
conn = sqlite3.connect('test.db')
print(Marketdata.get_Pairs(conn,file="PairTrading.csv",header=0))
