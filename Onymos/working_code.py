import threading
import random
import time

MAX_TICKERS = 1024
MAX_ORDERS = 100000

BUY = 0
SELL = 1

g_tickerSymbols = [None] * MAX_TICKERS
g_numTickers = 0

class Order:
    __slots__ = ("isActive", "isBuy", "tickerId", "quantity", "price")
    
    def __init__(self):
        self.isActive = False
        self.isBuy = False
        self.tickerId = -1
        self.quantity = 0
        self.price = 0.0

g_orders = [Order() for _ in range(MAX_ORDERS)]
nextOrderIndex = 0
indexLock = threading.Lock()

def getTickerIndex(t):
    global g_numTickers
    
    for i in range(g_numTickers):
        if g_tickerSymbols[i] == t:
            return i
    
    if g_numTickers < MAX_TICKERS:
        g_tickerSymbols[g_numTickers] = t
        g_numTickers += 1
        return g_numTickers - 1
    
    return -1

def addOrder(oType, t, q, p):
    global nextOrderIndex
    
    i = getTickerIndex(t)
    if i < 0:
        return -1
    
    with indexLock:
        idx = nextOrderIndex
        nextOrderIndex += 1
    
    if idx >= MAX_ORDERS:
        return -1
    
    o = g_orders[idx]
    o.isActive = True
    o.isBuy = (oType == BUY)
    o.tickerId = i
    o.quantity = q
    o.price = p
    
    return idx

def matchOrder(idx):
    if idx < 0 or idx >= MAX_ORDERS:
        return
    
    no = g_orders[idx]
    if not no.isActive:
        return
    
    with indexLock:
        c = nextOrderIndex
    
    for i in range(c):
        if i == idx:
            continue
        
        bo = g_orders[i]
        
        if not bo.isActive:
            continue
        
        if bo.isBuy == no.isBuy:
            continue
        
        if bo.tickerId != no.tickerId:
            continue
        
        if no.isBuy and no.price < bo.price:
            continue
        
        if not no.isBuy and bo.price < no.price:
            continue
        
        mq = min(no.quantity, bo.quantity)
        bo.quantity -= mq
        no.quantity -= mq
        
        if bo.quantity == 0:
            bo.isActive = False
        
        if no.quantity == 0:
            no.isActive = False
            break

def simulate(n):
    s = ["META", "APPLE", "AMAZON", "NETFLIX", "GOOGLE"]
    
    for _ in range(n):
        ot = BUY if random.randint(0, 1) == 0 else SELL
        tk = s[random.randint(0, len(s) - 1)]
        q = random.randint(1, 100)
        p = float(random.randint(1, 1000))
        
        i = addOrder(ot, tk, q, p)
        if i >= 0:
            matchOrder(i)

def run():
    random.seed(int(time.time()))
    
    ts = []
    for _ in range(4):
        t = threading.Thread(target=simulate, args=(1000,))
        ts.append(t)
        t.start()
    
    for t in ts:
        t.join()
    
    with indexLock:
        c = nextOrderIndex
    
    a = sum(1 for i in range(c) if g_orders[i].isActive)
    
    print(f"Total orders which are placed: {c}")
    print(f"Total active orders: {a}")

run()
