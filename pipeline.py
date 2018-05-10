from decimal import Decimal
from operator import itemgetter
from pprint import pprint


def too_old(x, trade):
    return trade['timeStamp'] - x['timeStamp'] > 60 * 1000


def pipeline(db, trade):
    instrument_name = trade['instrument']

    if instrument_name not in db:
        db[instrument_name] = []

    relevant_items = [x for x in db[instrument_name] if not too_old(x, trade)]
    relevant_items.append(trade)
    db[instrument_name] = sorted(relevant_items, key=itemgetter('tradeSeq'))


def vwap_all(db):
    for instrument_name in db.keys():
        print(instrument_name + ": " + str(vwap(db[instrument_name])))


def vwap(instrument):
    prices = [Decimal(str(x['price'])) for x in instrument]
    quantities = [Decimal(str(x['quantity'])) for x in instrument]

    value_traded = sum([a*b for a,b in zip(prices,quantities)])
    return value_traded / sum(quantities)
