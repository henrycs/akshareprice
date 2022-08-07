import time
import datetime
import asyncio
import akshare as ak
import pickle
import numpy as np
import aioredis
from aioredis.client import Pipeline


#stock_sz_a_spot_em_df = ak.stock_sz_a_spot_em()
#print(stock_sz_a_spot_em_df)




def save_data():
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    with open("/home/henry/myapps/aksharetest/all_secs.pik", "wb") as f:
        pickle.dump(stock_zh_a_spot_em_df, f, protocol=4)


def load_data():
    all_secs = None
    with open("/home/henry/myapps/aksharetest/all_secs.pik", "rb") as f:
        all_secs = pickle.load(f)

    if all_secs is None:
        return
    
    print(len(all_secs))
    for i in range(0, len(all_secs)):
        code = all_secs.iloc[i]['代码']
        if code.find("666") != -1:
            print(code, all_secs.iloc[i]['名称'], all_secs.iloc[i]['最新价'], all_secs.iloc[i]['今开'], all_secs.iloc[i]['昨收'])


def get_data_test():
    t0 = time.time()
    all_secs = ak.stock_zh_a_spot_em()
    if all_secs is None or len(all_secs) == 0:
        return None
    t1 = time.time()
    print("    call: %f" % (t1 - t0))
    
    data = all_secs[["代码", "最新价", "今开", "昨收"]]
    t2  = time.time()
    print("    convert: %f" % (t2 - t1))

    return data



async def main():
    #load_data()
    #save_data()

    dsn = "redis://192.168.100.101:56379/3"
    db = aioredis.from_url(dsn, encoding="utf-8", decode_responses=True)
    key = "security:latest_price_test"

    download_times = 0
    now = datetime.datetime.now()
    print(now)
    while True:
        t0 = time.time()
        data = get_data_test()
        if data is None:
            break

        code = "002227"
        _data = data.to_numpy()

        T0 = time.time()
        await db.hset(key, mapping = {k:v for k,v in zip(_data[:,0], _data[:, 1])})
        print("hset cost ", time.time() - T0)

        download_times += 1
        print("times to download all secs data: ", download_times)

        t1 = time.time()
        delta = 5 - (t1 - t0)  # 每5秒调一次
        print("sleep %f seconds, cost: %f" % (delta, (t1 - t0)))
        if delta > 0:
            time.sleep(delta)


asyncio.run(main())