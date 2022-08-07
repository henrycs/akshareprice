import datetime
import logging
import pickle

import akshare as ak
import httpx
import gzip

from apscheduler.schedulers.asyncio import AsyncIOScheduler
import cfg4py

from datasync import data_writter, index_data_writter, reset_cache


logger = logging.getLogger(__name__)


def load_cron_task(scheduler):
    scheduler.add_job(
        reset_cache_at_serverside,
        "cron",
        hour=9,
        minute=29,
        second=55,
        name="reset_cache_at_serverside",
    )

    scheduler.add_job(
        fetch_price_from_akshare,
        "cron",
        hour=9,
        minute="30-59",
        second="*/5",
        name="fetch_price_task",
    )
    scheduler.add_job(
        fetch_price_from_akshare,
        "cron",
        hour=10,
        second="*/5",
        name="fetch_price_task",
    )
    scheduler.add_job(
        fetch_price_from_akshare,
        "cron",
        hour=11,
        minute="0-29",
        second="*/5",
        name="fetch_price_task",
    )
    scheduler.add_job(
        fetch_price_from_akshare,
        "cron",
        hour=11,
        minute=30,
        second="0,5",
        name="fetch_price_task",
    )

    scheduler.add_job(
        fetch_price_from_akshare,
        "cron",
        hour=13,
        second="*/5",
        name="fetch_price_task",
    )
    scheduler.add_job(
        fetch_price_from_akshare,
        "cron",
        hour=14,
        second="*/5",
        name="fetch_price_task",
    )
    scheduler.add_job(
        fetch_price_from_akshare,
        "cron",
        hour=15,
        minute=0,
        second="0,5",
        name="fetch_price_task",
    )


async def start_cron_tasks():
    scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")
    load_cron_task(scheduler)
    scheduler.start()


def load_data():
    all_secs = None
    with open("/home/henry/myapps/aksharetest/all_secs.pik", "rb") as f:
        all_secs = pickle.load(f)

    if all_secs is None:
        return    
    data = all_secs[["代码", "最新价"]]
    return data


def get_akshare_data_em():
    try:
        all_secs = ak.stock_zh_a_spot_em()
        if all_secs is None or len(all_secs) == 0:
            return None
        
        data = all_secs[["代码", "最新价"]]
        return data
    except Exception as e:
        logger.error("exception found while getting data from akshare: %s", e)
        return None


def get_akshare_index_data_sina():
    try:
        all_secs = ak.stock_zh_index_spot()
        if all_secs is None or len(all_secs) == 0:
            return None
        
        data = all_secs[["代码", "最新价"]]
        return data
    except Exception as e:
        logger.error("exception found while getting index data from akshare: %s", e)
        return None


async def reset_cache_at_serverside():
    now = datetime.datetime.now()
    if now.weekday() >= 5:  # 周末不运行
        return True

    cfg = cfg4py.get_instance()
    running_mode = cfg.running_mode
    if running_mode == "server":
        await reset_cache()


async def process_stock_price(cfg, dt: datetime.datetime):
    running_mode = cfg.running_mode

    data = get_akshare_data_em()
    if data is None:
        logger.info("no data returned from akshare: %s", dt)
        return False

    if running_mode == "server":
        await data_writter(data, running_mode)
        return True

    # pack data and send to server
    binary = pickle.dumps(data, protocol=4)
    _zipped = gzip.compress(binary)

    server_url = cfg.server
    url = f"http://{server_url}/api/akshare/upload"
    headers = {'ClientTime': dt.strftime("%Y-%m-%d %H:%M:%S"), 'ClientSource': running_mode}
    rsp = httpx.post(url, content=_zipped, headers=headers)
    if rsp.status_code != 200:
        logger.info("response of post: %s", rsp.text)


async def process_index_price(cfg, dt: datetime.datetime):
    running_mode = cfg.running_mode

    data = get_akshare_index_data_sina()
    if data is None:
        logger.info("no data returned from akshare for index: %s", dt)
        return False

    if running_mode == "server":
        await index_data_writter(data, running_mode)
        return True

    # pack data and send to server
    binary = pickle.dumps(data, protocol=4)
    _zipped = gzip.compress(binary)

    server_url = cfg.server
    url = f"http://{server_url}/api/akshare/upload_idx"
    headers = {'ClientTime': dt.strftime("%Y-%m-%d %H:%M:%S"), 'ClientSource': running_mode}
    rsp = httpx.post(url, content=_zipped, headers=headers)
    if rsp.status_code != 200:
        logger.info("response of post: %s", rsp.text)


async def fetch_price_from_akshare():
    cfg = cfg4py.get_instance()
    running_mode = cfg.running_mode

    now = datetime.datetime.now()
    if now.weekday() >= 5:  # 周末不运行
        return True

    seconds = now.second
    _seq_num = seconds / 5

    if running_mode == "server":
        if _seq_num % 3 != 0:  # running at 0, 15, 30, 45
            return False        
    elif running_mode == "aliyun":
        if _seq_num % 3 != 1:  # running at 5, 20, 35, 50
            return False
    else:  # telecom ecs
        if _seq_num % 3 != 2:  # running at 10, 25, 40, 55
            return False

    logger.info("%s side: %s", running_mode, now)

    await process_stock_price(cfg, now)

    # await process_index_price(cfg, now)