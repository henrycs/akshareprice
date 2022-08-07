import aioredis
import datetime
import logging

import cfg4py

logger = logging.getLogger(__name__)


db = None
key = "security:latest_price"


def init_redis_connection():
    global db
    cfg = cfg4py.get_instance()
    db = aioredis.from_url(cfg.redis.dsn, encoding="utf-8", decode_responses=True)


async def reset_cache():
    await db.delete(key)
    logger.info("cache reset: %s", key)


async def data_writter(secs_data, client_src: str):
    now = datetime.datetime.now()

    data = secs_data.to_numpy()
    await db.hset(key, mapping = {k:v for k,v in zip(data[:,0], data[:, 1])})
    logger.info("data saved into cache: %s, %s, %d", client_src, now, len(data))


async def index_data_writter(secs_data, client_src: str):
    now = datetime.datetime.now()

    data = secs_data.to_numpy()
    for item in data:
        code = item[0]
        prefix = code[:2]        
        if prefix == 'sh':
            item[0] = f"{code[2:]}.XSHG"
        elif prefix == 'sz':
            item[0] = f"{code[2:]}.XSHE"
        else:
            continue

    await db.hset(key, mapping = {k:v for k,v in zip(data[:,0], data[:, 1])})
    logger.info("data saved into cache: %s, %s, %d", client_src, now, len(data))
