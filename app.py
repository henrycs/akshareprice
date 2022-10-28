import asyncio
import os
from os import path
import cfg4py
import logging

from webserver import start_cron_tasks, start_sanic_server


def get_config_dir():
    current_dir = os.getcwd()
    if cfg4py.envar in os.environ and os.environ[cfg4py.envar] == "DEV":
        module_dir = path.dirname(__file__)
        return path.normpath(path.join(module_dir, "config"))
    else:
        return path.normpath(path.join(current_dir, "config"))


def init_config():
    config_dir = get_config_dir()
    print("config dir:", config_dir)

    try:
        cfg4py.init(config_dir, False)
    except Exception as e:
        print(e)
        os._exit(1)


def init_log_path(log_dir):
    if os.path.exists(log_dir):
        return 0

    try:
        os.makedirs(log_dir)
    except Exception as e:
        print(e)
        exit("failed to create log folder")

    return 0


def run():
    current_dir = os.getcwd()
    print("current dir:", current_dir)
    init_config()

    log_dir = os.path.normpath(os.path.join(current_dir, "logs"))
    init_log_path(log_dir)

    logger = logging.getLogger(__file__)
    logger.info("start server")

    cfg = cfg4py.get_instance()
    running_mode = cfg.running_mode
    if running_mode == "server":
        start_sanic_server()
    else:
        loop = asyncio.get_event_loop()
        loop.create_task(start_cron_tasks())
        loop.run_forever()


if __name__ == "__main__":
    run()


#pip install akshare  --upgrade
#pip install sanic aioredis httpx arrow cfg4py