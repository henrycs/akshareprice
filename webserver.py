import pickle
import logging
import arrow
import datetime
from sanic import Blueprint, Sanic, response, Request
import gzip
from datasync import data_writter, idx_data_writter, init_redis_connection

from task import start_cron_tasks


logger = logging.getLogger(__name__)


app = Sanic("trade-server")
bp_akshare = Blueprint("akshare", url_prefix="/api/akshare/", strict_slashes=False)


@bp_akshare.route("/upload", methods=["POST"])
async def bp_admin_upload(request: Request):
    client_ts = request.headers.get("ClientTime", None)
    if client_ts is None:
        return response.text("No ClientTime found", status=400)
    client_src = request.headers.get("ClientSource", None)
    if client_src is None:
        return response.text("No ClientSource found", status=400)

    now = datetime.datetime.now()
    raw_data = request.body
    try:
        _date = arrow.get(client_ts).naive
        delta = now - _date
        if delta.seconds > 5:
            logger.error("received data expired: %s", client_ts)
            return response.text("Data expired", status=400)        

        if raw_data is None or len(raw_data) < 20000:
            return response.text("Invalid data size", status=400)

        _unzip = gzip.decompress(raw_data)
        secs_data = pickle.loads(_unzip)
        await data_writter(secs_data, client_src)
        return response.text("OK")
    except Exception as e:
        logger.error("exception when decompress and save data, %s", e)
        return response.text("Exeception found")


@bp_akshare.route("/upload_index", methods=["POST"])
async def bp_admin_upload_idx(request: Request):
    client_ts = request.headers.get("ClientTime", None)
    if client_ts is None:
        return response.text("No ClientTime found for index", status=400)
    client_src = request.headers.get("ClientSource", None)
    if client_src is None:
        return response.text("No ClientSource found for index", status=400)

    now = datetime.datetime.now()
    raw_data = request.body
    try:
        _date = arrow.get(client_ts).naive
        delta = now - _date
        if delta.seconds > 5:
            logger.error("received index data expired: %s", client_ts)
            return response.text("Index data expired", status=400)        

        if raw_data is None or len(raw_data) < 20000:
            return response.text("Invalid index data size", status=400)

        _unzip = gzip.decompress(raw_data)
        index_data = pickle.loads(_unzip)
        await idx_data_writter(index_data, client_src)
        return response.text("OK")
    except Exception as e:
        logger.error("exception when decompress and save index data, %s", e)
        return response.text("Exeception found for index")


async def initialize_server(app, loop):
    await start_cron_tasks()


def set_initialize_start(app):
    app.before_server_start(initialize_server)


def start_sanic_server():
    # init redis connection
    init_redis_connection()

    set_initialize_start(app)
    app.blueprint(bp_akshare)

    app.run(host="0.0.0.0")
