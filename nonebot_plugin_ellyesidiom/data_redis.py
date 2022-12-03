from redis import StrictRedis
from datetime import datetime
from .consts import global_config

rd_host = global_config.redis_host
rd_port = global_config.redis_port
rd_db = global_config.redis_db

rd = StrictRedis(host=rd_host, port=rd_port, db=rd_db)

def set_ratelimited(name, time):
    rd.set("RL_"+name, datetime.now().strftime("%m/%d/%Y, %H:%M:%S"), ex=time)

def get_ratelimited(name):
    return rd.get("RL_"+name)