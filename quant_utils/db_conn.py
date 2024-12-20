from quant_utils.constant import DB_CONFIG
from quant_utils.database import DBWrapper
from urllib.parse import quote

# 本地mysql连接
local_mysql = {
    "db_type": "mysql",
    "host": "192.20.57.188",
    "user": "chen",
    "pwd": "@1991727Asdf",
    "database": "1min",
    "port": 3306,
}


def crate_database_uri(config: dict) -> str:
    return f"{config['db_type']}://{quote(config['user'])}:{quote(config['pwd'])}@{quote(config['host'])}:{config['port']}/{quote(config['database'])}"


JJTG_URI = crate_database_uri(DB_CONFIG["jjtg"])
JY_URI = crate_database_uri(DB_CONFIG["jy"])
JY_LOCAL_URI = crate_database_uri(DB_CONFIG["jy_local"])
PG_DATA_URI = crate_database_uri(DB_CONFIG["pg_data"])
DATAYES_URI = crate_database_uri(DB_CONFIG["datayes"])
WIND_URI = crate_database_uri(DB_CONFIG["wind"])
PG_LOCAL_URI = crate_database_uri(DB_CONFIG["pg_local"])

DB_CONN_JY = DBWrapper(JY_URI)
DB_CONN_DATAYES = DBWrapper(DATAYES_URI)
DB_CONN_WIND = DBWrapper(WIND_URI)
DB_CONN_LOCAL_MYSQL = DBWrapper(crate_database_uri(local_mysql))
DB_CONN_WIND_TEST = DBWrapper(WIND_URI)
DB_CONN_JY_TEST = DBWrapper(JY_URI)
DB_CONN_JJTG_DATA = DBWrapper(JJTG_URI)
DB_CONN_JY_LOCAL = DBWrapper(JY_LOCAL_URI)
DB_CONN_PG_LOCAL = DBWrapper(PG_LOCAL_URI)
