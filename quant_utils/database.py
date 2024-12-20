# -*- coding: utf-8 -*-


import numpy as np
import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, inspect, text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert

from quant_utils import constant
from quant_utils.utils import display_time

from quant_utils.logger import Logger

logger = Logger()


class MySQL:
    """
    MySQL数据库操作封装
    """

    def __init__(
        self,
        uri: str,
    ):
        self.uri = uri
        self.engine = self._create_engine()

    def _create_engine(self):
        # MySQL 连接字符串
        # 创建引擎并配置连接池
        return create_engine(self.uri, pool_size=5, max_overflow=10, pool_timeout=30)

    @display_time()
    def exec_query(self, query: str) -> pd.DataFrame:
        """
        执行查询语句

        Parameters
        ----------
        query : str
            查询语句

        Returns
        -------
        pd.DataFrame
            查询结果
        """
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
        return pd.DataFrame(result.fetchall(), columns=result.keys())
        # return pl.read_database_uri(query, self.uri).to_pandas()

    @display_time()
    def exec_non_query(self, query: str) -> None:
        """
        执行非查询语句

        Parameters
        ----------
        query : str
            非查询语句
        """
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
            affected_rows = result.rowcount
            logger.info("受影响行数为%d行", affected_rows)
            connection.commit()

    def __upsert_sql(
        self, table: Table, df: pd.DataFrame, unique_constraint: list[str]
    ):
        insert_stmt = mysql_insert(table).values(df.to_dict(orient="records"))
        if upsert_dict := {
            col: insert_stmt.inserted[col]
            for col in df.columns
            if col not in unique_constraint
        }:
            return insert_stmt.on_duplicate_key_update(upsert_dict)
        return insert_stmt.prefix_with("IGNORE")

    def get_db_table_unque_index(self, table_name: str) -> list[list[str]]:
        # 创建数据库引擎
        # 创建元数据对象
        inspector = inspect(self.engine)
        # 获取指定表的索引列表
        indexes = inspector.get_indexes(table_name)
        return [index["column_names"] for index in indexes if index["unique"] is True]

    def get_unique_constraint(self, table: str) -> list[str]:
        """
        获取表的主键或唯一索引

        Parameters
        ----------
        table : str
            表名

        Returns
        -------
        list[str]
            主键或唯一索引列名列表
        """
        unique_constraint = []
        unique_indexes = self.get_db_table_unque_index(table)
        if not unique_indexes:
            return unique_constraint

        for index in unique_indexes:
            if "ID" in index:
                continue
            if "JSID" in index:
                unique_constraint = ["JSID"]
                break
            unique_constraint = index
            break
        return unique_constraint

    @display_time()
    def upsert(
        self, df_to_upsert: pd.DataFrame, table: str, batch_size: int = 10000
    ) -> None:
        """
        批量插入数据到MySQL数据库

        Parameters
        ----------
        df_to_upsert : pd.DataFrame
            需要插入的数据
        table : str
            表名
        batch_size : int, optional
            每次插入的行数, by default 10000
        """

        def _split_dataframe(df: pd.DataFrame, batch_size: int):
            total_rows = df.shape[0]
            num_chunks = (total_rows + batch_size - 1) // batch_size
            for i in range(num_chunks):
                start = i * batch_size
                end = min(start + batch_size, total_rows)
                yield df.iloc[start:end]

        if df_to_upsert.empty or df_to_upsert is None:
            logger.info("数据为空,不需要写入")
            return

        df = df_to_upsert.copy()
        df = df.replace(
            {np.nan: None, np.inf: None, -np.inf: None, "inf": None, "-inf": None}
        )

        affected_rows_total = 0
        sql_table = Table(table, MetaData(), autoload_with=self.engine)
        unique_constraint = self.get_unique_constraint(table)
        for df_chunk in _split_dataframe(df, batch_size):
            on_duplicate_key_stmt = self.__upsert_sql(
                sql_table, df_chunk, unique_constraint
            )
            with self.engine.connect() as connection:
                try:
                    result = connection.execute(on_duplicate_key_stmt)
                    affected_rows_total += result.rowcount
                    connection.commit()
                except Exception as e:
                    logger.error(f"{table}插入数据失败,错误信息为{e}")
                    connection.rollback()
        logger.info(f"{table}更新插入{affected_rows_total}行数据")


class DBWrapper:
    """
    数据库操作类
    """

    def __init__(
        self,
        uri: str,
    ):
        self.uri = uri
        self.engine = self._create_engine()
        self.db_type = self._determine_db_type()

    def _create_engine(self):
        # MySQL 连接字符串
        # 创建引擎并配置连接池
        return create_engine(self.uri, pool_size=5, max_overflow=10, pool_timeout=30)

    def _determine_db_type(self) -> str:
        if "mysql" in self.uri:
            return "mysql"
        if "postgresql" in self.uri:
            return "postgresql"
        else:
            raise ValueError("Unsupported database type")

    def _prepare_query_sql(self, query: str) -> str:
        result = query
        if self.db_type == "postgresql":
            result = result.lower().replace("`", '"').replace('"', "'")
        return result

    @display_time()
    def exec_query(self, query: str) -> pd.DataFrame:
        """
        执行查询语句

        Parameters
        ----------
        query : str
            查询语句

        Returns
        -------
        pd.DataFrame
            查询结果
        """
        query = self._prepare_query_sql(query)
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
        return pd.DataFrame(result.fetchall(), columns=result.keys())

    @display_time()
    def exec_non_query(self, query: str) -> None:
        """
        执行非查询语句

        Parameters
        ----------
        query : str
            非查询语句
        """
        query = self._prepare_query_sql(query)
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
            affected_rows = result.rowcount
            logger.info(f"受影响行数为{affected_rows}行")
            connection.commit()

    def __pg_upsert_sql(
        self, table: Table, df: pd.DataFrame, unique_constraint: list[str]
    ):
        df = df.copy()
        df.columns = [col.lower() for col in df.columns]
        insert_stmt = pg_insert(table).values(df.to_dict("records"))
        if not unique_constraint:
            return insert_stmt
        if upsert_dict := {
            col: insert_stmt.excluded[col]
            for col in df.columns
            if col not in unique_constraint
        }:
            return insert_stmt.on_conflict_do_update(
                index_elements=unique_constraint, set_=upsert_dict
            )
        return insert_stmt.on_conflict_do_nothing()

    def __mysql_upsert_sql(
        self, table: Table, df: pd.DataFrame, unique_constraint: list[str]
    ):
        insert_stmt = mysql_insert(table).values(df.to_dict("records"))
        if not unique_constraint:
            return insert_stmt
        if upsert_dict := {
            col: insert_stmt.inserted[col]
            for col in df.columns
            if col not in unique_constraint
        }:
            return insert_stmt.on_duplicate_key_update(upsert_dict)
        return insert_stmt.prefix_with("IGNORE")

    def __upsert_sql(
        self, table: Table, df: pd.DataFrame, unique_constraint: list[str]
    ):
        upsert_dict = {
            "mysql": self.__mysql_upsert_sql,
            "postgresql": self.__pg_upsert_sql,
        }
        return upsert_dict[self.db_type](table, df, unique_constraint)

    def get_db_table_unque_index(self, table_name: str) -> list[list[str]]:
        # 创建数据库引擎
        # 创建元数据对象
        inspector = inspect(self.engine)
        # 获取指定表的索引列表
        indexes = inspector.get_indexes(table_name)
        return [index["column_names"] for index in indexes if index["unique"] is True]

    def get_db_table_columns(self, table_name: str) -> list[str]:
        # 创建数据库引擎
        # 创建元数据对象
        inspector = inspect(self.engine)
        # 获取指定表的索引列表
        columns = inspector.get_columns(table_name)
        return [column["name"] for column in columns]

    def get_db_table_names(self) -> Table:
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        return list(tables)

    def get_unique_constraint(self, table: str) -> list[str]:
        """
        获取表的主键或唯一索引

        Parameters
        ----------
        table : str
            表名

        Returns
        -------
        list[str]
            主键或唯一索引列名列表
        """
        unique_constraint = []
        unique_indexes = self.get_db_table_unque_index(table)
        if not unique_indexes:
            return unique_constraint

        for index in unique_indexes:
            if "ID" in index:
                continue
            if "JSID" in index:
                unique_constraint = ["JSID"]
                break
            unique_constraint = index
            break
        return unique_constraint

    @display_time()
    def upsert(
        self, df_to_upsert: pd.DataFrame, table: str, batch_size: int = 10000
    ) -> None:
        """
        批量插入数据到MySQL数据库

        Parameters
        ----------
        df_to_upsert : pd.DataFrame
            需要插入的数据
        table : str
            表名
        batch_size : int, optional
            每次插入的行数, by default 10000
        """

        def _split_dataframe(df: pd.DataFrame, batch_size: int):
            total_rows = df.shape[0]
            num_chunks = (total_rows + batch_size - 1) // batch_size
            for i in range(num_chunks):
                start = i * batch_size
                end = min(start + batch_size, total_rows)
                yield df.iloc[start:end]

        if df_to_upsert.empty:
            print("数据为空,不需要写入")
            return

        df = df_to_upsert.copy()
        df = df.replace(
            {np.nan: None, np.inf: None, -np.inf: None, "inf": None, "-inf": None}
        )
        affected_rows_total = 0
        sql_table = Table(table, MetaData(), autoload_with=self.engine)
        unique_constraint = self.get_unique_constraint(table)
        with self.engine.connect() as connection:
            try:
                for df_chunk in _split_dataframe(df, batch_size):
                    on_duplicate_key_stmt = self.__upsert_sql(
                        sql_table, df_chunk, unique_constraint
                    )
                    result = connection.execute(on_duplicate_key_stmt)
                    affected_rows_total += result.rowcount
                connection.commit()
            except Exception as e:
                connection.rollback()
                logger.error(f"{table}插入数据失败,错误信息为{e}")
        logger.info(f"{table}更新插入{affected_rows_total}行数据")
