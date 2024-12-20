import os
import time

import numpy as np
import pandas as pd
import polars as pl
import polars.selectors as cs
from dateutil.parser import parse


import quant_utils.data_moudle as dm
from fund_db.fund_db_updates import update_fund_performance_rank
from quant_pl.performance_pl import PerformancePL
from quant_pl.pl_func import write_pl_dataframe
from quant_utils.constant_varialbles import LAST_TRADE_DT
from quant_utils.db_conn import DB_CONN_JJTG_DATA, DB_CONN_JY_LOCAL

from quant_utils.utils import display_time

INIT_DATE = "20210903"


def get_fund_nav_by_pl(
    start_date: str, end_date: str, parquet_path: str = "F:/data_parquet/fund_nav/"
) -> pl.DataFrame:
    start_date = parse(start_date)
    end_date = parse(end_date)

    # lazy_df_list = [
    #     pl.scan_parquet(x)
    #     .select(
    #         [
    #             pl.col("END_DATE").cast(pl.Date),
    #             pl.col("TICKER_SYMBOL").cast(pl.String),
    #             pl.col("ADJ_NAV").cast(pl.Float64).alias("NAV"),
    #         ]
    #     )
    #     .filter((pl.col("END_DATE") >= start_date) & (pl.col("END_DATE") <= end_date))
    #     for x in glob.glob(f"{parquet_path}*.parquet")
    # ]
    return (
        pl.scan_parquet(f"{parquet_path}*.parquet")
        .select(
            [
                pl.col("END_DATE").cast(pl.Date),
                pl.col("TICKER_SYMBOL").cast(pl.String),
                pl.col("ADJ_NAV").cast(pl.Float64).alias("NAV"),
            ]
        )
        .filter((pl.col("END_DATE") >= start_date) & (pl.col("END_DATE") <= end_date))
        .collect()
    )


def get_portfolio_constant_date(
    end_date: str = None, start_date: str = None
) -> pd.DataFrame:
    """
    获取组合的固定日期,包括成立日期与对客日期

    Parameters
    ----------
    end_date : str, optional
        结束日期, by default None
    start_date : str, optional
        开始日期, by default None

    Returns
    -------
    pd.DataFrame
        columns=[START_DATE, END_DATE, DATE_NAME, PORTFOLIO_NAME]
    """
    if start_date is None:
        start_date = end_date
    trade_dts = dm.get_trade_cal(start_date, end_date)
    if not trade_dts:
        raise ValueError("交易日为空")
    query_sql = f"""
    SELECT DISTINCT
        DATE_FORMAT( LISTED_DATE, "%Y%m%d" ) AS START_DATE,
        "成立日" AS DATE_NAME,
        PORTFOLIO_NAME
    FROM
        `portfolio_info` 
    WHERE
        1 = 1 
        AND IFNULL( DELISTED_DATE, "20991231" ) >= '{end_date}'
        and LISTED_DATE <= '{start_date}' UNION
    SELECT DISTINCT
        DATE_FORMAT( TO_CLIENT_DATE, "%Y%m%d" ) AS START_DATE,
        "对客日" AS DATE_NAME,
        PORTFOLIO_NAME
    FROM
        `portfolio_info` 
    WHERE
        1 = 1 
        AND IFNULL( DELISTED_DATE, "20991231" ) >= '{end_date}'
        and LISTED_DATE <= '{start_date}'
    HAVING
        START_DATE IS NOT NULL 
        AND START_DATE <= '{end_date}'
    ORDER BY
        START_DATE
    """
    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    reulst_list = []
    for trade_dt in trade_dts:
        temp_df = df.copy()
        temp_df["END_DATE"] = trade_dt
        reulst_list.append(temp_df)
    return pd.concat(reulst_list).query("END_DATE > START_DATE")


def get_constant_date(end_date: str = None, start_date: str = None) -> pd.DataFrame:
    """
    获取固定日期

    Parameters
    ----------
    end_date : str, optional
        结束日期, by default None
    start_date : str, optional
        开始日期, by default None

    Returns
    -------
    pd.DataFrame
        columns=[START_DATE, END_DATE, DATE_NAME, PORTFOLIO_NAME]
    """
    query_sql = f"""
    select * 
    from md_trade_calender 
    where 
    if_trading_day = 1 
    and trade_dt between '{start_date}' and '{end_date}'
    """
    df = DB_CONN_JJTG_DATA.exec_query(query_sql)
    df = (
        pl.from_pandas(df)
        .with_columns(pl.col("TRADE_DT").alias("END_DATE"))
        .select(pl.all().exclude(["ID", "UPDATE_TIME", "IF_TRADING_DAY", "TRADE_DT"]))
    )
    df = df.unpivot(
        index="END_DATE", variable_name="DATE_NAME", value_name="START_DATE"
    )
    df = df.with_columns(
        pl.lit("ALL").alias("PORTFOLIO_NAME"),
    )
    rename_dict = {
        "WEEK_END_DATE": "WTD",
        "MONTH_END_DATE": "MTD",
        "YEAR_END_DATE": "YTD",
        "QUARTER_END_DATE": "QTD",
        "PREV_TRADE_DATE": "近1日",
    }
    date_dict = {"WEEK": [1], "MONTH": [1, 2, 3, 6, 9], "YEAR": list(range(1, 11))}
    cn_dict = {
        "WEEK": "周",
        "MONTH": "月",
        "YEAR": "年",
    }
    for date_type, date_list in date_dict.items():
        for date in date_list:
            rename_dict[f"PREV_{date}_{date_type}"] = f"近{date}{cn_dict[date_type]}"
    df = df.with_columns(
        pl.col("START_DATE").dt.strftime("%Y%m%d").alias("START_DATE"),
        pl.col("END_DATE").dt.strftime("%Y%m%d").alias("END_DATE"),
    )
    return df.with_columns(pl.col("DATE_NAME").replace(rename_dict)).to_pandas()


def get_special_date(end_date: str = None, start_date: str = None) -> pd.DataFrame:
    """
    获取特殊日期

    Parameters
    ----------
    end_date : str, optional
        结束日期, by default None
    start_date : str, optional
        开始日期, by default None

    Returns
    -------
    pd.DataFrame
    """
    trade_dts = dm.get_trade_cal(start_date, end_date)
    period_end_date_list = []
    for date in trade_dts:
        # 特别日期
        if date > "20240329":
            period_end_date_list.append(
                {"DATE_NAME": "TGDS_1", "START_DATE": "20240329", "END_DATE": date}
            )
        if date > "20240930":
            period_end_date_list.append(
                {"DATE_NAME": "TGDS_2", "START_DATE": "20240930", "END_DATE": date}
            )
    if not period_end_date_list:
        return None
    period_end_date_df = pd.DataFrame(period_end_date_list)
    period_end_date_df["PORTFOLIO_NAME"] = "ALL"
    return period_end_date_df


def cal_needed_dates_df(end_date: str = None, start_date: str = None) -> pd.DataFrame:
    """
    获取需要计算的日期DataFrame

    Parameters
    ----------
    end_date : str, optional
        结束日期, by default None
    start_date : str, optional
        开始日期, by default None

    Returns
    -------
    pd.DataFrame
        columns=[START_DATE, END_DATE],
        index格式为[成立日_组合名称, 对客日_组合名称, 近X日...]
    """
    if start_date is None:
        start_date = end_date

    portfolio_df = get_portfolio_constant_date(start_date=start_date, end_date=end_date)
    constant_dates_df = get_constant_date(start_date=start_date, end_date=end_date)

    special_dates_df = get_special_date(start_date=start_date, end_date=end_date)

    dates_df = pd.concat(
        [portfolio_df, constant_dates_df, special_dates_df]
    ).reset_index(drop=True)
    dates_df = dates_df.query("END_DATE > START_DATE").sort_values(
        by=["START_DATE", "END_DATE"]
    )
    DB_CONN_JJTG_DATA.upsert(dates_df, table="portfolio_dates")


def get_needed_dates_df(end_date: str = None, start_date: str = None) -> pd.DataFrame:
    query_sql = f"""
    SELECT
        date_format(START_DATE,'%Y%m%d') as START_DATE,
        date_format(END_DATE,'%Y%m%d') as END_DATE,
        DATE_NAME
    FROM
        `portfolio_dates`
    WHERE
        1=1
        and (END_DATE BETWEEN '{start_date}' AND '{end_date}')
    """
    return DB_CONN_JJTG_DATA.exec_query(query_sql)


class BasePerformance:
    rename_dict = {
        "起始日期": "START_DATE",
        "结束日期": "END_DATE",
        "最大回撤日": "MAXDD_DATE",
        "累计收益率": "CUM_RETURN",
        "年化收益率": "ANNUAL_RETURN",
        "年化波动率": "ANNUAL_VOLATILITY",
        "收益波动比": "SHARP_RATIO_ANNUAL",
        "最大回撤": "MAXDD",
        "年化收益回撤比": "CALMAR_RATIO_ANNUAL",
        "最大回撤修复": "MAXDD_RECOVER",
        "波动率": "VOLATILITY",
        "累计收益波动比": "SHARP_RATIO",
    }

    def __init__(self, end_date: str, start_date: str = None) -> None:
        self.end_date = end_date
        self.start_date = start_date or self.end_date

    @display_time()
    def calculate(self, data_name_list: str = None) -> pl.DataFrame:
        time_stamp1 = time.time()
        dates_df = get_needed_dates_df(
            start_date=self.start_date, end_date=self.end_date
        )

        if data_name_list is not None:
            dates_df = dates_df[dates_df["DATE_NAME"].isin(data_name_list)]
        dates_df = dates_df[["START_DATE", "END_DATE"]].drop_duplicates()

        time_stamp2 = time.time()

        print(f"日期处理完成, 用时{time_stamp2 - time_stamp1:.4f}s")
        df_nav_temp = self.get_nav()
        if isinstance(df_nav_temp, pd.DataFrame):
            df_nav_temp = (
                pl.from_pandas(df_nav_temp)
                .lazy()
                .with_columns(pl.col("END_DATE").str.to_datetime(format="%Y%m%d"))
            )
        # if df_nav_temp.is_empty():
        #     print("净值数据为空")
        #     return None
        if isinstance(df_nav_temp, pl.DataFrame):
            df_nav_temp = df_nav_temp.lazy()
        time_stamp_nav = time.time()
        print(f"净值处理完成, 用时{time_stamp_nav - time_stamp2:.4f}s")
        # update_desc_flag = 0
        result_list = []
        for _, (start_date, end_date) in dates_df.iterrows():
            perf = PerformancePL(df_nav_temp, start_date=start_date, end_date=end_date)
            result = perf.stats()
            result_list.append(result)
        return pl.concat(result_list).collect() if result_list else None

    @display_time()
    def write_data(self, table, if_to_db: bool = True):
        result_df = self.calculate()
        if result_df is None:
            print("数据为空")
            return
        result_df = result_df.with_columns(
            pl.when(pl.col(pl.Float64) > 10**8)
            .then(np.inf)
            .otherwise(pl.col(pl.Float64))
            .name.keep()
        ).with_columns(
            pl.when(pl.col(pl.Float64) < -(10**8))
            .then(-np.inf)
            .otherwise(pl.col(pl.Float64))
            .name.keep(),
        )
        print("计算完成,准备写入")
        file_path = f"F:/data_parquet/{table}/"
        os.makedirs(file_path, exist_ok=True)
        write_pl_dataframe(
            result_df,
            file_path=file_path,
            partition_cols=["END_DATE"],
            if_exists_action="update",
            unique_cols=["TICKER_SYMBOL", "END_DATE", "START_DATE"],
        )
        if if_to_db:
            result_df = result_df.with_columns(
                cs.temporal().dt.strftime("%Y%m%d").name.keep(),
                cs.float().round(6).name.keep(),
            )

            DB_CONN_JJTG_DATA.upsert(result_df.to_pandas(), table=table)
        print("写入完成")

    def get_nav(self):
        pass

    def update_desc(self):
        pass


class FundPerformance(BasePerformance):
    def get_nav(self):
        query_sql = f"""
        SELECT 
            TICKER_SYMBOL 
        FROM 
            fund_perf_desc 
        WHERE 
            1=1
            # and NAV_END_DATE > IFNULL( FUND_PERF_END_DATE, '20000101' ) 
            AND NAV_END_DATE >= '{self.start_date}'
        """
        ticker_df = DB_CONN_JJTG_DATA.exec_query(query_sql)
        ticker_df = pl.from_pandas(ticker_df)
        start_date = dm.offset_trade_dt(self.start_date, 365 * 11)
        dates = dm.get_trade_cal(start_date, self.end_date)
        date_df = pd.DataFrame(dates, columns=["END_DATE"])
        date_df = pl.from_pandas(date_df).select(
            pl.col("END_DATE").str.to_date("%Y%m%d")
        )
        return (
            get_fund_nav_by_pl(start_date, self.end_date)
            .join(date_df, how="right", on=["END_DATE"])
            .join(ticker_df, how="inner", on=["TICKER_SYMBOL"])
            .with_columns(
                NAV=pl.col("NAV")
                .fill_null(strategy="forward")
                .over("TICKER_SYMBOL", order_by="END_DATE"),
            )
            .drop_nulls(subset=["NAV"])
            .sort(["TICKER_SYMBOL", "END_DATE"])
        )

    def update_desc(self):
        query_sql = """
        SELECT
            TICKER_SYMBOL,
            max( END_DATE ) AS FUND_PERF_END_DATE,
            min( END_DATE ) AS FUND_PERF_START_DATE 
        FROM
            fund_performance_inner 
        GROUP BY
            TICKER_SYMBOL
        """
        df = DB_CONN_JJTG_DATA.exec_query(query_sql)
        DB_CONN_JJTG_DATA.upsert(df, table="fund_perf_desc")


class PortfolioPerformance(BasePerformance):
    def get_nav(self):
        query_sql = f"""
        SELECT
            DATE_FORMAT( TRADE_DT, "%Y%m%d" ) AS END_DATE,
            a.PORTFOLIO_NAME AS TICKER_SYMBOL,
            PORTFOLIO_NAV AS NAV 
        FROM
            portfolio_nav a
            JOIN portfolio_info b ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
        WHERE
            1 = 1 
            AND a.TRADE_DT >= b.LISTED_DATE
            AND a.TRADE_DT <= '{self.end_date}'
        """
        return DB_CONN_JJTG_DATA.exec_query(query_sql)


class BenchmarkPerformance(BasePerformance):
    def get_nav(self):
        query_sql = f"""
        SELECT
            DATE_FORMAT( TRADE_DT, "%Y%m%d" ) AS END_DATE,
            a.PORTFOLIO_NAME AS TICKER_SYMBOL,
            (1 + BENCHMARK_RET_ACCUMULATED_INNER/100) AS NAV 
        FROM
            portfolio_derivatives_ret a
            JOIN portfolio_info b ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
        WHERE
            1 = 1 
            AND a.TRADE_DT >= b.LISTED_DATE
            AND a.TRADE_DT <= '{self.end_date}'
        """
        return DB_CONN_JJTG_DATA.exec_query(query_sql)


class PeerPortfolioPerformance(BasePerformance):
    def get_nav(self):
        query_sql = f"""
        SELECT
            InnerCode AS TICKER_SYMBOL,
            DATE_FORMAT( EndDate, '%Y%m%d' ) AS END_DATE,
            DataValue + 1 as NAV
        FROM
            mf_portfolioperform 
        WHERE
            1 = 1 
            and (EndDate BETWEEN '{INIT_DATE}' AND '{self.end_date}')
            and StatPeriod = 999
            AND IndicatorCode = 66
        """
        df = DB_CONN_JY_LOCAL.exec_query(query_sql)
        df["NAV"] = df["NAV"].astype("float")
        return df


class PortfolioDerivatiesPerformance(BasePerformance):
    def get_nav(self):
        query_sql = f"""
        SELECT
            DATE_FORMAT( TRADE_DT, "%Y%m%d" ) AS END_DATE,
            a.PORTFOLIO_NAME AS TICKER_SYMBOL,
            (portfolio_ret_ACCUMULATED/100 + 1) AS NAV 
        FROM
            portfolio_derivatives_ret a
            JOIN portfolio_info b ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
        WHERE
            1 = 1 
            AND a.TRADE_DT >= b.LISTED_DATE
            AND a.TRADE_DT <= '{self.end_date}'
        """
        return DB_CONN_JJTG_DATA.exec_query(query_sql)


class BenchmarkPerformanceOutter(BasePerformance):
    def get_nav(self):
        query_sql = f"""
        SELECT
            DATE_FORMAT(TRADE_DT, "%Y%m%d") AS END_DATE,
            a.PORTFOLIO_NAME AS TICKER_SYMBOL,
            BENCHMARK_NAV AS NAV 
        FROM
            portfolio_nav a
            JOIN portfolio_info b ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
        WHERE
            1 = 1 
            AND a.TRADE_DT >= b.LISTED_DATE 
            AND a.TRADE_DT <= '{self.end_date}'
        """
        return DB_CONN_JJTG_DATA.exec_query(query_sql)


def update_performance_inner(start_date, end_date):
    # cal_needed_dates_df(start_date=start_date, end_date=end_date)
    print("基金绩效更新开始")
    func_dict = {
        "fund_performance_inner": FundPerformance,
        "portfolio_performance_inner": PortfolioPerformance,
        "portfolio_derivatives_performance_inner": PortfolioDerivatiesPerformance,
        "peer_performance_inner": PeerPortfolioPerformance,
        "benchmark_performance_inner": BenchmarkPerformance,
        "benchmark_performance_outter": BenchmarkPerformanceOutter,
    }
    for key, value in func_dict.items():
        print("==" * 30)
        print(f"{key}更新开始")
        value(start_date=start_date, end_date=end_date).write_data(key)
        print(f"{key}更新结束")


def update_fund_desc():
    # file_list = glob.glob("f:/data_parquet/fund_performance_inner/*.parquet")
    df = (
        pl.scan_parquet("f:/data_parquet/fund_performance_inner/*.parquet")
        .group_by("TICKER_SYMBOL")
        .agg(
            pl.col("END_DATE").min().alias("FUND_PERF_START_DATE"),
            pl.col("END_DATE").max().alias("FUND_PERF_END_DATE"),
        )
    )
    df = df.collect().to_pandas()
    DB_CONN_JJTG_DATA.upsert(df, "fund_perf_desc")


def main():
    start_date = dm.offset_trade_dt(LAST_TRADE_DT, 2)
    end_date = LAST_TRADE_DT
    date_list = dm.get_trade_cal(start_date, end_date)
    cal_needed_dates_df(start_date=start_date, end_date=end_date)
    update_performance_inner(start_date=start_date, end_date=end_date)
    for date in date_list:
        print(date)
        update_fund_performance_rank(date)
        print("==" * 30)
    update_fund_desc()


if __name__ == "__main__":
    main()
