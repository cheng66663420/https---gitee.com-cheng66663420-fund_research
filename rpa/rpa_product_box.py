import time

import pandas as pd
import polars as pl
import logging
import quant_utils.data_moudle as dm
from quant_utils.constant_varialbles import TODAY

from quant_utils.db_conn import JJTG_URI
from wrapper.excel_wrapper import ExcelWrapper
from wrapper.wx_wrapper import WxWrapper
from quant_utils.logger import Logger

DATA_URI = JJTG_URI


logger = Logger()


def get_nav_end_date():
    sql = """
    WITH a AS (
    SELECT
        a.PORTFOLIO_NAME,
        max(a.TRADE_DT) AS END_DATE 
    FROM
        portfolio_derivatives_ret a
        JOIN portfolio_info b ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
    WHERE
        1 = 1 
        AND IF_LISTED = 1 
        AND INCLUDE_QDII = 0 
    GROUP BY
        a.PORTFOLIO_NAME UNION
    SELECT
        a.PORTFOLIO_NAME,
        max(c.PREV_TRADE_DATE) AS END_DATE 
    FROM
        portfolio_derivatives_ret a
        JOIN portfolio_info b ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME
        JOIN md_tradingdaynew c ON c.TRADE_DT = a.TRADE_DT 
        AND c.SECU_MARKET = 83 
    WHERE
        1 = 1 
        AND IF_LISTED = 1 
        AND INCLUDE_QDII = 1 
    GROUP BY
    a.PORTFOLIO_NAME) SELECT
    a.PORTFOLIO_NAME as TICKER_SYMBOL,
    a.END_DATE,
    (b.PORTFOLIO_RET_ACCUMULATED / 100 + 1) AS UNIT_NAV 
    FROM
    a
    JOIN portfolio_derivatives_ret b ON a.PORTFOLIO_NAME = b.PORTFOLIO_NAME 
    AND a.END_DATE = b.TRADE_DT UNION
    SELECT
    a.TICKER_SYMBOL,
    b.END_DATE,
    b.UNIT_NAV 
    FROM
    fund_perf_desc a
    JOIN fund_adj_nav b ON a.TICKER_SYMBOL = b.TICKER_SYMBOL 
    AND a.NAV_END_DATE = b.END_DATE 
    WHERE
    1 = 1 
    AND a.NAV_END_DATE >= date_add(now(), INTERVAL - 5 DAY)
    """
    return pl.read_database_uri(sql, DATA_URI)


def get_fund_last_asset():
    sql = """
    SELECT 
    TICKER_SYMBOL, 
    NET_ASSET / 100000000 AS NET_ASSET 
    FROM fund_asset_own 
    WHERE 
    1 = 1 
    AND report_date = (SELECT max(report_date) FROM fund_asset_own)
    """
    return pl.read_database_uri(sql, DATA_URI)


def get_fund_perf():
    sql = """
    SELECT
    a.TICKER_SYMBOL,
    c.DATE_NAME,
    c.END_DATE,
    a.CUM_RETURN,
    -a.MAXDD as MAXDD
    FROM
    fund_performance_inner a
    JOIN fund_info b ON a.TICKER_SYMBOL = b.TICKER_SYMBOL
    JOIN portfolio_dates c ON c.END_DATE = a.END_DATE 
    AND c.START_DATE = a.START_DATE 
    AND c.PORTFOLIO_NAME = "ALL"
    JOIN fund_perf_desc d ON d.FUND_PERF_END_DATE = a.END_DATE 
    AND d.TICKER_SYMBOL = a.TICKER_SYMBOL 
    AND d.NAV_END_DATE >= date_add(now(), INTERVAL - 5 DAY) 
    WHERE
    1 = 1 
    AND c.DATE_NAME IN ("近1日", "近1周", "近1月", "YTD") union SELECT
    a.TICKER_SYMBOL,
    c.DATE_NAME,
    c.END_DATE,
    a.CUM_RETURN,
    - a.MAXDD AS MAXDD 
    FROM
    portfolio_derivatives_performance_inner a
    JOIN portfolio_info b ON b.PORTFOLIO_NAME = a.TICKER_SYMBOL
    JOIN portfolio_dates c ON c.END_DATE = a.END_DATE 
    AND c.START_DATE = a.START_DATE 
    AND c.PORTFOLIO_NAME = "ALL"
    JOIN md_tradingdaynew d ON d.PREV_TRADE_DATE = a.END_DATE 
    WHERE
    1 = 1 
    AND c.DATE_NAME IN ("近1日", "近1周", "近1月", "YTD") 
    AND b.IF_LISTED = 1 
    AND b.INCLUDE_QDII = 1 
    AND d.SECU_MARKET = 83 
    and d.TRADE_DT = (
        SELECT max(END_DATE) from portfolio_derivatives_performance_inner 
    )
    union 
    SELECT
    a.TICKER_SYMBOL,
    c.DATE_NAME,
    c.END_DATE,
    a.CUM_RETURN,
    - a.MAXDD AS MAXDD 
    FROM
    portfolio_derivatives_performance_inner a
    JOIN portfolio_info b ON b.PORTFOLIO_NAME = a.TICKER_SYMBOL
    JOIN portfolio_dates c ON c.END_DATE = a.END_DATE 
    AND c.START_DATE = a.START_DATE 
    AND c.PORTFOLIO_NAME = "ALL"
    WHERE
    1 = 1 
    AND c.DATE_NAME IN ("近1日", "近1周", "近1月", "YTD") 
    AND b.IF_LISTED = 1 
    AND b.INCLUDE_QDII = 0
    and a.END_DATE = (
        SELECT max(END_DATE) from portfolio_derivatives_performance_inner 
    )
    """
    return pl.read_database_uri(sql, DATA_URI)


def get_fund_desc():
    sql = """
    SELECT
    a.TICKER_SYMBOL,
    a.SEC_SHORT_NAME,
    a.MANAGEMENT_COMPANY_NAME,
    b.LEVEL_1,
    b.LEVEL_2,
    b.LEVEL_3 
    FROM
    fund_info a
    left JOIN fund_type_own b ON a.TICKER_SYMBOL = b.TICKER_SYMBOL 
    AND b.REPORT_DATE = (SELECT max(REPORT_DATE) FROM fund_type_own)
    WHERE
    1 = 1 
    union 
    select
    PORTFOLIO_NAME as TICKER_SYMBOL,
    PORTFOLIO_NAME as SEC_SHORT_NAME,
    "兴证知己团队" as MANAGEMENT_COMPANY_NAME,
    LEVEL_1,
    LEVEL_2,
    LEVEL_3 
    from 
    portfolio_info
    """
    return pl.read_database_uri(sql, DATA_URI)


def get_fund_perf_rank():
    sql = """
    SELECT
    a.TRADE_DT AS END_DATE,
    a.TICKER_SYMBOL,
    a.`LEVEL`,
    F_AVGRETURN_DAY,
    F_AVGRETURN_WEEK,
    F_AVGRETURN_MONTH,
    F_AVGRETURN_THISYEAR,
    F_MAXDOWNSIDE_WEEK,
    F_MAXDOWNSIDE_MONTH,
    F_MAXDOWNSIDE_THISYEAR 
    FROM
    fund_performance_rank a
    JOIN fund_perf_desc b ON a.TRADE_DT = b.NAV_END_DATE and a.TICKER_SYMBOL = b.TICKER_SYMBOL
    AND b.NAV_END_DATE >= date_add(now(), INTERVAL - 5 DAY) 
    WHERE
    1 = 1;
    """
    df = pl.read_database_uri(sql, DATA_URI)
    df = df.unpivot(
        index=["END_DATE", "TICKER_SYMBOL", "LEVEL"],
        on=[
            "F_AVGRETURN_DAY",
            "F_AVGRETURN_WEEK",
            "F_AVGRETURN_MONTH",
            "F_AVGRETURN_THISYEAR",
            "F_MAXDOWNSIDE_WEEK",
            "F_MAXDOWNSIDE_MONTH",
            "F_MAXDOWNSIDE_THISYEAR",
        ],
        variable_name="DATE_NAME",
    )
    df = df.with_columns(
        pl.col("DATE_NAME").str.split("_").list.get(1).alias("IDICATOR"),
        pl.col("DATE_NAME").str.split("_").list.get(2).alias("DATE_NAME"),
    ).with_columns(
        pl.col("DATE_NAME").replace_strict(
            {
                "DAY": "近1日",
                "WEEK": "近1周",
                "MONTH": "近1月",
                "THISYEAR": "YTD",
            }
        )
    )
    df = (
        df.pivot(
            index=["END_DATE", "TICKER_SYMBOL", "LEVEL", "DATE_NAME"],
            on="IDICATOR",
        )
        .select(
            pl.col("TICKER_SYMBOL"),
            pl.col("END_DATE"),
            pl.col("DATE_NAME"),
            pl.col("LEVEL"),
            pl.col("AVGRETURN").alias("CUM_RETURN"),
            pl.col("MAXDOWNSIDE").alias("MAXDD"),
        )
        .to_pandas()
    )
    return df


def get_portfolio_rank():
    sql = """
    WITH temp AS (SELECT "LEVEL_1" AS LEVEL UNION SELECT "LEVEL_2" AS LEVEL UNION SELECT "LEVEL_3" AS LEVEL) SELECT
    END_DATE,
    TICKER_SYMBOL,
    CYCLE AS DATE_NAME,
    LEVEL,
    INDICATOR,
    PEER_RANK 
    FROM
    portfolio_derivatives_performance
    JOIN temp 
    WHERE
    1 = 1 
    AND END_DATE >= date_add(now(), INTERVAL - 5 DAY) 
    AND INDICATOR IN ("累计收益率", "最大回撤") 
    AND CYCLE IN ("近1日", "近1周", "近1月", "YTD")
    """
    df = pl.read_database_uri(sql, DATA_URI)
    df = df.with_columns(
        pl.col("INDICATOR").replace_strict(
            {"最大回撤": "MAXDD", "累计收益率": "CUM_RETURN"}
        )
    )
    return df.pivot(
        index=["END_DATE", "TICKER_SYMBOL", "LEVEL", "DATE_NAME"],
        on="INDICATOR",
        values="PEER_RANK",
    ).to_pandas()


def main_helper(if_send: bool = True):
    if not dm.if_trade_dt(TODAY):
        return
    file_path = r"E:\基金投顾自动化\日报周报\结果\策略盒子.xlsx"
    ticker_sheet = pl.read_excel(file_path, sheet_name="基金代码").select(
        pl.col("TICKER_SYMBOL")
    )
    fund_desc = get_fund_desc()
    ticker_sheet = ticker_sheet.join(fund_desc, on="TICKER_SYMBOL", how="left")
    ticker_sheet = ticker_sheet.with_columns(
        pl.when(pl.col("LEVEL_1") == "债券指数")
        .then(pl.lit("LEVEL_2"))
        .when(pl.col("LEVEL_2") == "短债")
        .then(pl.lit("LEVEL_3"))
        .when(pl.col("LEVEL_2") == "中长债")
        .then(pl.lit("LEVEL_3"))
        .when(pl.col("LEVEL_2") == "指数增强")
        .then(pl.lit("LEVEL_3"))
        .when(pl.col("LEVEL_1") == "固收+")
        .then(pl.lit("LEVEL_2"))
        .when(pl.col("LEVEL_1") == "货币")
        .then(pl.lit("LEVEL_1"))
        .otherwise(pl.lit("LEVEL_1"))
        .alias("LEVEL")
    ).to_pandas()

    # 基金净值及规模
    fund_nav = (
        get_nav_end_date()
        .join(get_fund_last_asset(), on="TICKER_SYMBOL", how="left")
        .filter(pl.col("TICKER_SYMBOL").is_in(ticker_sheet["TICKER_SYMBOL"]))
        .to_pandas()
    ).fillna("--")

    # 基金业绩表现
    fund_perf = get_fund_perf().filter(
        pl.col("TICKER_SYMBOL").is_in(ticker_sheet["TICKER_SYMBOL"])
    )
    fund_perf = (
        get_fund_perf()
        .filter(pl.col("TICKER_SYMBOL").is_in(ticker_sheet["TICKER_SYMBOL"]))
        .to_pandas()
    )
    fund_perf = (
        get_fund_perf()
        .filter(pl.col("TICKER_SYMBOL").is_in(ticker_sheet["TICKER_SYMBOL"]))
        .to_pandas()
    )
    fund_rank = get_fund_perf_rank()
    port_rank = get_portfolio_rank()
    perf_df = pd.concat([fund_rank, port_rank]).rename(
        columns={
            "MAXDD": "MAXDD_RANK",
            "CUM_RETURN": "CUM_RETURN_RANK",
        }
    )

    perf_sheet = (
        ticker_sheet[["TICKER_SYMBOL", "LEVEL"]]
        .merge(
            fund_nav[["TICKER_SYMBOL", "END_DATE"]], on=["TICKER_SYMBOL"], how="left"
        )
        .merge(fund_perf, how="left")
        .merge(perf_df, how="left")
    )[
        [
            "TICKER_SYMBOL",
            "DATE_NAME",
            "END_DATE",
            "CUM_RETURN",
            "MAXDD",
            "CUM_RETURN_RANK",
            "MAXDD_RANK",
            "LEVEL",
        ]
    ]
    perf_sheet.fillna("--", inplace=True)

    with ExcelWrapper("E:/基金投顾自动化/日报周报/模板/策略盒子.xlsx") as excel_handler:
        _sheet_helper(excel_handler, "基金代码", ticker_sheet)
        _sheet_helper(excel_handler, "基金净值和规模", fund_nav)
        _sheet_helper(excel_handler, "基金业绩表现", perf_sheet)
        excel_handler.save("E:/基金投顾自动化/日报周报/结果/策略盒子.xlsx")
        logger.info("策略盒子.xlsx生成成功")
        excel_handler.select_sheet("结果")
        excel_handler.save_as_image("E:/基金投顾自动化/结果/策略盒子.png")
    logger.info("策略盒子.png生成成功")

    if if_send:
        wx = WxWrapper()
        mentioned_mobile_list = wx.get_mentioned_moble_list_by_name(["黄犁之"])
        wx.send_image("E:/基金投顾自动化/结果/策略盒子.png")
        wx.send_file("E:/基金投顾自动化/日报周报/结果/策略盒子.xlsx")
        wx.send_text(
            "今日策略盒子日报已生成,请查收", mentioned_mobile_list=mentioned_mobile_list
        )
        logger.info("策略盒子日报已发送")


def _sheet_helper(excel_handler, sheet_name, df_to_write):
    try:
        excel_handler.select_sheet(sheet_name)
        excel_handler.clear_sheet()
        sheet_range = excel_handler.select_range("A:H")
        excel_handler.set_cell_style(range_obj=sheet_range, number_format="@")
        excel_handler.write_dataframe(
            df_to_write, if_write_header=True, if_write_index=False
        )
    except Exception as e:
        logger.error(f"写入{sheet_name}失败, 原因是:{e}")
        excel_handler.close()
        raise e


def main(if_send: bool = True):
    logger.info("策略盒子日报任务开始")
    try:
        if not dm.if_trade_dt(TODAY):
            logger.info("非交易日，不执行")
            return
    except Exception as e:
        logger.error(f"判断交易日失败, 原因是:{e}")

    for _ in range(10):
        try:
            main_helper(if_send)
            logger.info("策略盒子日报生成成功")
            return
        except Exception as e:
            logger.error(f"策略盒子日报生成失败, 原因是:{e}")
            continue


if __name__ == "__main__":
    main(if_send=False)
