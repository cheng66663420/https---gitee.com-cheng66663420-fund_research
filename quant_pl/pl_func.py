import polars as pl
import os
import datetime
from quant_utils.utils import display_time
import polars.selectors as cs


def _writer_helper(
    df: pl.DataFrame,
    file_path: str,
    if_exists_action: str = "update",
    unique_cols: list = None,
    exclued_cols: list = None,
) -> None:
    """
    Helper function for writing a Polars DataFrame to a Parquet file.
    """
    df_new = None
    if os.path.exists(file_path) and if_exists_action == "update":
        df_new = pl.read_parquet(file_path)
        df = df.select(df_new.columns)
        df_new = (
            pl.concat([df_new, df], how="vertical_relaxed")
            .pipe(df_datetime_parse)
            .pipe(df_infinit_parse)
            .unique(subset=unique_cols, keep="last")
        )

    if not os.path.exists(file_path) or if_exists_action == "overwrite":
        df_new = (
            df.pipe(df_datetime_parse)
            .pipe(df_infinit_parse)
            .unique(subset=unique_cols, keep="last")
        )
    if df_new is None or df.is_empty():
        return

    if exclued_cols:
        df_new = df_new.select(pl.all().exclude(exclued_cols))
    df_new.write_parquet(file_path)


@display_time()
def write_pl_dataframe(
    df: pl.DataFrame,
    file_path: str,
    partition_cols: list = None,
    if_exists_action: str = "update",
    unique_cols: list = None,
    exclued_cols: list = None,
) -> None:
    """
    Writes a DataFrame to a specified file path in Parquet format, with optional partitioning. The function allows for both partitioned and non-partitioned writes based on the provided parameters.

    Args:
        df (pl.DataFrame): The DataFrame to be written to file.
        file_path (str): The destination file path where the DataFrame will be saved.
        partition_cols (list, optional): List of columns to partition the DataFrame by. If None, the DataFrame will be written without partitioning.
        if_exists_action (str, optional): Action to take if the file already exists. Defaults to "update".
        unique_cols (list, optional): List of columns to ensure uniqueness in the DataFrame.

    Raises:
        ValueError: If partition_cols is None and file_path does not end with '.parquet'.
        ValueError: If any specified partition column is not found in the DataFrame.

    Examples:
        write_pl_dataframe(df, 'output.parquet')
        write_pl_dataframe(df, 'output_dir', partition_cols=['date'], if_exists_action='overwrite')
    """
    if if_exists_action not in ["update", "overwrite", "skip"]:
        raise ValueError(
            "if_exists_action must be one of ['update', 'overwrite', 'skip']"
        )
    if partition_cols is None:
        if ".parquet" not in file_path:
            raise ValueError("当没有partition_cols参数时必须.parquet结尾")
        _writer_helper(df, file_path, if_exists_action, unique_cols, exclued_cols)

    else:
        if isinstance(partition_cols, str):
            partition_cols = [partition_cols]
        for col in partition_cols:
            if col not in df.columns:
                raise ValueError(f"Column {col} not found in dataframe")

        partition_df_dict = df.partition_by(by=partition_cols, as_dict=True)
        for partition_key, partition_df in partition_df_dict.items():
            partition_list = []
            for i in partition_key:
                if isinstance(i, (datetime.datetime, datetime.date)):
                    partition_list.append(i.strftime("%Y%m%d"))
                else:
                    partition_list.append(str(i))
            partition_path = os.path.join(file_path, *partition_list)
            partition_path += ".parquet"
            _writer_helper(
                partition_df,
                partition_path,
                if_exists_action,
                unique_cols,
                exclued_cols,
            )


def df_datetime_parse(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        cs.datetime().cast(pl.Datetime("us")).name.keep(),
        cs.date().cast(pl.Datetime("us")).name.keep(),
    )


def df_infinit_parse(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(cs.numeric().is_infinite())
        .then(None)
        .otherwise(cs.numeric())
        .name.keep()
    )
