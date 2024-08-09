import polars as pl


def fill_empty_addresses(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        pl.when(pl.col("via_nombre").is_null())
        .then(pl.col("denominacion"))
        .otherwise(pl.col("via_nombre"))
        .alias("via_nombre")
    )
    return df
