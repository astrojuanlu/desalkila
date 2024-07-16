import polars as pl


def preprocess_barrios(df: pl.DataFrame) -> pl.DataFrame:
    return df.select("CODDIS", "NOMDIS", "COD_BAR", "NOMBRE")
