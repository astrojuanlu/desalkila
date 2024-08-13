import polars as pl


def preprocess_registro_cam(df: pl.DataFrame) -> pl.DataFrame:
    df = df.filter(pl.col("localidad") == "Madrid").select(
        "signatura",
        pl.all().exclude("signatura"),
    )
    return df
