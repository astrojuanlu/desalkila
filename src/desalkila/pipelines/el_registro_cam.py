import polars as pl


def preprocess_registro_cam(df: pl.DataFrame) -> pl.DataFrame:
    df = df.filter(
        (pl.col("alojamiento_tipo") == "VIVIENDAS DE USO TU")
        & (pl.col("localidad") == "Madrid")
    ).select(
        pl.col("signatura"),
        pl.all().exclude("alojamiento_tipo", "categoria", "signatura"),
    )
    return df


def fill_empty_addresses(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        pl.when(pl.col("via_nombre").is_null())
        .then(pl.col("denominacion"))
        .otherwise(pl.col("via_nombre"))
        .alias("via_nombre")
    )
    return df
