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
