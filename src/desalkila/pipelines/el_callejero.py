import polars as pl


def _get_expr_parse_dms(coord):
    return (
        pl.col(coord).struct[0].cast(pl.Int64)
        + pl.col(coord).struct[1].cast(pl.Int64) / 60
        + pl.col(coord).struct[2].cast(pl.Float64) / 3600
    ) * pl.when(pl.col(coord).struct[3].is_in(["S", "W"])).then(-1).otherwise(1)


def preprocess_callejero_vigente(df: pl.DataFrame) -> pl.DataFrame:
    df = (
        df.with_columns(
            pl.col("UTMX_ETRS", "UTMY_ETRS").str.replace(",", ".").cast(pl.Float64),
            pl.col("LONGITUD", "LATITUD").str.extract_groups(
                r"(\d+)°(\d+)'([\d\.]+)'' ([NSEW])"
            ),
            pl.col("CALIFICADOR").str.strip_chars(),
        )
        .with_columns(
            _get_expr_parse_dms(coord).alias(coord) for coord in ["LONGITUD", "LATITUD"]
        )
        .select(pl.all().exclude("UTMX_ED", "UTMY_ED"))
    )
    return df


def preprocess_callejero_historico(df: pl.DataFrame) -> pl.DataFrame:
    df = (
        df.with_columns(
            pl.col("FECHA_DE_ALTA", "FECHA_DE_BAJA").str.to_date("%d/%m/%Y"),
            pl.col("UTMX_ETRS", "UTMY_ETRS", "ANGULO_ROTULACION")
            .str.replace(",", ".")
            .cast(pl.Float64),
            pl.col("LONGITUD", "LATITUD").str.extract_groups(
                r"(\d+)°(\d+)'([\d\.]+)'' ([NSEW])"
            ),
            pl.col("CALIFICADOR").str.strip_chars(),
        )
        .with_columns(
            _get_expr_parse_dms(coord).alias(coord) for coord in ["LONGITUD", "LATITUD"]
        )
        .select(pl.all().exclude("UTMX_ED", "UTMY_ED"))
    )
    return df
