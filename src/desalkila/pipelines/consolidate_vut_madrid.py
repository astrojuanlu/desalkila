import polars as pl


def strip_redundant_data(df: pl.DataFrame) -> pl.DataFrame:
    # Los datos redundantes de ubicación se pueden eliminar con seguridad
    # puesto que los datos ya tienen COD_NDP.
    # Se puede comprobar con este código:
    #
    # ```python
    # import polars as pl
    # (
    #     df.select("COD_NDP", "DIRECCION")
    #     .join(
    #         callejero.select(
    #             "COD_NDP", ..., "NÚMERO", "CALIFICADOR"
    #         ),
    #         on="COD_NDP",
    #         how="left",
    #     )
    #     .with_columns(
    #         (
    #             pl.col("VIA_CLASE")
    #             + " "
    #             + pl.col("VIA_NOMBRE")
    #             + " "
    #             + pl.col("NÚMERO").cast(pl.Utf8)
    #         ).alias("DIRECCION_"),
    #         pl.col("DIRECCION").str.strip_chars().str.replace_all(r"\s+", " "),
    #     )
    #     .with_columns(
    #         pl.when(pl.col("CALIFICADOR").is_null())
    #         .then(pl.col("DIRECCION_"))
    #         .otherwise(pl.col("DIRECCION_") + " " + pl.col("CALIFICADOR"))
    #         .alias("DIRECCION_")
    #     )
    #     .filter(pl.col("DIRECCION") != pl.col("DIRECCION_"))
    # )
    # ```
    #
    # Solo hay un puñado de filas que no tienen coincidencia exacta
    # y aun así se observa que las direcciones coinciden:
    #
    # ```
    # shape: (6, 8)
    # ┌──────────┬────────────────────────┬─...─┬──────────────────────────┐
    # │ COD_NDP  ┆ DIRECCION              ┆ ... ┆ DIRECCION_               │
    # │ ---      ┆ ---                    ┆ ... ┆ ---                      │
    # │ i64      ┆ str                    ┆ ... ┆ str                      │
    # ╞══════════╪════════════════════════╪═...═╪══════════════════════════╡
    # │ 11012428 ┆ FERNAN GONZALEZ 50     ┆ ... ┆ CALLE FERNAN GONZALEZ 50 │
    # │ 31018886 ┆ CALLE ALCALA 222       ┆ ... ┆ CALLE ALCALA 222 B       │
    # │ 20148260 ┆ CALLE ALVAREZ 2        ┆ ... ┆ CALLE ALVAREZ 2 B        │
    # │ 11002455 ┆ CALLE JESUS Y MARÍA 25 ┆ ... ┆ CALLE JESUS Y MARIA 25   │
    # │ 11142291 ┆ AVENIDA SAN LUIS 27    ┆ ... ┆ AVENIDA SAN LUIS 27 A    │
    # │ 11142291 ┆ AVENIDA SAN LUIS 27    ┆ ... ┆ AVENIDA SAN LUIS 27 A    │
    # └──────────┴────────────────────────┴─...─┴──────────────────────────┘
    # ```

    return df.select(pl.all().exclude("DISTRITO", "DIRECCION"))
