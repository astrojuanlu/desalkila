import polars as pl
import polars.selectors as cs


def augment_addresses(df_callejero: pl.DataFrame) -> pl.DataFrame:
    # Algunas direcciones solo existen con `CALIFICADOR``,
    # pero no hay ningún registro de la CAM que concuerde.
    # En estos casos no podríamos encontrar el `COD_NDP` correspondiente.

    # Para cada dirección con un `CALIFICADOR` no vacío
    # insertamos una idéntica con los siguientes cambios:
    # `CALIFICADOR` vacío, `FECHA_DE_ALTA` vacía, `TIPO_NDP=SINTÉTICO/DESALKILA`

    # Además, para aumentar la probabilidad de éxito,
    # generamos direcciones extra probando todas las partículas a lo bruto
    # (aproximadamente 1 millón de filas)

    # Este paso ayuda a ubicar más de 980 registros
    # que de otra forma no se encontrarían

    df_synthetic_addresses = (
        df_callejero.with_columns(
            pl.lit(None).alias("CALIFICADOR"),
            pl.lit(None).alias("FECHA_DE_ALTA"),
            pl.lit("SINTÉTICO/DESALKILA").alias("TIPO_NDP"),
        )
        .unique(subset=["COD_VIA", "NÚMERO"])
        .sort(by=["COD_VIA", "COD_NDP"])
    )
    df_joined = (
        pl.concat([df_callejero, df_synthetic_addresses])
        .unique(subset=["COD_VIA", "NÚMERO", "CALIFICADOR"])
        .sort(by=["COD_VIA", "COD_NDP"])
    )

    # TODO: Is there a better way to generate this?
    df_all_particles = pl.DataFrame(
        {
            "VIA_PAR": [
                None,
                "DE LA",
                "DE",
                "LA",
                "DEL",
                "DE LOS",
                "A LA",
                "DE LAS",
                "AL",
            ]
        }
    )

    df = df_joined.select(pl.all().exclude("VIA_PAR")).join(
        df_all_particles, how="cross"
    )

    return df


def match_registro_cam(
    df_registro: pl.DataFrame, df_callejero: pl.DataFrame
) -> pl.DataFrame:
    df_registro_prep = df_registro.with_columns(
        (pl.col("via_tipo") + " " + pl.col("via_nombre") + " " + pl.col("numero"))
        .str.to_lowercase()
        .alias("_via_full"),
        pl.col("cdpostal").alias("_cod_postal"),
    )

    df_callejero_prep = (
        df_callejero.with_columns(
            # Reassemble address in 1 string
            (
                pl.when(pl.col("VIA_PAR").is_null())
                .then(
                    pl.col("VIA_CLASE")
                    + " "
                    + pl.col("VIA_NOMBRE_ACENTOS")
                    + " "
                    + pl.col("NÚMERO").cast(pl.Utf8)
                )
                .otherwise(
                    pl.col("VIA_CLASE")
                    + " "
                    + pl.col("VIA_PAR")
                    + " "
                    + pl.col("VIA_NOMBRE_ACENTOS")
                    + " "
                    + pl.col("NÚMERO").cast(pl.Utf8)
                )
            )
            .str.to_lowercase()
            .alias("_via_full"),
            pl.col("COD_POSTAL").alias("_cod_postal"),
        )
        .with_columns(
            pl.when(pl.col("CALIFICADOR").is_null())
            .then(pl.col("_via_full"))
            .otherwise(
                pl.col("_via_full") + " " + pl.col("CALIFICADOR").str.to_lowercase()
            )
            .alias("_via_full")
        )
        .select(
            cs.starts_with("VIA_"),
            cs.starts_with("COD_"),
            "NÚMERO",
            "CALIFICADOR",
            "TIPO_NDP",
            "_via_full",
            "_cod_postal",
        )
    )

    df = (
        df_registro_prep.join(
            df_callejero_prep,
            on=[
                "_via_full",
                # Sometimes postal code is empty!
                # "_cod_postal",
            ],
            how="left",
        )
        # Leave the merging fields for debugging purposes
        # .select(pl.all().exclude("_via_full", "_cod_postal"))
    )

    return df


def generate_matchings_registro(df: pl.DataFrame) -> pl.DataFrame:
    return df.select("signatura", "COD_NDP")
