import polars as pl
import polars.selectors as cs

FULL_ADDRESS_COL_NAME = "_direccion_full"
POSTAL_CODE_COL_NAME = "_cod_postal"

MATCHING_TYPE_COL_NAME = "matching_type"


def augment_addresses(df_callejero: pl.DataFrame) -> pl.DataFrame:
    # Algunas direcciones solo existen con `CALIFICADOR``,
    # pero no hay ningún registro de la CAM que concuerde.
    # En estos casos no podríamos encontrar el `COD_NDP` correspondiente.

    # Para cada dirección con un `CALIFICADOR` no vacío
    # insertamos una idéntica con los siguientes cambios:
    # `CALIFICADOR` vacío, `FECHA_DE_ALTA` vacía, `TIPO_NDP=SINTÉTICO/DESALKILA`

    # Además, para aumentar la probabilidad de éxito,
    # generamos direcciones extra probando todas las partículas a lo bruto
    # asumiendo que en un mismo código postal no habrá dos vías con mismo nombre
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


def prepare_registro(
    df: pl.DataFrame,
    full_address_col_name: str = FULL_ADDRESS_COL_NAME,
    postal_code_col_name: str = POSTAL_CODE_COL_NAME,
) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("via_tipo") + " " + pl.col("via_nombre") + " " + pl.col("numero"))
        .str.to_lowercase()
        .alias(full_address_col_name),
        pl.col("cdpostal").alias(postal_code_col_name),
    )


def prepare_callejero(
    df: pl.DataFrame,
    full_address_col_name: str = FULL_ADDRESS_COL_NAME,
    postal_code_col_name: str = POSTAL_CODE_COL_NAME,
) -> pl.DataFrame:
    df = (
        df.with_columns(
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
            .alias(full_address_col_name),
            pl.col("COD_POSTAL").alias(postal_code_col_name),
        )
        .with_columns(
            pl.when(pl.col("CALIFICADOR").is_null())
            .then(pl.col(full_address_col_name))
            .otherwise(pl.col(full_address_col_name) + " " + pl.col("CALIFICADOR"))
            .str.to_lowercase()
            .alias(full_address_col_name)
        )
        .select(
            cs.starts_with("VIA_"),
            cs.starts_with("COD_"),
            "NÚMERO",
            "CALIFICADOR",
            "TIPO_NDP",
            FULL_ADDRESS_COL_NAME,
            POSTAL_CODE_COL_NAME,
        )
    )

    return df


def match_registro_cam_exact(
    df_registro_prep: pl.DataFrame,
    df_callejero_prep: pl.DataFrame,
) -> pl.DataFrame:
    df = (
        df_registro_prep.drop_nulls("cdpostal")
        .join(
            df_callejero_prep.drop_nulls("COD_POSTAL"),
            on=[
                FULL_ADDRESS_COL_NAME,
                POSTAL_CODE_COL_NAME,
            ],
            how="inner",
        )
        .with_columns(pl.lit("exact").alias(MATCHING_TYPE_COL_NAME))
    )

    return df


def match_registro_cam_no_postal_code(
    df_registro_prep: pl.DataFrame,
    df_callejero_prep: pl.DataFrame,
    df_matchings_exact: pl.DataFrame,
) -> pl.DataFrame:
    df = (
        df_registro_prep.join(
            df_matchings_exact,
            on="signatura",
            how="anti",
        )
        .join(
            df_callejero_prep,
            on=[
                FULL_ADDRESS_COL_NAME,
            ],
            how="inner",
        )
        .with_columns(pl.lit("no_postal_code").alias(MATCHING_TYPE_COL_NAME))
    )

    return df


def consolidate_matchings_registro(
    df_exact: pl.DataFrame, df_no_postal_code: pl.DataFrame
) -> pl.DataFrame:
    df = pl.concat(
        [
            df_exact.select("signatura", "COD_NDP", MATCHING_TYPE_COL_NAME),
            df_no_postal_code.select("signatura", "COD_NDP", MATCHING_TYPE_COL_NAME),
        ]
    ).sort(by=pl.col("signatura").str.split("-").list.get(1).cast(pl.Int64))

    return df
