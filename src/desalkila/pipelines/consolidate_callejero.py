import geopandas
import polars as pl

VIA_NAME_REPLACEMENTS = {
    "JARDÍNES": "JARDINES",
    "MANUEL GONZÁLEZ LONGORÍA": "MANUEL GONZÁLEZ LONGORIA",
    "GÓMEZNARRO": "GOMEZNARRO",
    "PADRE JESÚS ORDOÑEZ": "PADRE JESÚS ORDÓÑEZ",
    "MULLER": "MÜLLER",
    "WAD-RÁS": "WAD-RAS",
    "SAN VICENTE DE PAUL": "SAN VICENTE DE PAÚL",
}


def join_callejero(
    df_vigente: pl.DataFrame, df_historico: pl.DataFrame
) -> pl.DataFrame:
    """Consolida los datos del callejero.

    En el callejero vigente hay "direcciones" (vial + número) antiguas.
    Por ejemplo, (445700.89, 4480711.73) en 28033 es al mismo tiempo
    Calvo Sotelo 1 y Mar de Coral 1 (COD_NDP=11119328).
    Además, hay líneas duplicadas.

    En el callejero histórico se ve que las denominaciones "calle | de | calvo sotelo"
    y "calle | de | rusia" se dieron de baja en 1953,
    y que la que está sin fecha de baja "calle | del | mar de coral".

    Por tanto, tomamos como punto de partida el callejero histórico,
    eliminamos las direcciones que están dadas de baja,
    unimos con el callejero vigente para completar con los códigos de
    distrito y barrio así como el código postal,
    y eliminamos duplicados.

    """
    df = (
        df_historico.filter(pl.col("FECHA_DE_BAJA").is_null())
        .join(
            df_vigente.select(
                pl.col("COD_NDP", "DISTRITO", "BARRIO", "COD_POSTAL")
            ).unique(),
            on="COD_NDP",
            how="left",
        )
        .unique()
    )
    return df


def fix_callejero(df: pl.DataFrame) -> pl.DataFrame:
    """Arregla algunos nombres de vías.

    Supuestamente el callejero oficial es la fuente primaria,
    pero en algunos casos se observan erratas, problemas con las tildes,
    o inconsistencias con otras fuentes de datos.

    """
    return df.with_columns(pl.col("VIA_NOMBRE_ACENTOS").replace(VIA_NAME_REPLACEMENTS))


def compute_location(df: pl.DataFrame) -> pl.DataFrame:
    locations = geopandas.GeoSeries.from_xy(
        x=df["UTMX_ETRS"], y=df["UTMY_ETRS"], crs="EPSG:25830"
    )

    df = (
        df.with_columns(pl.Series("location_epsg_25830", locations.to_wkb()))
        # We get rid of X, Y, latitude, longitude,
        # since they are encoded in the geometry column
        .select(pl.all().exclude("UTMX_ETRS", "UTMY_ETRS", "LATITUD", "LONGITUD"))
    )
    return df
