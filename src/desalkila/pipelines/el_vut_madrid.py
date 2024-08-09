import geopandas
import polars as pl


def preprocess_vut_madrid(gdf: geopandas.GeoDataFrame) -> pl.DataFrame:
    # NOTE: Geo data not fully supported, we are living on the bleeding edge
    # See https://github.com/delta-io/delta-rs/issues/1681,
    # https://github.com/pola-rs/polars/issues/9112
    # https://github.com/apache/parquet-format/pull/240
    # We can encode the data as WKB,
    # but the CRS will be lost!
    # So we will note it in the column name

    import pyarrow as pa

    df = pl.from_arrow(pa.table(gdf.to_crs(epsg=25830).to_arrow())).rename(
        {"geometry": "geometry_epsg_25830"}
    )

    assert df["DECRETO_LU"].drop_nulls().n_unique() == 1
    assert df["TIPO"].drop_nulls().n_unique() == 1

    # DISTRITO can be obtained later from COD_NDP and callejero
    # Same goes for DIRECCION but this one is retained for debugging purposes
    df = df.with_columns(pl.col("RESOLUCION").dt.date()).select(
        pl.all().exclude("DISTRITO", "DECRETO_LU", "TIPO")
    )

    return df
