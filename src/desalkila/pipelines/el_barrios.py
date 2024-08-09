import geopandas
import polars as pl

DISTRITO_REPLACEMENTS = {
    "San Bals - Canillejas": "San Blas - Canillejas",
}


def preprocess_barrios(gdf: geopandas.GeoDataFrame) -> pl.DataFrame:
    gdf_filtered = gdf[["CODDIS", "NOMDIS", "COD_BAR", "NOMBRE", "geometry"]]

    # NOTE: Geo data not fully supported, we are living on the bleeding edge
    # See https://github.com/delta-io/delta-rs/issues/1681,
    # https://github.com/pola-rs/polars/issues/9112
    # https://github.com/apache/parquet-format/pull/240
    # We can encode the data as WKB,
    # but the CRS will be lost!
    # So we will note it in the column name

    import pyarrow as pa

    df = pl.from_arrow(pa.table(gdf_filtered.to_crs(epsg=25830).to_arrow())).rename(
        {"geometry": "geometry_epsg_25830"}
    )

    assert df["COD_BAR"].is_unique().all()

    df = df.with_columns(pl.col("CODDIS").str.pad_start(2, "0"))

    # Including a bit of data cleaning here, not worth creating another pipeline
    df = df.with_columns(
        pl.col("NOMDIS").replace(DISTRITO_REPLACEMENTS),
    )

    return df
