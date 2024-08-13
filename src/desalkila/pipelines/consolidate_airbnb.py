import geopandas
import polars as pl
import polars.selectors as cs

REGEX_LIST = {
    "registro_cam": r"(?i)((?:VU?T|HM|AM|HH|CM)(?:\D*)\d+)",  # VT-553
    "expediente_madrid": r"(\d+(?:\/|\.)\d{4}(?:\/|\.)\d+)",  # 500/2022/05637
    "expediente_cam": r"(\d{2}\/\d{6}\.\d\/\d{2})",  # 09/173747.9/18, 10/289696.9/19
    "cif": r"((?:A|B|C|D|E|F|G|H|J|P|Q|R|S|U|V)-?\d{8})",  # B86899440, B66438409
    "nif": r"((?:K|L|M|X|Y|Z|\d)\d{7}\p{Alphabetic})",  # 00389594C, Y5026027N
    "referencia_catastral": r"([0-9A-Z]{20})",  # 4994531VK4749F0001TS
}


def separate_hosts(df: pl.DataFrame) -> pl.DataFrame:
    df_hosts = df.select(cs.starts_with("host_")).unique().sort(by="host_id")
    df_no_hosts = df.select(~cs.starts_with("host_"), "host_id")
    return df_no_hosts, df_hosts


def compute_approximate_locations(df: pl.DataFrame) -> pl.DataFrame:
    locations = (
        geopandas.GeoSeries.from_xy(
            x=df["longitude"], y=df["latitude"], crs="EPSG:4326"
        )
        # Convert to local CRS
        .to_crs(epsg=25830)
        # "In practice, this means the location for a listing on the map, or in the data
        # will be from 0-450 feet (150 metres) of the actual address"
        # https://insideairbnb.com/data-assumptions/
        .buffer(150)
    )

    df = (
        df.with_columns(pl.Series("approximate_location", locations.to_wkb()))
        # We get rid of longitude and latitude,
        # since they are approximate anyway
        .select(pl.all().exclude("longitude", "latitude"))
    )
    return df


def parse_licenses(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        pl.col("license").str.extract(regex).alias(field_name)
        for field_name, regex in REGEX_LIST.items()
    )
    return df


def fix_registro_cam(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        pl.col("registro_cam").str.extract_groups(r"(?i)(VU?T|HM|AM)(?:\D*)(\d+)")
    ).with_columns(
        (
            pl.col("registro_cam").struct[0].str.to_uppercase().str.replace("VUT", "VT")
            + "-"
            + pl.col("registro_cam").struct[1]
        ).alias("registro_cam")
    )
    return df
