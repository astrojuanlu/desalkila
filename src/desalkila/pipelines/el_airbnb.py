import polars as pl


def preprocess_airbnb_madrid(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        (
            pl.col("host_response_rate", "host_acceptance_rate")
            .str.extract(r"(\d+)%")
            .cast(pl.Int64)
            .name.suffix("_pct")
        ),
        (
            pl.col(
                "host_is_superhost",
                "host_has_profile_pic",
                "host_identity_verified",
                "has_availability",
                "instant_bookable",
            ).replace_strict({"t": True, "f": False})
        ),
        (
            pl.col("host_verifications")
            .str.replace_all(r"\[\'|\'\]", "")
            .str.replace_all(r"\', \'", ",")
            .replace({"": None, "None": None, "[]": None})
            .str.split(",")
        ),
        pl.col("neighbourhood_group_cleansed").alias("district_cleansed"),
        (
            pl.col("amenities")
            .str.replace_all(r"\[\"|\"\]", "")
            .str.replace_all(r"\", \"", ",")
            .replace({"": None, "None": None, "[]": None})
            .str.split(",")
            .list.eval(pl.element().str.strip_chars(" "))
        ),
        pl.col("price").str.extract(r"\$([\d\.]+)").cast(pl.Decimal).alias("price_usd"),
    ).select(
        pl.all().exclude(
            "neighbourhood_group_cleansed",
            "price",
            "host_response_rate",
            "host_acceptance_rate",
            "calendar_updated",
        )
    )

    return df
