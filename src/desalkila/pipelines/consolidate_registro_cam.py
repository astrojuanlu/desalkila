import polars as pl

PLANTA_REPLACEMENTS = {
    "BAJO": "BAJA",
    "Bajo": "BAJA",
    "BJO": "BAJA",
    "BAJ": "BAJA",
    "Bj": "BAJA",
    "BJ": "BAJA",
    "Baja": "BAJA",
    "bajo": "BAJA",
    "PB": "BAJA",
    "00": "0",
    "Local": "LOCAL",
    "SOT": "SÓTANO",
    "Sótano": "SÓTANO",
    "SS": "SÓTANO",
    "SOTANO": "SÓTANO",
    "1º": "1",
    "3º": "3",
    "4º": "4",
}

VIA_CLASS_REPLACEMENTS = {
    "ACCES": "ACCESO",
    "AVDA": "AVENIDA",
    "CLLON": "CALLEJÓN",
    "CMNO": "CAMINO",
    "CRA": "CARRERA",
    "CSTAN": "COSTANILLA",
    "CTRA": "CARRETERA",
    "CUSTA": "CUESTA",
    "GTA": "GLORIETA",
    "PSAJE": "PASAJE",
    "PZO": "PASADIZO",
    "TRVA": "TRAVESÍA",
}

VIA_NAME_REPLACEMENTS = {
    "de Carlos Paino": "de Carlos Paíno",
    "de Alfredo Marquerie": "de Alfredo Marqueríe",
    "de Fucar": "de Fúcar",
    "de Pavia": "de Pavía",
    "de Ardemáns": "de Ardemans",
    "de Sánchez Balderás": "de Sánchez Balderas",
    "de Eguilaz": "de Eguílaz",
    "de Mercedes Formica": "de Mercedes Fórmica",
    "del Conde del Serrallo": "del Conde de Serrallo",
    "de Sebastián Elcano": "de Juan Sebastián Elcano",
    "de Argós": "de Argos",
    "del Alcalde Sáinz de Baranda": "del Alcalde Sainz de Baranda",
    "de Sierra de Meirá": "de Sierra de Meira",
    "de Algemesi": "de Algemesí",
    "del Zodíaco": "del Zodiaco",
    "de Francisco Paino": "de Francisco Paíno",
    "de Manuel Alexandre": "de Manuel Aleixandre",
    "del Doctor Guiu": "del Doctor Guíu",
    "de Sierra Gador": "de Sierra Gádor",
    "de Sánchez Barcaiztegui": "de Sánchez Barcáiztegui",
    "de Carlos Trias Bertrán": "de Carlos Trías Bertrán",
    "de González Sola": "de González Solá",
    "del General Oraá": "del General Oráa",
    "de Sarriá": "de Sarria",
    "de Alejandro Saint Aubín": "de Alejandro Saint Aubin",
    "de Ponteáreas": "de Ponteareas",
    "del Ansar": "del Ánsar",
    "del Españoleto": "de El Españoleto",
    "de Maiquez": "de Máiquez",
    "de Salustiano Olozaga": "de Salustiano Olózaga",
    "de Federico Carlos Sáinz de Robles": "de Federico Carlos Sainz de Robles",
    "de Eugenio Selles": "de Eugenio Sellés",
    "de Wad Ras": "de Wad-Ras",
    "de María Panes": "de María Panés",
    "de Luis Larrainza": "de Luis Larraínza",
}


def fill_empty_addresses(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        pl.when(pl.col("via_nombre").is_null())
        .then(pl.col("denominacion"))
        .otherwise(pl.col("via_nombre"))
        .alias("via_nombre")
    )
    return df


def fix_addresses(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        pl.col("via_tipo").replace(VIA_CLASS_REPLACEMENTS),
        pl.col("via_nombre").replace(VIA_NAME_REPLACEMENTS),
        pl.col("planta").replace(PLANTA_REPLACEMENTS),
    )
    return df
