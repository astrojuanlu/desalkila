from __future__ import annotations

import typing as t

import geopandas
from kedro.io import AbstractDataset


class GeoPandasGenericDataset(AbstractDataset):
    def __init__(self, filepath: str, load_args: dict[str, t.Any] | None = None):
        self._filepath = filepath
        self._load_args = load_args or {}

    def _load(self):
        return geopandas.read_file(self._filepath, **self._load_args)

    def _save(self, data):
        raise NotImplementedError("Generic save not implemented, use GeoParquetDataset")

    def _describe(self):
        return {"filepath": self._filepath}
