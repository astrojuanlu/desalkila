import typing as t

import polars as pl
from deltalake.writer import try_get_deltatable
from kedro.io import AbstractDataset


class SimplePolarsCSVDataset(AbstractDataset[pl.DataFrame, pl.DataFrame]):
    DEFAULT_LOAD_ARGS: dict[str, t.Any] = {}

    def __init__(
        self,
        filepath: str,
        load_args: dict[str, t.Any] | None = None,
        credentials: dict[str, t.Any] | None = None,
        storage_options: dict[str, t.Any] | None = None,
        metadata: dict[str, t.Any] | None = None,
    ):
        self._filepath = filepath
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self._credentials = credentials or {}
        self._storage_options = storage_options or {}

        self._storage_options.update(self._credentials)
        self._metadata = metadata or {}

    def _load(self) -> pl.DataFrame:
        return pl.read_csv(
            self._filepath, storage_options=self._storage_options, **self._load_args
        )

    def _save(self, data: pl.DataFrame) -> None:
        raise NotImplementedError("Just use another format")

    def _describe(self) -> dict[str, t.Any]:
        return dict(
            filepath=self._filepath,
            load_args=self._load_args,
            storage_options=self._storage_options,
            metadata=self._metadata,
        )


class PolarsDeltaDataset(AbstractDataset[pl.DataFrame, pl.DataFrame]):
    DEFAULT_LOAD_ARGS: dict[str, t.Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, t.Any] = {}

    def __init__(
        self,
        filepath: str,
        load_args: dict[str, t.Any] | None = None,
        save_args: dict[str, t.Any] | None = None,
        credentials: dict[str, t.Any] | None = None,
        storage_options: dict[str, t.Any] | None = None,
        metadata: dict[str, t.Any] | None = None,
    ):
        self._filepath = filepath
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self._save_args = {**self.DEFAULT_SAVE_ARGS, **(save_args or {})}
        self._credentials = credentials or {}
        self._storage_options = storage_options or {}

        self._storage_options.update(self._credentials)
        self._metadata = metadata or {}
        self._merge_predicate = self._save_args.pop("merge_predicate", None)

    def _load(self) -> pl.DataFrame:
        return pl.read_delta(
            self._filepath, storage_options=self._storage_options, **self._load_args
        )

    def _save(self, data: pl.DataFrame) -> None:
        if self._save_args.get("mode") == "merge":
            # https://github.com/delta-io/delta-rs/issues/2662
            dt = try_get_deltatable(
                self._filepath, storage_options=self._storage_options
            )
            if dt is None:
                # Table does not exist yet, write it for the first time without merge
                no_merge_save_args = self._save_args.copy()
                no_merge_save_args.pop("mode")
                data.write_delta(
                    self._filepath,
                    storage_options=self._storage_options,
                    **no_merge_save_args,
                )
            else:
                if not self._merge_predicate:
                    raise ValueError("Missing `merge_predicate` for merge mode")

                # Merge table normally
                (
                    data.write_delta(
                        self._filepath,
                        storage_options=self._storage_options,
                        delta_merge_options={
                            "predicate": self._merge_predicate,
                            "source_alias": "s",
                            "target_alias": "t",
                        },
                        **self._save_args,
                    )
                    .when_matched_update_all()
                    .when_not_matched_insert_all()
                    .execute()
                )
        else:
            data.write_delta(
                self._filepath, storage_options=self._storage_options, **self._save_args
            )

    def _describe(self) -> dict[str, t.Any]:
        return dict(
            filepath=self._filepath,
            load_args=self._load_args,
            save_args=self._save_args,
            storage_options=self._storage_options,
            metadata=self._metadata,
        )
