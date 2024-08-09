from kedro.pipeline import Pipeline, node
from kedro.pipeline import pipeline as make_pipeline

from .pipelines.consolidate_callejero import fix_callejero, join_callejero
from .pipelines.el_barrios import preprocess_barrios
from .pipelines.el_callejero import (
    preprocess_callejero_historico,
    preprocess_callejero_vigente,
)
from .pipelines.el_registro_cam import fill_empty_addresses, preprocess_registro_cam


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns
    -------
        A mapping from pipeline names to ``Pipeline`` objects.

    """
    pipelines = {
        "el_callejero": make_pipeline(
            [
                node(
                    func=preprocess_callejero_vigente,
                    inputs="callejero_vigente_source",
                    outputs="callejero_vigente_raw",
                ),
                node(
                    func=preprocess_callejero_historico,
                    inputs="callejero_historico_source",
                    outputs="callejero_historico_raw",
                ),
            ]
        ),
        "el_barrios": make_pipeline(
            [
                node(
                    func=preprocess_barrios,
                    inputs="barrios_source",
                    outputs="barrios",
                )
            ]
        ),
        "el_registro_cam": make_pipeline(
            [
                node(
                    func=preprocess_registro_cam,
                    inputs="registro_cam_source",
                    outputs="registro_cam_raw",
                )
            ]
        ),
        "consolidate_callejero": make_pipeline(
            [
                node(
                    func=join_callejero,
                    inputs=["callejero_vigente_raw", "callejero_historico_raw"],
                    outputs="callejero_joined",
                ),
                node(
                    func=fix_callejero,
                    inputs="callejero_joined",
                    outputs="callejero",
                ),
            ]
        ),
        "consolidate_registro_cam": make_pipeline(
            [
                node(
                    func=fill_empty_addresses,
                    inputs="registro_cam_raw",
                    outputs="registro_cam",
                )
            ]
        ),
    }
    # https://github.com/kedro-org/kedro/issues/2526
    pipelines["__default__"] = sum(pipelines.values(), start=Pipeline([]))
    return pipelines
