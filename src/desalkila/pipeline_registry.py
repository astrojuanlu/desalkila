from kedro.pipeline import Pipeline, node
from kedro.pipeline import pipeline as make_pipeline

from .pipelines.el_callejero import (
    preprocess_callejero_historico,
    preprocess_callejero_vigente,
)


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
        )
    }
    # https://github.com/kedro-org/kedro/issues/2526
    pipelines["__default__"] = sum(pipelines.values(), start=Pipeline([]))
    return pipelines
