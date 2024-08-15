from kedro.pipeline import Pipeline, node
from kedro.pipeline import pipeline as make_pipeline

from .pipelines.consolidate_airbnb import (
    compute_approximate_locations,
    fix_registro_cam,
    parse_licenses,
    separate_hosts,
)
from .pipelines.consolidate_callejero import (
    compute_location,
    fix_callejero,
    join_callejero,
)
from .pipelines.consolidate_registro_cam import fill_empty_addresses, fix_addresses
from .pipelines.consolidate_vut_madrid import strip_redundant_data
from .pipelines.el_airbnb import preprocess_airbnb_madrid
from .pipelines.el_barrios import preprocess_barrios
from .pipelines.el_callejero import (
    preprocess_callejero_historico,
    preprocess_callejero_vigente,
)
from .pipelines.el_registro_cam import preprocess_registro_cam
from .pipelines.el_vut_madrid import preprocess_vut_madrid
from .pipelines.match_registro_cam import (
    augment_addresses,
    consolidate_matchings_registro,
    match_registro_cam_exact,
    match_registro_cam_no_postal_code,
    prepare_callejero,
    prepare_registro,
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
        "el_vut_madrid": make_pipeline(
            [
                node(
                    func=preprocess_vut_madrid,
                    inputs="vut_madrid_source",
                    outputs="vut_madrid_raw",
                )
            ]
        ),
        "el_airbnb_madrid": make_pipeline(
            [
                node(
                    func=preprocess_airbnb_madrid,
                    inputs="airbnb_madrid_source",
                    outputs="airbnb_madrid_raw",
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
                    outputs="callejero_fixed",
                ),
                node(
                    func=compute_location,
                    inputs="callejero_fixed",
                    outputs="callejero",
                ),
            ]
        ),
        "consolidate_registro_cam": make_pipeline(
            [
                node(
                    func=fill_empty_addresses,
                    inputs="registro_cam_raw",
                    outputs="registro_cam_filled",
                ),
                node(
                    func=fix_addresses,
                    inputs="registro_cam_filled",
                    outputs="registro_cam",
                ),
            ]
        ),
        "consolidate_vut_madrid": make_pipeline(
            [
                node(
                    func=strip_redundant_data,
                    inputs="vut_madrid_raw",
                    outputs="vut_madrid",
                )
            ]
        ),
        "consolidate_airbnb": make_pipeline(
            [
                node(
                    func=separate_hosts,
                    inputs="airbnb_madrid_raw",
                    outputs=["airbnb_madrid_only_properties", "airbnb_madrid_hosts"],
                ),
                node(
                    func=compute_approximate_locations,
                    inputs="airbnb_madrid_only_properties",
                    outputs="airbnb_madrid_with_locations",
                ),
                node(
                    func=parse_licenses,
                    inputs="airbnb_madrid_with_locations",
                    outputs="airbnb_madrid_with_licenses",
                ),
                node(
                    func=fix_registro_cam,
                    inputs="airbnb_madrid_with_licenses",
                    outputs="airbnb_madrid",
                ),
            ]
        ),
        "generate_matchings_registro_cam": make_pipeline(
            [
                node(
                    func=augment_addresses,
                    inputs="callejero",
                    outputs="callejero_augmented",
                ),
                node(
                    func=prepare_callejero,
                    inputs="callejero_augmented",
                    outputs="callejero_augmented_prepared",
                ),
                node(
                    func=prepare_registro,
                    inputs="registro_cam",
                    outputs="registro_cam_prepared",
                ),
                node(
                    func=match_registro_cam_exact,
                    inputs=["registro_cam_prepared", "callejero_augmented_prepared"],
                    outputs="matchings_registro_exact",
                ),
                node(
                    func=match_registro_cam_no_postal_code,
                    inputs=[
                        "registro_cam_prepared",
                        "callejero_augmented_prepared",
                        "matchings_registro_exact",
                    ],
                    outputs="matchings_registro_no_postal_code",
                ),
                node(
                    func=consolidate_matchings_registro,
                    inputs=[
                        "matchings_registro_exact",
                        "matchings_registro_no_postal_code",
                    ],
                    outputs="matchings_registro",
                    name="consolidate_matchings_registro_node",
                ),
            ]
        ),
    }
    # https://github.com/kedro-org/kedro/issues/2526
    pipelines["__default__"] = sum(pipelines.values(), start=Pipeline([]))
    return pipelines
