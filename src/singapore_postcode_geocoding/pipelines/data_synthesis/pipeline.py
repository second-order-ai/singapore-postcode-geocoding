from kedro.pipeline import Pipeline, node

from .nodes import synthesise_postcode_data


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=synthesise_postcode_data,
                inputs=[
                    "EntitiesRegisteredwithACRA",
                    "params:synthesis_config_EntitiesRegisteredwithACRA",
                ],
                outputs="synthesised_EntitiesRegisteredwithACRA",
                name="synthesise_EntitiesRegisteredwithACRA_data",
            ),
        ]
    )
