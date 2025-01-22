from kedro.pipeline import Pipeline, node

from .nodes import (
    enrich_open_postcode,
    extend_onemap,
    format_onemap,
    format_opendata,
    format_postcodebase,
    postcode_full_type_conversion,
)


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=format_onemap,
                inputs="one_map_scrape",
                outputs="formatted_onemap",
                name="format_onemap",
            ),
            node(
                func=format_opendata,
                inputs="open_data_postal_code",
                outputs="formatted_opendata",
                name="format_opendata",
            ),
            node(
                func=format_postcodebase,
                inputs="sg_postcode_based_via_getdata",
                outputs="formatted_postcodebase",
                name="format_postcodebase",
            ),
            node(
                func=enrich_open_postcode,
                inputs=["formatted_opendata", "formatted_postcodebase"],
                outputs="enriched_opendata",
                name="enrich_opendata",
            ),
            node(
                func=extend_onemap,
                inputs=["formatted_onemap", "enriched_opendata"],
                outputs="extended_onemap",
                name="extend_onemap",
            ),
            node(
                func=postcode_full_type_conversion,
                inputs="extended_onemap",
                outputs="singapore_postcodes_geocoded",
                name="convert_types",
            ),
        ]
    )
