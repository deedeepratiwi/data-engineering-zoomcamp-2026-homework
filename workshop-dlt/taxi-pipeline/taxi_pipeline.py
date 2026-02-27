"""dlt pipeline to ingest NYC taxi data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def taxi_pipeline():
    """Define dlt resources from NYC taxi REST API endpoint.
    
    The API returns paginated JSON with 1,000 records per page.
    Pagination stops automatically when an empty page is returned.
    """
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net",
        },
        "resources": [
            "/data_engineering_zoomcamp_api",
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name='taxi_pipeline',
    destination='duckdb',
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_pipeline())
    print(load_info)  # noqa: T201
