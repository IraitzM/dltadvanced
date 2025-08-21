import os
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

client = RESTClient(
    base_url="https://jaffle-shop.scalevector.ai/api/v1",
    paginator=PageNumberPaginator(
        base_page=1,
        page_param="page",
        total_path=None,
        stop_after_empty_page=True,
        maximum_page=100 # For initial testing
    ),
)

@dlt.resource(name="customers")
def customers():
    for page in client.paginate("customers", params={"pageSize": 20}):
        yield page

@dlt.resource(name="orders")
def orders():
    for page in client.paginate("orders", params={"pageSize": 20}):
        yield page


@dlt.resource(name="products")
def products():
    for page in client.paginate("products", params={"pageSize": 20}):
        yield page



def jaffle_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop_extract",
        destination="duckdb",
        dataset_name="jaffle_data",
        dev_mode=False,
    )

    load_info = pipeline.run([customers, orders, products])
    print(pipeline.last_trace)  # noqa: T201


if __name__ == "__main__":
    jaffle_pipeline()
