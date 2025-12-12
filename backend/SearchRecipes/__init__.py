import azure.functions as func
import json
import os
import logging
from azure.cosmos import CosmosClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        search_term = req.params.get('q', '')
        diet_filter = req.params.get('diet', '')
        page = int(req.params.get('page', 1))
        limit = int(req.params.get('limit', 10))
        offset = (page - 1) * limit

        conn_str = os.environ["COSMOS_CONNECTION_STRING"]
        client = CosmosClient.from_connection_string(conn_str)
        container = client.get_database_client("DietDb").get_container_client("Recipes")

        query = "SELECT * FROM c WHERE 1=1"
        parameters = []

        if search_term:
            query += " AND CONTAINS(c.Recipe_name, @search, true)"
            parameters.append({"name": "@search", "value": search_term})

        if diet_filter:
            query += " AND c.diet_type = @diet"
            parameters.append({"name": "@diet", "value": diet_filter})

        query += f" OFFSET {offset} LIMIT {limit}"

        items = list(container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        ))

        return func.HttpResponse(json.dumps(items), mimetype="application/json")

    except Exception as e:
        return func.HttpResponse(str(e), status_code=500)