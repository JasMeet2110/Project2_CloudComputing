import azure.functions as func
import json
import os
import logging
from azure.cosmos import CosmosClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('DietInsights: Fetching cached stats.')

    try:
        # 1. Connect to Cosmos DB
        conn_str = os.environ["COSMOS_CONNECTION_STRING"]
        client = CosmosClient.from_connection_string(conn_str)
        database = client.get_database_client("DietDb")
        stats_container = database.get_container_client("Stats")

        # 2. Fetch the pre-calculated charts (The "Performance" Requirement)
        # We look for the item with id "global_stats"
        try:
            item = stats_container.read_item(item="global_stats", partition_key="global_stats")
            
            # 3. Construct the response payload to match your Frontend's expectation
            payload = {
                "meta": {
                    "records": "Cached", 
                    "msg": "Served from Cosmos DB (Performance Optimized)"
                },
                "charts": {
                    "macros_by_diet": item.get("macros_by_diet", []),
                    "calories_by_diet": item.get("calories_by_diet", []),
                    "trend": item.get("trend", [])
                }
            }
            return func.HttpResponse(json.dumps(payload), mimetype="application/json")

        except Exception as e:
            # If stats aren't found (e.g., file hasn't been uploaded yet)
            logging.warning(f"Stats not found: {e}")
            return func.HttpResponse(
                json.dumps({"error": "No stats found. Please upload All_Diets.csv to trigger processing."}),
                status_code=404, mimetype="application/json"
            )

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return func.HttpResponse(f"Server Error: {str(e)}", status_code=500)