import logging
import azure.functions as func
import pandas as pd
import io
import os
import uuid
import json
from azure.cosmos import CosmosClient

def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name} \n"
                 f"Blob Size: {myblob.length} bytes")

    try:
        # --- 1. READ & CLEAN DATA ---
        blob_bytes = myblob.read()
        df = pd.read_csv(io.BytesIO(blob_bytes))

        # Normalize Columns
        lower_map = {c.lower().strip(): c for c in df.columns}
        def pick(*names):
            for n in names:
                if n.lower() in lower_map: return lower_map[n.lower()]
            return None 

        c_diet = pick("diet_type", "diet")
        c_prot = pick("protein(g)", "protein")
        c_carbs = pick("carbs(g)", "carbohydrates", "carbs")
        c_fat = pick("fat(g)", "fat")

        c_cal = pick("calories", "kcal") 
        if not c_cal:
            df["calories"] = (df[c_prot] * 4) + (df[c_carbs] * 4) + (df[c_fat] * 9)
            c_cal = "calories"

        df = df.rename(columns={
            c_diet: "diet_type",
            c_prot: "protein_g",
            c_carbs: "carbs_g",
            c_fat: "fat_g",
            c_cal: "calories"
        })

        keep_cols = ["diet_type", "protein_g", "carbs_g", "fat_g", "calories", "Recipe_name", "Cuisine_type"]
        actual_cols = [c for c in keep_cols if c in df.columns]
        df = df[actual_cols]

        for col in ["protein_g", "carbs_g", "fat_g", "calories"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # --- 2. PERFORM CALCULATIONS ---
        macros_by_diet = df.groupby("diet_type")[["protein_g", "carbs_g", "fat_g"]].mean().reset_index().to_dict(orient="records")
        calories_by_diet = df.groupby("diet_type")[["calories"]].mean().reset_index().to_dict(orient="records")
        trend_data = df[["diet_type", "protein_g", "carbs_g"]].to_dict(orient="records")

        stats_doc = {
            "id": "global_stats",
            "partition_key": "global_stats",
            "macros_by_diet": macros_by_diet,
            "calories_by_diet": calories_by_diet,
            "trend": trend_data,
            "updated_at": str(pd.Timestamp.now())
        }

        # --- 3. SAVE TO COSMOS DB ---
        cosmos_conn = os.environ["COSMOS_CONNECTION_STRING"]
        client = CosmosClient.from_connection_string(cosmos_conn)
        db = client.get_database_client("DietDb")

        # Save Stats
        stats_container = db.get_container_client("Stats")
        stats_container.upsert_item(stats_doc)
        logging.info("Charts/Stats saved to Cosmos DB.")

        # Save Recipes
        recipes_container = db.get_container_client("Recipes")
        recipes = df.to_dict(orient="records")

        logging.info(f"Uploading {len(recipes)} recipes to Cosmos...")
        for recipe in recipes:
            recipe['id'] = str(uuid.uuid4())
            if 'diet_type' not in recipe: recipe['diet_type'] = "Unknown"
            recipes_container.create_item(body=recipe)

        logging.info("Data processing complete.")

    except Exception as e:
        logging.exception("Error processing data blob")