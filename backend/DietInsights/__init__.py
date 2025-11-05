import os, io, json, time, logging
import azure.functions as func
import pandas as pd
from azure.storage.blob import BlobServiceClient, ContentSettings

def main(req: func.HttpRequest) -> func.HttpResponse:
    t0 = time.time()
    logging.info("DietInsights HTTP trigger started")

    cuisine = req.params.get("cuisine")
    min_protein = req.params.get("min_protein")
    save = req.params.get("save", "0")

    try:
        conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        src_container = os.environ.get("DIETS_CONTAINER", "diets")
        src_blob = os.environ.get("DIETS_BLOB", "All_Diets.csv")
        out_container = os.environ.get("RESULTS_CONTAINER", "results")
        out_blob = os.environ.get("RESULTS_BLOB", "avg_macros_by_diet.json")

        bsc = BlobServiceClient.from_connection_string(conn_str)
        csv_bytes = bsc.get_blob_client(container=src_container, blob=src_blob).download_blob().readall()
        df = pd.read_csv(io.BytesIO(csv_bytes))

        lower_map = {c.lower().strip(): c for c in df.columns}
        def pick(*names):
            for n in names:
                if n.lower() in lower_map: return lower_map[n.lower()]
            raise KeyError(names)
        c_diet = pick("diet_type","diet")
        c_prot = pick("protein(g)","protein")
        c_carbs = pick("carbs(g)","carbohydrates","carbs")
        c_fat = pick("fat(g)","fat")
        c_cal = pick("calories","kcal") if any(k in lower_map for k in ["calories","kcal"]) else None

        use = df[[c_diet,c_prot,c_carbs,c_fat] + ([c_cal] if c_cal else [])].copy()
        cols = ["diet","protein_g","carbs_g","fat_g"] + (["calories"] if c_cal else [])
        use.columns = cols
        for col in ["protein_g","carbs_g","fat_g"] + (["calories"] if c_cal else []):
            use[col] = pd.to_numeric(use[col], errors="coerce")

        if min_protein:
            try:
                mp = float(min_protein)
                use = use[use["protein_g"] >= mp]
            except: pass

        macros_by_diet = use.groupby("diet")[["protein_g","carbs_g","fat_g"]].mean().reset_index()

        # --- Calculate calories by diet (estimate if missing) ---
        if "calories" in use.columns:
            calories_by_diet = use.groupby("diet")[["calories"]].mean().reset_index()
        else:
            use["calories_est"] = (use["protein_g"] * 4) + (use["carbs_g"] * 4) + (use["fat_g"] * 9)
            calories_by_diet = use.groupby("diet")[["calories_est"]].mean().reset_index()
            calories_by_diet.rename(columns={"calories_est": "calories"}, inplace=True)

        use["idx"] = use.groupby("diet").cumcount()
        trend = use.groupby(["diet","idx"])[["protein_g","carbs_g"]].mean().reset_index()

        payload = {
            "meta": {
                "records": len(use),
                "filters": {"cuisine": cuisine, "min_protein": min_protein},
                "exec_ms": int((time.time()-t0)*1000)
            },
            "charts": {
                "macros_by_diet": macros_by_diet.to_dict(orient="records"),
                "calories_by_diet": calories_by_diet.to_dict(orient="records"),
                "trend": trend.to_dict(orient="records")
            }
        }

        if save == "1":
            out_client = bsc.get_blob_client(container=out_container, blob=out_blob)
            out_client.upload_blob(json.dumps(payload).encode("utf-8"),
                                   overwrite=True,
                                   content_settings=ContentSettings(content_type="application/json"))

        return func.HttpResponse(json.dumps(payload), mimetype="application/json")

    except Exception as e:
        logging.exception("DietInsights error")
        return func.HttpResponse(str(e), status_code=500)
