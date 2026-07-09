from fastapi import APIRouter, UploadFile, File, Query
import pandas as pd
import io
import json

router = APIRouter()

@router.get("/functions/tags")
async def list_tags():
    return {"tags": ["datetime", "feature", "engineering"]}

@router.get("/functions/list")
async def list_functions():
    return {
        "functions": [
            {
                "name": "DatetimeFeatures",
                "description": "Extract datetime features from timestamp column",
                "tags": ["datetime", "feature"]
            }
        ]
    }

@router.post("/functions/apply")
async def apply_function(
    feature_function_name: str = Query(...),
    feature_function_kwargs_str: str = Query("{}"),
    file: UploadFile = File(...)
):
    """
    Apply a feature engineering function to the uploaded data.
    """
    # Read the CSV file
    content = await file.read()
    df = pd.read_csv(io.StringIO(content.decode('utf-8')))
    
    # Parse kwargs
    kwargs = json.loads(feature_function_kwargs_str) if feature_function_kwargs_str else {}
    
    # Apply the function based on name
    if feature_function_name == "DatetimeFeatures":
        # Extract datetime features
        if "timestamp" in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['hour'] = df['timestamp'].dt.hour
            df['day'] = df['timestamp'].dt.day
            df['month'] = df['timestamp'].dt.month
            df['year'] = df['timestamp'].dt.year
            if kwargs.get("season") == "hour":
                df['season_hour'] = df['hour'] % 12
    else:
        # Default: just pass through
        pass
    
    # Return as JSON
    return df.to_dict(orient='records')
