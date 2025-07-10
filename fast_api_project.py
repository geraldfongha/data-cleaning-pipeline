# import fast api 
from fastapi import FastAPI, UploadFile, File 
import uvicorn # FastAPI server
# import Data_Cleaning_pipeline
from Data_Cleaning_pipeline import pipeline
import io # import pandas
import pandas as pd
import json # import json
app = FastAPI() # Create FastAPI app

# Run the server with: uvicorn fast_api:app --reload
@app.post("/clean-data/")
async def clean_data(file: UploadFile = File(...), data_name: str = "automobile"):
    contents = await file.read()
    df = pd.read_csv(io.BytesIO(contents))
    
    cleaned_df = pipeline(df, data_name)

    output = io.StringIO()
    cleaned_df.to_csv(output, index=False)
    return {"cleaned_data_csv": output.getvalue()}
 