import pandas as pd
from Data_Cleaning_pipeline import pipeline

def test_pipeline_runs():
    # Create a small example DataFrame
    df = pd.DataFrame({
        'ABS_presence': ['True', 'False', 'True'],
        'ESC_presence': ['True', 'False', 'True'],
        'TCS_presence': ['True', 'False', 'True'],
        'TPMS_presence': ['True', 'False', 'True'],
        'speed': [30, 60, 90]
    })
    cleaned_df = pipeline(df, 'automobile')
    # Check output is not empty
    assert not cleaned_df.empty
