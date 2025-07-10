# Data Cleaning Pipeline
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


def identify_inconsistencies(data):
    cat_cols = data.select_dtypes(include=['object']).columns
    print('Categorical columns:', cat_cols)
    for cols in cat_cols:
        print(cols)
        print(data[cols].unique())
        print('\n')

def deal_with_inconsistencie_automobile(data):
    # Replace inconsistent values
    data['ABS_presence'] = data['ABS_presence'].replace('True', '1.0')
    data['ABS_presence'] = data['ABS_presence'].replace('False', '0.0')
    data['ESC_presence'] = data['ESC_presence'].replace('True', '1.0')
    data['ESC_presence'] = data['ESC_presence'].replace('False', '0.0')
    data['TCS_presence'] = data['TCS_presence'].replace('True', '1.0')
    data['TCS_presence'] = data['TCS_presence'].replace('False', '0.0')
    data['TPMS_presence'] = data['TPMS_presence'].replace('True', '1.0')
    data['TPMS_presence'] = data['TPMS_presence'].replace('False', '0.0')


    # Convert to float
    # Convert the categorical columns to numerical columns
    data['ABS_presence'] = data['ABS_presence'].astype(float)
    data['ESC_presence'] = data['ESC_presence'].astype(float)
    data['TCS_presence'] = data['TCS_presence'].astype(float)
    data['TPMS_presence'] = data['TPMS_presence'].astype(float)

    return data

def deal_with_inconsistencie_energy(data):
    #data['building_class'] = data['building_class'].replace('commercial', 'Commercial')
    print('No inconsistency in energy data')
    
    return data

def deal_with_inconsistencie_health(data):
    # Replace inconsistent values
    print('Nothing to do for health data')
    return data

def identify_missing(data):
    columns = data.columns
    print('Columns:', columns)
    for col in columns:
        print(col)
        print(data[col].isnull().sum())
        print('\n') 

def handle_missing(data):
    num_cols = data.select_dtypes(include=['float64', 'int64']).columns
    cat_cols = data.select_dtypes(include=['object']).columns
    print('Numerical columns:', num_cols)
    print('Categorical columns:', cat_cols)
    for col in num_cols:
        # Replace the missing with median
        data[col] = data[col].fillna(data[col].median())
    for col in cat_cols:
        data[col] = data[col].fillna('Unknown')

    
    return data

def identify_duplicates(data):
    print('Number of duplicate rows:', data.duplicated().sum())
    print('Duplicate rows:', data[data.duplicated()])

def handle_duplicates(data):
    data = data.drop_duplicates() # data.drop_duplicates(inplace=True)    
    return data

def identify_and_handle_outliers(data):
    num_cols = data.select_dtypes(include=['float64', 'int64']).columns
    for col in num_cols:
        m = data[col].mean()
        std = data[col].std()
        lower_bound = m - (3*std)
        upper_bound = m + (3*std)
        # Identify the outliers
        print('Column:', col)
        print('Outliers:', data[(data[col] < lower_bound) | (data[col] > upper_bound)])
        # Handle the outliers - replace with median
        data.loc[data[col] < lower_bound, col] = data[col].median()
        data.loc[data[col] > upper_bound, col] = data[col].median()

    return data  

def identify_correlation(data):
    num_cols = data.select_dtypes(include=['float64', 'int64']).columns
    correlation = data[num_cols].corr()
    # reduce decimal points to 2
    correlation = correlation.round(2)
    plt.figure(figsize=(10, 8))
    sns.heatmap(correlation, annot=True)
    plt.savefig("correlation_plot.png")  # Save to file if needed
    plt.close()

def pipeline(data, data_name):
    # stage 1: identify inconsistencies
    print('Stage 1: identify inconsistencies')
    identify_inconsistencies(data)
    # stage 2: deal with inconsistencies
    #print('Stage 2: deal with inconsistencies')
    if data_name == 'automobile':
        data = deal_with_inconsistencie_automobile(data)
    elif data_name == 'energy':
        data = deal_with_inconsistencie_energy(data)
    else:
        data = deal_with_inconsistencie_health(data)  

    # stage 3: identify missing values
    print('Stage 3: identify missing values')
    identify_missing(data)
    # stage 4: handle missing values
    print('Stage 4: handle missing values')
    data = handle_missing(data)
    # stage 5: identify duplicates
    print('Stage 5: identify duplicates')
    identify_duplicates(data)
    # stage 6: handle duplicates
    print('Stage 6: handle duplicates')
    data = handle_duplicates(data)
    # stage 7: identify and handle outliers
    print('Stage 7: identify and handle outliers')
    data = identify_and_handle_outliers(data)
    # stage 8: identify correlation
    print('Stage 8: identify correlation')
    identify_correlation(data)
    

    return data