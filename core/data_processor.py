"""
Data processing module for ETL operations.
"""
import logging
import pandas as pd
from common.transforms import (
    infer_and_transform_date, 
    transform_price, 
    remove_special_characters, 
    separate_camel_case,
    transform_provider_name, 
    transform_description,
    extract_measure_and_unit
)


def apply_transformations(df):
    """Apply data transformations to the DataFrame."""
    try:
        # Map column names to standard names if needed
        column_mapping = {
            'Producto': 'Description',
            'Fecha 1': 'LastReviewDt', 
            'Provedor': 'ProviderName',
            'Precio': 'Price'
        }
        
        # Rename columns if they exist
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df = df.rename(columns={old_col: new_col})
        
        # Apply price transformations
        if 'Price' in df.columns:
            df['RawPrice'] = df['Price'].astype(str)
            df['CleanPrice'] = df['Price'].apply(lambda x: transform_price(str(x)) if pd.notna(x) else None)
            df['IsValidPrice'] = df['Price'].notna() & (df['CleanPrice'].notna())
            logging.info(f"Price transformation: {df['IsValidPrice'].sum()}/{len(df)} valid prices")
        
        # Apply date transformations
        if 'LastReviewDt' in df.columns:
            df['RawLastReviewDt'] = df['LastReviewDt'].astype(str)
            df['CleanLastReviewDt'] = df['LastReviewDt'].apply(lambda x: infer_and_transform_date(str(x)) if pd.notna(x) else None)
            valid_dates = df['CleanLastReviewDt'].notna().sum()
            logging.info(f"Date transformation: {valid_dates}/{len(df)} valid dates")
        
        # Apply description transformations
        if 'Description' in df.columns:
            df['RawDescription'] = df['Description'].astype(str)
            
            # Clean special characters and apply camel case separation
            df['CleanDescription'] = df['Description'].apply(
                lambda x: separate_camel_case(remove_special_characters(str(x))) if pd.notna(x) else None
            )
            
            # Alternative: Use the transform_description function (combines both operations)
            df['TransformedDescription'] = df['Description'].apply(
                lambda x: transform_description(str(x)) if pd.notna(x) else None
            )
            
            # Extract measure and unit information
            measure_unit_data = df['Description'].apply(lambda x: extract_measure_and_unit(str(x)) if pd.notna(x) else (None, None, None))
            df['Measure'] = measure_unit_data.apply(lambda x: x[0] if x else None)
            df['UnitOfMeasure'] = measure_unit_data.apply(lambda x: x[1].lower() if x and x[1] else None)
            df['PackageUnits'] = measure_unit_data.apply(lambda x: x[2] if x else None)
            
            # Log extraction results
            measures_found = df['Measure'].notna().sum()
            units_found = df['UnitOfMeasure'].notna().sum()
            packages_found = df['PackageUnits'].notna().sum()
            logging.info(f"Description extraction: {measures_found} measures, {units_found} units, {packages_found} package units found")
        
        # Apply provider name transformations
        if 'ProviderName' in df.columns:
            df['RawProviderName'] = df['ProviderName'].astype(str)
            df['CleanProviderName'] = df['ProviderName'].apply(lambda x: transform_provider_name(str(x)).title() if pd.notna(x) else None)
        
        # Remove rows with all null values
        df = df.dropna(how='all')
        
        logging.info(f"Applied transformations to {len(df)} rows")
        logging.info(f"Columns after transformation: {list(df.columns)}")
        return df
        
    except Exception as e:
        logging.error(f"Error applying transformations: {str(e)}")
        raise