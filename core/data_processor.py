"""
Data processing module for ETL operations.
"""
import logging
import pandas as pd
from dateutil.parser import parse
from decimal import *
import re

# Regex to find the measure and unit in the string
measure_regex = r"(\d+\.?\d*)\s*([a-zA-Z]{1,3})"
packageUnits_regex = r"[x]\s*(\d+)"

# Define a function to infer the date format and transform the date string
def infer_and_transform_date(date_str):
    try:
        parsed_date = parse(date_str, dayfirst=True, fuzzy=True)
        return parsed_date.strftime("%Y-%m-%d")
    except Exception as e:
        return None

# Define a function to transform the Price column
def transform_price(price_str):
    try:
        cleaned_price_str = price_str.replace(".", "").replace(",", "").replace("$", "").replace(" ", "")
        return Decimal(cleaned_price_str)
    except Exception as e:
        return None

# Define a function to remove special characters
def remove_special_characters(text):
    try:
        return re.sub(r'[^A-Za-z0-9/% ]+', '', text)
    except Exception as e:
        return None

# Define a function to separate camel case
def separate_camel_case(text):
    try:
        return re.sub(r'([a-z])([A-Z0-9])', r'\1 \2', text)
    except Exception as e:
        return None

# Define a function to transform the ProviderName column
def transform_provider_name(provider_name):
    try:
        cleaned_name = remove_special_characters(provider_name)
        separated_name = separate_camel_case(cleaned_name)
        return separated_name
    except Exception as e:
        return None

def capitalize_first_letter(text):
    """Capitalize the first letter of each word using built-in string methods."""
    try:
        if not text or not isinstance(text, str):
            return text
        return text.strip().title()
    except Exception as e:
        return text
    
def transform_description(description): 
    try:
        return capitalize_first_letter(description)
    except Exception as e:
        return None

# Define a function to extract the unit of measure and the measure from a given string
def extract_measure(measure_str):
    try:
        measure = re.findall(measure_regex, measure_str)
        return measure[0][0] if measure else None
    except Exception:
        return None

def extract_unit(measure_str):
    try:
        measure = re.findall(measure_regex, measure_str)
        return measure[0][1] if measure else None
    except Exception:
        return None

def extract_package_units(measure_str):
    try:
        packageUnits = re.findall(packageUnits_regex, measure_str)
        return packageUnits[0] if packageUnits else None
    except Exception:
        return None

def extract_measure_and_unit(measure_str):
    measure = extract_measure(measure_str)
    unit = extract_unit(measure_str)
    measure_str = remove_unit(measure_str)
    package_units = extract_package_units(measure_str)
    return (measure, unit, package_units)

def remove_measure(measure_str):
    try:
        return re.sub(measure_regex, "", measure_str)
    except Exception:
        return measure_str

def remove_unit(measure_str):
    # Since measure and unit are matched together, this is handled by remove_measure
    # This function is kept for symmetry and future extension
    return measure_str

def remove_package_units(measure_str):
    try:
        return re.sub(packageUnits_regex, "", measure_str)
    except Exception:
        return measure_str

def remove_measure_and_unit(measure_str):
    try:
        s = remove_measure(measure_str)
        s = remove_package_units(s)
        s = s.strip()
        return s
    except Exception:
        return measure_str
    
def extract_iva(description):
    """Extract IVA percentage from the description."""
    try:
        # Look for pattern like (G13), (g13), (G 13), (g 13), or (G1 ) at the end of the description
        iva_pattern = r'\(\s*[Gg]\s*(\d+)\s*\)'
        match = re.search(iva_pattern, description)
        if match:
            return int(match.group(1))
        return None
    except Exception as e:
        return None

def apply_transformations(df):
    """Apply data transformations to the DataFrame."""
    try:
        # Map column names to standard names if needed
        column_mapping = {
            'Producto': 'Description',
            'Fecha 1': 'LastReviewDt', 
            'Provedor': 'ProviderName',
            'Precio': 'Price',
            "IVA": "PercentageIVA"
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
        
        # Apply date transformations
        if 'LastReviewDt' in df.columns:
            df['RawLastReviewDt'] = df['LastReviewDt'].astype(str)
            df['CleanLastReviewDt'] = df['LastReviewDt'].apply(lambda x: infer_and_transform_date(str(x)) if pd.notna(x) else None)

        # Apply description transformations
        if 'Description' in df.columns:
            df['RawDescription'] = df['Description'].astype(str)
            
            df['CleanDescription'] = df['Description'].apply(
                lambda x: transform_description(str(x)) if pd.notna(x) else None
            )
            
            measure_unit_data = df['Description'].apply(lambda x: extract_measure_and_unit(str(x)) if pd.notna(x) else (None, None, None))
            df['Measure'] = measure_unit_data.apply(lambda x: x[0] if x else None)
            df['UnitOfMeasure'] = measure_unit_data.apply(lambda x: x[1].lower() if x and x[1] else None)
            df['PackageUnits'] = measure_unit_data.apply(lambda x: x[2] if x else None)
            
            if 'PercentageIVA' in df.columns:
                df['PercentageIVA'] = df['Description'].apply(lambda x: extract_iva(str(x)) if pd.notna(x) else None)
                
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