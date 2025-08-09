'''
Data processing module for ETL operations.
'''
import logging
import re
from re import Match
from datetime import datetime
from decimal import Decimal

import pandas as pd
from dateutil.parser import parse

measure_regex = r'(\d+\.?\d*)\s*([a-zA-Z]{1,3})'
package_units_regex = r'[x]\s*(\d+)'

def infer_and_transform_date(date_str: str) -> str | None:
    try:
        parsed_date: datetime = parse(timestr=date_str, dayfirst=True, fuzzy=True)
        return parsed_date.strftime(format='%Y-%m-%d')
    except Exception:
        return None

def transform_price(price_str: str) -> Decimal | None:
    try:
        cleaned_price: str = price_str.replace('.', '').replace(',', '').replace('$', '').replace(' ', '')
        return Decimal(cleaned_price)
    except Exception:
        return None

def remove_special_characters(text: str) -> str | None:
    try:
        return re.sub(pattern=r'[^A-Za-z0-9/% ]+', repl='', string=text)
    except Exception:
        return None

def separate_camel_case(text: str) -> str | None:
    try:
        return re.sub(pattern=r'([a-z])([A-Z0-9])', repl=r'\1 \2', string=text)
    except Exception:
        return None

def transform_provider_name(provider_name: str) -> str | None:
    try:
        cleaned_name: str | None = remove_special_characters(provider_name)
        separated_name: str | None = separate_camel_case(cleaned_name) if cleaned_name else None
        return separated_name.title() if separated_name else None
    except Exception:
        return None

def capitalize_first_letter(text: str) -> str | None:
    try:
        return text if not text else text.strip().title()
    except Exception:
        return None

def transform_description(description: str) -> str | None:
    try:
        return capitalize_first_letter(description)
    except Exception:
        return None

def extract_measure(measure_str: str) -> str | None:
    try:
        measure: list[tuple[str, str]] = re.findall(pattern=measure_regex, string=measure_str)
        return measure[0][0] if measure else None
    except Exception:
        return None

def extract_unit(measure_str: str) -> str | None:
    try:
        measure: list[tuple[str, str]] = re.findall(pattern=measure_regex, string=measure_str)
        return measure[0][1] if measure else None
    except Exception:
        return None

def extract_package_units(measure_str: str) -> str | None:
    try:
        package_units: list[str] = re.findall(pattern=package_units_regex, string=measure_str)
        return package_units[0] if package_units else None
    except Exception:
        return None

def extract_measure_and_unit(measure_str: str) -> tuple[str | None, str | None, str | None]:
    measure: str | None = extract_measure(measure_str)
    unit: str | None = extract_unit(measure_str)
    unit = unit.lower() if unit else None
    package_units: str | None = extract_package_units(measure_str)
    return (measure, unit, package_units)

def remove_package_units(measure_str: str) -> str | None:
    try:
        return re.sub(pattern=package_units_regex, repl='', string=measure_str)
    except Exception:
        return measure_str

def extract_iva(description: str) -> int | None:
    '''Look for patterns like (G13), (g13), (G 13), (g 13), or (G1 ) at the end of the description.'''
    try:
        iva_pattern = r'\(\s*[Gg]\s*(\d+)\s*\)'
        match: Match[str] | None = re.search(pattern=iva_pattern, string=description)
        return int(match.group(1)) if match else None
    except Exception:
        return None

def map_columns_to_apply_transformations(df: pd.DataFrame) -> pd.DataFrame:

    column_mapping: dict[str, str] = {
        'Producto': 'Description',
        'Fecha 1': 'LastReviewDt', 
        'Provedor': 'ProviderName',
        'Precio': 'Price',
        "IVA": "PercentageIVA",
        "Porcentaje de IVA": "PercentageIVA"
    }

    df = df.rename(columns={old: new for old, new in column_mapping.items() if old in df.columns})

    return df

def apply_transformations(df: pd.DataFrame) -> pd.DataFrame:
    '''Apply data transformations to the DataFrame.'''
    try:
        df = _apply_transformations_logic(df)
        return df
    except Exception as e:
        logging.error(f'Error applying transformations: {str(e)}')
        raise

def _apply_transformations_logic(df: pd.DataFrame) -> pd.DataFrame:

    if 'LastReviewDt' not in df.columns:
        df['LastReviewDt'] = datetime.now().strftime('%Y-%m-%d')
    
    if 'Price' in df.columns:
        df['RawPrice'] = df['Price'].astype(str)
        df['CleanPrice'] = df['Price'].map(lambda x: transform_price(str(x)))
        df['IsValidPrice'] = df['Price'].notna() & (df['CleanPrice'].notna())
    
    if 'LastReviewDt' in df.columns:
        df['RawLastReviewDt'] = df['LastReviewDt'].astype(str)
        df['CleanLastReviewDt'] = df['LastReviewDt'].map(infer_and_transform_date)

    if 'Description' in df.columns:
        df['RawDescription'] = df['Description'].astype(str)
        df['CleanDescription'] = df['Description'].map(transform_description)
        
        # Extract measure, unit, and package units
        measure_unit_data = df['Description'].map(extract_measure_and_unit)
        df['Measure'] = measure_unit_data.map(lambda x: x[0] if x else None)
        df['UnitOfMeasure'] = measure_unit_data.map(lambda x: x[1] if x else None)
        df['PackageUnits'] = measure_unit_data.map(lambda x: x[2] if x else None)

        # Extract IVA percentage if not already present
        if 'PercentageIVA' not in df.columns:
            df['PercentageIVA'] = df['Description'].map(extract_iva)

    # Apply provider name transformations
    if 'ProviderName' in df.columns:
        df['RawProviderName'] = df['ProviderName'].astype(str)
        df['CleanProviderName'] = df['ProviderName'].map(transform_provider_name)
        
    df = df.dropna(how='all') # type: ignore

    logging.info(f'Applied transformations to {len(df)} rows')
    logging.info(f'Columns after transformation: {list(df.columns)}')

    return df