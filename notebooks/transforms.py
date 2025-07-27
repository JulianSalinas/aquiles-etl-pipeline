from dateutil.parser import parse
from decimal import *
import re

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
        return re.sub(r'[^A-Za-z0-9/ ]+', '', text)
    except Exception as e:
        return None

# Define a function to separate camel case
def separate_camel_case(text):
    try:
        return re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
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
    
# Define a function to extract the unit of measure and the measure from a given string
def extract_measure_and_unit(measure_str):
    try:
        measure = re.findall(r"(\d+\.?\d*)\s*([a-zA-Z]{1,2})", measure_str)
        packageUnits =  re.findall(r"[x]\s*(\d+)", measure_str)
        return (measure[0][0] if measure else None, measure[0][1] if measure else None, packageUnits[0] if packageUnits else None)
    except Exception as e:
        return (None, None, None)
    
if __name__ == "__main__":
    # Test the functions
    print(extract_measure_and_unit("Coke 1000mg/5ml"))