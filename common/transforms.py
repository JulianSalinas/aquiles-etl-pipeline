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