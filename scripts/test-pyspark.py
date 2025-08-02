import re

# Regex to find the measure and unit in the string
measure_regex = r"(\d+\.?\d*)\s*([a-zA-Z]{1,2})"
packageUnits_regex = r"[x]\s*(\d+)"


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
    # Uses the split helper functions
    return (
        extract_measure(measure_str),
        extract_unit(measure_str),
        extract_package_units(measure_str)
    )

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
    
if __name__ == "__main__":
    measure = extract_measure_and_unit("Rice of 12.5kg, Glue 5.5kg, Beans 3.2kg x93 x88")
    print(f"Extracted Measure: {measure}")
    new_measure_str = remove_measure_and_unit("Rice of 12.5kg, Glue 5.5kg, Beans 3.2kg x93 x88")
    print(f"New Measure String: {new_measure_str}")
    measure = extract_measure_and_unit("Nothig to extract here")
    print(f"Extracted Measure: {measure}")
    measure = extract_measure_and_unit("Chicle Lengüeton 9g x 24")
    print(f"Extracted Measure: {measure}")
    measure = extract_measure_and_unit("Acondionador Pantene x24*")
    print(f"Extracted Measure: {measure}")
    measure = extract_measure_and_unit("Acondionador Pantene 4ml")
    print(f"Extracted Measure: {measure}")