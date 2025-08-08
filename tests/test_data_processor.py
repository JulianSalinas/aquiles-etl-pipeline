import sys
import os
import pytest
from decimal import Decimal

# Add the parent directory to the path so we can import from common
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.data_processor import (
    infer_and_transform_date,
    transform_price,
    remove_special_characters,
    separate_camel_case,
    transform_provider_name,
    transform_description,
    extract_measure,
    extract_unit,
    extract_package_units,
    extract_measure_and_unit,
    remove_measure,
    remove_package_units,
    remove_measure_and_unit,
    extract_iva
)


# Pytest markers for categorizing tests
pytestmark = pytest.mark.transforms


@pytest.mark.date
class TestDateTransforms:
    """Tests for date transformation functions."""
    
    @pytest.mark.parametrize("input_date,expected", [
        ("15/03/2024", "2024-03-15"),
        ("03/15/2024", "2024-03-15"),
        ("March 15, 2024", "2024-03-15"),
        ("2024-03-15", "2024-03-15")
    ])
    def test_infer_and_transform_date_valid_formats(self, input_date, expected):
        """Test date inference with various valid formats."""
        assert infer_and_transform_date(input_date) == expected
    
    @pytest.mark.parametrize("invalid_input", [
        "",
        "not a date",
        "None"
    ])
    def test_infer_and_transform_date_edge_cases(self, invalid_input):
        """Test date inference with edge cases."""
        assert infer_and_transform_date(invalid_input) is None


@pytest.mark.price
class TestPriceTransforms:
    """Tests for price transformation functions."""
    
    @pytest.mark.parametrize("input_price,expected", [
        ("1000", Decimal("1000")),
        ("1,000", Decimal("1000")),
        ("$1,000", Decimal("1000")),
        ("1.000", Decimal("1000")),
        ("$ 1.500,50", Decimal("150050"))
    ])
    def test_transform_price_valid_formats(self, input_price, expected):
        """Test price transformation with various valid formats."""
        assert transform_price(input_price) == expected
    
    @pytest.mark.parametrize("invalid_input", [
        "",
        "not a price", 
        "$.,",
    ])
    def test_transform_price_edge_cases(self, invalid_input):
        """Test price transformation with edge cases."""
        assert transform_price(invalid_input) is None


@pytest.mark.text
class TestTextTransforms:
    """Tests for text transformation functions."""
    
    @pytest.mark.parametrize("input_text,expected", [
        ("Harina@de#Trigo!", "HarinadeTrigo"),
        ("Producto 100% Natural/Organico", "Producto 100% Natural/Organico"),
        ("Producto123@#$", "Producto123")
    ])
    def test_remove_special_characters_valid(self, input_text, expected):
        """Test removing special characters from text."""
        assert remove_special_characters(input_text) == expected
    
    @pytest.mark.parametrize("input_text,expected", [
        ("", ""),
        ("@#$!", "")
    ])
    def test_remove_special_characters_edge_cases(self, input_text, expected):
        """Test removing special characters with edge cases."""
        assert remove_special_characters(input_text) == expected
    
    @pytest.mark.parametrize("input_text,expected", [
        ("HarinaDeTrigo", "Harina De Trigo"),
        ("Producto123ABC", "Producto 123ABC"),
        ("Already Separated", "Already Separated"),
        ("Word", "Word")
    ])
    def test_separate_camel_case_valid(self, input_text, expected):
        """Test separating camel case text."""
        assert separate_camel_case(input_text) == expected
    
    @pytest.mark.parametrize("input_text,expected", [
        ("", ""),
        ("lowercase", "lowercase"),
        ("UPPERCASE", "UPPERCASE")
    ])
    def test_separate_camel_case_edge_cases(self, input_text, expected):
        """Test separating camel case with edge cases."""
        assert separate_camel_case(input_text) == expected


@pytest.mark.provider
class TestProviderTransforms:
    """Tests for provider name transformation functions."""
    
    @pytest.mark.parametrize("input_name,expected", [
        ("ProveedorABC@123", "Proveedor ABC123"),
        ("MiProveedor", "Mi Proveedor"),
        ("Proveedor123ABC", "Proveedor 123ABC")
    ])
    def test_transform_provider_name_valid(self, input_name, expected):
        """Test provider name transformation."""
        assert transform_provider_name(input_name) == expected
    
    @pytest.mark.parametrize("input_name,expected", [
        ("", ""),
        ("@#$!", "")
    ])
    def test_transform_provider_name_edge_cases(self, input_name, expected):
        """Test provider name transformation with edge cases."""
        assert transform_provider_name(input_name) == expected
    
    @pytest.mark.parametrize("input_desc,expected", [
        ("ProductoEspecial@123", "Productoespecial@123"),
        ("MiProducto", "Miproducto"),
        ("hello world", "Hello World"),
        ("HELLO WORLD", "Hello World"),
        ("hello-world test", "Hello-World Test"),
        ("product description here", "Product Description Here"),
        ("", ""),
        ("a", "A")
    ])
    def test_transform_description_valid(self, input_desc, expected):
        """Test description transformation - only capitalizes using title()."""
        assert transform_description(input_desc) == expected


@pytest.mark.extraction
class TestMeasureExtractionFunctions:
    """Tests for measure and unit extraction functions."""
    
    @pytest.mark.parametrize("input_text,expected", [
        ("500g de harina", "500"),
        ("1.5kg arroz", "1.5"),
        ("500g y 200ml", "500")  # Should get first
    ])
    def test_extract_measure_valid(self, input_text, expected):
        """Test measure extraction from text."""
        assert extract_measure(input_text) == expected
    
    @pytest.mark.parametrize("input_text", [
        "solo texto",
        ""
    ])
    def test_extract_measure_edge_cases(self, input_text):
        """Test measure extraction with edge cases."""
        assert extract_measure(input_text) is None
    
    @pytest.mark.parametrize("input_text,expected", [
        ("500g de harina", "g"),
        ("1.5kg arroz", "kg"),
        ("200ml agua", "ml")
    ])
    def test_extract_unit_valid(self, input_text, expected):
        """Test unit extraction from text."""
        assert extract_unit(input_text) == expected
    
    @pytest.mark.parametrize("input_text", [
        "solo texto",
        ""
    ])
    def test_extract_unit_edge_cases(self, input_text):
        """Test unit extraction with edge cases."""
        assert extract_unit(input_text) is None
    
    @pytest.mark.parametrize("input_text,expected", [
        ("Arroz x 12 unidades", "12"),
        ("Producto x 6 piezas", "6"),
        ("Itemx24", "24")
    ])
    def test_extract_package_units_valid(self, input_text, expected):
        """Test package units extraction from text."""
        assert extract_package_units(input_text) == expected
    
    @pytest.mark.parametrize("input_text", [
        "solo producto",
        ""
    ])
    def test_extract_package_units_edge_cases(self, input_text):
        """Test package units extraction with edge cases."""
        assert extract_package_units(input_text) is None
    
    @pytest.mark.parametrize("input_text,expected", [
        ("Arroz 500g x 12 unidades", ("500", "g", "12")),
        ("Harina 1.5kg", ("1.5", "kg", None)),
        ("Producto x 6", (None, None, "6")),
        ("Solo texto", (None, None, None))
    ])
    def test_extract_measure_and_unit_combined(self, input_text, expected):
        """Test combined measure and unit extraction."""
        assert extract_measure_and_unit(input_text) == expected


@pytest.mark.removal
class TestMeasureRemovalFunctions:
    """Tests for measure and unit removal functions."""
    
    def test_remove_measure_valid(self):
        """Test measure removal from text."""
        # Basic removal
        result = remove_measure("Arroz 500g especial")
        assert "500g" not in result
        assert "Arroz" in result and "especial" in result
        
        # Multiple measures
        result = remove_measure("Producto 500g y 200ml")
        assert "500g" not in result and "200ml" not in result
    
    def test_remove_measure_edge_cases(self):
        """Test measure removal with edge cases."""
        # No measure to remove
        assert remove_measure("Solo texto") == "Solo texto"
        
        # Empty string
        assert remove_measure("") == ""
    
    def test_remove_package_units_valid(self):
        """Test package units removal from text."""
        # Basic removal
        result = remove_package_units("Arroz x 12 unidades")
        assert "x 12" not in result
        assert "Arroz" in result and "unidades" in result
    
    def test_remove_package_units_edge_cases(self):
        """Test package units removal with edge cases."""
        # No package units to remove
        assert remove_package_units("Solo texto") == "Solo texto"
        
        # Empty string
        assert remove_package_units("") == ""
    
    def test_remove_measure_and_unit_complete(self):
        """Test complete measure and unit removal."""
        # Complete removal
        result = remove_measure_and_unit("Arroz Premium 500g x 12 unidades")
        # Should remove "500g" and "x 12", leaving "Arroz Premium unidades"
        assert "500g" not in result
        assert "x 12" not in result
        assert "Arroz" in result
        assert "Premium" in result
        
        # Clean text (no measure/units)
        result = remove_measure_and_unit("Producto Normal")
        assert result == "Producto Normal"


@pytest.mark.iva
class TestIVAExtractionFunctions:
    """Tests for IVA percentage extraction functions."""
    
    @pytest.mark.parametrize("description,expected_iva", [
        ("MINI PAPA KITTY 1X30(G13)", 13),
        ("KIKUA CHIPOTLE KITTY 1X25(G13)", 13),
        ("GUSITITOS SALSA Y QUESO BD 17G 1X12(G13)", 13),
        ("GUSITITOS SUPER CHOMBO BD 15G 1X12(G13)", 13),
        ("QUESITRIX SALSA PICANTE BD 10 G 1X12(G13)", 13),
        ("BUENACHOS SALSAPEÑO BD 18G 1X 12(G13)", 13),
        ("BORRACHO GUAYABA MOANA 1X24(G 13)", 13),
        ("ROSCA QUESO MOANA 1X24(G1)", 1),
        ("PALO QUESO MOANA 1X30(G1)", 1),
        ("ARGOLLADO PREMIUM DELI 1X32(G 13)", 13),
        ("TAPON MARSELLEZA 1X26(G13)", 13),
        ("EMPANADA PINA KEYMAR 1X40(G1)", 1),
        ("GALLETA NATILLA MOANA 1X28(G1 )", 1),
    ])
    def test_extract_iva_valid_patterns(self, description, expected_iva):
        """Test IVA extraction with valid patterns from real data."""
        result = extract_iva(description)
        assert result == expected_iva
    
    @pytest.mark.parametrize("description,expected_iva", [
        ("PRODUCTO (g13)", 13),  # lowercase g
        ("PRODUCTO (g1)", 1),    # lowercase g with single digit
        ("PRODUCTO (g 13)", 13), # lowercase g with space
        ("PRODUCTO (g1 )", 1),   # lowercase g with trailing space
        ("PRODUCTO ( g1 )", 1),   # lowercase g with trailing spaces
    ])
    def test_extract_iva_case_insensitive(self, description, expected_iva):
        """Test IVA extraction with case-insensitive patterns."""
        result = extract_iva(description)
        assert result == expected_iva
    
    @pytest.mark.parametrize("description", [
        "PRODUCTO SIN IVA",
        "DESCRIPCION NORMAL",
        "PRODUCTO CON MEDIDA 500g",
        "OTRO PRODUCTO (SIN G)",
        "PRODUCTO (G)",  # G without number
        "",
        None
    ])
    def test_extract_iva_no_pattern(self, description):
        """Test IVA extraction when no valid pattern is found."""
        result = extract_iva(description)
        assert result is None
    
    def test_extract_iva_edge_cases(self):
        """Test IVA extraction with edge cases."""
        # Multiple G patterns (should find the first one)
        result = extract_iva("PRODUCTO (G5) OTRO (G13)")
        assert result == 5
        
        # Different case sensitivity - both should work
        result = extract_iva("PRODUCTO (g13)")  # lowercase g
        assert result == 13  # Should now match lowercase
        
        result = extract_iva("PRODUCTO (G13)")  # uppercase G
        assert result == 13  # Should still match uppercase
        
        # Numbers with multiple digits
        result = extract_iva("PRODUCTO (G123)")
        assert result == 123


def run_all_tests():
    """Run all tests using pytest with various options."""
    import subprocess
    
    print("🧪 Running Transform Function Tests with pytest")
    print("=" * 60)
    
    # Run with verbose output and coverage if available
    result = subprocess.run([
        'pytest', __file__, 
        '-v',                   # verbose output
        '--tb=short',           # shorter traceback format
        '--durations=10',       # show 10 slowest tests
        '-x'                    # stop on first failure
    ], capture_output=True, text=True)
    
    print(result.stdout)
    if result.stderr:
        print("Errors:", result.stderr)
    
    # Additional summary
    if result.returncode == 0:
        print("\n🎉 All tests passed!")
        print("💡 You can run specific test categories with:")
        print("   pytest tests/test_transforms.py -m date")
        print("   pytest tests/test_transforms.py -m price") 
        print("   pytest tests/test_transforms.py -m text")
        print("   pytest tests/test_transforms.py -m provider")
        print("   pytest tests/test_transforms.py -m extraction")
        print("   pytest tests/test_transforms.py -m removal")
        print("   pytest tests/test_transforms.py -m iva")
    else:
        print("\n❌ Some tests failed!")
    
    return result.returncode == 0


if __name__ == "__main__":
    run_all_tests()
