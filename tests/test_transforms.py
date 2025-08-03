import sys
import os
from decimal import Decimal

# Add the parent directory to the path so we can import from common
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.transforms import (
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
    remove_measure_and_unit
)


class TestDateTransforms:
    """Tests for date transformation functions."""
    
    def test_infer_and_transform_date_valid_formats(self):
        """Test date inference with various valid formats."""
        # DD/MM/YYYY format
        assert infer_and_transform_date("15/03/2024") == "2024-03-15"
        
        # MM/DD/YYYY format (should be interpreted as day first due to dayfirst=True)
        assert infer_and_transform_date("03/15/2024") == "2024-03-15"
        
        # Text format
        assert infer_and_transform_date("March 15, 2024") == "2024-03-15"
        
        # ISO format
        assert infer_and_transform_date("2024-03-15") == "2024-03-15"
    
    def test_infer_and_transform_date_edge_cases(self):
        """Test date inference with edge cases."""
        # Empty string
        assert infer_and_transform_date("") is None
        
        # Invalid date
        assert infer_and_transform_date("not a date") is None
        
        # None input (converted to string)
        assert infer_and_transform_date("None") is None


class TestPriceTransforms:
    """Tests for price transformation functions."""
    
    def test_transform_price_valid_formats(self):
        """Test price transformation with various valid formats."""
        # Basic price
        assert transform_price("1000") == Decimal("1000")
        
        # Price with commas (thousands separator)
        assert transform_price("1,000") == Decimal("1000")
        
        # Price with dollar sign
        assert transform_price("$1,000") == Decimal("1000")
        
        # Price with decimal points as thousands separators
        assert transform_price("1.000") == Decimal("1000")
        
        # Complex price format
        assert transform_price("$ 1.500,50") == Decimal("150050")
    
    def test_transform_price_edge_cases(self):
        """Test price transformation with edge cases."""
        # Empty string
        assert transform_price("") is None
        
        # Invalid price
        assert transform_price("not a price") is None
        
        # Only symbols
        assert transform_price("$.,") is None


class TestTextTransforms:
    """Tests for text transformation functions."""
    
    def test_remove_special_characters_valid(self):
        """Test removing special characters from text."""
        # Basic text with special characters
        assert remove_special_characters("Harina@de#Trigo!") == "HarinadeTrigo"
        
        # Text with allowed characters (letters, numbers, /, %, space)
        assert remove_special_characters("Producto 100% Natural/Organico") == "Producto 100% Natural/Organico"
        
        # Text with numbers
        assert remove_special_characters("Producto123@#$") == "Producto123"
    
    def test_remove_special_characters_edge_cases(self):
        """Test removing special characters with edge cases."""
        # Empty string
        assert remove_special_characters("") == ""
        
        # Only special characters
        assert remove_special_characters("@#$!") == ""
        
        # None should return None (handled by exception)
        # This would be called with str(None) = "None" in real usage
    
    def test_separate_camel_case_valid(self):
        """Test separating camel case text."""
        # Basic camel case
        assert separate_camel_case("HarinaDeTrigo") == "Harina De Trigo"
        
        # Camel case with numbers
        assert separate_camel_case("Producto123ABC") == "Producto 123ABC"
        
        # Already separated text
        assert separate_camel_case("Already Separated") == "Already Separated"
        
        # Single word
        assert separate_camel_case("Word") == "Word"
    
    def test_separate_camel_case_edge_cases(self):
        """Test separating camel case with edge cases."""
        # Empty string
        assert separate_camel_case("") == ""
        
        # All lowercase
        assert separate_camel_case("lowercase") == "lowercase"
        
        # All uppercase
        assert separate_camel_case("UPPERCASE") == "UPPERCASE"


class TestProviderTransforms:
    """Tests for provider name transformation functions."""
    
    def test_transform_provider_name_valid(self):
        """Test provider name transformation."""
        # Provider with special characters and camel case
        assert transform_provider_name("ProveedorABC@123") == "Proveedor ABC123"
        
        # Simple provider name
        assert transform_provider_name("MiProveedor") == "Mi Proveedor"
        
        # Provider with numbers
        assert transform_provider_name("Proveedor123ABC") == "Proveedor 123ABC"
    
    def test_transform_provider_name_edge_cases(self):
        """Test provider name transformation with edge cases."""
        # Empty string
        assert transform_provider_name("") == ""
        
        # Only special characters
        assert transform_provider_name("@#$!") == ""
    
    def test_transform_description_valid(self):
        """Test description transformation."""
        # Description with special characters and camel case
        assert transform_description("ProductoEspecial@123") == "Producto Especial 123"
        
        # Simple description
        assert transform_description("MiProducto") == "Mi Producto"


class TestMeasureExtractionFunctions:
    """Tests for measure and unit extraction functions."""
    
    def test_extract_measure_valid(self):
        """Test measure extraction from text."""
        # Basic measure
        assert extract_measure("500g de harina") == "500"
        
        # Decimal measure
        assert extract_measure("1.5kg arroz") == "1.5"
        
        # Multiple measures (should get first)
        assert extract_measure("500g y 200ml") == "500"
    
    def test_extract_measure_edge_cases(self):
        """Test measure extraction with edge cases."""
        # No measure
        assert extract_measure("solo texto") is None
        
        # Empty string
        assert extract_measure("") is None
    
    def test_extract_unit_valid(self):
        """Test unit extraction from text."""
        # Basic unit
        assert extract_unit("500g de harina") == "g"
        
        # Different unit
        assert extract_unit("1.5kg arroz") == "kg"
        
        # Three letter unit
        assert extract_unit("200ml agua") == "ml"
    
    def test_extract_unit_edge_cases(self):
        """Test unit extraction with edge cases."""
        # No unit
        assert extract_unit("solo texto") is None
        
        # Empty string
        assert extract_unit("") is None
    
    def test_extract_package_units_valid(self):
        """Test package units extraction from text."""
        # Basic package units
        assert extract_package_units("Arroz x 12 unidades") == "12"
        
        # With spaces
        assert extract_package_units("Producto x 6 piezas") == "6"
        
        # No space after x
        assert extract_package_units("Itemx24") == "24"
    
    def test_extract_package_units_edge_cases(self):
        """Test package units extraction with edge cases."""
        # No package units
        assert extract_package_units("solo producto") is None
        
        # Empty string
        assert extract_package_units("") is None
    
    def test_extract_measure_and_unit_combined(self):
        """Test combined measure and unit extraction."""
        # Complete extraction
        result = extract_measure_and_unit("Arroz 500g x 12 unidades")
        assert result == ("500", "g", "12")
        
        # Only measure and unit
        result = extract_measure_and_unit("Harina 1.5kg")
        assert result == ("1.5", "kg", None)
        
        # Only package units
        result = extract_measure_and_unit("Producto x 6")
        assert result == (None, None, "6")
        
        # Nothing found
        result = extract_measure_and_unit("Solo texto")
        assert result == (None, None, None)


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


def run_all_tests():
    """Run all tests manually without pytest."""
    print("Running Transform Function Tests")
    print("=" * 50)
    
    test_classes = [
        TestDateTransforms(),
        TestPriceTransforms(),
        TestTextTransforms(),
        TestProviderTransforms(),
        TestMeasureExtractionFunctions(),
        TestMeasureRemovalFunctions()
    ]
    
    total_tests = 0
    passed_tests = 0
    
    for test_class in test_classes:
        class_name = test_class.__class__.__name__
        print(f"\n{class_name}:")
        
        # Get all test methods
        test_methods = [method for method in dir(test_class) if method.startswith('test_')]
        
        for method_name in test_methods:
            total_tests += 1
            try:
                method = getattr(test_class, method_name)
                method()
                print(f"  ✅ {method_name}")
                passed_tests += 1
            except AssertionError as e:
                print(f"  ❌ {method_name}: Assertion failed - {e}")
            except Exception as e:
                print(f"  ❌ {method_name}: Error - {e}")
    
    print(f"\n" + "=" * 50)
    print(f"Tests completed: {passed_tests}/{total_tests} passed")
    
    if passed_tests == total_tests:
        print("🎉 All tests passed!")
    else:
        print(f"⚠️  {total_tests - passed_tests} tests failed")
    
    return passed_tests == total_tests


if __name__ == "__main__":
    run_all_tests()
