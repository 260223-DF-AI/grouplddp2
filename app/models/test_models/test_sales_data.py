import pytest
from sales_data import SalesData

@pytest.fixture
def valid_row():
    return [1,2025-08-15,'S003',"Belltown Seattle","West",'WA','C007',"George Costanza","Consumer",'P103',"Ergonomic Chair x200",'Furniture','Seating',1,86.51,0.22,5.4,15.26,88.14]

@pytest.fixture
def valid_entry():
    return SalesData(1,2025-08-15,'S003',"Belltown Seattle","West",'WA','C007',"George Costanza","Consumer",'P103',"Ergonomic Chair x200",'Furniture','Seating',1,86.51,0.22,5.4,15.26,88.14)

@pytest.fixture
def invalid_missing_col_row():
    return [1,2025-08-15,'S003',"Belltown Seattle","West",'WA','C007',"George Costanza","Consumer",'P103',"Ergonomic Chair x200",'Furniture','Seating',1,86.51,0.22,5.4,15.26]

class TestSalesData:
    
    def test_convert_csv_types(self, valid_row, valid_entry):
        # ARRANGE - SETUP
        expected = valid_entry

        # ACT - DO THE THING
        actual = SalesData.convert_csv_types(SalesData, valid_row)

        # ASSERT - VERIFY
        assert actual == expected

    
    def test_convert_csv_types_mismatched_columns(self, invalid_missing_col_row):
        # Act & Assert
        with pytest.raises(ValueError):
            SalesData.convert_csv_types(SalesData, invalid_missing_col_row)

    def test_to_row(self, valid_row, valid_entry):
        # Arrange
        expected = tuple(valid_row)
        
        # Actual
        actual = valid_entry.to_row()
        
        # Expected
        actual == expected
        

    @pytest.mark.parametrize("name,expected", [
        ("Bob", "Hello, Bob!"),
        ("Charlie", "Hello, Charlie!"),
    ])
    def test_greet_different_names(self, name, expected):
        user = User(name, f"{name.lower()}@example.com")
        assert user.greet() == expected

    @pytest.mark.parametrize("a, b, expected", [
        (2, 2, 0),
        (3, 2, 1),
        (4, 2, 2),
        (5, 2, 3),
        (0, -2, 2),
        (2, 4, -2),
        (-1, -1, 0)
    ])
