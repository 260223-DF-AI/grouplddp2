import pytest
from datetime import datetime
from app.models.sales_data import SalesData


# -----------------------------
# Fixtures
# -----------------------------
@pytest.fixture
def valid_row():
    return [
        1,
        "2025-08-15",
        "S003",
        "Belltown Seattle",
        "West",
        "WA",
        "C007",
        "George Costanza",
        "Consumer",
        "P103",
        "Ergonomic Chair x200",
        "Furniture",
        "Seating",
        1,
        86.51,
        0.22,
        5.4,
        15.26,
        88.14,
    ]


@pytest.fixture
def valid_entry():
    return SalesData(
        1,
        datetime.fromisoformat("2025-08-15"),
        "S003",
        "Belltown Seattle",
        "West",
        "WA",
        "C007",
        "George Costanza",
        "Consumer",
        "P103",
        "Ergonomic Chair x200",
        "Furniture",
        "Seating",
        1,
        86.51,
        0.22,
        5.4,
        15.26,
        88.14,
    )


@pytest.fixture
def invalid_missing_col_row():
    # Missing the last column
    return [
        1,
        "2025-08-15",
        "S003",
        "Belltown Seattle",
        "West",
        "WA",
        "C007",
        "George Costanza",
        "Consumer",
        "P103",
        "Ergonomic Chair x200",
        "Furniture",
        "Seating",
        1,
        86.51,
        0.22,
        5.4,
        15.26,
    ]


# -----------------------------
# Tests
# -----------------------------
class TestSalesData:

    def test_convert_csv_types(self, valid_row, valid_entry):
        # ACT
        actual = SalesData.convert_csv_types(valid_row)

        # ASSERT — compare fields, not object identity
        assert actual.to_row() == valid_entry.to_row()

    def test_convert_csv_types_mismatched_columns(self, invalid_missing_col_row):
        with pytest.raises(ValueError):
            SalesData.convert_csv_types(invalid_missing_col_row)

    def test_to_row(self, valid_row, valid_entry):
        valid_row[1] = datetime.fromisoformat(valid_row[1])
        
        expected = tuple(valid_row)
        actual = valid_entry.to_row()
        assert actual == expected

    @pytest.mark.parametrize(
        "entry,raises",
        [
            (SalesData(1, datetime.now(), "S001", "Loc", "R", "WA", "C001", "Name",
                       "Seg", "P001", "Prod", "Cat", "Sub", -1, 10.0, 0.1, 1.0, 2.0, 3.0),
             True),  # negative quantity

            (SalesData(1, datetime.now(), "S001", "Loc", "R", "WA", "C001", "Name",
                       "Seg", "P001", "Prod", "Cat", "Sub", 1, -10.0, 0.1, 1.0, 2.0, 3.0),
             True),  # negative unit price

            (SalesData(1, datetime.now(), "S001", "Loc", "R", "WA", "C001", "Name",
                       "Seg", "P001", "Prod", "Cat", "Sub", 1, 10.0, 0.1, 1.0, 2.0, 3.0),
             False),  # valid
        ]
    )
    def test__validate(self, entry, raises):
        if raises:
            with pytest.raises(ValueError):
                entry._validate()
        else:
            entry._validate()  # should NOT raise
