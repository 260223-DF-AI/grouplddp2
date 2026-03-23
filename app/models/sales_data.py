from datetime import datetime
import re

class SalesData():
    """Class to convert and validate field types of sales transaction data

    Raises:
        ValueError: If row does not match expected number of columns
        ValueError: If field type is incorrect

    Returns:
        SalesData: Object with correct field types
    """
    columns = [
        "transaction_id",
        "date",
        "store_id",
        "store_location",
        "region",
        "state",
        "customer_id",
        "customer_name",
        "segment",
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "quantity",
        "unit_price",
        "discount_percent",
        "tax_amount",
        "shipping_cost",
        "total_amount"
    ]
    
    NUM_OF_COLS = len(columns)
    
    ID_PATTERNS = {
    "S": re.compile(r"S\d{3}"),
    "C": re.compile(r"C\d{3}"),
    "P": re.compile(r"P\d{3}")
    }

    def __init__(self, transaction_id, date, store_id, store_location, region, state, customer_id, 
                customer_name, segment, product_id, product_name, category, subcategory, quantity, 
                unit_price, discount_percent, tax_amount, shipping_cost, total_amount):
        self.transaction_id = transaction_id 
        self.date = date
        self.store_id = store_id
        self.store_location = store_location
        self.region = region
        self.state = state
        self.customer_id = customer_id
        self.customer_name = customer_name
        self.segment = segment
        self.product_id = product_id
        self.product_name = product_name
        self.category = category
        self.subcategory = subcategory
        self.quantity = quantity
        self.unit_price = unit_price
        self.discount_percent = discount_percent
        self.tax_amount = tax_amount
        self.shipping_cost = shipping_cost
        self.total_amount = total_amount
    
    @classmethod
    def convert_csv_types(cls: SalesData, row: list[str]) -> SalesData:
        """Factory method to convert types and format field values

        Args:
            cls (SalesData): Class object
            row (list[str]): .csv row to be validated

        Raises:
            ValueError: If row does not match expected number of columns 

        Returns:
            SalesData: Class object with cleaned values
        """
        if len(row) != cls.NUM_OF_COLS:
            raise ValueError(f"Expected {cls.NUM_OF_COLS} fields, received {len(row)}")
        
        transaction_id =  int(row[0])
        date = datetime.fromisoformat(row[1])
        store_id = row[2].strip() # validate format: S001 - start w/ S then 3 [0-9]
        store_location = row[3].strip()
        region = row[4].strip()
        state = row[5].strip().upper()
        customer_id = row[6].strip() # validate format: C001 - start w/ C then 3 [0-9]
        customer_name = row[7].strip()
        segment = row[8].strip()
        product_id = row[9].strip() # validate format: P001 - start w/ P then 3 [0-9]
        product_name = row[10].strip()
        category = row[11].strip()
        subcategory = row[12].strip()
        quantity = int(row[13])
        unit_price = float(row[14])
        discount_percent = float(row[15]) 
        tax_amount = float(row[16])
        shipping_cost = float(row[17])
        total_amount = float(row[18])
        
        return cls(transaction_id, date, store_id, store_location, region, state, customer_id, customer_name, 
                segment, product_id, product_name, category, subcategory, quantity, unit_price, discount_percent, 
                tax_amount, shipping_cost, total_amount)
    
    def to_row(self) -> tuple:
        """Converts class object into tuple of attribute values.

        Returns:
            tuple: returns row values as tuple
        """
        # self._validate() # Commented out for efficiency
        
        return (
            self.transaction_id,
            self.date,
            self.store_id,
            self.store_location,
            self.region,
            self.state,
            self.customer_id,
            self.customer_name,
            self.segment,
            self.product_id,
            self.product_name,
            self.category,
            self.subcategory,
            self.quantity,
            self.unit_price,
            self.discount_percent,
            self.tax_amount,
            self.shipping_cost,
            self.total_amount
        )
    
    def _validate(self):
        """Left function in as example - current data sets to not need validation beyond type conversion"""
        if self.date > datetime.now():
            raise ValueError(f"Date must be equal to or prior than toady's date: {datetime.now().date()}")
        if len(self.state) != 2:
            raise ValueError(f"State must be in abbreviated format.")
        if self.quantity <= 0:
            raise ValueError("Quantity must be greater than 0")
        if self.discount_percent > 1 :
            raise ValueError("Discount percent can not exceed 100%")
        
        self._validate_positive_value(self.unit_price, "unit_price")
        self._validate_positive_value(self.tax_amount, "tax_amount")
        self._validate_positive_value(self.shipping_cost, "shipping_cost")
        self._validate_positive_value(self.total_amount, "total_amount")
        
        self._validate_id(self.store_id, "S")
        self._validate_id(self.customer_id, "C")
        self._validate_id(self.product_id, "P")
        
    def _validate_positive_value(self, value, name):
        if value < 0: 
            raise ValueError(f"{name} cannot be negative")
    
    def _validate_id(self, value, char):
        if not self.ID_PATTERNS[char].fullmatch(value):
            raise ValueError(f"Invalid ID: {value}. Proper format: '{char}123'")
