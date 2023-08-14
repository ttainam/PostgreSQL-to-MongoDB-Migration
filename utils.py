from decimal import Decimal
from datetime import date


def convert_value(value):
    if isinstance(value, Decimal):
        return float(value)
    elif isinstance(value, date):
        return value.isoformat()
    elif isinstance(value, memoryview):
        return bytes(value)
