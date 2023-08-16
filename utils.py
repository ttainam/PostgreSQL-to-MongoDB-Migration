from decimal import Decimal
from datetime import date


def convert_value(value):
    if isinstance(value, Decimal):
        return float(value)
    elif isinstance(value, date):
        return value.isoformat()
    elif isinstance(value, memoryview):
        return bytes(value)


def update_collection(collection, filter_document, update_document):
  """Updates a collection with the given filter document and update document.

  Args:
    collection: The collection to update.
    filter_document: The document to filter the collection by.
    update_document: The document to update the collection with.

  Returns:
    The number of documents updated.
  """
  return collection.update_one(filter_document, update_document)
