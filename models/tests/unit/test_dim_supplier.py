import polars as pl
import pytest
from polars.testing import assert_frame_equal

from models.etl.silver import dim_supplier

# Sample data for testing
@pytest.fixture
def supplier():
    return pl.DataFrame(
        {
            "suppkey": [1, 2],
            "name": ["Supplier1", "Supplier2"],
            "nationkey": [101, 102],
        }
    )
@pytest.fixture
def nation():
    return pl.DataFrame(
        {
            "nationkey": [101, 102],
            "name_nation": ["Nation1", "Nation2"],
            "comment_nation": ["Comment1", "Comment2"]
        }
    )

# Function to test
def test_dim_supplier_create_dataset(
        supplier, nation
):
    results_df = dim_supplier.create_dataset(supplier, nation)
    expected_df = pl.DataFrame(
        {
            "suppkey": [1, 2],
            "name": ["Supplier1", "Supplier2"],
            "nationkey": [101, 102],
            "nation_name": ["Nation1", "Nation2"],
            "nation_comment": ["Comment1", "Comment2"],
        }
    )
    assert_frame_equal(results_df, expected_df)