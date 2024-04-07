import pandas as pd
import pytest

from assignment2 import createTargetDf


def test_createTargetDf():
    # Execute the function
    df = createTargetDf()

    # Check if the result is a pandas DataFrame
    assert isinstance(df, pd.DataFrame), "The result should be a pandas DataFrame"

    expected_dtypes = {
        "Day of the Week": "int64",
        "Time of Day": "int64",
        "Weather": "int64",
        "Location": "object",
        "Location Rank": "int64",
        "Side of Town": "object",
        "Incident": "int64",
        "Incident Rank": "int64",
        "Nature": "object",
        "EMSSSTAT": "bool",
    }

    pd.testing.assert_series_equal(
        df.dtypes, pd.Series(expected_dtypes), check_names=True
    )
