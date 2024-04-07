from unittest.mock import patch

import pytest

from assignment2 import getWMOCode

successful_response = {
    "hourly": {
        "time": ["2023-04-01T14:00"],
        "temperature_2m": [15],
        "weather_code": [100],
    }
}


@patch("assignment2.requests.get")
def test_getWMOCode_success(mock_get):

    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = successful_response

    lat, lon = 40.7128, -74.0060
    start_date, end_date = "04/01/2023", "04/01/2023"
    timeOfDay = "14:00"

    expected_weather_code = 100

    # Function execution
    result = getWMOCode(lat, lon, start_date, end_date, timeOfDay)

    assert (
        result == expected_weather_code
    ), f"Expected {expected_weather_code}, got {result}"
