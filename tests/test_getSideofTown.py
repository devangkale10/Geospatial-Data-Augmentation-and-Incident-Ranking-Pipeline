import pytest

from assignment2 import getSideofTown


@pytest.mark.parametrize(
    "lat, lon, expected",
    [
        (35.221, -97.442, "NE"),  # North-East
        (35.219, -97.442, "SE"),  # South-East
        (35.221, -97.445, "NW"),  # North-West
        (35.219, -97.445, "SW"),  # South-West
    ],
)
def test_determine_side_of_town(lat, lon, expected):

    result = getSideofTown(lat, lon)
    assert result == expected
