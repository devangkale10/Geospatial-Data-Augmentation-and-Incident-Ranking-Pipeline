import pytest

from assignment2 import extractDayandTime


@pytest.mark.parametrize(
    "input_row, expected",
    [
        (["2/2/2024 15:00"], ["02/02/2024", "Friday", "15:00"]),
        ([""], ["12/31/2023", "Saturday", "00:00"]),
        (["*"], ["12/31/2023", "Saturday", "00:00"]),
    ],
)
def test_extractDayandTime(input_row, expected):

    result = extractDayandTime(input_row)

    assert result == expected, f"Expected {expected}, but got {result}"
