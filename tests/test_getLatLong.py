from assignment2 import getLatLong


def test_getLatLong_direct_coordinates():

    row = ["", "", "40.7128,-74.0060"]
    expected = ("40.7128", "-74.0060")
    result = getLatLong(row)
    assert result == expected
