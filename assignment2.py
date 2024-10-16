import argparse
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta

import openmeteo_requests
import pandas as pd
import requests
import requests_cache
from geopy.geocoders import Nominatim
from meteostat import Hourly, Stations
from opencage.geocoder import OpenCageGeocode
from pypdf import PdfReader
from retry_requests import retry

from incident_parser import createdb, extractincidents, populate_db, processincidents

# api_key = os.getenv('API_KEY')
url = f"https://us1.locationiq.com/v1/search.php"
wmo_url = f"https://archive-api.open-meteo.com/v1/archive"

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)


latLongCache = {}
wmo_weather_codes = {
    1: "Clear",
    2: "Fair",
    3: "Cloudy",
    4: "Overcast",
    5: "Fog",
    6: "Freezing Fog",
    7: "Light Rain",
    8: "Rain",
    9: "Heavy Rain",
    10: "Freezing Rain",
    11: "Heavy Freezing Rain",
    12: "Sleet",
    13: "Heavy Sleet",
    14: "Light Snowfall",
    15: "Snowfall",
    16: "Heavy Snowfall",
    17: "Rain Shower",
    18: "Heavy Rain Shower",
    19: "Sleet Shower",
    20: "Heavy Sleet Shower",
    21: "Snow Shower",
    22: "Heavy Snow Shower",
    23: "Lightning",
    24: "Hail",
    25: "Thunderstorm",
    26: "Heavy Thunderstorm",
    27: "Storm",
}

# Days of the week mapping
days_of_week = {
    "Sunday": 1,
    "Monday": 2,
    "Tuesday": 3,
    "Wednesday": 4,
    "Thursday": 5,
    "Friday": 6,
    "Saturday": 7,
    None: "*",
}


def download_pdf(url):
    response = requests.get(url)
    path = "docs/"
    filename = url.split("/")[-1]
    filepath = os.path.join(path, filename)
    with open(filepath, "wb") as f:
        f.write(response.content)
    return filename


def process_urls(filename):
    urls = pd.read_csv(filename, header=None).squeeze().tolist()
    data = []
    for url in urls:
        try:
            pdf_filename = download_pdf(url)
            # text = extract_text_from_pdf(pdf_filename)
            # data.append({"url": url, "text": text})
        except Exception as e:
            print(f"Failed to process {url}: {str(e)}")
    return pd.DataFrame(data)


def processCSV():
    parentDir = "docs/"
    for filename in os.listdir(parentDir):
        if not filename.endswith(".pdf"):
            continue
        filePath = os.path.join(parentDir, filename)
        extractedText = extractincidents(filePath)
        # print(extractedText)
        processedText = processincidents(extractedText)
        # print(processedText)
        populate_db(processedText)


def getLatLong(row):
    location = row[2]
    regexCoordinates = r"^-?\d{1,2}\.\d+,\s*-?\d{1,3}\.\d+$"
    pattern = re.compile(regexCoordinates)
    if "*" in location or location == "":
        return 35.2220833, -97.443611  # Center of town coordinates
    elif re.match(pattern, location):
        latitute, longitude = location.split(
            ","
        )  # IF coordinates are already provided in Location
        return latitute, longitude
    elif "/" in location:
        location = location.split("/")[0]  # Remove the second location if it exists

    # geocoder = OpenCageGeocode(key)
    # geolocator = Nominatim(user_agent="DevangGeoAPI")
    params = {"key": token, "q": location, "format": "json"}
    response = requests.get(url, params=params)

    if response.status_code == 200:
        if location not in latLongCache:
            data = response.json()
            # geoLocation = geocoder.geocode(location)
            latitute = float(data[0]["lat"])
            longitude = float(data[0]["lon"])
            latLongCache[location] = (latitute, longitude)
            # print(f"Latitude: {latitute}, Longitude: {longitude}")
            # print(latLongCache[location])
            return latLongCache[location]
        else:
            # print("Retrieved from cache")
            return latLongCache[location]
    else:
        # print("Failed to retrieve data")

        # Failed request, hence return center of town coordinates
        return 35.2220833, -97.443611


def getWMOCode(lat, lon, start_date, end_date, timeOfDay):
    dateObj = datetime.strptime(start_date, r"%m/%d/%Y")
    newDate = dateObj.strftime(r"%Y-%m-%d")
    # desiredTime = f"{newDate}T{timeOfDay}:00"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": newDate,
        "end_date": newDate,
        "hourly": ["temperature_2m", "weather_code"],
    }
    # responses = openmeteo.weather_api(wmo_url, params=params)

    # Get the WMO station code from Meteo API
    response = requests.get(wmo_url, params=params)
    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        desiredTime = str(timeOfDay).split(":")[0] + ":00"
        # print(desiredTime)
        for hour_data in data.get("hourly", {}).get("time", []):
            if desiredTime in hour_data:
                index = data["hourly"]["time"].index(hour_data)
                temperature = data["hourly"]["temperature_2m"][index]
                weather_code = data["hourly"]["weather_code"][index]
                # print(f"Temperature: {temperature}, Weather Code: {weather_code}")
                return weather_code
                break
    else:
        # Failed to retrieve weather code, hence return 9999
        return 9999


def getSideofTown(lat, lon):
    center_lat, center_lon = 35.220833, -97.443611  # Center of town coordinates

    # Determine the direction based on latitude
    if lat > center_lat:
        lat_direction = "N"
    else:
        lat_direction = "S"

    # Determine the direction based on longitude
    if lon > center_lon:
        lon_direction = "E"
    else:
        lon_direction = "W"

    # Combine directions for intercardinal points, or use cardinal if one of the directions is neutral
    if lat_direction in ["N", "S"] and lon_direction in ["E", "W"]:
        direction = lat_direction + lon_direction
    else:
        direction = lat_direction or lon_direction

    return direction


# def createTargetDB():
#     dirResources = "resources"
#     targetdbName = "normanpd_augmented.db"
#     targetdbPath = os.path.join(dirResources, targetdbName)
#     conn = sqlite3.connect(targetdbPath)
#     cur = conn.cursor()
#     query = """
#     CREATE TABLE IF NOT EXISTS augincidents (
#         day_of_the_week INTEGER,
#         time_of_day INTEGER,
#         weather INTEGER,
#         location_rank INTEGER,
#         side_of_town TEXT,
#         incident_rank INTEGER,
#         nature TEXT,
#         EMSSTAT BOOLEAN
#     );
#     """
#     cur.execute(query)
#     conn.commit()

#     conn.close()


def extractDayandTime(row):
    dateTime = row[0]
    extracts = []
    # dateTime = "2/2/2024 0:04"

    # Return default values if dateTime is empty or contains *
    if dateTime == "" or "*" in dateTime:
        extracts.append("12/31/2023")
        extracts.append("Saturday")
        extracts.append("00:00")
        return extracts

    dateObj = datetime.strptime(dateTime, r"%m/%d/%Y %H:%M")

    dateOfIncident = dateObj.strftime("%m/%d/%Y")
    dayOfWeek = dateObj.strftime("%A")
    timeOfDay = dateObj.strftime("%H:%M")
    extracts.append(dateOfIncident)
    extracts.append(dayOfWeek)
    extracts.append(timeOfDay)
    return extracts


def getStationCode(lat, lon):
    stations = Stations()
    stations = stations.nearby(lat, lon)
    station = stations.fetch(1)
    return station.iloc[0]["wmo"]


def createTargetDf():

    df = pd.DataFrame(
        {
            "Day of the Week": pd.Series(dtype="int"),
            "Time of Day": pd.Series(dtype="int"),
            "Weather": pd.Series(dtype="int"),
            "Location": pd.Series(dtype="str"),
            "Location Rank": pd.Series(dtype="int"),
            "Side of Town": pd.Series(dtype="str"),
            "Incident": pd.Series(dtype="int"),
            "Incident Rank": pd.Series(dtype="int"),
            "Nature": pd.Series(dtype="str"),
            "EMSSSTAT": pd.Series(dtype="bool"),
        }
    )
    return df


# def processRow(df):


def main(urls_filename):
    data = process_urls(urls_filename)
    # create_datasheet(data, "dataset_datasheet.csv")
    rows = []
    createdb()
    # createTargetDB()
    processCSV()
    df = createTargetDf()
    # print("Dataset and datasheet have been created.")
    # print("Database has been created and populated.")
    dirResources = "resources"
    dbName = "normanpd.db"
    dbPath = os.path.join(dirResources, dbName)
    conn = sqlite3.connect(dbPath)
    cur = conn.cursor()
    query = """
    SELECT * FROM incidents;
    """
    result = cur.execute(query).fetchall()
    for row in result:
        extracts = extractDayandTime(row)
        extractsList = list(extracts)
        if len(extractsList) < 3:
            extractsList.append("00:00")

        dateOfIncident, dayOfWeek, timeOfDay = extractsList
        # print(dateOfIncident, dayOfWeek, timeOfDay)
        dayOfWeekEncoded = days_of_week[dayOfWeek]
        lat, lon = getLatLong(row)
        # time.sleep(1)
        # stationCode = getStationCode(lat, lon)
        wmoCode = getWMOCode(lat, lon, dateOfIncident, dateOfIncident, timeOfDay)
        sideOfTown = getSideofTown(lat, lon)
        natureOfIncident = row[3]
        location = row[2]
        locationRank = None
        incident = row[3]
        incidentRank = None
        emsStat = row[4]
        row_dict = {
            "Day of the Week": dayOfWeekEncoded,
            "Time of Day": timeOfDay,
            "Weather": wmoCode,
            "Location": location,
            "Location Rank": locationRank,
            "Side of Town": sideOfTown,
            "Incident": incident,
            "Incident Rank": incidentRank,
            "Nature": natureOfIncident,
            "EMSSSTAT": emsStat,
        }
        rows.append(row_dict)

    # print(len(rows))
    rowsDF = pd.DataFrame(rows)
    df = pd.concat([df, rowsDF], ignore_index=True)
    df.replace("*", pd.NA, inplace=True)

    location_counts = df["Location"].value_counts()

    location_freq_df = location_counts.reset_index()
    location_freq_df.columns = ["Location", "Frequency"]

    # Rank the frequencies; ties get the same rank and the next rank is incremented by number of ties

    location_freq_df["Location Rank"] = (
        location_freq_df["Frequency"].rank(method="min", ascending=False).astype(int)
    )

    df = df.merge(
        location_freq_df[["Location", "Location Rank"]], on="Location", how="left"
    )

    df["Location Rank_y"] = df["Location Rank_y"].astype("Int64")

    nature_counts = df["Nature"].value_counts()

    nature_freq_df = nature_counts.reset_index()
    nature_freq_df.columns = ["Nature", "Frequency"]

    # Rank the frequencies; ties get the same rank and the next rank is incremented by number of ties

    nature_freq_df["Incident Rank"] = (
        nature_freq_df["Frequency"].rank(method="min", ascending=False).astype(int)
    )

    df = df.merge(nature_freq_df[["Nature", "Incident Rank"]], on="Nature", how="left")

    df["Incident Rank_y"] = df["Incident Rank_y"].astype("Int64")

    df["EMSSTAT"] = df["EMSSSTAT"] == "EMSSTAT"

    df.sort_values(by=["Time of Day", "Location"], inplace=True)

    df["EMSSTAT"] = df["EMSSTAT"].astype(bool)

    df["temp_index"] = df.index

    df["next_1_EMSSTAT"] = df["EMSSTAT"].shift(-1)
    df["next_2_EMSSTAT"] = df["EMSSTAT"].shift(-2)

    df["next_1_Time"] = df["Time of Day"].shift(-1)
    df["next_2_Time"] = df["Time of Day"].shift(-2)
    df["next_1_Location"] = df["Location"].shift(-1)
    df["next_2_Location"] = df["Location"].shift(-2)

    for index, row in df.iterrows():
        if row["EMSSTAT"]:
            continue
        if (
            row["next_1_EMSSTAT"]
            and row["Time of Day"] == row["next_1_Time"]
            and row["Location"] == row["next_1_Location"]
        ):
            df.at[index, "EMSSTAT"] = True
        elif (
            row["next_2_EMSSTAT"]
            and row["Time of Day"] == row["next_2_Time"]
            and row["Location"] == row["next_2_Location"]
        ):
            df.at[index, "EMSSTAT"] = True

    df.drop(
        columns=[
            "temp_index",
            "next_1_EMSSTAT",
            "next_2_EMSSTAT",
            "next_1_Time",
            "next_2_Time",
            "next_1_Location",
            "next_2_Location",
        ],
        inplace=True,
    )

    location_freq = df["Location"].value_counts().rename("Location_Freq")

    nature_freq = df["Nature"].value_counts().rename("Nature_Freq")

    df = df.merge(location_freq, left_on="Location", right_index=True)
    df = df.merge(nature_freq, left_on="Nature", right_index=True)

    # Sort by location frequency rank first, then nature frequency rank

    df_sorted = df.sort_values(
        by=["Location_Freq", "Nature_Freq"], ascending=[False, False]
    )

    df_sorted.drop(["Location_Freq", "Nature_Freq"], axis=1, inplace=True)

    df_sorted["Location Rank_x"] = df_sorted["Location Rank_y"]
    df_sorted["Incident Rank_x"] = df_sorted["Incident Rank_y"]

    df_sorted.drop(["Location Rank_y", "Incident Rank_y"], axis=1, inplace=True)

    df_sorted.rename(
        columns={
            "Location Rank_x": "Location Rank",
            "Incident Rank_x": "Incident Rank",
        },
        inplace=True,
    )

    df_sorted.drop(["Location", "Incident", "EMSSSTAT"], axis=1, inplace=True)

    # Drop minutes from Time of Day
    df_sorted["Time of Day"] = df_sorted["Time of Day"].str.split(":", expand=True)[0]

    df_sorted.to_csv("augmentedData.csv", index=False)
    print(df_sorted.to_string(index=False))
    # print("Dataset and datasheet have been created.")

    # Delete all PDF files from the /docs directory
    for file in os.listdir("docs"):
        if file.endswith(".pdf"):
            os.remove(os.path.join("docs", file))

    # Remove the Norman PD Database
    os.remove("resources/normanpd.db")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process URLs from a file and create a dataset datasheet."
    )
    parser.add_argument(
        "--urls", type=str, help="Filename containing URLs to PDF files.", required=True
    )
    args = parser.parse_args()
    main(args.urls)
