## Assignment 2 for the Course CIS6930 - Data Engineering
#   Author
- Name: Devang Kale
- Email: devangkale@ufl.edu
- UFID: 3340-9661

# Description
As a subsequent part of our data processing pipeline for the Norman PD Incident Report Dataset, we will now perform Data Augmentation on the dataset.  
Data Augmentation is a technique used to increase the size of the dataset by creating new data points from the existing data points. This is done by applying various transformations to the existing data points.  
We augment the data by calculating Day of the Week, isolating Time of the Incident, calling Weather APIs to determine weather at the time, date and location of the incident. We further calculate what Side of Town the incident took place. Finally, we implement ranking algorithms to rank the location and incidents based on frequency.

## Table of Contents

- [Requirements](#requirements)
- [Installation and Usage](#installation-and-usage)
- [Flag Information](#flag-information)
- [Function Descriptions](#function-descriptions)
- [Ranking](#ranking)
- [Handling Edge Cases](#logic-behind-the-code)
- [Bugs and Assumptions](#bugs-and-assumptions)
- [Testing](#testing)
- [Resources](#resources)

# Requirements
- Python 3.11
- Libraries: pandas, numpy, requests, json, datetime, geopy, LocationIQ API, OpenCage API, OpenMeteo API

# Installation and Usage

First use 
```bash
pipenv install 
```
to install all the dependencies. 

Then use 
```bash
pipenv shell 
```
to activate the virtual environment.  

Finally, run the following command to execute the code:
```bash
pipenv run python assignment2.py --urls files.csv
```
to run the code.

# Flag Information
- `--urls`: The path to the CSV file containing the URLs of the PDFs to be downloaded, processed and augmented from the Norman PD Website.

# Function Descriptions
- `process_urls(filename: str) -> pd.DataFrame`: Processes the URLs in the CSV file and returns a DataFrame. Calls the `download_pdf` funnction for every URL in the CSV file.  
- `download_pdf(url: str) -> str`: Downloads the PDF from the URL and saves it in the `docs/` folder. Returns the filename of the downloaded PDF.
- `processCSV()`: Processes the PDFs in the `docs/` folder and extracts the text from the PDFs. Calls the `extractincidents` function from Assignment 0 for every PDF in the `docs/` folder. Further, calls the `processincidents` function to process the extracted text. Finally, calls the `populate_db` function to populate the database.
- `createTargetDf() -> pd.DataFrame`: Creates a target DataFrame with the following columns: "Day of the Week", "Time of Day", "Weather", "Location", "Location Rank", "Side of Town", "Incident", "Incident Rank", "Nature", "EMSSSTAT".
- `extractDayandTime(row: list) -> list`: Extracts the Day of the Week, Time of the Incident and Date of the Incident from the each row of the dataset.

- `getLatLong(row: list) -> tuple`: Forward Geocoding the Location of the incident. Returns the Latitude and Longitude of the location of the incident. Calls the LocationIQ API to get the Latitude and Longitude of the location of the incident. Caches the Latitude and Longitude of the location of the incident to avoid repeated API calls. Whenever cached data is available, it is used to get the Latitude and Longitude of the location of the incident. This enables us to avoid repeated API calls and speeds up the process of getting the Latitude and Longitude of the location of the incident. If the function fails to Forward Geocode the Location, it returns the coordinates of the center of the town.  
**IMPORTANT**: The LocationIQ API uses an API key to make API calls. The API key is present in the code itself. The API key is rate limited to 5000 requests/day. If the rate limit is exceeded, the API call will fail.

- `getWMOCode(lat: float, lon: float, start_date: str, end_date: str, timeOfDay: str) -> int`: Calls the OpenMeteo API to get the Weather Code at the location of the incident at the time of the incident. Returns the Weather Code. If the function fails to get the Weather Code, it returns 9999.

- `determine_side_of_town(lat: float, lon: float) -> str`: Determines the Side of Town from the center of the town where the incident took place. The Side of Town is any of the 8 directions - `N, S, E, W, NE, NW, SE, SW`. Returns the Side of Town. 

# Ranking
- Ranking is done in the main function after the target DataFrame is created. We rank first on the frequency of locations with ties preserved. We then rank on the frequency of incidents with ties preserved.
- Using the `rank` function from the pandas library, we rank the locations and incidents based on frequency. We use the `method='min'` parameter to Shallow Rank the locations and incidents based on frequency. The DataFrame is sorted first by Location Rank, then by Incident Rank.

# Handling Edge Cases
- In the function `getLatLong`, we handle the case when the API call fails to get the Latitude and Longitude of the location of the incident. In such cases, we return the coordinates of the center of the town.
- In the function `getWMOCode`, we handle the case when the API call fails to get the Weather Code at the location of the incident at the time of the incident. In such cases, we return 9999.
- In the function `extractDayandTime`, we handle the case when the date and time of the incident is not available. In such cases, we return the date as "12/31/2023", the day of the week as "Saturday" and the time of the incident as "00:00".
- Other edge cases implemented in Assignment 0 are also handled in this assignment.


# Bugs and Assumptions
- The Forward Geocoding API calls to LocationIQ are rate limited. If the rate limit is exceeded, the API call will fail. I have tried to handle this by caching the data and avoiding repeated API calls. However, the API rate limit currently is at 5000 requests/day. If the rate limit is exceeded, the API call will fail.
- The Weather API calls are dependent on the Latitude and Longitude of the location of the incident. If the Latitude and Longitude of the location of the incident is not available, the Weather API call will fail. In such cases, we return 9999 as the Weather Code.
- The code has to handle a lot of API calls and do a lot of processing. It might be slow for a large dataset. I have tried to optimize the code as much as possible. However, the code might still run a bit slow for a large dataset.
- The final output has been sorted on the Location Rank and then the Incident Rank. The sorting is done in ascending order.
- The code has been tested on the Norman PD Incident Report Dataset. It might not work as expected for other datasets.

# Testing
- We run tests using the `pytest` library. We have written tests for all functions in the code. We test the functions for correctness and edge cases.
- To run the tests, use the following command:
```bash
pipenv run python -m pytest
```

# Resources
- LocationIQ API: https://locationiq.com/
- OpenCage API: https://opencagedata.com/
- OpenMeteo API: https://open-meteo.com/
- Pandas Documentation: https://pandas.pydata.org/docs/
- Numpy Documentation: https://numpy.org/doc/stable/
- Requests Documentation: https://docs.python-requests.org/en/master/
- Geopy Documentation: https://geopy.readthedocs.io/en/stable/
- Python Documentation: https://docs.python.org/3/
- StackOverflow: https://stackoverflow.com/questions/47659249/calculate-cardinal-direction-from-gps-coordinates-in-python
- StackOverflow: https://math.stackexchange.com/questions/796243/how-to-determine-the-direction-of-one-point-from-another-given-their-coordinate
- CIS6930 Lecture Slides