import pypdf
import re
import sqlite3
import os
import requests
import argparse

empCount = 0


def fetchincidents(url, save_path):
    # Send an HTTP GET request to the provided URL
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.27 Safari/537.17"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        with open(save_path, "wb") as f:
            f.write(response.content)
    except requests.exceptions.RequestException as e:
        print(f"Failed to download PDF: {e}")


def extractincidents(pdfPath):
    # This function extracts the text from the PDF
    extractedText = ""
    with open(pdfPath, "rb") as file:
        reader = pypdf.PdfReader(file)
        for page in reader.pages:
            extractedText += page.extract_text()
    return extractedText


def processincidents(extractedText):
    global empCount

    # Remove the header
    deleteText = """NORMAN POLICE DEPARTMENT
Daily Incident Summary (Public)"""

    extractedText = extractedText.replace(deleteText, "")

    # Regex pattern to insert a newline where a new page starts
    # This checks for either OK0140200, 14005, or EMSSTAT followed by a date, and inserts a newline before the date
    newLinePattern = r"(OK0140200|14005|EMSSTAT)(?=\d{1,2}/\d{1,2}/\d{4})"

    extractedText = re.sub(newLinePattern, r"\1\n", extractedText)

    # Split the extractedText into individual lines
    individualLines = extractedText.split("\n")

    # Remove first and last line [Column headers and the timestamp at the end of the file]
    individualLines = individualLines[1:-1]

    # Check for empty nature field
    for line in individualLines:
        parts = line.split()
        if len(parts) < 6:
            empCount += 1

    extractedText = "\n".join(individualLines)

    return extractedText  # Return the processed text


def createdb():

    dirResources = "resources"
    databaseName = "normanpd.db"

    # Create the resources directory if it doesn't exist
    if not os.path.exists(dirResources):
        os.makedirs(dirResources)

    # Specify the path to the database
    databasePath = os.path.join(dirResources, databaseName)

    """ Remove the database if it already exists 
        to create a new instance on each run"""

    if os.path.exists(databasePath):
        os.remove(databasePath)

    # Connect to SQLite database at the specified path
    conn = sqlite3.connect(databasePath)
    cur = conn.cursor()

    # Create the table
    cur.execute(
        """CREATE TABLE IF NOT EXISTS incidents
               (incident_time TEXT, incident_number TEXT, incident_location TEXT, nature TEXT, incident_ori TEXT)"""
    )

    # Commit changes and close the connection
    conn.commit()
    conn.close()


def populate_db(extractedText):
    # global extractedText

    # Keywords to look for in the incident_location field
    """These are the keywords that will be removed from the incident_location field
    and added to the nature field"""
    keywords = ["COP", "MVA", "EMS", "911"]
    # Keywords to look for in the nature field
    """These are the keywords that will be checked in the nature field in case of an address spanning multiple lines
    and subsequently removed from the nature field"""
    ORIkeywords = ["OK0140200", "EMSSTAT", "14005"]

    # Define the regex pattern to extract the required fields
    # pattern = r"(\d{1,2}/\d{1,2}/\d{4}\s\d{1,2}:\d{2})\s(\d{4}-\d{8})\s([A-Z0-9\s\-\/]+)\s([A-Za-z\s\/]*)\s([A-Z0-9]+)$"
    pattern = r"(\d{1,2}/\d{1,2}/\d{4}\s\d{1,2}:\d{2})\s(\d{4}-\d{8})\s([A-Z0-9\s\-\/.;]+)\s([A-Za-z\s\/-]*)\s([A-Z0-9]+)$"

    dirResources = "resources"
    databaseName = "normanpd.db"

    databasePath = os.path.join(dirResources, databaseName)

    conn = sqlite3.connect(databasePath)
    cur = conn.cursor()

    for line in extractedText.split("\n"):
        match = re.match(pattern, line)
        if match:
            dateTime = match.group(1)
            incidentNumber = match.group(2)
            location = match.group(3).strip()
            nature = match.group(4).strip()
            incidentORI = match.group(5)
            cur.execute(
                "INSERT INTO incidents VALUES (?, ?, ?, ?, ?)",
                (dateTime, incidentNumber, location, nature, incidentORI),
            )
        else:
            natureString = ""
            # print(line)
            for i in range(len(line)):
                if line[i].isupper() and line[i + 1].islower():
                    natureString = line[i:]
                    # print(natureString)
                    break

            # Remove keywords from natureString
            for keyword in ORIkeywords:
                natureString = natureString.replace(keyword, "").strip()
                # print(natureString)
            cur.execute(
                "INSERT INTO incidents VALUES (?, ?, ?, ?, ?)",
                ("*", "*", "*", natureString, "*"),
            )

    conn.commit()
    cur.execute("SELECT rowid, * FROM incidents")
    records = cur.fetchall()

    # Update the nature and incident_location fields where necessary
    for record in records:
        (
            rowid,
            incident_time,
            incident_number,
            incident_location,
            nature,
            incident_ori,
        ) = record
        foundKeyword = False
        # for keyword in keywords:
        #     if keyword in incident_location:
        #         new_location = incident_location.replace(keyword, "").strip()
        #         new_nature = f"{keyword} {nature}".strip()
        #         cur.execute(
        #             "UPDATE incidents SET nature = ?, incident_location = ? WHERE rowid = ?",
        #             (new_nature, new_location, rowid),
        #         )
        #         break
        for keyword in keywords:
            if incident_location.endswith(keyword):
                new_location = incident_location[: -len(keyword)].strip()
                new_nature = f"{keyword} {nature}".strip()

                cur.execute(
                    "UPDATE incidents SET nature = ?, incident_location = ? WHERE rowid = ?",
                    (new_nature, new_location, rowid),
                )
                foundKeyword = True
                break
        if not foundKeyword:
            pass
    conn.commit()
    conn.close()


def status():
    global empCount
    dirResources = "resources"
    databaseName = "normanpd.db"

    databasePath = os.path.join(dirResources, databaseName)

    conn = sqlite3.connect(databasePath)
    cur = conn.cursor()

    query = """
    SELECT nature, COUNT(*) as cnt
    FROM incidents
    GROUP BY nature
    ORDER BY cnt DESC, nature ASC
    """

    cur.execute(query)

    # Fetch all results
    res = cur.fetchall()

    for nature, count in res:
        if nature != "":
            print(f"{nature}|{count}\n", end="")
    if empCount > 0:
        print(f"{''}|{empCount}\n", end="")

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Download the incident PDF and parse it"
    )
    parser.add_argument(
        "--incidents",
        type=str,
        required=True,
        help="Provide the URL to the incidents PDF here",
    )
    args = parser.parse_args()

    if args.incidents:
        # Dynamically determine the path for PDF storage
        currDirectory = os.path.dirname(os.path.abspath(__file__))
        parDirectory = os.path.dirname(currDirectory)
        tmpDirectory = os.path.join(parDirectory, "tmp")
        os.makedirs(tmpDirectory, exist_ok=True)

        pdfPath = os.path.join(tmpDirectory, os.path.basename(args.incidents))

        # Fetch the incidents PDF
        fetchincidents(args.incidents, pdfPath)

        # Extract the incidents from the PDF and process them
        extractedText = extractincidents(pdfPath)
        processedText = processincidents(extractedText)

        # Create the database and populate it with the processed incidents
        createdb()
        populate_db(processedText)
        # Print the summary of incidents and their counts
        status()
    else:
        print("Please provide the URL to the incidents PDF using the --incidents flag.")


if __name__ == "__main__":
    main()
