import requests
import pandas as pd
import datetime
from io import StringIO
import base64

spacex_url = "https://api.spacexdata.com/v4/launches/past"
response = requests.get(spacex_url)
data = response.json()
df = pd.json_normalize(data)

# Lets take a subset of our dataframe keeping only the features we want and the flight number, and date_utc.
data = df[['rocket', 'payloads', 'launchpad', 'cores', 'flight_number', 'date_utc']]

# We will remove rows with multiple cores because those are falcon rockets with 2 extra rocket boosters and rows that have multiple payloads in a single rocket.
data = data[data['cores'].map(len) == 1]
data = data[data['payloads'].map(len) == 1]

# Since payloads and cores are lists of size 1 we will also extract the single value in the list and replace the feature.
data['cores'] = data['cores'].map(lambda x: x[0])
data['payloads'] = data['payloads'].map(lambda x: x[0])

# We also want to convert the date_utc to a datetime datatype and then extracting the date leaving the time
data['date'] = pd.to_datetime(data['date_utc']).dt.date

# Using the date we will restrict the dates of the launches
data = data[data['date'] <= datetime.date(2020, 11, 13)]

# Global variables
BoosterVersion = []
PayloadMass = []
Orbit = []
LaunchSite = []
Outcome = []
Flights = []
GridFins = []
Reused = []
Legs = []
LandingPad = []
Block = []
ReusedCount = []
Serial = []
Longitude = []
Latitude = []

# Takes the dataset and uses the rocket column to call the API and append the data to the list
def getBoosterVersion(data):
    for x in data['rocket']:
        if x:
            try:
                response = requests.get("https://api.spacexdata.com/v4/rockets/" + str(x))
                response.raise_for_status()
                rocket_data = response.json()
                BoosterVersion.append(rocket_data['name'])
            except requests.exceptions.RequestException as e:
                print(f"Error fetching rocket data for ID {x}: {e}")
                BoosterVersion.append(None)
            except KeyError:
                print(f"Error: 'name' key not found in rocket data for ID {x}")
                BoosterVersion.append(None)
        else:
            BoosterVersion.append(None)

# Takes the dataset and uses the launchpad column to call the API and append the data to the list
def getLaunchSite(data):
    for x in data['launchpad']:
        if x:
            try:
                response = requests.get("https://api.spacexdata.com/v4/launchpads/" + str(x))
                response.raise_for_status()
                launchpad_data = response.json()
                Longitude.append(launchpad_data['longitude'])
                Latitude.append(launchpad_data['latitude'])
                LaunchSite.append(launchpad_data['name'])
            except requests.exceptions.RequestException as e:
                print(f"Error fetching launchpad data for ID {x}: {e}")
                Longitude.append(None)
                Latitude.append(None)
                LaunchSite.append(None)
            except KeyError:
                print(f"Error: Missing key in launchpad data for ID {x}")
                Longitude.append(None)
                Latitude.append(None)
        else:
            Longitude.append(None)
            Latitude.append(None)
            LaunchSite.append(None)

# Takes the dataset and uses the payloads column to call the API and append the data to the lists
def getPayloadData(data):
    for load in data['payloads']:
        if load:
            try:
                response = requests.get("https://api.spacexdata.com/v4/payloads/" + load)
                response.raise_for_status()
                payload_data = response.json()
                PayloadMass.append(payload_data['mass_kg'])
                Orbit.append(payload_data['orbit'])
            except requests.exceptions.RequestException as e:
                print(f"Error fetching payload data for ID {load}: {e}")
                PayloadMass.append(None)
                Orbit.append(None)
            except KeyError:
                print(f"Error: Missing key in payload data for ID {load}")
                PayloadMass.append(None)
                Orbit.append(None)
        else:
            PayloadMass.append(None)
            Orbit.append(None)

# Takes the dataset and uses the cores column to call the API and append the data to the lists
def getCoreData(data):
    for core in data['cores']:
        if core:
            try:
                if core['core'] != None:
                    response = requests.get("https://api.spacexdata.com/v4/cores/" + core['core'])
                    response.raise_for_status()
                    core_data = response.json()
                    Block.append(core_data['block'])
                    ReusedCount.append(core_data['reuse_count'])
                    Serial.append(core_data['serial'])
                else:
                    Block.append(None)
                    ReusedCount.append(None)
                    Serial.append(None)
                Outcome.append(str(core['landing_success']) + ' ' + str(core['landing_type']))
                Flights.append(core['flight'])
                GridFins.append(core['gridfins'])
                Reused.append(core['reused'])
                Legs.append(core['legs'])
                LandingPad.append(core['landpad'])
            except requests.exceptions.RequestException as e:
                print(f"Error fetching core data: {e}")
                Block.append(None)
                ReusedCount.append(None)
                Serial.append(None)
                Outcome.append(None)
                Flights.append(None)
                GridFins.append(None)
                Reused.append(None)
                Legs.append(None)
                LandingPad.append(None)
            except KeyError:
                print(f"Error: Missing key in core data")
                Block.append(None)
                ReusedCount.append(None)
                Serial.append(None)
                Outcome.append(None)
                Flights.append(None)
                GridFins.append(None)
                Reused.append(None)
                Legs.append(None)
                LandingPad.append(None)
        else:
            Block.append(None)
            ReusedCount.append(None)
            Serial.append(None)
            Outcome.append(None)
            Flights.append(None)
            GridFins.append(None)
            Reused.append(None)
            Legs.append(None)
            LandingPad.append(None)

# Call getLaunchSite
getLaunchSite(data)
# Call getPayloadData
getPayloadData(data)
# Call getCoreData
getCoreData(data)
getBoosterVersion(data)

# Construct the launch dictionary
launch_dict = {
    'FlightNumber': list(data['flight_number']),
    'Date': list(data['date']),
    'BoosterVersion': BoosterVersion,
    'PayloadMass': PayloadMass,
    'Orbit': Orbit,
    'LaunchSite': LaunchSite,
    'Outcome': Outcome,
    'Flights': Flights,
    'GridFins': GridFins,
    'Reused': Reused,
    'Legs': Legs,
    'LandingPad': LandingPad,
    'Block': Block,
    'ReusedCount': ReusedCount,
    'Serial': Serial,
    'Longitude': Longitude,
    'Latitude': Latitude
}

# Create a Pandas DataFrame from launch_dict
launch_df = pd.DataFrame(launch_dict)

# Filter the DataFrame to only include Falcon 9 launches
data_falcon9 = launch_df[launch_df['BoosterVersion'] == 'Falcon 9']

# Export to CSV string (instead of file)
csv_string = data_falcon9.to_csv(index=False)

# Encode the CSV string for use in a data URI
csv_base64 = base64.b64encode(csv_string.encode()).decode()
csv_url = f"data:text/csv;base64,{csv_base64}"

# Print the CSV download link
print(f"Download the Falcon 9 data as CSV: {csv_url}")
