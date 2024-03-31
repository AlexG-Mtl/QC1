import requests
from bs4 import BeautifulSoup
import json

# Base URL before the page parameter
base_url = 'https://www.quebec.ca/en/health/health-system-and-services/service-organization/quebec-health-system-and-its-services/situation-in-emergency-rooms-in-quebec'

# Parameters common to all requests
params = {
    'id': '24981',
    'tx_solr[location]': '',
    'tx_solr[pt]': '',
    'tx_solr[sfield]': 'geolocation_location'
}

# Initialize an empty list to hold all hospital data
all_hospitals = []

# Fetch the first page to get the "Last update" information
response = requests.get(base_url, params=params)
soup = BeautifulSoup(response.text, 'html.parser')

# Extract the "Last update" information
last_update_info = soup.find('div', class_='last-update-info').get_text(strip=True, separator=" ").replace("Last update: ", "", 1)

# Loop through pages 1 to 11
for page in range(1, 12):
    params['tx_solr[page]'] = page  # Set the current page in the parameters
    response = requests.get(base_url, params=params)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all sections containing hospital information
    hospital_elements = soup.find_all('div', class_='hospital_element')

    for hospital_element in hospital_elements:
        hospital_info = {}

        # Extract hospital name
        name = hospital_element.find('div', class_='font-weight-bold')
        hospital_info['name'] = name.text.strip() if name else None

        # Extract address
        address = hospital_element.find('div', class_='adresse')
        hospital_info['address'] = " ".join(address.stripped_strings) if address else None

        # Extract other details like estimated waiting time, number of people waiting, etc.
        details = hospital_element.find_all('li', class_='hopital-item')
        for detail in details:
            detail_text = detail.text.strip()
            key = " ".join(detail_text.split(":")[0:-1]).strip()
            value = detail_text.split(":")[-1].strip()
            hospital_info[key] = value

        all_hospitals.append(hospital_info)

# Add the "Last update" information to the output
output = {
    "last_update": last_update_info,
    "hospitals": all_hospitals
}

# Convert all collected data to JSON
json_data = json.dumps(output, indent=4)

# Specify the output file name
output_file_name = 'hospital_data.json'

# Save to a file in the current working directory
with open(output_file_name, "w") as json_file:
    json_file.write(json_data)

print(f'Data saved to {output_file_name}')

