import json
import logging
import traceback
import requests
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pytz
from urllib.parse import urlencode
import csv

# Hardcoded Authentication Credentials
USERNAME = "bala+telyon_admin@ecosuite.io"
PASSWORD = "Vanaja@13882"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('ecosuite_data_extractor.log', mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EcoSuiteDataExtractorDebugger:
    def __init__(self, api_key):
        self.api_key = api_key
        self.debug_info = {
            'api_calls': [],
            'errors': []
        }

    def log_api_call(self, endpoint, method, status_code, response_time):
        api_call = {
            'endpoint': endpoint,
            'method': method,
            'status_code': status_code,
            'timestamp': datetime.now().isoformat(),
            'response_time': response_time
        }
        
        self.debug_info['api_calls'].append(api_call)

    def log_error(self, context, error):
        error_entry = {
            'context': context,
            'error_type': type(error).__name__,
            'error_message': str(error),
            'traceback': traceback.format_exc(),
            'timestamp': datetime.now().isoformat()
        }
        self.debug_info['errors'].append(error_entry)
        logger.error(f"Error in {context}: {error}")

    def export_debug_report(self, output_file='ecosuite_debug_report.json'):
        try:
            with open(output_file, 'w') as f:
                json.dump(self.debug_info, f, indent=2)
            logger.info(f"Debug report exported to {output_file}")
        except Exception as e:
            logger.error(f"Failed to export debug report: {e}")

def save_to_json(filename: str, data: dict, project_id: str, start_date: str, end_date: str, folder_path: str) -> str:
    """Save data to JSON file with project-specific naming"""
    filename = f"{project_id}_{start_date}_{end_date}_{filename}"
    full_path = os.path.join(folder_path, filename)
    
    try:
        with open(full_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
        logger.info(f"Data successfully saved to {full_path}")
        return full_path
    except Exception as e:
        logger.error(f"Failed to save data to {full_path}: {str(e)}")
        return None
    

def format_number(value):
    """
    Format a numeric value with commas as thousand separators
    preserving full decimal precision
    """
    if isinstance(value, (int, float)):
        # Format with commas but preserve all decimal places
        if value == int(value):
            # For whole numbers, show no decimal places
            return f"{value:,.0f}"
        else:
            # For decimal numbers, show full precision
            return f"{value:,}"
    else:
        # Return as is if it's not a number
        return value
    
def create_project_folder(project_name: str, start_date: str, end_date: str) -> str:
    """Create project folder with date-based naming"""
    project_folder = f"{project_name}_{start_date}_{end_date}"
    raw_data_folder = os.path.join(project_folder, "raw_data")
    os.makedirs(raw_data_folder, exist_ok=True)
    return raw_data_folder

def fetch_project_details(project_id: str, api_token: str, debugger: EcoSuiteDataExtractorDebugger) -> Optional[Dict]:
    """Fetch project details from EcoSuite API"""
    base_url = "https://api.ecosuite.io/projects"
    endpoint = f"{base_url}/{project_id}"
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    start_time = datetime.now()
    try:
        response = requests.get(endpoint, headers=headers)
        
        response_time = (datetime.now() - start_time).total_seconds()
        debugger.log_api_call(endpoint, 'GET', response.status_code, response_time)
        
        if response.status_code == 200:
            data = response.json()
            logger.info("Successfully fetched project details")
            return data
        else:
            debugger.log_error("fetch_project_details", 
                             Exception(f"Error {response.status_code}: {response.text}"))
            return None
    except requests.exceptions.RequestException as e:
        debugger.log_error("fetch_project_details", e)
        return None

def fetch_price_data(project_id: str, api_token: str, debugger: EcoSuiteDataExtractorDebugger) -> Optional[Dict]:
    """Fetch price data from EcoSuite API"""
    base_url = "https://api.ecosuite.io/projects"
    endpoint = f"{base_url}/{project_id}/pro-forma"
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    start_time = datetime.now()
    try:
        response = requests.get(endpoint, headers=headers)
        
        response_time = (datetime.now() - start_time).total_seconds()
        debugger.log_api_call(endpoint, 'GET', response.status_code, response_time)
        
        if response.status_code == 200:
            data = response.json()
            logger.info("Successfully fetched price data")
            return data
        else:
            debugger.log_error("fetch_price_data", 
                             Exception(f"Error {response.status_code}: {response.text}"))
            return None
    except requests.exceptions.RequestException as e:
        debugger.log_error("fetch_price_data", e)
        return None

def adjust_end_date(end_date: str) -> str:
    """
    Adjust end date by adding one day to include the specified end date in results
    """
    try:
        # Parse the date string
        date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Add one day
        adjusted_date = date_obj + timedelta(days=1)
        
        # Return in the same format
        return adjusted_date.strftime("%Y-%m-%d")
    except Exception as e:
        logger.error(f"Error adjusting end date: {e}")
        return end_date  # Return original if there's an error

def fetch_ecosuite_energy_datums(project_id: str, start_date: str, end_date: str, 
                                  aggregation: str, api_token: str, 
                                  raw_data_folder: str,
                                  debugger: EcoSuiteDataExtractorDebugger) -> Optional[Dict]:
    """
    Fetch energy datums from the EcoSuite API and save to file
    """
    base_url = "https://api.ecosuite.io/energy/datums"
    endpoint = f"{base_url}/projects/{project_id}"
    
    # Adjust end date to include the specified end date in results
    adjusted_end_date = adjust_end_date(end_date)
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    params = {
        "start": start_date,
        "end": adjusted_end_date,  # Use adjusted end date
        "aggregation": aggregation
    }
    
    start_time = datetime.now()
    try:
        response = requests.get(endpoint, headers=headers, params=params)
        
        response_time = (datetime.now() - start_time).total_seconds()
        debugger.log_api_call(endpoint, 'GET', response.status_code, response_time)
        
        if response.status_code == 200:
            data = response.json()
            logger.info("Successfully fetched energy datums")
            
            # Save the raw energy datums to a JSON file
            filename = f"{project_id}_{start_date}_{end_date}_energy_datums.json"
            full_path = os.path.join(raw_data_folder, filename)
            
            try:
                with open(full_path, 'w') as json_file:
                    json.dump(data, json_file, indent=4)
                logger.info(f"Energy datums saved to {full_path}")
                print(f"Energy datums saved to {full_path}")
            except Exception as save_error:
                logger.error(f"Failed to save energy datums: {save_error}")
                print(f"Failed to save energy datums: {save_error}")
            
            return data
        else:
            debugger.log_error("fetch_ecosuite_energy_datums", 
                             Exception(f"Error {response.status_code}: {response.text}"))
            return None
    except requests.exceptions.RequestException as e:
        debugger.log_error("fetch_ecosuite_energy_datums", e)
        return None
    except json.JSONDecodeError as e:
        debugger.log_error("fetch_ecosuite_energy_datums", 
                         Exception(f"JSON Decode Error: {e}"))
        return None

def fetch_expected_generation_with_project_ids(project_id: str, start_date: str, end_date: str, 
                               aggregation: str, api_token: str, 
                               raw_data_folder: str,
                               debugger: EcoSuiteDataExtractorDebugger) -> Optional[Dict]:
    """
    Fetch expected generation data from the EcoSuite API using projectIds parameter and save to file
    """
    base_url = "https://api.ecosuite.io/energy/datums/generation/expected"
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    # Adjust end date to include the specified end date in results
    adjusted_end_date = adjust_end_date(end_date)
    
    params = {
        "start": start_date,
        "end": adjusted_end_date,
        "projectIds": project_id
    }
    
    # Add aggregation only if it's provided
    if aggregation:
        params["aggregation"] = aggregation
    
    # Log the API call endpoint for debugging
    endpoint = f"{base_url}?start={start_date}&end={adjusted_end_date}&projectIds={project_id}"
    if aggregation:
        endpoint += f"&aggregation={aggregation}"
    
    start_time = datetime.now()
    try:
        response = requests.get(base_url, headers=headers, params=params)
        
        response_time = (datetime.now() - start_time).total_seconds()
        debugger.log_api_call(endpoint, 'GET', response.status_code, response_time)
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"Successfully fetched expected generation data with projectIds={project_id}")
            
            # Save the raw expected generation data to a JSON file
            filename = f"expected_generation_{project_id}.json"
            full_path = os.path.join(raw_data_folder, filename)
            
            try:
                with open(full_path, 'w') as json_file:
                    json.dump(data, json_file, indent=4)
                logger.info(f"Expected generation data with projectIds saved to {full_path}")
                print(f"Expected generation data with projectIds saved to {full_path}")
            except Exception as save_error:
                logger.error(f"Failed to save expected generation data with projectIds: {save_error}")
                print(f"Failed to save expected generation data with projectIds: {save_error}")
            
            return data
        else:
            debugger.log_error("fetch_expected_generation_with_project_ids", 
                             Exception(f"Error {response.status_code}: {response.text}"))
            return None
    except requests.exceptions.RequestException as e:
        debugger.log_error("fetch_expected_generation_with_project_ids", e)
        return None
    except json.JSONDecodeError as e:
        debugger.log_error("fetch_expected_generation_with_project_ids", 
                         Exception(f"JSON Decode Error: {e}"))
        return None

def fetch_ecosuite_weather_datums(project_id: str, start_date: str, end_date: str, 
                                  aggregation: str, api_token: str, 
                                  raw_data_folder: str,
                                  debugger: EcoSuiteDataExtractorDebugger) -> Optional[Dict]:
    """
    Fetch weather datums from the EcoSuite API and save to file
    """
    base_url = "https://api.ecosuite.io/weather/datums"
    endpoint = f"{base_url}/projects/{project_id}"
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    params = {
        "start": start_date,
        "end": end_date,
        "aggregation": aggregation
    }
    
    start_time = datetime.now()
    try:
        response = requests.get(endpoint, headers=headers, params=params)
        
        response_time = (datetime.now() - start_time).total_seconds()
        debugger.log_api_call(endpoint, 'GET', response.status_code, response_time)
        
        if response.status_code == 200:
            data = response.json()
            logger.info("Successfully fetched weather datums")
            
            # Save the raw weather datums to a JSON file
            filename = f"{project_id}_{start_date}_{end_date}_weather_datums.json"
            full_path = os.path.join(raw_data_folder, filename)
            
            try:
                with open(full_path, 'w') as json_file:
                    json.dump(data, json_file, indent=4)
                logger.info(f"Weather datums saved to {full_path}")
                print(f"Weather datums saved to {full_path}")
            except Exception as save_error:
                logger.error(f"Failed to save weather datums: {save_error}")
                print(f"Failed to save weather datums: {save_error}")
            
            return data
        else:
            debugger.log_error("fetch_ecosuite_weather_datums", 
                             Exception(f"Error {response.status_code}: {response.text}"))
            return None
    except requests.exceptions.RequestException as e:
        debugger.log_error("fetch_ecosuite_weather_datums", e)
        return None
    except json.JSONDecodeError as e:
        debugger.log_error("fetch_ecosuite_weather_datums", 
                         Exception(f"JSON Decode Error: {e}"))
        return None

def read_project_codes_from_csv(csv_file_path: str) -> List[str]:
    """
    Read project codes from a CSV file with improved error handling
    """
    project_codes = []
    try:
        with open(csv_file_path, 'r') as csvfile:
            # First try to read the file content
            content = csvfile.read()
            print(f"CSV file content preview: {content[:200]}...")
            
            # Reset file pointer
            csvfile.seek(0)
            
            # Try with common delimiters instead of auto-detection
            for delimiter in [',', ';', '\t']:
                try:
                    csvfile.seek(0)  # Reset file pointer
                    reader = csv.reader(csvfile, delimiter=delimiter)
                    
                    # Read first row to check
                    first_row = next(reader, None)
                    if first_row and len(first_row) > 0:
                        print(f"Successfully read CSV with delimiter '{delimiter}'. First row: {first_row}")
                        
                        # Assume first column contains project codes
                        # Reset file pointer
                        csvfile.seek(0)
                        reader = csv.reader(csvfile, delimiter=delimiter)
                        
                        # Skip header if present
                        header = next(reader, None)
                        if header:
                            print(f"CSV header: {header}")
                        
                        # Read project codes from first column
                        for row in reader:
                            if row and len(row) > 0 and row[0].strip():
                                project_codes.append(row[0].strip())
                        
                        print(f"Read {len(project_codes)} project codes from {csv_file_path}")
                        return project_codes
                except Exception as e:
                    print(f"Failed with delimiter '{delimiter}': {e}")
                    continue
            
            # If we get here, none of the common delimiters worked
            # Try a more manual approach
            csvfile.seek(0)
            lines = csvfile.readlines()
            for line in lines:
                # Remove any newline characters and white space
                line = line.strip()
                if line:
                    # Just take the first word as the project code
                    project_code = line.split()[0].strip()
                    if project_code:
                        project_codes.append(project_code)
            
            if project_codes:
                print(f"Read {len(project_codes)} project codes using fallback method")
                return project_codes
            
            print(f"Could not read project codes from CSV file: {csv_file_path}")
            return []
    
    except Exception as e:
        logger.error(f"Error reading project codes from CSV: {e}")
        print(f"Error reading project codes from CSV: {e}")
        return []

def get_x_sn_date(now):
    """Generate x-sn-date header value in required format"""
    return now.strftime("%Y%m%d%H%M%S")

def generate_auth_header(token, secret, method, path, query_params, headers, request_body, now):
    """Generate authentication header for SolarNetwork API"""
    # This function should contain the SolarNetwork auth logic
    # Simplified implementation for example purposes
    return f"SolarNetworkWS {token}:{secret}"

class SolarNetworkClient:
    def __init__(self, token: str, secret: str, debugger=None) -> None:
        self.token = token
        self.secret = secret
        self.debugger = debugger

    def list(self, nodeIds: List[int], sourceIds: List[str], startDate: str, endDate: str, projectId: str, aggregation: str = "") -> Dict:
        """
        Fetch data from SolarNetwork API
        """
        now = datetime.now(pytz.UTC)
        date = get_x_sn_date(now)
        path = "/solarquery/api/v1/sec/datum/list"
        start_time = datetime.now()

        headers = {"host": "data.solarnetwork.net", "x-sn-date": date}
        
        nodeIds_str = ','.join(map(str, nodeIds))
        sourceIds_str = ','.join(f"/{projectId}{source}" for source in sourceIds)
        
        # Use same aggregation logic for all data types
        query_params = {
            "aggregation": aggregation if aggregation else None,
            "endDate": endDate,
            "nodeIds": nodeIds_str,
            "sourceIds": sourceIds_str,
            "startDate": startDate,
        }

        query_params = {k: v for k, v in query_params.items() if v is not None}
        encoded_params = urlencode(query_params)

        auth = generate_auth_header(
            self.token, self.secret, "GET", path, encoded_params, headers, "", now
        )

        url = f"https://data.solarnetwork.net{path}?{encoded_params}"
        
        logger.debug(f"Making API call to {url}")
        
        try:
            resp = requests.get(
                url=url,
                headers={
                    "host": "data.solarnetwork.net",
                    "x-sn-date": date,
                    "Authorization": auth,
                },
            )

            response_time = (datetime.now() - start_time).total_seconds()
            
            if self.debugger:
                self.debugger.log_api_call(url, "GET", resp.status_code, response_time)

            v = resp.json()
            if v.get("success") != True:
                error_msg = f"Unsuccessful API call: {v}"
                if self.debugger:
                    self.debugger.log_error("SolarNetwork API", Exception(error_msg))
                raise Exception("Unsuccessful API call")

            return v.get("data", {})
            
        except Exception as e:
            if self.debugger:
                self.debugger.log_error("SolarNetwork API", e)
            raise e

def convert_to_utc(date_str: str, tz_str: str, is_start: bool = True) -> str:
    """
    Convert a local date to UTC ISO format for API requests
    """
    local_tz = pytz.timezone(tz_str)
    local_date = datetime.strptime(date_str, "%Y-%m-%d")
    
    if is_start:
        local_date = local_date.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        local_date = local_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    local_dt = local_tz.localize(local_date)
    utc_dt = local_dt.astimezone(pytz.UTC)
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3]+'Z'

def fetch_solarnetwork_weather_data(project_id: str, 
                                  project_details: Dict,
                                  start_date: str, 
                                  end_date: str,
                                  aggregation: str,
                                  raw_data_folder: str,
                                  debugger: EcoSuiteDataExtractorDebugger) -> Optional[Dict]:
    """
    Fetch weather data from SolarNetwork API and save to file
    """
    try:
        # Get credentials
        sn_token = "mrqiO-fM0ocfG2ca_V9G"
        sn_secret = "zXrLA2hMS73kX3wFoavkl-uBE4k-jMw"
        
        # Create SolarNetwork client
        sn_client = SolarNetworkClient(sn_token, sn_secret, debugger)
        
        # Get project timezone
        project_tz = project_details.get('project', {}).get('sites', {}).get('S1', {}).get('timezone', 'UTC')
        
        # Use original end date for file naming but adjusted date for the API call
        adjusted_end_date = adjust_end_date(end_date)
        
        # Convert dates to UTC for SolarNetwork API
        utc_start_date = convert_to_utc(start_date, project_tz, True)
        utc_end_date = convert_to_utc(adjusted_end_date, project_tz, False)
        
        # Get number of systems
        systems = project_details.get('project', {}).get('sites', {}).get('S1', {}).get('systems', {})
        num_systems = len(systems)
        
        if num_systems == 0:
            logger.error(f"No systems found for project {project_id}")
            return None
        
        # Initialize all_weather_data
        all_weather_data = {
            "project_id": project_id,
            "start_date": start_date,
            "end_date": end_date,
            "systems": {}
        }
        
        # For each system, fetch weather data
        for system_num in range(1, num_systems + 1):
            system_id = f"R{system_num}"
            
            try:
                # Fetch weather data from SolarNetwork
                weather_result = sn_client.list(
                    [739],  # Node ID for weather data
                    [f"/S1/{system_id}/WEATHER/1"],  # Source ID pattern
                    utc_start_date, 
                    utc_end_date, 
                    project_id, 
                    aggregation
                )
                
                # Store in all_weather_data
                all_weather_data["systems"][system_id] = {
                    "weather_data": weather_result.get("results", [])
                }
                
                logger.info(f"Successfully fetched SolarNetwork weather data for system {system_id}")
                
            except Exception as e:
                logger.error(f"Error fetching SolarNetwork weather data for system {system_id}: {e}")
                all_weather_data["systems"][system_id] = {
                    "weather_data": [],
                    "error": str(e)
                }
        
        # Save to JSON file
        sn_weather_filename = f"{project_id}_{start_date}_{end_date}_solarnetwork_weather.json"
        sn_weather_path = os.path.join(raw_data_folder, sn_weather_filename)
        
        with open(sn_weather_path, 'w') as json_file:
            json.dump(all_weather_data, json_file, indent=4)
        
        logger.info(f"SolarNetwork weather data saved to {sn_weather_path}")
        print(f"SolarNetwork weather data saved to {sn_weather_path}")
        
        return all_weather_data
        
    except Exception as e:
        logger.error(f"Error in fetch_solarnetwork_weather_data: {e}")
        debugger.log_error("fetch_solarnetwork_weather_data", e)
        return None

def fetch_expected_generation(project_id: str, start_date: str, end_date: str, 
                             aggregation: str, api_token: str, 
                             raw_data_folder: str,
                             debugger: EcoSuiteDataExtractorDebugger) -> Optional[Dict]:
    """
    Fetch expected generation data from the EcoSuite API and save to file
    """
    base_url = "https://api.ecosuite.io/energy/datums/generation/expected"
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    params = {
        "start": start_date,
        "end": end_date,
        "aggregation": aggregation
    }
    
    start_time = datetime.now()
    try:
        response = requests.get(base_url, headers=headers, params=params)
        
        response_time = (datetime.now() - start_time).total_seconds()
        debugger.log_api_call(base_url, 'GET', response.status_code, response_time)
        
        if response.status_code == 200:
            data = response.json()
            logger.info("Successfully fetched expected generation data")
            
            # Save the raw expected generation data to a JSON file
            filename = f"{project_id}_{start_date}_{end_date}_expected_generation.json"
            full_path = os.path.join(raw_data_folder, filename)
            
            try:
                with open(full_path, 'w') as json_file:
                    json.dump(data, json_file, indent=4)
                logger.info(f"Expected generation data saved to {full_path}")
                print(f"Expected generation data saved to {full_path}")
            except Exception as save_error:
                logger.error(f"Failed to save expected generation data: {save_error}")
                print(f"Failed to save expected generation data: {save_error}")
            
            return data
        else:
            debugger.log_error("fetch_expected_generation", 
                             Exception(f"Error {response.status_code}: {response.text}"))
            return None
    except requests.exceptions.RequestException as e:
        debugger.log_error("fetch_expected_generation", e)
        return None
    except json.JSONDecodeError as e:
        debugger.log_error("fetch_expected_generation", 
                         Exception(f"JSON Decode Error: {e}"))
        return None


def process_project(project_id: str, start_date: str, end_date: str, aggregation: str, 
                    debugger: EcoSuiteDataExtractorDebugger, token: str) -> bool:
    """
    Process a single project by fetching and saving data as JSON files only
    """
    try:
        # Fetch project details
        project_details = fetch_project_details(project_id, token, debugger)
        if not project_details:
            logger.error(f"Failed to fetch project details for project {project_id}")
            return False
        
        # Create project folder
        project_name = project_details.get('project', {}).get('name', project_id)
        raw_data_folder = create_project_folder(project_name, start_date, end_date)
        
        # Save project details
        save_to_json("project_details.json", project_details, project_id, start_date, end_date, raw_data_folder)
        
        # Fetch and save price data
        price_data = fetch_price_data(project_id, token, debugger)
        if price_data:
            save_to_json("price_data.json", price_data, project_id, start_date, end_date, raw_data_folder)
        
        # Fetch and save energy datums
        energy_data = fetch_ecosuite_energy_datums(
            project_id, 
            start_date, 
            end_date, 
            aggregation.lower(), 
            token, 
            raw_data_folder,
            debugger
        )
        
        # Fetch and save expected generation data
        expected_gen_data = fetch_expected_generation(
            project_id,
            start_date,
            end_date,
            aggregation.lower(),
            token,
            raw_data_folder,
            debugger
        )
        
        # Fetch expected generation with project IDs
        expected_gen_with_ids_data = fetch_expected_generation_with_project_ids(
            project_id,
            start_date,
            end_date,
            aggregation.lower(),
            token,
            raw_data_folder,
            debugger
        )
        
        # Fetch EcoSuite weather data
        ecosuite_weather = fetch_ecosuite_weather_datums(
            project_id, 
            start_date, 
            end_date, 
            aggregation.lower(), 
            token, 
            raw_data_folder,
            debugger
        )
        
        # Fetch SolarNetwork weather data
        solarnetwork_weather = fetch_solarnetwork_weather_data(
            project_id,
            project_details,
            start_date,
            end_date,
            aggregation,
            raw_data_folder,
            debugger
        )
        
        # Generate CSV report
        csv_report_path = generate_csv_report(
            project_id,
            start_date,
            end_date,
            raw_data_folder,
            aggregation
        )
        
        # Save a metadata file with info about fetched data
        metadata = {
            "project_id": project_id,
            "project_name": project_name,
            "start_date": start_date,
            "end_date": end_date,
            "aggregation": aggregation,
            "data_fetched": {
                "project_details": project_details is not None,
                "price_data": price_data is not None,
                "energy_data": energy_data is not None,
                "expected_generation": expected_gen_data is not None,
                "expected_generation_with_ids": expected_gen_with_ids_data is not None,
                "ecosuite_weather": ecosuite_weather is not None,
                "solarnetwork_weather": solarnetwork_weather is not None
            },
            "csv_report": csv_report_path is not None,
            "timestamp": datetime.now().isoformat()
        }
        
        save_to_json("metadata.json", metadata, project_id, start_date, end_date, raw_data_folder)
        
        print(f"Successfully processed project {project_id}")
        return True

    except Exception as e:
        logger.error(f"Error processing project {project_id}: {e}")
        return False


#---------------------------------------------------

import csv
import calendar
from datetime import datetime

def generate_csv_report(project_id: str, start_date: str, end_date: str, 
                        raw_data_folder: str, aggregation: str) -> str:
    """
    Generate a CSV report from the extracted data with specific fields
    """
    try:
        # Define the output CSV path
        csv_filename = f"{project_id}_{start_date}_{end_date}_performance_report.csv"
        csv_path = os.path.join(raw_data_folder, csv_filename)
        
        # Load all the necessary JSON files
        try:
            # Load energy datums
            energy_file = f"{project_id}_{start_date}_{end_date}_energy_datums.json"
            energy_path = os.path.join(raw_data_folder, energy_file)
            with open(energy_path, 'r') as f:
                energy_data = json.load(f)
            logger.info(f"Loaded energy data successfully")
            
            # Load expected generation with project IDs
            expected_gen_file = f"expected_generation_{project_id}.json"
            expected_gen_path = os.path.join(raw_data_folder, expected_gen_file)
            with open(expected_gen_path, 'r') as f:
                expected_gen_data = json.load(f)
            logger.info(f"Loaded expected generation data successfully")
            
            # Load price data
            price_file = f"{project_id}_{start_date}_{end_date}_price_data.json"
            price_path = os.path.join(raw_data_folder, price_file)
            with open(price_path, 'r') as f:
                price_data = json.load(f)
            logger.info(f"Loaded price data successfully")
            
            # Load project details
            details_file = f"{project_id}_{start_date}_{end_date}_project_details.json"
            details_path = os.path.join(raw_data_folder, details_file)
            with open(details_path, 'r') as f:
                project_details = json.load(f)
            logger.info(f"Loaded project details successfully")
            
        except FileNotFoundError as e:
            logger.error(f"Required data file not found: {e}")
            return None
        # Get forecast generation from acEnergy
        forecast_generation = 0
        try:
            # Extract month name from start date
            report_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            month_name = report_date_obj.strftime('%B').lower()
            
            # Get the forecast generation for the current month from systems
            systems = project_details.get('project', {}).get('sites', {}).get('S1', {}).get('systems', {})
            if systems:
                for system_id, system_data in systems.items():
                    if 'forecast' in system_data and 'acEnergy' in system_data['forecast']:
                        if 'monthlyACEnergy' in system_data['forecast']['acEnergy']:
                            monthly_ac_energy = system_data['forecast']['acEnergy']['monthlyACEnergy']
                            forecast_generation = monthly_ac_energy.get(month_name, 0)
                            logger.info(f"Found forecast generation for {month_name}: {forecast_generation} kWh")
                            break
        except Exception as e:
            logger.warning(f"Could not extract forecast generation data: {e}")
        # Extract monthly irradiance data from project details
        monthly_irradiance = {}
        try:
            systems = project_details.get('project', {}).get('sites', {}).get('S1', {}).get('systems', {})
            if systems:
                for system_id, system_data in systems.items():
                    if 'forecast' in system_data and 'irradiance' in system_data['forecast']:
                        if 'monthlyIrradiance' in system_data['forecast']['irradiance']:
                            monthly_irradiance = system_data['forecast']['irradiance']['monthlyIrradiance']
                            logger.info(f"Found monthly irradiance data")
                            break
        except Exception as e:
            logger.warning(f"Could not extract monthly irradiance data: {e}")
        
        # Create CSV file
        with open(csv_path, 'w', newline='') as csvfile:
            # Write metadata and documentation link at the top of the file
            csvfile.write("CSV file generated by the Ecosuite Variance Report tool\n")
            csvfile.write("For documentation on how these metrics are calculated, visit: https://docs.google.com/document/d/e/2PACX-1vTeN8uIWsHdDujwMb_2GRVfBn_EnYLOyvYal2a7wIY9utzMAcaD7lyKflud1hVW1qaTJRUYxori_ocV/pub\n\n")
            
            headers = [
                'Project Code',
                'Project Name',
                'State',
                'COD',
                'Size (kW)',
                'Start Date (YYYY-MM-DD)',
                'End Date (YYYY-MM-DD)',
                'Actual Generation (kWh)',
                'Expected Generation (kWh)',
                'Forecast Generation (kWh)',
                'Variance with Expected Generation (%)',
                'Availability Loss (kWh)',
                'Actual + Availability Loss (kWh)',
                'Total Variance with Expected Generation (%)',
                'Actual Insolation (kWh/m2)',
                'Forecast Insolation (kWh/m2)',
                'Weather Adjusted Forecast Generation (kWh)',
                'Weather Adjusted Generation Variance (kWh)',
                'Av PPA Price ($/kWh)',
                'Anticipated PPA Revenue ($)',
                'Expected PPA Revenue ($)',
                'PPA Revenue Variance ($)',  
                'Av REC Sale Price ($/MWh)',
                'Actual RECs Generated',
                'Expected RECs',
                'Anticipated RECs Revenue ($)',
                'Expected RECs Revenue ($)',
                'REC Revenue Variance ($)',  
                'Total Anticipated Revenue ($)',
                'Total Expected Revenue ($)',
                'Total Revenue Variance ($)' 
            ]
            
            
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader() 

            # Extract project details
            project_name = project_details.get('project', {}).get('name', 'Unknown')
            project_state = project_details.get('project', {}).get('state', '')
            cod_date = project_details.get('project', {}).get('productionStartDate', '')
            
            # Get system size
            system_size = project_details.get('project', {}).get('dcSize', 0)
            
            # Get PPA and REC rates from price data
            # Get PPA rate - looking for PPA cashflow
            ppa_rate = 0
            # Get REC rate - looking for SREC Revenue cashflow
            rec_rate = 0
            
            if 'proForma' in price_data and 'cashFlows' in price_data['proForma']:
                for cashflow in price_data['proForma']['cashFlows']:
                    if cashflow.get('category') == 'Income' and cashflow.get('account') == 'PPA/FIT':
                        for payment in cashflow.get('payments', []):
                            if 'recurrence' in payment and 'startRate' in payment['recurrence']:
                                ppa_rate = payment['recurrence']['startRate']
                                logger.info(f"Found PPA rate: {ppa_rate}")
                                break
                    
                    if cashflow.get('category') == 'Income' and cashflow.get('account') == 'SREC Revenue':
                        logger.info(f"Found SREC Revenue cashflow")
                        for payment in cashflow.get('payments', []):
                            if 'recurrence' in payment:
                                logger.info(f"Recurrence type: {payment['recurrence'].get('rateType')}")
                                # If it's a fixed rate
                                if payment['recurrence'].get('rateType') == 'fixed' and 'startRate' in payment['recurrence']:
                                    rec_rate = payment['recurrence']['startRate']
                                    logger.info(f"Found fixed REC rate: {rec_rate}")
                                    break
                                # If it's a monthly rate
                                elif 'rates' in payment['recurrence']:
                                    try:
                                        # Calculate years since start date
                                        start_date_obj = datetime.strptime(payment['recurrence'].get('startDate', '2018-01-01'), '%Y-%m-%d')
                                        report_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
                                        years_diff = report_date_obj.year - start_date_obj.year
                                        
                                        # Get month name
                                        month_name = report_date_obj.strftime('%B').lower()
                                        
                                        logger.info(f"Looking for REC rate for {month_name} in year {years_diff}")
                                        logger.info(f"Available years: {len(payment['recurrence']['rates'])}")
                                        
                                        # Get the rate for the current year and month
                                        if years_diff < len(payment['recurrence']['rates']):
                                            year_rates = payment['recurrence']['rates'][years_diff]
                                            logger.info(f"Year rates: {year_rates}")
                                            if month_name in year_rates:
                                                rec_rate = year_rates[month_name]
                                                logger.info(f"Found REC rate: {rec_rate}")
                                    except Exception as e:
                                        logger.error(f"Error extracting REC rate: {e}")
                                    break
            
            logger.info(f"PPA Rate: {ppa_rate}, REC Rate: {rec_rate}")
            
            # Get expected generation from expected_gen_data
            total_expected_gen = 0
            try:
                # Check if project exists in the data
                if project_id in expected_gen_data.get('projects', {}):
                    # Sum all expectedGeneration values
                    project_data = expected_gen_data['projects'][project_id]
                    
                    # Option 1: Use the aggregated total for the project if available
                    if 'expectedGeneration' in project_data:
                        total_expected_gen = project_data['expectedGeneration']
                    else:
                        # Option 2: Sum from the aggregatedTotals
                        for date_key, data in project_data.get('aggregatedTotals', {}).items():
                            if date_key.startswith(start_date[:7]):  # Compare year-month part
                                total_expected_gen += data.get('expectedGeneration', 0)
                    
                    # Convert from Wh to kWh
                    total_expected_gen = total_expected_gen / 1000
                    
                    logger.info(f"Total expected generation: {total_expected_gen} kWh")
                else:
                    logger.warning(f"Project {project_id} not found in expected generation data")
            except Exception as e:
                logger.error(f"Error extracting expected generation: {e}")
            
            # Get irradiance hours
            irradiance_hours = 0
            try:
                if project_id in expected_gen_data.get('projects', {}):
                    # Get irradiance hours
                    irradiance_hours = expected_gen_data['projects'][project_id].get('irradianceHours', 0)
                    logger.info(f"Irradiance hours: {irradiance_hours}")
            except Exception as e:
                logger.error(f"Error extracting irradiance hours: {e}")
            
            # Process energy data using the aggregatedTotals structure
            aggregated_totals = energy_data.get('project', {}).get('aggregatedTotals', {})
            if not aggregated_totals:
                # Try another possible location
                aggregated_totals = energy_data.get('aggregatedTotals', {})
            
            if aggregated_totals:
                # Sum all generation values for the month
                total_actual_gen = 0
                
                for date_key, data in aggregated_totals.items():
                    # Check if date is within our range
                    if date_key.startswith(start_date[:7]):  # Compare year-month part
                        # Extract generation value (in watt-hours, convert to kWh)
                        generation = data.get('generation', 0)
                        total_actual_gen += generation / 1000  # Convert Wh to kWh
                
                logger.info(f"Total actual generation: {total_actual_gen} kWh")
                
                # Get forecast irradiance for the current month
                report_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
                month_name = report_date_obj.strftime('%B').lower()
                forecast_insolation = monthly_irradiance.get(month_name, 0)
                
                # Calculate Lost Generation - Availability
                # For this example, we'll assume it's 0 since we don't have this data
                lost_gen_availability = 0
                
                # Calculate Actual + Available
                total_gen = total_actual_gen + lost_gen_availability
                
                # Calculate variances
                if total_expected_gen > 0:
                    gen_variance_pct = ((total_actual_gen - total_expected_gen) / total_expected_gen) * 100
                    combined_variance_pct = ((total_gen - total_expected_gen) / total_expected_gen) * 100
                else:
                    gen_variance_pct = 0
                    combined_variance_pct = 0

                # For Actual Irradiance (divide by 1000)
                actual_insolation = irradiance_hours / 1000

                # For Forecast Irradiance (multiply by number of days in month)
                # Get the number of days in the month
                last_day_of_month = calendar.monthrange(report_date_obj.year, report_date_obj.month)[1]
                forecast_insolation = monthly_irradiance.get(month_name, 0) * last_day_of_month
                
                # Weather normalized calculations
                weather_normalized_expected = total_expected_gen
                if forecast_insolation > 0 and actual_insolation > 0:
                    weather_normalized_expected = forecast_generation * (actual_insolation / forecast_insolation)
                
                weather_normalized_variance = total_actual_gen - weather_normalized_expected
                
                # Calculate revenue
                actual_revenue = total_actual_gen * ppa_rate
                expected_revenue = total_expected_gen * ppa_rate
                revenue_variance = actual_revenue - expected_revenue
                
                # Calculate RECs (1 REC per MWh)
                actual_recs = total_actual_gen / 1000  # kWh to MWh
                expected_recs = total_expected_gen / 1000  # kWh to MWh
                
                # Calculate REC revenue
                # Multiply REC rate by 1000 for Av Actual REC Sale Price
                rec_rate_display = rec_rate * 1000
                actual_rec_revenue = actual_recs * rec_rate_display
                expected_rec_revenue = expected_recs * rec_rate_display
                rec_revenue_variance = actual_rec_revenue - expected_rec_revenue
                
                # Calculate total revenue
                total_actual_revenue = actual_revenue + actual_rec_revenue
                total_expected_revenue = expected_revenue + expected_rec_revenue
                total_revenue_variance = total_actual_revenue - total_expected_revenue
                
                # Write row to CSV
                writer.writerow({
                    'Project Code': project_id,
                    'Project Name': project_name,
                    'State': project_state,
                    'COD': cod_date,
                    'Size (kW)': format_number(system_size),
                    'Start Date (YYYY-MM-DD)': start_date,
                    'End Date (YYYY-MM-DD)': end_date,
                    'Actual Generation (kWh)': format_number(total_actual_gen),
                    'Expected Generation (kWh)': format_number(total_expected_gen),
                    'Forecast Generation (kWh)': format_number(forecast_generation),
                    'Variance with Expected Generation (%)': f"{gen_variance_pct:.2f}%",
                    'Availability Loss (kWh)': format_number(lost_gen_availability),
                    'Actual + Availability Loss (kWh)': format_number(total_gen),
                    'Total Variance with Expected Generation (%)': f"{combined_variance_pct:.2f}%",
                    'Actual Insolation (kWh/m2)': format_number(actual_insolation),
                    'Forecast Insolation (kWh/m2)': format_number(forecast_insolation),
                    'Weather Adjusted Forecast Generation (kWh)': format_number(weather_normalized_expected),
                    'Weather Adjusted Generation Variance (kWh)': format_number(weather_normalized_variance),
                    'Av PPA Price ($/kWh)': format_number(ppa_rate),
                    'Anticipated PPA Revenue ($)': format_number(actual_revenue),
                    'Expected PPA Revenue ($)': format_number(expected_revenue),
                    'PPA Revenue Variance ($)': format_number(revenue_variance),
                    'Av REC Sale Price ($/MWh)': format_number(rec_rate_display),
                    'Actual RECs Generated': format_number(actual_recs),
                    'Expected RECs': format_number(expected_recs),
                    'Actual RECs Revenue ($)': format_number(actual_rec_revenue),
                    'Expected RECs Revenue ($)': format_number(expected_rec_revenue),
                    'REC Revenue Variance ($)': format_number(rec_revenue_variance),
                    'Total Anticipated Revenue ($)': format_number(total_actual_revenue),
                    'Total Expected Revenue ($)': format_number(total_expected_revenue),
                    'Total Revenue Variance ($)': format_number(total_revenue_variance)
                })
                logger.info(f"Wrote data to CSV")
            else:
                logger.error("No aggregatedTotals found in energy data")
                return None
            
            logger.info(f"CSV report generated successfully: {csv_path}")
            print(f"CSV report generated successfully: {csv_path}")
            return csv_path
            
    except Exception as e:
        logger.error(f"Error generating CSV report: {e}", exc_info=True)
        print(f"Error generating CSV report: {e}")
        return None
    

    #---------------------------------------------------

def process_bulk_projects(project_codes: List[str], start_date: str, end_date: str, aggregation: str, 
                        debugger: EcoSuiteDataExtractorDebugger, token: str, csv_input_filename: str = None) -> bool:
    """
    Process multiple projects and generate a consolidated CSV report
    """
    try:
        # Create a consolidated CSV file with the input CSV filename if provided
        if csv_input_filename:
            # Extract just the filename without path and extension
            import os
            base_filename = os.path.basename(csv_input_filename)
            base_filename = os.path.splitext(base_filename)[0]
            consolidated_csv_filename = f"{base_filename}_{start_date}_{end_date}_consolidated_report.csv"
        else:
            consolidated_csv_filename = f"consolidated_report_{start_date}_{end_date}.csv"
        
        with open(consolidated_csv_filename, 'w', newline='') as consolidated_file:
            # Write metadata and documentation link at the top of the file
            consolidated_file.write("CSV file generated by the Ecosuite Variance Report tool\n")
            consolidated_file.write("For documentation on how these metrics are calculated, visit: https://docs.google.com/document/d/e/2PACX-1vTeN8uIWsHdDujwMb_2GRVfBn_EnYLOyvYal2a7wIY9utzMAcaD7lyKflud1hVW1qaTJRUYxori_ocV/pub\n\n")
            
            # Define CSV headers with unique column names for variance amounts
            headers = [
                'Project Code',
                'Project Name',
                'State',
                'COD',
                'Size (kW)',
                'Start Date (YYYY-MM-DD)',
                'End Date (YYYY-MM-DD)',
                'Actual Generation (kWh)',
                'Expected Generation (kWh)',
                'Forecast Generation (kWh)',
                'Variance with Expected Generation (%)',
                'Availability Loss (kWh)',
                'Actual + Availability Loss (kWh)',
                'Total Variance with Expected Generation (%)',
                'Actual Insolation (kWh/m2)',
                'Forecast Insolation (kWh/m2)',
                'Weather Adjusted Forecast Generation (kWh)',
                'Weather Adjusted Generation Variance (kWh)',
                'Av PPA Price ($/kWh)',
                'Anticipated PPA Revenue ($)',
                'Expected PPA Revenue ($)',
                'PPA Revenue Variance ($)',
                'Av REC Sale Price ($/MWh)',
                'Actual RECs Generated',
                'Expected RECs',
                'Actual RECs Revenue ($)',
                'Expected RECs Revenue ($)',
                'REC Revenue Variance ($)',
                'Total Anticipated Revenue ($)',
                'Total Expected Revenue ($)',
                'Total Revenue Variance ($)'
            ]

            consolidated_writer = csv.DictWriter(consolidated_file, fieldnames=headers)
            consolidated_writer.writeheader()
            
            # Process each project
            successful_projects = []
            failed_projects = []
            
            for project_id in project_codes:
                print(f"\nProcessing project: {project_id}")
                
                try:
                    # Fetch project details
                    project_details = fetch_project_details(project_id, token, debugger)
                    if not project_details:
                        logger.error(f"Failed to fetch project details for project {project_id}")
                        failed_projects.append(project_id)
                        continue
                    
                    # Create project folder
                    project_name = project_details.get('project', {}).get('name', project_id)
                    raw_data_folder = create_project_folder(project_name, start_date, end_date)
                    
                    # Save project details
                    save_to_json("project_details.json", project_details, project_id, start_date, end_date, raw_data_folder)
                    
                    # Fetch and save price data
                    price_data = fetch_price_data(project_id, token, debugger)
                    if price_data:
                        save_to_json("price_data.json", price_data, project_id, start_date, end_date, raw_data_folder)
                    
                    # Fetch and save energy datums
                    energy_data = fetch_ecosuite_energy_datums(
                        project_id, 
                        start_date, 
                        end_date, 
                        aggregation.lower(), 
                        token, 
                        raw_data_folder,
                        debugger
                    )
                    
                    # Fetch and save expected generation with project IDs
                    expected_gen_data = fetch_expected_generation_with_project_ids(
                        project_id,
                        start_date,
                        end_date,
                        aggregation.lower(),
                        token,
                        raw_data_folder,
                        debugger
                    )
                    
                    # Fetch EcoSuite weather data
                    ecosuite_weather = fetch_ecosuite_weather_datums(
                        project_id, 
                        start_date, 
                        end_date, 
                        aggregation.lower(), 
                        token, 
                        raw_data_folder,
                        debugger
                    )
                    
                    # Fetch SolarNetwork weather data
                    solarnetwork_weather = fetch_solarnetwork_weather_data(
                        project_id,
                        project_details,
                        start_date,
                        end_date,
                        aggregation,
                        raw_data_folder,
                        debugger
                    )
                    
                    # Save a metadata file with info about fetched data
                    metadata = {
                        "project_id": project_id,
                        "project_name": project_name,
                        "start_date": start_date,
                        "end_date": end_date,
                        "aggregation": aggregation,
                        "data_fetched": {
                            "project_details": project_details is not None,
                            "price_data": price_data is not None,
                            "energy_data": energy_data is not None,
                            "expected_generation_with_ids": expected_gen_data is not None,
                            "ecosuite_weather": ecosuite_weather is not None,
                            "solarnetwork_weather": solarnetwork_weather is not None
                        },
                        "timestamp": datetime.now().isoformat()
                    }
                    
                    save_to_json("metadata.json", metadata, project_id, start_date, end_date, raw_data_folder)
                    
                    # Generate data for the project
                    project_data = generate_project_data(
                        project_id,
                        start_date,
                        end_date,
                        raw_data_folder,
                        aggregation
                    )
                    
                    if project_data:
                        # Add row to consolidated CSV
                        consolidated_writer.writerow(project_data)
                        successful_projects.append(project_id)
                    else:
                        failed_projects.append(project_id)
                    
                except Exception as e:
                    logger.error(f"Error processing project {project_id}: {e}")
                    failed_projects.append(project_id)
            
            # Print summary
            print("\n--- Processing Summary ---")
            print(f"Total Projects: {len(project_codes)}")
            print(f"Successful Projects: {len(successful_projects)} ({successful_projects})")
            print(f"Failed Projects: {len(failed_projects)} ({failed_projects})")
            print(f"Consolidated report saved to: {consolidated_csv_filename}")
            
            return len(failed_projects) == 0
            
    except Exception as e:
        logger.error(f"An unexpected error occurred in bulk processing: {str(e)}", exc_info=True)
        debugger.log_error("Bulk Project Processing", e)
        return False

def generate_project_data(project_id: str, start_date: str, end_date: str, 
                        raw_data_folder: str, aggregation: str) -> dict:
    """
    Generate data for a single project for the consolidated report
    Similar to generate_csv_report but returns a dict instead of writing to CSV
    """
    try:
        # Load all the necessary JSON files
        try:
            # Load energy datums
            energy_file = f"{project_id}_{start_date}_{end_date}_energy_datums.json"
            energy_path = os.path.join(raw_data_folder, energy_file)
            with open(energy_path, 'r') as f:
                energy_data = json.load(f)
            
            # Load expected generation with project IDs
            expected_gen_file = f"expected_generation_{project_id}.json"
            expected_gen_path = os.path.join(raw_data_folder, expected_gen_file)
            with open(expected_gen_path, 'r') as f:
                expected_gen_data = json.load(f)
            
            # Load price data
            price_file = f"{project_id}_{start_date}_{end_date}_price_data.json"
            price_path = os.path.join(raw_data_folder, price_file)
            with open(price_path, 'r') as f:
                price_data = json.load(f)
            
            # Load project details
            details_file = f"{project_id}_{start_date}_{end_date}_project_details.json"
            details_path = os.path.join(raw_data_folder, details_file)
            with open(details_path, 'r') as f:
                project_details = json.load(f)
            
        except FileNotFoundError as e:
            logger.error(f"Required data file not found: {e}")
            return None
        

        # Get forecast generation from acEnergy
        forecast_generation = 0
        try:
            # Extract month name from start date
            report_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            month_name = report_date_obj.strftime('%B').lower()
            
            # Get the forecast generation for the current month from systems
            systems = project_details.get('project', {}).get('sites', {}).get('S1', {}).get('systems', {})
            if systems:
                for system_id, system_data in systems.items():
                    if 'forecast' in system_data and 'acEnergy' in system_data['forecast']:
                        if 'monthlyACEnergy' in system_data['forecast']['acEnergy']:
                            monthly_ac_energy = system_data['forecast']['acEnergy']['monthlyACEnergy']
                            forecast_generation = monthly_ac_energy.get(month_name, 0)
                            break
        except Exception as e:
            logger.warning(f"Could not extract forecast generation data: {e}")
        # Extract monthly irradiance data from project details
        monthly_irradiance = {}
        try:
            systems = project_details.get('project', {}).get('sites', {}).get('S1', {}).get('systems', {})
            if systems:
                for system_id, system_data in systems.items():
                    if 'forecast' in system_data and 'irradiance' in system_data['forecast']:
                        if 'monthlyIrradiance' in system_data['forecast']['irradiance']:
                            monthly_irradiance = system_data['forecast']['irradiance']['monthlyIrradiance']
                            break
        except Exception as e:
            logger.warning(f"Could not extract monthly irradiance data: {e}")
        
        # Extract project details
        project_name = project_details.get('project', {}).get('name', 'Unknown')
        project_state = project_details.get('project', {}).get('state', '')
        cod_date = project_details.get('project', {}).get('productionStartDate', '')
        
        # Get system size
        system_size = project_details.get('project', {}).get('dcSize', 0)
        
        # Get PPA and REC rates from price data
        # Get PPA rate - looking for PPA cashflow
        ppa_rate = 0
        # Get REC rate - looking for SREC Revenue cashflow
        rec_rate = 0
        
        if 'proForma' in price_data and 'cashFlows' in price_data['proForma']:
            for cashflow in price_data['proForma']['cashFlows']:
                if cashflow.get('category') == 'Income' and cashflow.get('account') == 'PPA/FIT':
                    for payment in cashflow.get('payments', []):
                        if 'recurrence' in payment and 'startRate' in payment['recurrence']:
                            ppa_rate = payment['recurrence']['startRate']
                            break
                
                if cashflow.get('category') == 'Income' and cashflow.get('account') == 'SREC Revenue':
                    for payment in cashflow.get('payments', []):
                        if 'recurrence' in payment:
                            # If it's a fixed rate
                            if payment['recurrence'].get('rateType') == 'fixed' and 'startRate' in payment['recurrence']:
                                rec_rate = payment['recurrence']['startRate']
                                break
                            # If it's a monthly rate
                            elif 'rates' in payment['recurrence']:
                                try:
                                    # Calculate years since start date
                                    start_date_obj = datetime.strptime(payment['recurrence'].get('startDate', '2018-01-01'), '%Y-%m-%d')
                                    report_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
                                    years_diff = report_date_obj.year - start_date_obj.year
                                    
                                    # Get month name
                                    month_name = report_date_obj.strftime('%B').lower()
                                    
                                    # Get the rate for the current year and month
                                    if years_diff < len(payment['recurrence']['rates']):
                                        year_rates = payment['recurrence']['rates'][years_diff]
                                        if month_name in year_rates:
                                            rec_rate = year_rates[month_name]
                                except Exception as e:
                                    logger.error(f"Error extracting REC rate: {e}")
                                break
        
        # Get expected generation from expected_gen_data
        total_expected_gen = 0
        try:
            # Check if project exists in the data
            if project_id in expected_gen_data.get('projects', {}):
                # Sum all expectedGeneration values
                project_data = expected_gen_data['projects'][project_id]
                
                # Option 1: Use the aggregated total for the project if available
                if 'expectedGeneration' in project_data:
                    total_expected_gen = project_data['expectedGeneration']
                else:
                    # Option 2: Sum from the aggregatedTotals
                    for date_key, data in project_data.get('aggregatedTotals', {}).items():
                        if date_key.startswith(start_date[:7]):  # Compare year-month part
                            total_expected_gen += data.get('expectedGeneration', 0)
                
                # Convert from Wh to kWh
                total_expected_gen = total_expected_gen / 1000
            else:
                logger.warning(f"Project {project_id} not found in expected generation data")
        except Exception as e:
            logger.error(f"Error extracting expected generation: {e}")
        
        # Get irradiance hours
        irradiance_hours = 0
        try:
            if project_id in expected_gen_data.get('projects', {}):
                # Get irradiance hours
                irradiance_hours = expected_gen_data['projects'][project_id].get('irradianceHours', 0)
        except Exception as e:
            logger.error(f"Error extracting irradiance hours: {e}")
        
        # Calculate actual irradiance (divide by 1000)
        actual_insolation = irradiance_hours / 1000
        
        # Calculate forecast irradiance (multiply by days in month)
        report_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        month_name = report_date_obj.strftime('%B').lower()
        
        # Get the number of days in the month
        import calendar
        last_day_of_month = calendar.monthrange(report_date_obj.year, report_date_obj.month)[1]
        forecast_insolation = monthly_irradiance.get(month_name, 0) * last_day_of_month
        
        # Process energy data using the aggregatedTotals structure
        aggregated_totals = energy_data.get('project', {}).get('aggregatedTotals', {})
        if not aggregated_totals:
            # Try another possible location
            aggregated_totals = energy_data.get('aggregatedTotals', {})
        
        # Sum all generation values for the month
        total_actual_gen = 0
        
        if aggregated_totals:
            for date_key, data in aggregated_totals.items():
                # Check if date is within our range
                if date_key.startswith(start_date[:7]):  # Compare year-month part
                    # Extract generation value (in watt-hours, convert to kWh)
                    generation = data.get('generation', 0)
                    total_actual_gen += generation / 1000  # Convert Wh to kWh
        
        # Calculate Lost Generation - Availability
        # For this example, we'll assume it's 0 since we don't have this data
        lost_gen_availability = 0
        
        # Calculate Actual + Available
        total_gen = total_actual_gen + lost_gen_availability
        
        # Calculate variances
        if total_expected_gen > 0:
            gen_variance_pct = ((total_actual_gen - total_expected_gen) / total_expected_gen) * 100
            combined_variance_pct = ((total_gen - total_expected_gen) / total_expected_gen) * 100
        else:
            gen_variance_pct = 0
            combined_variance_pct = 0
        
        # Weather normalized calculations
        weather_normalized_expected = total_expected_gen
        if forecast_insolation > 0 and actual_insolation > 0:
            weather_normalized_expected = forecast_generation * (actual_insolation / forecast_insolation)
        
        weather_normalized_variance = total_actual_gen - weather_normalized_expected
        
        # Calculate revenue
        actual_revenue = total_actual_gen * ppa_rate
        expected_revenue = total_expected_gen * ppa_rate
        revenue_variance = actual_revenue - expected_revenue
        
        # Calculate RECs (1 REC per MWh)
        actual_recs = total_actual_gen / 1000  # kWh to MWh
        expected_recs = total_expected_gen / 1000  # kWh to MWh
        
        # Calculate REC revenue
        # Multiply REC rate by 1000 for Av Actual REC Sale Price
        rec_rate_display = rec_rate * 1000
        actual_rec_revenue = actual_recs * rec_rate_display
        expected_rec_revenue = expected_recs * rec_rate_display
        rec_revenue_variance = actual_rec_revenue - expected_rec_revenue
        
        # Calculate total revenue
        total_actual_revenue = actual_revenue + actual_rec_revenue
        total_expected_revenue = expected_revenue + expected_rec_revenue
        total_revenue_variance = total_actual_revenue - total_expected_revenue

        # Return data as a dictionary
        return {
            'Project Code': project_id,
            'Project Name': project_name,
            'State': project_state,
            'COD': cod_date,
            'Size (kW)': format_number(system_size),
            'Start Date (YYYY-MM-DD)': start_date,
            'End Date (YYYY-MM-DD)': end_date,
            'Actual Generation (kWh)': format_number(total_actual_gen),
            'Expected Generation (kWh)': format_number(total_expected_gen),
            'Forecast Generation (kWh)': format_number(forecast_generation),
            'Variance with Expected Generation (%)': f"{gen_variance_pct:.2f}%",
            'Availability Loss (kWh)': format_number(lost_gen_availability),
            'Actual + Availability Loss (kWh)': format_number(total_gen),
            'Total Variance with Expected Generation (%)': f"{combined_variance_pct:.2f}%",
            'Actual Insolation (kWh/m2)': format_number(actual_insolation),
            'Forecast Insolation (kWh/m2)': format_number(forecast_insolation),
            'Weather Adjusted Forecast Generation (kWh)': format_number(weather_normalized_expected),
            'Weather Adjusted Generation Variance (kWh)': format_number(weather_normalized_variance),
            'Av PPA Price ($/kWh)': format_number(ppa_rate),
            'Anticipated PPA Revenue ($)': format_number(actual_revenue),
            'Expected PPA Revenue ($)': format_number(expected_revenue),
            'PPA Revenue Variance ($)': format_number(revenue_variance),
            'Av REC Sale Price ($/MWh)': format_number(rec_rate_display),
            'Actual RECs Generated': format_number(actual_recs),
            'Expected RECs': format_number(expected_recs),
            'Actual RECs Revenue ($)': format_number(actual_rec_revenue),
            'Expected RECs Revenue ($)': format_number(expected_rec_revenue),
            'REC Revenue Variance ($)': format_number(rec_revenue_variance),
            'Total Anticipated Revenue ($)': format_number(total_actual_revenue),
            'Total Expected Revenue ($)': format_number(total_expected_revenue),
            'Total Revenue Variance ($)': format_number(total_revenue_variance)
        }
        
    except Exception as e:
        logger.error(f"Error generating project data: {e}", exc_info=True)
        return None

def main():
    """
    Main function to orchestrate the data extraction process.
    """
    # Initialize debugger
    debugger = EcoSuiteDataExtractorDebugger(None)
    
    try:
        # Get authentication token
        from auth_manager import get_auth_token
        token = get_auth_token(USERNAME, PASSWORD, debugger)
        
        if not token:
            logger.error("Failed to obtain authentication token")
            return
        
        # Check if processing single project or batch from CSV
        process_type = input("Process single project (S) or batch from CSV (B)? ").upper()
        
        if process_type == 'S':
            # Prompt for project ID
            project_id = input("Enter the project ID: ")
            project_codes = [project_id]
            csv_file_path = None
        elif process_type == 'B':
            # Prompt for CSV file path
            csv_file_path = input("Enter the path to the CSV file with project codes: ")
            
            # Read project codes from CSV
            project_codes = read_project_codes_from_csv(csv_file_path)
            
            if not project_codes:
                print("No project codes found. Exiting.")
                return
        else:
            print("Invalid choice. Exiting.")
            return
        
        # Prompt for date range and aggregation
        start_date = input("Enter start date (YYYY-MM-DD): ")
        end_date = input("Enter end date (YYYY-MM-DD): ")
        
        # Choose aggregation
        aggregation_options = ['FiveMinute', 'FifteenMinute', 'ThirtyMinute', 'Hour', 'Day']
        print("\nSelect aggregation:")
        for i, option in enumerate(aggregation_options, 1):
            print(f"{i}. {option}")
        
        while True:
            try:
                choice = int(input("Enter choice (1-5): "))
                aggregation = aggregation_options[choice-1]
                break
            except (ValueError, IndexError):
                print("Invalid choice. Please try again.")
        
        # Process projects based on type
        if process_type == 'S':
            # Process single project
            success = process_project(project_codes[0], start_date, end_date, aggregation, debugger, token)
            if success:
                print(f"\nSuccessfully processed project {project_codes[0]}")
            else:
                print(f"\nFailed to process project {project_codes[0]}")
        else:
            # Process multiple projects with consolidated report, passing in the CSV filename
            success = process_bulk_projects(project_codes, start_date, end_date, aggregation, debugger, token, csv_file_path)
            if success:
                print("\nAll projects processed successfully")
            else:
                print("\nSome projects failed to process. Check the summary for details.")
        
        # Export debug report
        debugger.export_debug_report()
        
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}", exc_info=True)
        debugger.log_error("Main Execution", e)



if __name__ == "__main__":
    main()
