import requests

class APIClient:
    def __init__(self, api_url):
        self.api_url = api_url

    def fetch_data(self):
        """Fetches data from the public transport delays API.
        args:
            None
        returns:
            data (dict): The JSON data fetched from the API.
        Raises:
            Exception: If the API request fails or returns no data.
        """
        try:
            r = requests.get(self.api_url, verify=False, timeout=10) # Set a timeout for the request
            
            r.raise_for_status() # Check for HTTP errors
            
            self.data = r.json() # Parse JSON response
            
            print(f"Data fetched successfully from {self.api_url}") # Debugging statement
            
            return self.data
        except requests.exceptions.HTTPError as e:
            raise Exception(f"HTTP error: {r.status_code} - {r.text}") from e
        except requests.exceptions.Timeout:
            raise Exception("Request timed out.")
        except requests.exceptions.ConnectionError:
            raise Exception("Connection error.")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Unexpected request error: {e}") from e
        except ValueError as e:
            raise Exception(f"Error decoding JSON: {e}") from e