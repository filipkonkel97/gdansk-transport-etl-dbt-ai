def transform_delays_data(raw_data):
        """
        Transform the raw data into a list.
        Args:
            raw_data: The raw delays data fetched from the API.
        Returns:
            list: Transformed data as a list.
        Raises:
            Exception: If the raw data is not available.
        """
        try:
            transformed_data = [
                {
                    'stopid': key,
                    'id': departure['id'],
                    'delayInSeconds': departure['delayInSeconds'],
                    'estimatedTIme': departure['estimatedTime'],
                    'headsign': departure['headsign'],
                    'routeId': departure['routeId'],
                    'routeShortName': departure['routeShortName'],
                    'scheduledTripStartTime': departure['scheduledTripStartTime'],
                    'tripId': departure['tripId'],
                    'status': departure['status'],
                    'theoreticalTime': departure['theoreticalTime'],
                    'timestamp': departure['timestamp'],
                    'trip': departure['trip'],
                    'vehicleCode': departure['vehicleCode'],
                    'vehicleId': departure['vehicleId'],
                    'vehicleService': departure['vehicleService'],
                }
                for key, value in raw_data.items()
                for departure in value['departures']
                if departure['status'] != 'SCHEDULED'
            ]
            
            print('Data transformed successfully')

            return transformed_data
        
        except Exception as e:
            raise Exception(f"Error transforming data: {e}") from e
        

def transform_routes_data(raw_data):
    """
    Transform the raw data into a list.
    Args:
        None
    Returns:
        list: Transformed data as a list.
    Raises:
        Exception: If the raw data is not available.
    """
    
    try:
        extracted_data = [raw_data[key] for key in raw_data.keys()]
        
        print('Data transformed successfully')

        return extracted_data
    
    except Exception as e:
        raise Exception(f"Error transforming data: {e}") from e
    

def transform_stops_data(raw_data):
        """
        Transform the raw data into a list.
        Args:
            None
        Returns:
            list: Transformed data as a list.
        Raises:
            Exception: If the raw data is not available.
        """
        try:
            extracted_data = []
            for key, value in raw_data.items():
                for stop in value['stops']:
                    extracted_data.append(stop)

            print('Data transformed successfully')

            return extracted_data
        
        except Exception as e:
            raise Exception(f"Error transforming data: {e}") from e
        

def transform_trips_data(raw_data):
    """
    Transform the raw data into a list.
    Args:
        raw_data: The raw data to be transformed.
    Returns:
        list: Transformed data as a list.
    Raises:
        Exception: If the raw data is not available.
    """
    try:
        extracted_data = []

        for key, value in raw_data.items():
            for trip in value['trips']:
                extracted_data.append(trip)

        print('Data transformed successfully')
        return extracted_data
    
    except Exception as e:
        raise Exception(f"Error transforming data: {e}") from e
    

def transform_vehicle_data(raw_data):
    """
    Transform the raw data into a list.
    Args:
        raw_data: The raw data to be transformed.
    Returns:
        list: Transformed data as a list.
    Raises:
        Exception: If the raw data is not available.
    """
    try:
        extracted_data = [result for result in raw_data['results']]

        print('Data transformed successfully')
        return extracted_data
    
    except Exception as e:
        raise Exception(f"Error transforming data: {e}") from e
    

def transform_vehicle_psn_data(raw_data):
    """
    Transform the raw data into a list.
    Args:
        raw_data: The raw vehicles data fetched from the API
    Returns:
        list: Transformed data as a list.
    Raises:
        Exception: If the raw data is not available.
    """
    
    try:
        data_extracted = []

        for vehicle in raw_data['vehicles']:
            vehicle_data = {
                'generated': vehicle['generated'],
                'routeShortName': vehicle['routeShortName'],
                'tripId': vehicle['tripId'],
                'routeId': vehicle['routeId'],
                'headsign': vehicle['headsign'],
                'vehicleCode': vehicle['vehicleCode'],
                'vehicleService': vehicle['vehicleService'],
                'vehicleId': vehicle['vehicleId'],
                'speed': vehicle['speed'],
                'direction': vehicle['direction'],
                'delay': vehicle['delay'],
                'scheduledTripStartTime': vehicle['scheduledTripStartTime'] if vehicle['scheduledTripStartTime'] else None,
                'lat' : vehicle['lat'],
                'lon': vehicle['lon'],
                'gpsQuality': vehicle['gpsQuality'],
            }

            data_extracted.append(vehicle_data)

        print('Data transformed successfully')

        return data_extracted
    
    except Exception as e:
        raise Exception(f"Error transforming data: {e}") from e
