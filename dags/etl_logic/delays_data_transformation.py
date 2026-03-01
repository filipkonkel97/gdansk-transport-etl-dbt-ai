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