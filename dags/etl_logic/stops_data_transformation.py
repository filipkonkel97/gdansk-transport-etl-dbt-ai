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
