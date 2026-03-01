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