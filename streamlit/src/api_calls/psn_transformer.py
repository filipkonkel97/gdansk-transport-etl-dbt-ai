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

        for vehicle in raw_data["vehicles"]:
            vehicle_data = {
                "generated": vehicle["generated"],
                "routeShortName": vehicle["routeShortName"],
                "tripId": vehicle["tripId"],
                "routeId": vehicle["routeId"],
                "headsign": vehicle["headsign"],
                "vehicleCode": vehicle["vehicleCode"],
                "vehicleService": vehicle["vehicleService"],
                "vehicleId": vehicle["vehicleId"],
                "speed": vehicle["speed"],
                "direction": vehicle["direction"],
                "delay": vehicle["delay"],
                "scheduledTripStartTime": (
                    vehicle["scheduledTripStartTime"]
                    if vehicle["scheduledTripStartTime"]
                    else None
                ),
                "lat": vehicle["lat"],
                "lon": vehicle["lon"],
                "gpsQuality": vehicle["gpsQuality"],
            }

            data_extracted.append(vehicle_data)

        print("Data transformed successfully")

        return data_extracted

    except Exception as e:
        raise Exception(f"Error transforming data: {e}") from e
