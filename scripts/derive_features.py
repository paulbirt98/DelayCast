#calculate the delays at each station
stations = ['EUS', 'MKC', 'CRE', 'LIV']

for i, station in enumerate(stations):
    if i < (len(stations) -1):
        scheduled_col = f"{station} Scheduled Departure Time"
        actual_col = f"{station} Actual Departure Time"
    else:
        scheduled_col = f"{station} Scheduled Arrival Time"
        actual_col = f"{station} Actual Arrival Time"

    processed_service_details_df[f"{station} Delay Minutes"] = processed_service_details_df.apply(
        lambda row: calculate_delay(row[scheduled_col], row[actual_col]), axis=1
    )

    processed_service_details_df[f"{station} Delay Classification"] = processed_service_details_df[f"{station} Delay Minutes"].apply(calculate_delay_classification)

print(processed_service_details_df.columns[-10:])  # Show last few columns to check that classifications were appended