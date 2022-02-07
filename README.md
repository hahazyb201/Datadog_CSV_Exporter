# Datadog_CSV_Exporter
Fetch log from Datadog and save them to a local CSV file

DD_API_KEY and DD_APP_KEY env variable need to be set according to your Datadog.

## Usage
python3 ./datadog_logs.py --query 'error 404' --start 2022-01-20T00:00:00 --end 2022-01-20T08:00:00

The above command will fetch logs containing text 'error 404' during the period specified and save them to a local csv file.
