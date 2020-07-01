import coloredlogs
import logging
import os
import csv
import influxdb_client
import json
import datetime
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

authFile = "influxAuth.json"
dataPath = "COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/"

influxOrg = "covid-viewer"
influxBucket = "covid-daily"

# Create a logger object.
logger = logging.getLogger(__name__)

# By default the install() function installs a handler on the root logger,
# this means that log messages from your code and log messages from the
# libraries that you use will all show up on the terminal.
coloredlogs.install(level='DEBUG')

logger.info("Starting covid-viewer")

if not os.path.exists(authFile):
    logger.error('Please create influx auth token file : %s' % authFile)
    exit(-1)

with open(authFile) as f:
    try:
        influxAuth = json.load(f)
    except:
        logger.error('Failed to parse auth file : %s' % authFile)
        exit(-1)

logger.info('Using auth token : %s' % influxAuth['token'])

influxClient = InfluxDBClient(
    url="http://localhost:9999",
    token=influxAuth['token'],
    org=influxOrg,
    enable_gzip=True)

writeClient = influxClient.write_api(write_options=SYNCHRONOUS)
deleteClient = influxClient.delete_api()

logger.info('Wiping influx bucket %s' % influxBucket)

startDate = '%sZ' % datetime.datetime(year=2019, month=10, day=1).isoformat()
endDate = '%sZ' % datetime.datetime.utcnow().replace(microsecond=0).isoformat()

logger.info(startDate)
logger.info(endDate)

deleteClient.delete(
    start=startDate,
    stop=endDate,
    predicate='',
    bucket_id='05eef5edbf2e7001',
    org_id='05eef5edbf2e7000'
)


dataFiles = [f for f in os.listdir(dataPath
                                   ) if os.path.isfile(os.path.join(dataPath, f)) and str(f).endswith('csv')]

dataFiles.sort()

os.chdir(dataPath)

influxFields = ['Confirmed', 'Deaths', 'Recovered',
                'Active', 'Incidence_Rate', 'Case-Fatality_Ratio']

influxIgnoreTags = ['FIPS', 'Lat', 'Long_', 'Latitude',
                    'Longitude', 'Combined_Key', 'Last_Update', 'Country_Region']

for dataFile in dataFiles:
    with open(dataFile, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')

        # Patch up data fields
        reader.fieldnames = [fn.replace('/', '_') for fn in reader.fieldnames]
        reader.fieldnames = [fn.replace(' ', '_') for fn in reader.fieldnames]

        # logger.info('%s : %s' % (dataFile, reader.fieldnames))
        points = []

        for row in reader:
            if row['Country_Region'] == 'US':
                # Generate points for this row
                for fn in reader.fieldnames:
                    if fn in influxFields:
                        pointTags = {}
                        for tfn in reader.fieldnames:
                            if tfn not in influxFields and tfn not in influxIgnoreTags:
                                pointTags[tfn] = row[tfn]

                        value = row[fn]
                        lastUpdate = row['Last_Update']

                        try:
                            timestamp = datetime.datetime.fromisoformat(
                                lastUpdate)
                        except:
                            try:
                                timestamp = datetime.datetime.strptime(
                                    lastUpdate, '%m/%d/%Y %H:%M')
                            except:
                                timestamp = datetime.datetime.strptime(
                                    lastUpdate, '%m/%d/%y %H:%M')

                        point = {
                            'measurement': 'daily',
                            'tags': pointTags,
                            'fields': {fn: float(value) if len(value) else 0.0},
                            'time': timestamp
                        }

                        points.append(point)

        logger.info('%s : Writing %d points to influx' %
                    (dataFile, len(points)))
        writeClient.write(influxBucket, influxOrg, points)
