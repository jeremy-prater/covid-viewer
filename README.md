# covid-viewer

A tool to convert CSV files provided by JHU CSSE with covid-19 daily numbers into a time-series influx database

## How to use

### Influx setup

[https://portal.influxdata.com/downloads/](https://portal.influxdata.com/downloads/)

- Read the influx documentation to install/setup/login
- Create a bucket named `covid-viewer`
- Create a token that has write access to that bucket

### Configure credientials

- Edit the influxAuth.json file.
- Copy the token from influx into the `token` key
- Find the bucket id and organization id and copy them into bucket_id and org_id

### Setup python3

There are 2 ways to get the correct python3 dependancies

Choose one of the following

#### virtual env (preferred method)

- Make sure python 3 module virtual env is installed
- Run gen_venv.sh

##### Manually install packages (system wide)


Run the folowing commands to install the dependancies system wide
```
pip3 install coloredlogs
pip3 install influxdb-client
```

### Load data into influx

Just run 

```
python3 ./generateSet.py
```

It takes about 5 minutes. The covid-viewer bucket will be populated with data.