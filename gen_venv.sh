#!/bin/bash

OUTDIR="build"

rm -rf $OUTDIR
python3 -m venv $OUTDIR

source "$OUTDIR/bin/activate"

pip3 install coloredlogs
pip3 install influxdb-client
