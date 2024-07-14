#!/bin/sh

GOOGLE_APPLICATION_CREDENTIALS=~/.ssh/logkey.json redo quarantine/gcs.db
redo reports/summary.txt

