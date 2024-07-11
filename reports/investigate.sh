#!/bin/sh

exec sqlite3 -header -column -init reports/joins.sql quarantine/gcs.db

