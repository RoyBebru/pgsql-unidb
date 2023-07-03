#!/usr/bin/sh

SCRIPT_NAME="`/usr/bin/basename $0`"
SCRIPT_DIR="`/usr/bin/dirname $0`"

sudo docker run --name local-postgresql -p 5432:5432 \
    -v "$SCRIPT_DIR/pgsql-data":/var/lib/postgresql/data \
    -e POSTGRES_PASSWORD=my-secret \
    -e "TZ=Europe/Kiev" \
    postgres
