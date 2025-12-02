#!/bin/bash

${FLINK_HOME}/bin/sql-client.sh  -i /opt/sql-client/sql/ingest_ddl_s1.sql -f /opt/sql-client/sql/ingest_dml_s1.sql
