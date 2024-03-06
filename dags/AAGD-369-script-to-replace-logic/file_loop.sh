#/bin/bash

PATHFILE=`find /home/gvishesh/gcp-data-project/dags/sql/dml/integration/*/demographic/ -name '*.sql'`

for file in $PATHFILE
do 
#REPLACE COLUMN NAME in file as per sed_table_name.script
echo "$file"

sed -f sed_table_name.script $file>file_temp.sql

cat file_temp.sql>$file
done
