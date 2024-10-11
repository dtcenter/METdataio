#!/usr/bin/bash


echo ""
echo ""
echo ""
echo "*****************"
echo " USAGE"
echo "*****************"
echo ""
echo " ***Run under BASH shell***"
echo " SET the following ENV variables: "
echo " DB_PWD  - the password for the mvadmin user"
echo " DBNAME  - the name of the database you wish to create/drop/load schema"
echo " MVSCHEMA - the full path and name of the mv_mysql.sql script to load the schema"
echo " Then copy and paste the appropriate command for dropping, creating, or loading the schema after running the following on the command line:"
echo ""
echo " bash db_cmd.bash "
echo ""
echo "*****************"
echo ""
echo ""
echo ""

echo ""
echo "********"
echo "Command to drop database ${DBNAME} if it exists (or error message if it doesn't exist)..."
echo "********"
echo mysql -u mvadmin -p${DB_PWD} "'drop database" ${DBNAME}"';"


echo ""
echo "********"
echo "Command to create database ${DBNAME}..."
echo "********"
echo mysql -u mvadmin -p${DB_PWD} -e "'"create database ${DBNAME}"'";

echo ""
echo "********"
echo "Command to grant privileges to ${DBNAME}..."
echo "********"
echo mysql -u mvadmin -p${DB_PWD} -e "\""GRANT INSERT, DELETE, UPDATE, INDEX, DROP ON ${DBNAME}.* to "'"mvuser"'"@"'"%"'" "\"" 
echo ""


echo ""
echo "********"
echo "Command to load mv_mysql.sql schema ${DBNAME}..."
echo "********"
echo mysql -u mvadmin -p${DB_PWD} ${DBNAME} "<" ${MVSCHEMA}
echo ""
