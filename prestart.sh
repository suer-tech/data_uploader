echo "Starting"

psql -U user -d db -f create_table.sql

exec "$@"