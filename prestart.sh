echo "Starting"

export PGPASSWORD=password

psql -U user -d db -f /app/create_table.sql --host=postgres

exec "$@"
