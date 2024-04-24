echo "Starting"
export PGPASSWORD=password
psql -U $POSTGRES_USER -d $POSTGRES_DB -f /app/create_table.sql --host=$POSTGRES_HOST
exec "$@"
