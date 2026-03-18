#!/bin/bash
# test_db.sh
# chmod +x test_db.sh
export $(grep -v '^#' .env | xargs)
echo "Connecting to $POSTGRES_DB as $POSTGRES_USER..."
docker-compose exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT 'Connection Successful!' as Status;"

echo "Connecting to mongodb as $MONGO_USER..."
docker-compose exec mongodb mongosh -u $MONGO_INITDB_ROOT_USERNAME -p $MONGO_INITDB_ROOT_PASSWORD --eval "db.adminCommand('ping')"