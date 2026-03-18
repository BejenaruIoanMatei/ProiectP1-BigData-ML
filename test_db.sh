#!/bin/bash
# test_db.sh
# chmod +x test_db.sh
export $(grep -v '^#' .env | xargs)
echo "Connecting to $POSTGRES_DB as $POSTGRES_USER..."
docker-compose exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT 'Connection Successful!' as Status;"