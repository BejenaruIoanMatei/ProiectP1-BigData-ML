#!/bin/bash
# test_app.sh
# chmod +x test_app.sh

echo "Testing python scripts..."
docker compose exec app python scripts/insert_records.py
