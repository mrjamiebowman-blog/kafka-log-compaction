docker-compose -f ./docker/docker-compose.yml --env-file ./.env up -d

Start-Sleep -Seconds 10
docker ps -f "label=mrjamiebowman=kafka-log-compaction"