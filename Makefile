run-lock-server:
	LOCK_PORT=8080 \
	SERVER_TYPE=lock \
	go run cmd/api/main.go

run-api-server:
	API_PORT=9999 \
	SERVER_TYPE=api \
	DB_HOST=127.0.0.1 \
	DB_PORT=3306 \
	DB_USER=admin \
	DB_PASSWORD=qwerty \
	DB_NAME=rinha-db \
	LOCK_PORT=8080 \
	LOCK_HOST=127.0.0.1 \
	go run cmd/api/main.go