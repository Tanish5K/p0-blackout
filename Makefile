.PHONY: dev up down backend frontend logs

# Spin up RabbitMQ, backend, and frontend together.
# On this repo `make` targets run in parallel; first run pulls images.
dev: up
	@echo "RabbitMQ Management UI: http://localhost:15672"

up:
	docker compose up -d
	@echo "waiting for broker..."; until docker exec blackout-rabbitmq rabbitmq-diagnostics -q ping 2>/dev/null; do sleep 2; done
	go run ./backend

down:
	docker compose down

backend:
	go run ./backend

frontend:
	cd frontend && npm run dev

logs:
	docker compose logs -f rabbitmq
