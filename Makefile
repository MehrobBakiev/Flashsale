.PHONY: up down logs test test-integration lint loadtest tidy

up:            ## поднять весь стек (postgres, redis, kafka, api, worker, prometheus)
	docker compose up -d --build

down:          ## остановить и удалить данные
	docker compose down -v

logs:
	docker compose logs -f api worker

test:          ## unit-тесты с детектором гонок
	go test -race -count=1 ./...

test-integration: ## интеграционные тесты с Postgres из docker compose
	TEST_POSTGRES_DSN="postgres://flashsale:flashsale@localhost:5432/flashsale_test?sslmode=disable" \
		go test -race -count=1 -tags integration ./internal/storage/postgres/

lint:
	golangci-lint run ./...

loadtest:      ## 10 000 покупателей, 20 000 запросов на 100 единиц товара
	go run ./cmd/loadgen -stock 100 -users 10000 -attempts 2 -concurrency 200

tidy:
	go mod tidy
