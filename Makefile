.PHONY: up down run generate clean

up:
	docker compose up -d

down:
	docker compose down

generate:
	go run scripts/generate_data.go

run:
	go run main.go

local-run: generate
	DRY_RUN=true go run main.go

clean:
	rm -rf data/stable/* data/test/*
	rm orchestrator
