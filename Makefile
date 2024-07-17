.PHONY: build
build:
	CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w" -o bin/raft-cluster

.PHONY: run
run:
	go run main.go --iface ens160 --node-id node1 --raft-port 2222 --http-port 9095

.PHONY: docker-build
docker-build:
	docker build -t raft-cluster:latest .


.PHONY: raft-cluster
raft-cluster:
	 docker run --rm -it raft-cluster:latest sh


.PHONY: compose-build
compose-build:
	 docker-compose build

.PHONY: compose-up
compose-up:
	docker-compose up -d


.PHONY: compose-down
compose-down:
	docker-compose down

.PHONY: cluster-join
cluster-join:
	curl 'localhost:8222/join?followerAddr=localhost:2223&followerID=2'

.PHONY: add-key
add-key:
	curl -X POST 'localhost:8222/set' -d '{"key": "x", "value": "23"}' -H 'content-type: application/json'

.PHONY: get-key
get-key:
	curl 'localhost:8223/get?key=x'
	
