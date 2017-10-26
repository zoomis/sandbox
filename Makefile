clean:
	rm -rf target

java-build:
	mvn package
	cp target/heron-starter-*-jar-with-dependencies.jar ./docker/

build-local-image: clean java-build
	docker build docker/ -t streamlio/sandbox-local:latest

run-local-image:
	docker kill streamlio-sandbox-local; docker rm streamlio-sandbox-local
	docker run -d --name streamlio-sandbox-local -p 9000:9000 -p 8889:8889 -p 6650:6650 -p 8080:8080 -p 8000:8000 streamlio/sandbox-local:latest

build-image: clean java-build
	docker build docker/ -t streamlio/sandbox:latest

publish-image: build-image
	docker push streamlio/sandbox:latest
