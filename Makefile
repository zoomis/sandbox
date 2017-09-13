clean:
	rm -rf target docker/*.jar

build-image: clean
	mvn package
	cp target/heron-starter-*-jar-with-dependencies.jar ./docker/
	docker build docker/ -t streamlio/sandbox:latest

publish-image: build-image
	docker push streamlio/sandbox:latest