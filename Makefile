publish-image:
	rm -rf target
	mvn package
	cp target/heron-starter-*-jar-with-dependencies.jar ./docker/
	docker build docker/ -t streamlio/sandbox:latest
	docker push streamlio/sandbox:latest