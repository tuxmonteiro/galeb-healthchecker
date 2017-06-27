deploy-snapshot:
	mvn clean install -DskipTests deploy:deploy -DaltDeploymentRepository=oss-jfrog::default::http://oss.jfrog.org/artifactory/oss-snapshot-local

galeb: clean
	mvn package -DskipTests

test:
	mvn test

clean:
	mvn clean
