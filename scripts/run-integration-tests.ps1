$ErrorActionPreference = "Stop"
mvn -q -DskipTests package
mvn -q test-compile
java -ea -cp "target/classes;target/test-classes" com.aegisraft.integration.AegisRaftIntegrationSuite
