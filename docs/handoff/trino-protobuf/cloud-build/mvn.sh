#!/bin/bash
# Maven with JDK 25, the Confluent file repository and the Google mirror of Maven Central.
export JAVA_HOME=/home/user/jdk25
export PATH=$JAVA_HOME/bin:$PATH
export MAVEN_OPTS="-Xmx3g"
exec /home/user/maven-3.9.16/bin/mvn -B -ntp \
  -gs /tmp/claude-0/-home-user/884eeaf1-2a86-5052-bee7-0bc50ae48900/settings.xml \
  -P ci-google-maven-central \
  "$@"
