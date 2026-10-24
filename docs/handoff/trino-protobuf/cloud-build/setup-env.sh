#!/bin/bash
# Set up JDK 25, Maven 3.9.16, Docker and the Confluent 8.1.1 jars for building trino-kafka.
set -u
S=/tmp/claude-0/-home-user/884eeaf1-2a86-5052-bee7-0bc50ae48900/scratchpad
log() { echo "[$(date +%H:%M:%S)] $*"; }

retry() { local n=0; until "$@"; do n=$((n+1)); [ $n -ge 6 ] && return 1; sleep $((2**n)); done; }

# Docker daemon
if ! docker info >/dev/null 2>&1; then
  nohup dockerd > /tmp/claude-0/dockerd.log 2>&1 &
  for i in $(seq 1 30); do docker info >/dev/null 2>&1 && break; sleep 2; done
fi
docker info >/dev/null 2>&1 && log "docker up" || { log "docker FAILED"; }

# Maven 3.9.16
if [ ! -x /home/user/maven-3.9.16/bin/mvn ]; then
  cd /home/user
  retry curl -fsSL -o maven.zip https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.9.16/apache-maven-3.9.16-bin.zip \
    && unzip -q maven.zip && mv apache-maven-3.9.16 maven-3.9.16 && rm maven.zip && log "maven ok" || log "maven FAILED"
fi

# JDK 25 from the Temurin image
if [ ! -x /home/user/jdk25/bin/java ]; then
  retry docker pull eclipse-temurin:25-jdk && docker rm -f jdk25src >/dev/null 2>&1
  docker create --name jdk25src eclipse-temurin:25-jdk >/dev/null && docker cp jdk25src:/opt/java/openjdk /home/user/jdk25 && docker rm jdk25src >/dev/null
  [ -x /home/user/jdk25/bin/java ] && log "jdk25 ok" || log "jdk25 FAILED"
fi

# Confluent jars from the schema registry image
if [ ! -d "$S/cfjars" ]; then
  retry docker pull confluentinc/cp-schema-registry:8.1.1 && docker rm -f cfsrc >/dev/null 2>&1
  docker create --name cfsrc confluentinc/cp-schema-registry:8.1.1 >/dev/null && docker cp cfsrc:/usr/share/java "$S/cfjars" && docker rm cfsrc >/dev/null
  [ -d "$S/cfjars" ] && log "confluent jars ok: $(find $S/cfjars -name '*.jar' | wc -l)" || log "confluent jars FAILED"
fi
# Kafka images for the Testcontainers suite
retry docker pull confluentinc/cp-kafka:8.1.1 >/dev/null && log "cp-kafka ok" || log "cp-kafka FAILED"
log "done"
