#!/bin/bash
# Full verification of the changed modules: checks, Error Prone, unit and Docker tests.
S=/tmp/claude-0/-home-user/884eeaf1-2a86-5052-bee7-0bc50ae48900/scratchpad
cd /home/user/trino-kafka || exit 1
MODULES=lib/trino-record-decoder,plugin/trino-kafka
log() { echo "[$(date +%H:%M:%S)] $*" >> $S/verify-summary.log; }
: > $S/verify-summary.log

$S/mvn.sh install -DskipTests -P ci -pl $MODULES > $S/verify-ci.log 2>&1
log "install -P ci exit $?"

$S/mvn.sh clean test-compile -Dair.check.skip-all=true -P errorprone-compiler -pl $MODULES > $S/verify-errorprone.log 2>&1
log "errorprone exit $?"

$S/mvn.sh test -Dair.check.skip-all=true -pl lib/trino-record-decoder > $S/verify-test-decoder.log 2>&1
log "record-decoder tests exit $?"

$S/mvn.sh test -Dair.check.skip-all=true -pl plugin/trino-kafka \
  -Dtest='io/trino/plugin/kafka/protobuf/*,io/trino/plugin/kafka/schema/confluent/*,io/trino/plugin/kafka/encoder/protobuf/*,TestKafkaPlugin' \
  -Dsurefire.failIfNoSpecifiedTests=false > $S/verify-test-kafka.log 2>&1
log "kafka protobuf/confluent tests exit $?"
log "done"
