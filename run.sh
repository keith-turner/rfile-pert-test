#!/bin/bash

if (( $# < 2 )); then
  echo "Usage $0 <accumulo version> <test> [test arguments]"
  exit 1
fi

ACCUMULO_VER=$1
TEST=$2

case $TEST in
  rst)
    CLASS="rfpt.RandomSeekTest"
    ;;
  *)
    CLASS=$2
    ;;
esac

mvn -q clean compile exec:java -Daccumulo.version=$ACCUMULO_VER -Dexec.mainClass=$CLASS -Dexec.args="${*:3}"

