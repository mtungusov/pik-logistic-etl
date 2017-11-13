#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JAR="$(basename $DIR)".jar

lein uberjar && java -jar "target/$JAR"
