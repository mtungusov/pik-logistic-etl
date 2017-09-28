#!/usr/bin/env bash

lein uberjar && java -jar target/pik-logistic-etl.jar
