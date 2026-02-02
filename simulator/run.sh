#!/bin/bash

if [ "$#" -lt 1 ]; then
    echo "Provide at least one config file to run"
    exit 1
fi
mkdir -p logs
rm -rf logs/*
java -Xmx300000m -cp lib/djep-1.0.0.jar:lib/jep-2.3.0.jar:target/service-discovery-1.0-SNAPSHOT.jar:lib/gs-core-2.0.jar:lib/pherd-1.0.jar:lib/mbox2-1.0.jar:lib/gs-ui-swing-2.0.jar:lib//gson-2.10.1.jar -ea peersim.Simulator "$@"
