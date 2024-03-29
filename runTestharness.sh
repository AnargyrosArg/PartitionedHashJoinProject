#!/bin/bash
CALLER_DIR=$(pwd)

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

#Execute 'small' test
WORKLOAD_DIR=${1-$DIR/workloads/small}
WORKLOAD_DIR=$(echo $WORKLOAD_DIR | sed 's:/*$::')

cd $WORKLOAD_DIR

WORKLOAD=$(basename "$PWD")
echo execute $WORKLOAD ...
$DIR/harness *.init *.work *.result $CALLER_DIR/out

#Execute 'public' test
WORKLOAD_DIR=${1-$DIR/workloads/public}
WORKLOAD_DIR=$(echo $WORKLOAD_DIR | sed 's:/*$::')

cd $WORKLOAD_DIR

WORKLOAD=$(basename "$PWD")
echo execute $WORKLOAD ...
$DIR/harness *.init *.work *.result $CALLER_DIR/out

