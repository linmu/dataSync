#!/bin/bash

PROGRAM=$(basename $0)
VERSION="1.0"
CURDATE=$(date "+%Y%m%d")

cd $(dirname $0)
BIN_DIR=$(pwd)
DEPLOY_DIR=${BIN_DIR%/*}

CONF_DIR=$(cd $DEPLOY_DIR/conf && pwd)
DATA_DIR=$(cd $DEPLOY_DIR/data && pwd)
LOG_DIR=$(cd $DEPLOY_DIR/logs && pwd)

#clean DATA_DIR & LOG_DIR
rm -rf $DATA_DIR/*
rm -rf $LOG_DIR/*

if [[ $? -ne 0 ]]
then
    echo "clear $DATA_DIR & $LOG_DIR failed, exit..."
    exit 1;
else
    echo "clear $DATA_DIR & $LOG_DIR success!"
fi

#begin sync data
cd $BIN_DIR && perl syncDB.pl

echo "sync DB done!"
