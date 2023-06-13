#!/bin/bash

PROJECT_ID="historical-exchange-rate"
FUNCTION_NAME="launcher-cloud-function"
RUNTIME="python310"
TRIGGER="http"
ENTRY_POINT="launcher"
SOURCE_DIRECTORY="/home/kote/PycharmProjects/currency-rates-exchange/launcher.py"

gcloud config set project $PROJECT_ID

gcloud functions deploy $FUNCTION_NAME --runtime $RUNTIME --trigger-$TRIGGER --entry-point $ENTRY_POINT --source $SOURCE_DIRECTORY

