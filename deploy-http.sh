#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SOURCE_DIR="${DIR}/../src"

source "${DIR}/.env.local"

gcloud functions \
  deploy ${twitter_scrap} \
  --source=${https://github.com/saravanan1992/myownrepo/tree/main/src} \
  --runtime=python37 \
  --trigger-http \
  --allow-unauthenticated
