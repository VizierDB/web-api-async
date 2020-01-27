#!/bin/bash

#
# For instructions on configuring this script, see
# https://github.com/VizierDB/web-api-async/blob/master/doc/configuration.md
#

export VIZIERSERVER_ENGINE="MIMIR"
export VIZIERSERVER_NAME="Vizier"
export VIZIERSERVER_BASE_URL="http://localhost"
export VIZIERSERVER_SERVER_PORT="5000"
export VIZIERSERVER_SERVER_LOCAL_PORT="5000"
export VIZIERSERVER_MAX_UPLOAD_SIZE="16777216"
# export VIZIERSERVER_DEBUG="True"
export VIZIERENGINE_DATA_DIR="./vizier"
export VIZIERSERVER_PACKAGE_PATH="./resources/packages/common:./resources/packages/mimir"
export VIZIERSERVER_PROCESSOR_PATH="./resources/processors/common:./resources/processors/mimir"
export VIZIERSERVER_APP_PATH="/vizier-db/api/v1"

# The following line assumes web-ui is in the same directory as web-api-async
export WEB_UI_STATIC_FILES=`pwd`"/../web-ui/build/"

export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
if command -v pyenv 1>/dev/null 2>&1; then
  eval "$(pyenv init -)"
fi


cd $(dirname $0)
FLASK_APP=vizier.wsgi:app flask run
