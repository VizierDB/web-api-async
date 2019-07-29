#!/bin/bash

export FLASK_APP=vizier.api.webservice
export FLASK_ENV=development
python -m flask run --with-threads
