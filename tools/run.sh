#!/bin/bash

export FLASK_APP=vizier.api.webservice
export FLASK_ENV=development
flask run --with-threads
