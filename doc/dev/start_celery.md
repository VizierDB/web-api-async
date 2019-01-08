
export VIZIERWORKER_CONFIG=/home/heiko/projects/vizier/vizier-api/resources/config/config.dev-worker.yaml

export VIZIERCELERYBROKER_URL=amqp://vizierapp:vizierapp@localhost/vizierapp

celery -A vizier.engine.backend.remote.celery.worker worker -l info -c 1 -n default@%h
celery -A vizier.engine.backend.remote.celery.worker worker -l info -Q pycell -c 1 -n pycell@%h
