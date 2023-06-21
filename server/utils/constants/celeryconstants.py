from decouple import config
celery_broker_url = config('CELERY_BROKER_URL', default='amqp://guest:guest@localhost:5672//')
snapshots_count = int(config('SNAP_SHOT_COUNT', default=100))
