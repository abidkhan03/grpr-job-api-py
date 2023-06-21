from decouple import config
node_server_url = config('NODE_SERVER_URL', default='http://localhost:3020')
log_channel = config('NODE_LOG_CHANNEL', default='JOB_LOGS_BY_DJANGO')
progress_channel = config('NODE_PROGRESS_CHANNEL', default='JOB_PROGRESS_FROM_DJANGO')
status_channel = config('NODE_STATUS_CHANNEL', default='DJANGO_JOB_STATUS')
input_size_channel = config('INPUT_SIZE_CHANNEL', default='INPUT_SIZE_FROM_DJANGO')
