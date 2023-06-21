from decouple import config
project_base_dir = config("AG_GROUPER_HOME", default="/usr/local/ag_grouper")
