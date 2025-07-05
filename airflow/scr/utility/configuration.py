import configparser
import os

# config filepath for local
conf_filepath = "C:\\Users\\Public\\Documents\\Stream-data-pipelines\\python_code\\streamDataPipline\\resources\\config.ini"


# config file path for docker or VMs
# conf_filepath = "/tmp/airflow_kafka/config_docker.ini"

def load_config(filepath=conf_filepath) :
    conf = configparser.ConfigParser()
    conf.read(filepath)
    return conf


config = load_config()
# database_config = config["database"]
# api_config = config["api"]

data_dir = config["data"]["dir"]
# if not os.path.exists(data_dir):
#     os.makedirs(data_dir)

kafka_config = config["kafka"]

# # You can also make individual values directly accessible:
# database_host = config["database"]["host"]
