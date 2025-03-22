import configparser


def load_config(
        filepath="C:\\Users\\Public\\Documents\\Stream-data-pipelines\\python_code\\streamDataPipline\\resources\\config.ini") :
    conf = configparser.ConfigParser()
    conf.read(filepath)
    return conf


config = load_config()
# database_config = config["database"]
# api_config = config["api"]
data_dir = config["data"]["dir"]

# # You can also make individual values directly accessible:
# database_host = config["database"]["host"]
