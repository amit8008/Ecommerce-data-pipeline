import configparser


def load_config(
        filepath="C:\\Users\\Public\\Documents\\Stream-data-pipelines\\python_code\\streamDataPipline\\resources\\config.ini") :
    cnfg = configparser.ConfigParser()
    cnfg.read(filepath)
    return cnfg


config = load_config()
# database_config = config["database"]
# api_config = config["api"]
resources_path = config["resources"]["path"]

# # You can also make individual values directly accessible:
# database_host = config["database"]["host"]
