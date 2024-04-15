import configparser
import json
import subprocess
from mysqldb import DBConnector
    
def get_sysbench_info(default=True,file=""):
    if default:
        sysbench_config = {
            "report-interval": 1,
            "time": 300,
            "threads": 30,
            "tables": 30,
            "table-size": 800000,
            "db-ps-mode": "disable",
            "script": "oltp_read_write"
        }
        return sysbench_config
    else:
        sysbench_config = {}
        with open(file,'r') as f:
            sysbench_config = json.loads(f)
        return sysbench_config


def test():
    # load the config file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # get the db info
    db_type = config.get('database', 'db')
    host = config.get('database', 'host')
    port = config.get('database', 'port')
    user = config.get('database', 'user')
    passwd = config.get('database', 'passwd')
    dbname = config.get('database', 'dbname')

    # apply the sysbench
    # sysbench_config = json.loads(config.get('database', 'sysbench_config_xml'))

    # connect the mysql
    conn = DBConnector(host=host, port=port, user=user, password=passwd, database=dbname)
    conn.connect()

    # # apply the sysbench info
    # sysbench_config_file = 'sysbench_config.json'
    # with open(sysbench_config_file, 'w') as f:
    #     json.dump(sysbench_config, f)
    sysbench_config_file = get_sysbench_info('sysbench_config.json')

    # run the sysbench load
    sysbench_command = f"sysbench --config-file={sysbench_config_file} run"
    print(sysbench_command)
    subprocess.run(sysbench_command, shell=True)

    # disconnect the mysql
    conn.disconnect()

if __name__ == '__main__':
    test()