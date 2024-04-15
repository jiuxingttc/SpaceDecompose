import os
import statistics
import time
from read_log import IrrelatedPathAnalyzer
from dbconnector import DBConnector
import configparser
import json
import subprocess
import re
import sys

def get_sysbench_config(default=True,file=""):
    if default:
        sysbench_default_config = {
            "report_interval": 1,
            "run_time": 300,
            "threads": 30,
            "tables": 100,
            "table_size": 800000,
            "db-ps-mode": "disable",
            "script": "oltp_read_write"
        }
        return sysbench_default_config
    else:
        sysbench_config = {}
        with open(file, 'r') as f:
            sysbench_config = json.load(f)
        return sysbench_config
    
def parse(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    config_dict = {}
     # 将配置文件中的每个 section 转换为字典
    for section in config.sections():
        section_dict = {}
        # 遍历每个 section 中的键值对
        for key in config[section]:
            section_dict[key] = config[section][key]
        # 将 section 的字典添加到主字典中
        config_dict[section] = section_dict

    return config_dict

def print_config(config):
    for section in config:
        print("[{}]".format(section))
        for key in config[section]:
            print(key, "=", config[section][key])

def parse_sysbench_output(file):
    with open(file) as f:
        lines = f.read()

    # 定义正则表达式模式，用于匹配sysbench输出中的性能指标
    temporal_pattern = re.compile(
                "tps: (\d+.\d+) qps: (\d+.\d+) \(r/w/o: (\d+.\d+)/(\d+.\d+)/(\d+.\d+)\)"
                " lat \(ms,95%\): (\d+.\d+) err/s: (\d+.\d+) reconn/s: (\d+.\d+)")
    
    # 使用正则表达式匹配性能指标
    temporal = temporal_pattern.findall(lines)
    
    # 初始化变量
    tps, latency, qps = 0, 0, 0
    tpsL, latL ,qpsL = [], [], []
    
    # 遍历匹配到的性能指标列表
    for i in temporal:
        # 累加tps、latency、qps
        tps += float(i[0])
        latency += float(i[5])
        qps += float(i[1])
        # 将每次迭代的tps、latency、qps值存储到列表中
        tpsL.append(float(i[0]))
        latL.append(float(i[5]))
        qpsL.append(float(i[1]))
    
    # 计算样本数
    num_samples = len(temporal)

    tps /= num_samples
    qps /= num_samples
    latency /= num_samples
    tps_var = statistics.variance(tpsL)
    lat_var = statistics.variance(latL)
    qps_var = statistics.variance(qpsL)

    res = {
        "tps": tps,
        "qps": qps,
        "latency": latency,
        "tps_var": tps_var,
        "lat_var": lat_var,
        "qps_var": qps_var
    }

    return res  # 返回平均值和方差

def run_sysbench_cmd(db_info,default=True,file=""):
    if default:
        sysbench_config = get_sysbench_config(default)
    else:
        if not os.path.exists(file):
            print("sysbench config file not exists")
            sys.exit(1)
        sysbench_config = get_sysbench_config(file)

    timestamp = int(time.time())
    output_file = 'sysbench_run_{}.out'.format(timestamp)

    cmd = [
        "sysbench",
        f"--mysql-host={db_info['host']}",
        f"--mysql-port={db_info['port']}",
        f"--mysql-user={db_info['user']}",
        f"--mysql-password={db_info['passwd']}",
        f"--mysql-db={db_info['dbname']}",
        f"--table-size={sysbench_config['table_size']}",
        f"--tables={sysbench_config['tables']}",
        # "--table-size=80000",
        f"--threads={sysbench_config['threads']}",
        f"--report-interval={sysbench_config['report_interval']}",
        # "--report-interval=1"
        f"--time={sysbench_config['run_time']}",
        "--db-driver=mysql",
        "--db-ps-mode=disable",
        "--rand-type=uniform",
        "--mysql-storage-engine=innodb",
        f"{sysbench_config['script']}",
        f"run > {output_file}"
    ]

    return cmd,output_file

def prepare_sysbench_cmd(db_info,default=True,file=""):
    if default:
        sysbench_config = get_sysbench_config(default)
    else:
        if not os.path.exists(file):
            print("sysbench config file not exists")
            sys.exit(1)
        sysbench_config = get_sysbench_config(file)

    cmd = [
        "sysbench",
        f"--mysql-host={db_info['host']}",
        f"--mysql-port={db_info['port']}",
        f"--mysql-user={db_info['user']}",
        f"--mysql-password={db_info['passwd']}",
        f"--mysql-db={db_info['dbname']}",
        f"--table-size={sysbench_config['table_size']}",
        f"--tables={sysbench_config['tables']}",
        # "--table-size=80000",
        f"--threads={sysbench_config['threads']}",
        "--db-driver=mysql",
        f"{sysbench_config['script']}",
        "prepare > sysbench_prepare.out"
    ]

    return cmd

def sysbench_init(db,db_info,sysbench_file,default=True):
    db.execute("drop database if exists {};".format(db_info['dbname']))
    db.execute("create database {};".format(db_info['dbname']))
    cmd = prepare_sysbench_cmd(db_info,default,file=sysbench_file)
    cmd = " ".join(cmd)
    print(cmd)
    process = subprocess.Popen(cmd, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, close_fds=True)
    process.wait()

def sysbench_run(db_info,sysbench_file):
    cmd, output_file = run_sysbench_cmd(db_info,file=sysbench_file)
    cmd = " ".join(cmd)
    print(cmd)

    process = subprocess.Popen(cmd, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, close_fds=True)
    process.wait()

    return output_file

def get_external_metrics(filename):
        for _ in range(60):
            if os.path.exists(filename):
                break
            time.sleep(1)
        if not os.path.exists(filename):
            print("benchmark result file does not exist!")
            sys.exit(1)
        
        result = parse_sysbench_output(filename)
        return result

if __name__== "__main__":
    config_file = '/root/AI4DB/irrelated_knob_test/config.ini'
    # sysbench_file = '/root/AI4DB/irrelated_knob_test/sysbench_config.json'
    if not os.path.exists(config_file):
        print("config not exists")
        sys.exit(1)

    try:
        config = parse(config_file)
        #print_config(config)
    except Exception as e:
        print("Error reading config file:", str(e))
        sys.exit(1)
    db_info = config['database']
    sysbench_file = config['tune']['sysbench_config_file']
    knob_file = config['tune']['knobs_config_file']
    db = DBConnector(db_info['host'],db_info['port'],db_info['user'],db_info['passwd'],db_info['dbname'],kconfig=knob_file)
    db.connect()
    # if db.fetch_results(f"SHOW TABLES LIKE '{db_info['dbname']}'"):
    # config1,2,3
    # db.apply_knobs(knobs='apply')
    db.apply_knobs(knobs="default")
    sysbench_init(db,db_info,sysbench_file)
    output_file = sysbench_run(db_info,sysbench_file)
    # 获取性能指标
    external_metrics = get_external_metrics(output_file)
    print("external_metrics:", external_metrics)

    db.disconnect()
