import statistics
import sys
import os
import time
import pymysql
import configparser
import json
import subprocess
import re
  
def get_sysbench_config(default=True, file=""):
    if default:
        sysbench_default_config = {
            'range_size': 100,
            'events': 0,
            'tables': 150,
            'table_size': 80000,
            'threads': 32,
            'report_interval': 10,
            'run_time': 300,
            'warmup_time': 10,
            'script': "oltp_read_write",
        }
        return sysbench_default_config
    else:
        sysbench_config = {}
        with open(file, 'r') as f:
            sysbench_config = json.load(f)
        return sysbench_config

def parse_args(config_file):
    # 读取配置文件
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

def parse_sysbench_output(file_path):
    # 打开文件并读取所有内容
    with open(file_path) as f:
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

class MySQL(object):
    def __init__(self, args_db, knobs_file, sysbench_file):
        self.args_db = args_db
        self.knobs_file = knobs_file
        self.sysbench_file = sysbench_file

        # MySQL configuration
        self.host = args_db['host']
        self.port = int(args_db['port'])
        self.user = args_db['user']
        self.passwd = args_db['passwd']
        self.dbname = args_db['dbname']

        self.connection_info = {'host': self.host,
                                'port': self.port,
                                'user': self.user,
                                'passwd': self.passwd}

    def connect_db(self):
        try:
            self.cnn = pymysql.connect(**self.connection_info)
            self.cursor = self.cnn.cursor()
        except Exception as e:
            print("Error connecting to MySQL:", str(e))
            sys.exit(1)

    def close_db(self):
        self.cursor.close()
        self.cnn.close()

    def execute(self, sql):
        self.cursor.execute(sql)

    def fetch_results(self,sql,json=True):
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        if json:
            columns = [col[0] for col in self.cursor.description]
            return [dict(zip(columns, row)) for row in results]
        return results

    def set_knob_value(self, knob, value):
        sql = 'SHOW GLOBAL VARIABLES LIKE "{}";'.format(knob)
        r = self.fetch_results(sql)

        if len(r) == 0: # knob not found
            if str(value).isdigit():
                sql = "SET GLOBAL {}={}".format(knob, value)
            else:
                sql = "SET GLOBAL {}='{}'".format(knob, value)
            try:
                self.execute(sql)
            except:
                print("ERROR: {} is not allowed to be modified.".format(knob))
                sys.exit(1)
            return True
        
        # type convert
        if value == 'ON':
            value = 1
        elif value == 'OFF':
            value = 0
        if r[0]['Value'] == 'ON':
            value0 = 1
        elif r[0]['Value'] == 'OFF':
            value0 = 0
        else:
            try:
                value0 = eval(r[0]['Value'])
            except:
                value0 = r[0]['Value'].strip()

        if value0 == value: # no need to change
            return True


        if str(value).isdigit():
            sql = "SET GLOBAL {}={}".format(knob, value)
        else:
            sql = "SET GLOBAL {}='{}'".format(knob, value)

        try:
            self.execute(sql)
        except pymysql.Error as e:            
            _, error_msg = e.args
            print("ERROR: {}".format(error_msg))
            sys.exit(1)

        # while not self._check_apply(knob, value0):
        #     time.sleep(1)
        return True

    def get_knobs(self, knobs_file, num=-1):
        if num == -1:
            f = open(knobs_file)
            KNOB_DETAILS = json.load(f)
            KNOBS = list(KNOB_DETAILS.keys())
            f.close()
        else:
            f = open(knobs_file)
            knob_tmp = json.load(f)
            i = 0
            KNOB_DETAILS = {}
            while i < num:
                key = list(knob_tmp.keys())[i]
                KNOB_DETAILS[key] = knob_tmp[key]
                i = i + 1
            KNOBS = list(KNOB_DETAILS.keys())
            f.close()
        return KNOB_DETAILS

    def apply_knobs(self, apply_option):
        KNOB_DETAILS = self.get_knobs(self.knobs_file)
        #print("apply_knobs:")
        for knob in KNOB_DETAILS:
            value = KNOB_DETAILS[knob][apply_option]
            self.set_knob_value(knob, value)
            #print(knob,":", value)

    def sysbench_run_cmd(self, default=True, file=""):
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
            f"--mysql-host={self.host}",
            f"--mysql-port={self.port}",
            f"--mysql-user={self.user}",
            f"--mysql-password={self.passwd}",
            f"--mysql-db={self.dbname}",
            f"--range-size={sysbench_config['range_size']}",
            f"--events={sysbench_config['events']}",
            f"--table-size={sysbench_config['table_size']}",
            f"--tables={sysbench_config['tables']}",
            f"--threads={sysbench_config['threads']}",
            f"--report-interval={sysbench_config['report_interval']}",
            f"--time={sysbench_config['run_time']}",
            f"--warmup-time={sysbench_config['warmup_time']}",
            "--db-driver=mysql",
            "--db-ps-mode=disable",
            "--rand-type=uniform",
            "--mysql-storage-engine=innodb",
            f"{sysbench_config['script']}",
            f"run > {output_file}"
        ]  
        return cmd, output_file
    
    def sysbench_prepare_cmd(self, default=True, file=""):
        if default:
            sysbench_config = get_sysbench_config(default)
        else:
            if not os.path.exists(file):
                print("sysbench config file not exists")
                sys.exit(1)
            sysbench_config = get_sysbench_config(file)

        cmd = [
            "sysbench",
            f"--mysql-host={self.host}",
            f"--mysql-port={self.port}",
            f"--mysql-user={self.user}",
            f"--mysql-password={self.passwd}",
            f"--mysql-db={self.dbname}",
            f"--events={sysbench_config['events']}",
            f"--table-size={sysbench_config['table_size']}",
            f"--tables={sysbench_config['tables']}",
            f"--threads={sysbench_config['threads']}",
            "--db-driver=mysql",
            f"{sysbench_config['script']}",
            "prepare > sysbench_prepare.out"
        ]
        return cmd

    def sysbench_init(self, default=True):
        self.execute("drop database if exists {};".format(self.dbname))
        self.execute("create database {};".format(self.dbname))

        # prepare
        cmd = self.sysbench_prepare_cmd(default, file=self.sysbench_file)
        cmd = " ".join(cmd)
        print(cmd)

        process = subprocess.Popen(cmd, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, close_fds=True)
        process.wait()

    def sysbench_run(self, default=True):
        cmd, output_file = self.sysbench_run_cmd(default, file=self.sysbench_file)
        cmd = " ".join(cmd)
        print(cmd)

        process = subprocess.Popen(cmd, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, close_fds=True)
        process.wait()

        return output_file

    def get_internal_metrics(self):
        pass

    def get_external_metrics(self, filename):
        for _ in range(60):
            if os.path.exists(filename):
                break
            time.sleep(1)
        if not os.path.exists(filename):
            print("benchmark result file does not exist!")
            sys.exit(1)
        
        result = parse_sysbench_output(filename)
        return result

    def _check_apply(self, knob, value):
        sql = 'SHOW GLOBAL VARIABLES LIKE "{}";'.format(knob)
        r = self.fetch_results(sql)
        if r[0]['Value'] == value:
            return True
        return False


if __name__== "__main__":
    # Usage: python3 apply_knob.py your_config.ini
    
    config_file = sys.argv[1]
    if not os.path.exists(config_file):
        print("config not exists")
        sys.exit(1)

    try:
        config = parse_args(config_file)
        #print_config(config)
    except Exception as e:
        print("Error reading config file:", str(e))
        sys.exit(1)

    # 创建对象
    db = MySQL(config['database'], config['tune']['knobs_config_file'], config['tune']['sysbench_config_file'])
    db.connect_db()

    # 应用config
    db.apply_knobs(apply_option="default")

    # 初始化和数据准备
    db.sysbench_init(default=True)
    
    # 压测
    output_file = db.sysbench_run(default=True)

    # 获取性能指标
    external_metrics = db.get_external_metrics(output_file)
    print("external_metrics:", external_metrics)

    db.close_db()


    

