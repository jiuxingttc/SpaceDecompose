import json
import time
import psycopg2
import mysql.connector

# connect mysql
class DBConnector:
    def __init__(self, host, port, user, password, database,kconfig=True):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.knobs_config = kconfig

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port = self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            print("Connected to MySQL database!")
        except mysql.connector.Error as error:
            print("Failed to connect to MySQL database:", error)

    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Disconnected from MySQL database.")
    
    def fetch_results(self, sql, json=True):
        results = False
        cursor = self.connection.cursor()
        if self.connection:
            cursor.execute(sql)
            results = cursor.fetchall()
            if json:
                columns = [col[0] for col in cursor.description]
                return [dict(zip(columns, row)) for row in results]
        return results

    def execute(self, sql):
        results = False
        cursor = self.connection.cursor()
        if self.connection:
            cursor.execute(sql)

    def set_knob_value(self,knob,value):
        sql = 'SHOW GLOBAL VARIABLES LIKE "{}";'.format(knob)
        r = self.fetch_results(sql)
        print("r的结果：",r)
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

        if value0 == value:
            return True

        if str(value).isdigit():
            sql = "SET GLOBAL {}={}".format(knob, value)
        else:
            sql = "SET GLOBAL {}='{}'".format(knob, value)
        try:
            self.execute(sql)
        except:
            print("Failed: execute {}".format(sql))

        while not self._check_apply(knob, value0):
            time.sleep(1)
        return True


    def _check_apply(self,k, v0):
        sql = 'SHOW GLOBAL VARIABLES LIKE "{}";'.format(k)
        r = self.fetch_results(sql)
        if r[0]['Value'] == 'ON':
            vv = 1
        elif r[0]['Value'] == 'OFF':
            vv = 0
        else:
            vv = r[0]['Value'].strip()
        if vv == v0:
            return False
        return True
    
    def get_knobs(self,knobs_config,num=-1):
        if num == -1:
            f = open(knobs_config)
            KNOB_DETAILS = json.load(f)
            KNOBS = list(KNOB_DETAILS.keys())
            f.close()
        else:
            f = open(knobs_config)
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
    
    def apply_knobs(self,knobs):
        KNOBS = self.get_knobs(self.knobs_config)
        for key in KNOBS:
            value = KNOBS[key][knobs]
            self.set_knob_value(key, value)
            print("key的值：",key,"value的值：",value)


if __name__ == '__main__':
    conn = DBConnector(host='127.0.0.1',port=3306,user='root',password='123456',database='sbtest')
    conn.connect()

    query = "select count(1) from sbtest1"
    result = conn.execute(query)
    if result:
        for row in result:
            print(row)
    conn.disconnect()