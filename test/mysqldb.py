import psycopg2
import mysql.connector

# connect mysql
class DBConnector:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

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

    def execute_query(self, query):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except mysql.connector.Error as error:
            print("Error executing query:", error)

if __name__ == '__main__':
    conn = DBConnector(host='127.0.0.1',port=3306,user='root',password='123456',database='sbtest')
    conn.connect()

    query = "select count(1) from sbtest1"
    result = conn.execute_query(query)
    if result:
        for row in result:
            print(row)
    conn.disconnect()