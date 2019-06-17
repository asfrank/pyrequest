import pymysql
from os.path import abspath, dirname
import configparser

# ======== Reading db_config.ini setting ===========
base_dir = dirname(dirname(abspath(__file__))).replace('\\', '/')
file_path = base_dir + '/db_config.ini'

cf = configparser.ConfigParser()
cf.read(file_path)
host = cf.get('mysqlconf', 'host')
port = cf.get("mysqlconf", "port")
db   = cf.get("mysqlconf", "db_name")
user = cf.get("mysqlconf", "user")
password = cf.get("mysqlconf", "password")


# ======== MySql base operating ===================
class DB:
    def __init__(self):
        # 连接数据库
        try:
            self.connection = pymysql.connect(
                host=host,
                port=int(port),
                user=user,
                password=password,
                db=db,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
        except pymysql.err.OperationalError as e:
            print("Mysql Error %d: %s" % (e.args[0], e.args[1]))

    # clear table data
    def clear(self, table_name):
        sql = "delete from "+ table_name + ";"
        with self.connection.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
            cursor.execute(sql)
        self.connection.commit()

    # insert sql statement
    def insert(self, table_name, table_data):
        for key in table_data:
            table_data[key] = "'" + str(table_data[key]) +"'"
        keys = ','.join(table_data.keys())
        values = ','.join(table_data.values())
        sql = "insert into "+ table_name + " (" + keys + ") values (" + values + ");"
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
        self.connection.commit()

    # close database
    def close(self):
        self.connection.close()

    # 数据初始化
    def init_data(self, datas):
        for table, data in datas.items():
            self.clear(table)
            for d in data:
                self.insert(table, d)
        self.close()


if __name__ == '__main__':

    db = DB()
    table_name = "sign_event"
    data = {'id':1,'name':'红米','`limit`':2000,'status':1,'address':'北京会展中心','start_time':'2016-08-20 00:25:42'}
    db.clear(table_name)
    db.insert(table_name, data)
    db.close()




