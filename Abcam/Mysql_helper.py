# Author: HuangWei Time:2019/10/13
from pymysql import *


class MYSQL(object):
    def __init__(self, database):
        self.database = database
        self.conn = connect(host='localhost', port=3306, user='root', password='root', database=self.database)
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def insert_into_table(self, sql):
        self.cursor.execute(sql)
        self.conn.commit()

    def show_all(self, sql):
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result
        # for item in self.cursor.fetchall():
        #     print(item)


if __name__ == '__main__':
    mysql = MYSQL('db1')
   # mysql.insert_into_table('insert into jingdong(name) values ("xiaobangzhui");')
    r = mysql.show_all('select name from jingdong;')
    for i in range(len(r)):
        print(r[i][-1])