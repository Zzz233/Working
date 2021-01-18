from pymysql import *
import pymysql
 
 
class MYSQL(object):
    def __init__(self, database):
        self.database = database
        # self.conn = connect(host='192.168.124.10', port=3306, user='root', password='app1234', database=self.database)
        self.conn = connect(host='192.168.124.2', port=3306, user='root', password='app1234', database=self.database)
        # self.conn = connect(host='localhost', port=3306, user='root', password='app1234', database=self.database)
        # self.cursor = self.conn.cursor()
        self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)
 
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
    mysql = MYSQL('ceshi')
   # mysql.insert_into_table('insert into jingdong(name) values ("xiaobangzhui");')
    r = mysql.show_all('SELECT antiname, Catalog_Number FROM abcam_antibody_detail_no_geneid;')
    for i in range(len(r)):
        print(r[i][-1])