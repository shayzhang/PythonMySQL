"""
Test connection to MySQL
using mysql-client
"""
import MySQLdb
import datetime

class MySQLHandler():
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.cur_record = ()
        self.host = ''
        self.port = 0
        self.user = ''
        self.passwd = ''
        self.db = ''
        self.charset = 'utf8'

    def connect(self, host, port, user, passwd, db, charset):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.charset = charset

        try:
            self.conn = MySQLdb.connect(
                host = self.host,
                port = self.port,
                user = self.user,
                passwd = self.passwd,
                db = self.db,
                charset = self.charset
            )
            self.cursor = self.conn.cursor()
        except MySQLdb.Error as e:
            print("Conn MySQL Error, %d: %s" %(e.args[0], e.args[1]) )

    def execute_one(self, sql, args):
        self.cursor.execute(sql, args)

    def insert_one(self,sql,args):
        try:
            # 如果多条插入语句都没有异常，则直接通过commit()将数据写入db
            # 如果有任意一条语句产生异常，则进入except处理（还没有运行try中的commit)
            # except中可以全部不提交，或者提交没有触发异常的sql语句
            # 全部不提交, 则except中写入：self.conn.commit()
            # 部分提交（没有触发异常的语句结果），则except中写入：self.conn.commit()

            self.cursor.execute(sql,args)
            # write changes to db, if no exception
            self.conn.commit()
        except :
            # drop data, if any exception happens, no data saved to db
            self.conn.rollback()

    def delete_one(self,sql,args):
        try:
            # 如果多条delete语句都没有异常，则直接通过commit()将数据写入db
            # 如果有任意一条语句产生异常，则进入except处理（还没有运行try中的commit)
            # except中可以全部不提交，或者提交没有触发异常的sql语句
            # 全部不提交, 则except中写入：self.conn.commit()
            # 部分提交（没有触发异常的语句结果），则except中写入：self.conn.commit()

            self.cursor.execute(sql,args)
            # write changes to db, if no exception
            self.conn.commit()
        except :
            # drop data, if any exception happens, no data saved to db
            self.conn.rollback()

    def show(self):
        # show row count
        print("Row Count: " + str(self.cursor.rowcount))

        # get table column name
        columns = map(lambda e: e[0], self.cursor.description)
        columns = tuple(columns)

        # show all results
        self.cur_record = self.cursor.fetchone()
        while self.cur_record != None:
            print( dict(zip(columns, self.cur_record)) )
            self.cur_record = self.cursor.fetchone()

    def close(self):
        # close connection to mysql
        self.cursor.close()
        self.conn.close()

        # reset attributes
        self.conn = None
        self.cursor = None
        self.cur_record = ()
        self.host = ''
        self.port = 0
        self.user = ''
        self.passwd = ''
        self.db = ''
        self.charset = 'utf8'


# create connection
mysql_handler = MySQLHandler()
mysql_handler.connect('localhost', 3306, 'root', 'weilan0415', 'news', 'utf8')

# prepare sql to execute
# sql = 'select * from employees where salary > %s limit %s, %s'
# args = (2500, 0, 10)

# run 1 sql operation
# mysql_handler.execute_one(sql, args)

# insert one record into mysql
sql = 'insert into news (title, image, content, types, create_at, is_valid) ' \
      'value (%s, %s, %s, %s, %s, %s);'
cur_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
mysql_handler.insert_one(sql, ("new222", "D:\\Durian.jpg", "新闻内容222", "美食", cur_time, 1) )

# delete
sql = 'delete from news where id between 6 and 10'
mysql_handler.delete_one(sql, ())

# query
sql = 'select * from news'
mysql_handler.execute_one(sql, ())

# show result
mysql_handler.show()

# close connection
mysql_handler.close()











