import MySQLdb

# Pass these data structures as params to the DB class
dbconn_prod = {
    'user': 'ahwolf',
    'passwd': 'internet',
    'db': 'nbascrape'
}


dbconn_test = {
    'user': 'ahwolf',
    'passwd': 'internet',
    'db': 'nbascrape_test'
}

class Db:

    def __init__(self, credentials):
        self.credentials = credentials
        print self.credentials
        self.conn = MySQLdb.connect(**credentials)
        

    def query(self, sql):
        curs = self.curs()
        curs.execute(sql)
        
        return curs.fetchall()


    def curs(self):
        return self.conn.cursor()


    def query_dict(self, sql):
        curs = self.conn.cursor(MySQLdb.cursors.DictCursor)
        curs.execute(sql)
        
        return curs.fetchall()


    def insert_or_update(self, table_name, data):
        for line in data:
            headers = [key for key,val in sorted(line.items())]
            quoted_values = ['"%s"' % (val) for key,val in sorted(line.items())]
            duplicate_key_clauses = ['%s="%s"' % (key,val) for key,val in sorted(line.items())]

            self.query("""
                INSERT INTO %s
                (%s)
                VALUES (%s)
                ON DUPLICATE KEY UPDATE
                %s
            """ % (table_name, ','.join(headers), ','.join(quoted_values),','.join(duplicate_key_clauses)))

