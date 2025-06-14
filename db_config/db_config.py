import pymysql

class Database:
    def __init__(self, host, user, password, database, port=3306):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port

    def get_connection(self):
        try:
            return pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
        except pymysql.MySQLError as e:
            print(f"❌ Error getting new DB connection: {e}")
            return None

    def close(self):
        # not needed anymore unless you manage global connections
        pass
