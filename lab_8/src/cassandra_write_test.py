class CassandraClient:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None

    def connect(self):
        from cassandra.cluster import Cluster
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)

    def execute(self, query):
        self.session.execute(query)

    def close(self):
        self.session.shutdown()

    def insert_course_record(self, table, name, year, conducted):
        query = "INSERT INTO %s (name, year, conducted) VALUES ('%s', %d, %r)" % (table, name, year, conducted)
        self.execute(query)


if __name__ == '__main__':
    host = 'localhost'
    port = 9042
    keyspace = 'hw8_kukhar'
    table = 'my_courses'

    records_to_insert = [
        ('Big Data Processing', 2022, False),
        ('Soft Skills', 2022, True),
        ('Systems Design', 2021, True)
        ]

    client = CassandraClient(host, port, keyspace)
    client.connect()
    for record in records_to_insert:
        client.insert_course_record(table, record[0], record[1], record[2])
    client.close()
