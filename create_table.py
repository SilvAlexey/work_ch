from clickhouse_driver import Client


def create():
    client = Client('localhost')
    client.execute('CREATE TABLE test (dt DateTime, st String, fl Float32, ui UInt32) ENGINE = Memory')