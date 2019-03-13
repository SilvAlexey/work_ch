import logging

from datetime import datetime

import os
import csv
from clickhouse_driver import Client


def data_parsing(config):
    cn = list()
    for row in config:
        if row[0] == '#': continue
        cn.append(row.rstrip())
    filelog = cn[1] + '/' + cn[2]
    logging.basicConfig(filename=filelog, level=logging.INFO)

    data = insert_db(cn[3], cn[4])

    path = cn[0]+'/CH-DATA.csv'
    if os.path.isfile(path):
        parser = csv_read(path)

        n = 0

        for row in parser:
            if data[n][0] == datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S'):
                print(data)
                data.pop(n)
                continue
            n = n + 1

        parser.extend(data)
        csv_writer(parser, path)

    else:
        logging.info("Create file %s ..." % path)
        with open(r''+path, 'a', newline='') as csv_file:
            writer = csv.writer(
                csv_file,
                delimiter=',',
                quotechar=',',
                quoting=csv.QUOTE_MINIMAL
            )
            for line in data:
                writer.writerow(line)
        logging.info("Done!")


def csv_writer(data, path):
    logging.info("Rewrite file %s ..." % path)
    with open(r'' + path, 'a', newline='') as csv_file:
        writer = csv.writer(
            csv_file,
            delimiter=',',
            quotechar=',',
            quoting=csv.QUOTE_MINIMAL
        )
        for line in data:
            writer.writerow(line)

    logging.info("Done!")


def csv_read(path):
    logging.info("Reade file...")
    result = list()
    with open(path, newline='') as csvfile:
        read = csv.reader(csvfile, delimiter=',')
        for row in read:
            result.append(row)
    return result


def insert_db(host, table):
    logging.info("Reade data to a table %s..." % table)

    client = Client(host)
    td = list()
    result = []
    sql = 'SELECT * FROM %s' % table

    td.append(client.execute(sql))
    if not td[0]:
        logging.error("No data!")
        exit()
    for row in td:
        for string in row:
            result.append(list(string))

    logging.info("Done!")
    return result


data_parsing(open(r'/home/alexey/Рабочий стол/alexey/PycharmProjects/work_ch/config-parser.txt'))