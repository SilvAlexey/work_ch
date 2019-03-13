import logging
import numpy
from datetime import datetime
import glob
import os
import csv
from clickhouse_driver import Client


def data_loading(config):
    cn = list()
    backup = False
    for row in config:
        if row[0] == '#': continue
        cn.append(row.rstrip())

    filelog = cn[2] + '/' + cn[3]
    logging.basicConfig(filename=filelog, level=logging.INFO)

    if cn[1]:
        backup = True
    else:
        logging.warning("The backup directory is not specified or does not exist. The backup function is disabled!")

    for filename in glob.glob(os.path.join(cn[0], '*.csv')):
        logging.info("Read file %s" % filename)
        data = insert_db(filename, cn[4], cn[5], backup)

        if backup:
            path = cn[1] + "/" + filename[filename.rfind('/') + 1 : ]  #Именовать файлы по времени: str(datetime.now().timetz())
            logging.info("Writing data in %s" % path)
            csv_writer(data, path)
            os.remove(filename)
        else:
            try:
                os.remove(filename)
            except OSError:
                pass


def csv_writer(data, path):
    with open(r''+path, "w", newline="") as csv_file:
        writer = csv.writer(csv_file, delimiter=',',
                                quotechar=',', quoting=csv.QUOTE_MINIMAL)
        for line in data:
            writer.writerow(line)
    logging.info("Done!")


def insert_db(filename, host, table, backup=False):
    logging.info("Writing data to a table %s..." % table)
    client = Client(host)
    td = [['DataTime', 'String', 'Float32', 'UInt32']]

    sql = 'INSERT INTO %s (dt, st, fl, ui) FORMAT VALUES ' % table
    with open(filename, newline='') as csvfile:
        read = csv.reader(csvfile, delimiter=',')
        next(read)
        for row in read:
            if backup:
                td.append(row)
            client.execute(
                sql,
                [{
                    'dt': datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S'),
                    'st':row[1],
                    'fl':numpy.float32(row[2]),
                    'ui':int(row[3])
                }]
            )
    logging.info("Done!")
    return td


data_loading(open(r'/home/alexey/Рабочий стол/alexey/PycharmProjects/work_ch/config.txt'))


