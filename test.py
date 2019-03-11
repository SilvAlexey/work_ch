import csv

with open('MOCK_DATA.csv', newline='') as csvfile:
    read = csv.reader(csvfile, delimiter=',')
    n = 0
    for row in read:
        print(n, row[1])
        n += 1
