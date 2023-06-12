import csv
import psycopg2

conn = psycopg2.connect(
    host = 'localhost',
    database = 'north',
    user = 'postgres',
    password = '12345'
)
cur = conn.cursor()


with open('north_data/customers_data.csv', 'r', newline='') as csvfile:
    spamreader = csv.reader(csvfile)
    for row in spamreader:
        if row[0] == 'contact_name':
            continue
        cur.execute('INSERT INTO customers VALUES(%s, %s, %s)', (row[0], row[1], row[2]))
        conn.commit()

with open('north_data/employees_data.csv', 'r', newline='') as csvfile:
    spamreader = csv.reader(csvfile)
    for row in spamreader:
        if row[0] == 'employee_id':
            continue
        cur.execute('INSERT INTO employees VALUES(%s, %s, %s, %s, %s, %s)', (row[0], row[1], row[2], row[3], row[4], row[5]))
        conn.commit()

with open('north_data/orders_data.csv', 'r', newline='') as csvfile:
    spamreader = csv.reader(csvfile)
    for row in spamreader:
        if row[0] == 'order_id':
            continue
        cur.execute('INSERT INTO orders VALUES(%s, %s, %s, %s, %s)', (row[0], row[1], row[2], row[3], row[4]))
        conn.commit()

cur.close()
conn.close()