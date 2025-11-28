import json
import re
import psycopg2 as sql
from pprint import pprint

with open('task1_d.json', mode='r', encoding='UTF-8') as read_file:
    raw = read_file.read()

cleaned = re.sub(r'=>', ': ', raw)
cleaned = re.sub(r':(\w+)', r'"\1"', cleaned)
data = json.loads(cleaned)
pprint(data)

db = sql.connect(
    database = 'postgres',
    host = 'localhost',
    user = 'postgres',
    password = 'bekzod8077'
)

cursor = db.cursor()

cursor.execute("""CREATE TABLE IF NOT EXISTS task_1(
                id TEXT PRIMARY KEY,
                title VARCHAR(200),
                author VARCHAR(200),
                genre VARCHAR(100),
                publisher VARCHAR(200),
                year INT,
                price VARCHAR(20))

""")

insertion = '''INSERT INTO task_1(id, title, author, genre, publisher, year, price)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING'''

values = []
for info in data:
    value = (
        str(info['id']),
        info['title'],
        info['author'],
        info['genre'],
        info['publisher'],
        info['year'],
        info['price']
    )
    values.append(value)

for v in values:
    cursor.execute(insertion, v)

db.commit()
cursor.close()
db.close()