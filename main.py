import psycopg2
import os;
# from get_Data import conn

commad_list = [
    'hdfs dfs -rm -r /NewsAnalytics',
    'hdfs dfs -mkdir /NewsAnalytics',
    'hdfs dfs -put /datas/noticias.csv /NewsAnalytics',
    'hdfs dfs -rm -r /output',
    'hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.1.jar wordcount /NewsAnalytics /output',
    'rm /output/*',
    'hdfs dfs -get /output/part-r-00000 /output',
    'sort -k2,2nr /output/part-r-00000 > /output/sorted.txt',
]

# for ele in commad_list:
#     os.system(f'docker exec namenode bash -c "{ele}"')


file1 = open('output/sorted.txt', 'r')

text = file1.readline().split().pop(0)

print(text)

# conexion = getdata.conn

# key_word = '{text}'
sql = f'''
SELECT 
    id, 
    title,
    description,
    (LENGTH(LOWER(title)) - LENGTH(REPLACE(LOWER(title), '{text}', ''))) / LENGTH('{text}') +
    (LENGTH(LOWER(description)) - LENGTH(REPLACE(LOWER(description), '{text}', ''))) / LENGTH('{text}')
    AS cochabamba_count
FROM articles
WHERE
    LOWER(title) LIKE '%{text}%' OR LOWER(description) LIKE '%{text}%'
ORDER BY
    cochabamba_count DESC;
'''

conn = psycopg2.connect(
    dbname="noticias",
    user="postgres",
    password="34353435",
    host="localhost"
)

cursor = conn.cursor()

title = 'title'
description= 'description'

cursor.execute(sql)

rows = cursor.fetchall()

print(rows)

# cursor.close()
# conn.close()

