import psycopg2
import os;
# from get_Data import conn
def execute_hadoop_commands():
    commad_list = [
        'hdfs dfs -rm -R /NewsAnalytics',
        'hdfs dfs -mkdir /NewsAnalytics',
        'hdfs dfs -put /datas/noticias.csv /NewsAnalytics',
        'hdfs dfs -rm -r /output',
        'hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.1.jar wordcount /NewsAnalytics /output',
        'rm /output/*',
        'hdfs dfs -get /output/part-r-00000 /output',
        'sort -k2,2nr /output/part-r-00000 > /output/sorted.txt',
    ]
    for ele in commad_list:
        os.system(f'docker exec namenode bash -c "{ele}"')

# conexion = getdata.conn

# key_word = '{text}'
# def fetch_articles(start_date = '2024-04-01', end_date = '2024-04-01'):
def fetch_articles(start_date, end_date, sources):
    file1 = open('output/sorted.txt', 'r')
    text = file1.readline().split().pop(0)

    # Aquí debes asegurarte de que text es seguro para usar en SQL

    sql = f'''
    SELECT 
        id, 
        title,
        description,
        image_url,
        content_date,
        revista,
        (
            (LENGTH(LOWER(title)) - LENGTH(REPLACE(LOWER(title), '{text}', ''))) / LENGTH('{text}') +
            (LENGTH(LOWER(description)) - LENGTH(REPLACE(LOWER(description), '{text}', ''))) / LENGTH('{text}')
        ) AS text_count
    FROM articles
    WHERE
        (LOWER(title) LIKE '%{text}%' OR LOWER(description) LIKE '%{text}%') AND
        content_date >= '{start_date}' AND content_date <= '{end_date}'
    '''

    if sources and 'Todos' not in sources:
        placeholders = ', '.join(f"'{source}'" for source in sources)
        sql += f" AND revista IN ({placeholders})"

    sql += f' ORDER BY text_count DESC, content_date DESC;'

    conn = psycopg2.connect(
        dbname="noticias",
        user="postgres",
        password="34353435",
        host="localhost"
    )

    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    return rows


# print(rows)

# cursor.close()
# conn.close()
# execute_hadoop_commands()

