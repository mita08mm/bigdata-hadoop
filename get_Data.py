import psycopg2
import csv
import re

conn = psycopg2.connect(
    dbname="noticias",
    user="postgres",
    password="34353435",
    host="localhost"
)

# output_file = "C:\\Users\\Hp\\Documents\\projects\\docker-hadoop\\datas\\noticias.csv"
output_file = ".\\datas\\noticias.csv"

conectores = [
    "el", "la", "los", "las", "un", "una", "unos", "unas", "a", "ante", "bajo", "cabe", "con", 
    "contra", "de", "desde", "durante", "en", "entre", "hacia", "hasta", "mediante", "para", 
    "por", "según", "sin", "sobre", "tras", "versus", "vía", "y", "e", "ni", "que", "o", "u", 
    "pero", "sino", "aunque", "mas", "como", "cuando", "donde", "mientras", "porque", "pues", 
    "ya que", "puesto que", "para que", "a fin de que", "a pesar de que", "si", "con tal que", 
    "siempre que", "a menos que", "además", "incluso", "también", "tampoco", "por consiguiente", 
    "por lo tanto", "así que", "entonces", "luego", "después", "mientras tanto", "entretanto", 
    "primero", "segundo", "finalmente", "en primer lugar", "en segundo lugar", "en resumen", 
    "en conclusión", "en consecuencia", "que", "quien", "quienes", "cual", "cuales", "cuyo", 
    "cuya", "cuyos", "cuyas", "donde", "así", "ya que", "a pesar de", "sin embargo", "no obstante", 
    "por otro lado", "por otra parte", "de hecho", "en cambio", "por ejemplo", "en efecto", "al", "se", "del",
    "su", "La", "no", "Los", "El", "dos", "es", "esta","lo","sus","ya", "yo", "este", "alta", "como", "Esto"
]

def remove_connectores(conector):
    for con in conectores:
        conector = re.sub(f'\\b{con}\\b', '', conector)

    return conector

def write_news_file(start_date = '2024-04-01', end_date = '2024-04-01'):
    cursor = conn.cursor()

    query = f"SELECT title, description FROM articles WHERE content_date >= '{start_date}' AND content_date <= '{end_date}'"

    title = 'title'
    description= 'description'

    cursor.execute(query, (title, description))

    rows = cursor.fetchall()

    with open(output_file, 'w', encoding='utf-8') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow(['Title', 'Description'])

        for row in rows:
            title = row[0].replace(',', ' ')
            description = row[1].replace(',', ' ')

            title = re.sub('[^a-zA-Z-áéíóúüñ ]', '', title.lower())
            description = re.sub('[^a-zA-Z-áéíóúüñ ]', '', description.lower())

            title =  remove_connectores(title)
            description =  remove_connectores(description)

            csv_writer.writerow((title, description))


