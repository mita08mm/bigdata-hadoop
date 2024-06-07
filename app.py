from flask import Flask, render_template, request
from get_Data import write_news_file
from main import execute_hadoop_commands
from main import fetch_articles

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')  # Asegúrate de tener tu archivo HTML en la carpeta templates

@app.route('/buscar_noticias', methods=['POST'])
def buscar_noticias():
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')

    write_news_file(start_date, end_date)
    execute_hadoop_commands()

    articles = fetch_articles(start_date, end_date)

    # main: list de notitial

    print(articles)

    # Conecta con tu base de datos (ejemplo con SQLite)
    # conn = sqlite3.connect('noticias.db')
    # cursor = conn.cursor()

    # Ejecuta la consulta para obtener las noticias en el intervalo de fechas
    # cursor.execute("SELECT * FROM noticias WHERE fecha BETWEEN ? AND ?", (start_date, end_date))
    # noticias = cursor.fetchall()

    # conn.close()

    # return render_template('resultado.html', noticias=noticias)
    return render_template('index.html', data= articles)

if __name__ == '__main__':
    app.run(debug=True)
