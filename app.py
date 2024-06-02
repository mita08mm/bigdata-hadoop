from flask import Flask, render_template, request
# import sqlite3  # O utiliza otro motor de base de datos

app = Flask(__name__)

@app.route('/')
def index():
    # return '<h1>Hole</h1>'
    return render_template('index.html')  # Asegúrate de tener tu archivo HTML en la carpeta templates

# Ruta para manejar la búsqueda de noticias
@app.route('/buscar_noticias', methods=['POST'])
def buscar_noticias():
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')

    print(start_date)
    print(end_date)

    # Conecta con tu base de datos (ejemplo con SQLite)
    # conn = sqlite3.connect('noticias.db')
    # cursor = conn.cursor()

    # Ejecuta la consulta para obtener las noticias en el intervalo de fechas
    # cursor.execute("SELECT * FROM noticias WHERE fecha BETWEEN ? AND ?", (start_date, end_date))
    # noticias = cursor.fetchall()

    # conn.close()

    # return render_template('resultado.html', noticias=noticias)
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
