import time
import psycopg2
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, NoSuchElementException

# Ruta al geckodriver
drive_path = r'D:\\geckodriver.exe'

# Opciones del navegador Firefox
options = Options()
options.binary_location = r'C:\Program Files\Mozilla Firefox\firefox.exe'

# Conexión a la base de datos PostgreSQL
conn = psycopg2.connect(
    dbname="noticias",
    user="postgres",
    password="34353435",
    host="localhost"
)

# Crear un cursor
cursor = conn.cursor()

# Crear la tabla articles si no existe
create_table_query = '''
CREATE TABLE IF NOT EXISTS articles (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    image_url TEXT NOT NULL,
    article_url TEXT NOT NULL,
    author_name TEXT,
    content_date DATE,
    section TEXT NOT NULL,
    revista TEXT NOT NULL
);
'''
cursor.execute(create_table_query)
conn.commit()

# Función para convertir la fecha al formato YYYY-MM-DD
def convertir_fecha(fecha_str):
    try:
        meses = {
            'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
            'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
            'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
        }
        # Extraer la fecha
        partes = fecha_str.split()
        dia = partes[0]
        mes = meses[partes[2]]
        anio = partes[4]
        fecha_formateada = f"{anio}-{mes}-{dia}"
        return fecha_formateada
    except Exception as e:
        print(f"Error al convertir la fecha: {e}")
        return ""

# Función para configurar y obtener el navegador
def config(uri):
    service = Service(executable_path=drive_path)
    driver = webdriver.Firefox(service=service, options=options)
    driver.get(uri)
    return driver

# Función para capturar y guardar los datos en PostgreSQL
def capture_and_save_data(driver, section):
    wait = WebDriverWait(driver, 10)
    
    for page_num in range(1, 51):  # Paginación hasta la página 50
        try:
            # Esperar a que los artículos estén presentes
            wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".onm-new.content.image-top-left")))
        except TimeoutException:
            print(f"No se encontraron artículos en la página {page_num} de la sección {section}")
            break
        
        articles = driver.find_elements(By.CSS_SELECTOR, ".onm-new.content.image-top-left")
        
        for i in range(len(articles)):
            try:
                # Re-localizar los artículos en el DOM para evitar StaleElementReferenceException
                articles = driver.find_elements(By.CSS_SELECTOR, ".onm-new.content.image-top-left")
                if i >= len(articles):
                    break  # Asegurarse de que el índice no esté fuera de rango
                article = articles[i]

                try:
                    title_element = article.find_element(By.CSS_SELECTOR, ".title.title-article a")
                    title = title_element.text
                    article_url = title_element.get_attribute("href")
                except NoSuchElementException:
                    print("No se encontró el título del artículo.")
                    continue
                
                try:
                    description_element = article.find_element(By.CSS_SELECTOR, ".summary")
                    description = description_element.text
                except NoSuchElementException:
                    print("No se encontró la descripción del artículo.")
                    continue
                
                try:
                    image_element = article.find_element(By.CSS_SELECTOR, ".article-media img")
                    image_url = image_element.get_attribute("data-src")
                    if not image_url.startswith("http"):
                        image_url = "https://www.opinion.com.bo" + image_url
                except NoSuchElementException:
                    print("No se encontró la imagen del artículo.")
                    continue
                
                # Hacer clic en el título para navegar a la página del artículo
                driver.execute_script("arguments[0].click();", title_element)
                
                # Esperar a que se cargue la página del artículo
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "article.content-cochabamba, article.content-pais, article.content-mundo")))
                
                # Capturar detalles adicionales del artículo
                try:
                    author_name = driver.find_element(By.CSS_SELECTOR, ".author-name").text
                except:
                    author_name = ""
                
                try:
                    content_date = driver.find_element(By.CSS_SELECTOR, ".content-time").text
                    content_date = convertir_fecha(content_date)
                except:
                    content_date = ""
                
                print("Title:", title)
                print("Article URL:", article_url)
                print("Description:", description)
                print("Image URL:", image_url)
                print("Author Name:", author_name)
                print("Content Time:", content_date)
                print("Section:", section)
                print("-----------")
                
                # Verificar si el registro ya existe en la base de datos
                cursor.execute(
                    'SELECT * FROM articles WHERE title = %s AND description = %s AND image_url = %s AND article_url = %s',
                    (title, description, image_url, article_url)
                )
                existing_record = cursor.fetchone()
                
                if not existing_record:
                    # Insertar el nuevo registro solo si no existe en la base de datos
                    cursor.execute(
                        'INSERT INTO articles (title, description, image_url, article_url, author_name, content_date, section, revista) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
                        (title, description, image_url, article_url, author_name, content_date, section, "opinion")
                    )
                
                # Volver a la página de la sección original
                driver.back()
                
                # Esperar a que los artículos estén nuevamente presentes
                wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".onm-new.content.image-top-left")))
            except (StaleElementReferenceException, TimeoutException, NoSuchElementException) as e:
                print(f"Error al procesar el artículo: {e}")
                continue  # Si se produce la excepción, intenta nuevamente buscar los elementos
        
        # Navegar a la siguiente página
        try:
            next_button = driver.find_element(By.CSS_SELECTOR, f".pagination li a[href*='page={page_num + 1}']")
            driver.execute_script("arguments[0].click();", next_button)
            time.sleep(2)  # Esperar un momento para que la página se cargue
        except NoSuchElementException:
            break  # Salir del bucle si no hay más páginas

    conn.commit()

# Lista de secciones para extraer datos
sections = [
    ("Cochabamba", "https://www.opinion.com.bo/blog/section/cochabamba/"),
    ("País", "https://www.opinion.com.bo/blog/section/pais/"),
    ("Mundo", "https://www.opinion.com.bo/blog/section/mundo/")
]

# Ejecutar la función para capturar y guardar datos para cada sección
for section_name, uri in sections:
    print(f"Extracting data for section: {section_name}")
    driver = config(uri)
    capture_and_save_data(driver, section_name)
    driver.quit()

# Cerrar la conexión a la base de datos
cursor.close()
conn.close()