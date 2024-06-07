from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import csv
import psycopg2
import re
import json

def generate_urls(start_date, end_date, section='All'):
    start_date = datetime.strptime(start_date, "%m/%d/%Y")
    end_date = datetime.strptime(end_date, "%m/%d/%Y")
    delta = timedelta(days=1)
    urls = []

    current_date = start_date
    while current_date <= end_date:
        date_str_url = current_date.strftime("%m%%2F%d%%2F%Y")
        url = f"https://www.lostiempos.com/hemeroteca-fecha?fecha={date_str_url}&seccion={section}"
        urls.append(url)
        current_date += delta

    return urls

def get_page_content(url, timeout=30):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return BeautifulSoup(response.content, "html5lib")
    except requests.exceptions.Timeout:
        print(f"Request to {url} timed out.")
    except requests.exceptions.RequestException as e:
        print(f"Error during requests to {url}: {str(e)}")


# def get_page_content(url):
#     response = requests.get(url)
#     if response.status_code == 200:
#         return BeautifulSoup(response.content, "html5lib")
#     else:
#         return None

def extract_news(uri):
    news = []
    soup = get_page_content(uri)

    if soup:
        try:
            main = soup.find('main')
            if not main:
                raise AttributeError("No se encontró el elemento principal con la clase .content-column")

            region = main.find('div', class_='region')
            block_main = region.find('div', class_='block-main')
            content_article = block_main.find('div', class_='clearfix')
            article = content_article.find('article')

            title = article.find('h1', class_='node-title').get_text(strip=True)
            subsection = article.find('div', class_='subsection').get_text(strip=True)

            img_tag = article.find('img', class_='image-style-noticia-detalle')
            link = img_tag['src'] if img_tag else None

            date_publish_str = article.find('div', class_='date-publish').get_text(strip=True)
            date_publish = extract_date(date_publish_str)
            
            author_element = article.find('div', class_='autor').select_one('.field-item a')
            author = author_element.text if author_element else None

            body = article.find('div', class_='body')
            content = body.find('div', class_='field-items')
            arreglo = content.find_all('p')
            if arreglo:
                description = arreglo[0].get_text(strip=True)
            else:
                description = "Descripción no disponible."
            # for p in content.find_all('p', class_='rtejustify'):
            #     description += '\n' + ''.join(p.get_text(strip=True))
            
            print("Página escrapeada correctamente!")
            news.append({
                'title': title,
                'description': description,
                'image_url': link,
                'article_url': uri,
                'author_name': author,
                'content_date': date_publish,
                'section': subsection
            })
        except AttributeError as e:
            print(f"Error al procesar el contenido de la página: {e}")
            return news
    else:
        print("No se pudo obtener el contenido de la página.")
    
    return news

def find_uri_page(soup, classname):
    uri = soup.select_one(f'{classname} a') 
    if uri and 'href' in uri.attrs:
        return uri['href']
    else:
        return None

def scrape_news(start_date, end_date):
    urls = generate_urls(start_date, end_date)
    all_news = []
    i=0
    for current_url in urls:
        i=i+1
        print(f"Scraping {current_url}")
        soup = get_page_content(current_url)
        if not soup:
            continue
        
        view_content = soup.find('div', class_='view-content')
        articles = view_content.find_all('div', class_='views-row')
        for article in articles:
            uri_news = find_uri_page(article, '.field-content')
            if uri_news:
                news = extract_news(f'https://www.lostiempos.com{uri_news}')
                all_news.extend(news)
                
        # write_to_csv(all_news, filename+f"{i}.csv")
        # write_to_json(all_news, filename+f"{i}.json")
        save_article(all_news)
        all_news.clear()


def extract_date(date_str):
    try:
        date_match = re.search(r'\d{2}/\d{2}/\d{4}', date_str)
        if date_match:
            date_extracted = date_match.group(0)
            return datetime.strptime(date_extracted, '%d/%m/%Y').date()
        else:
            print("No se encontró una fecha en la cadena proporcionada.")
            return None
    except ValueError as e:
        print(f"Error al convertir la fecha: {e}")
        return None
    
def write_to_csv(news_data, csv_filename):
    fieldnames = ['title','description', 'image_url', 'article_url', 'content_date', 'author_name', 'section' ]

    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for news in news_data:
            writer.writerow(news)
            
def write_to_json(news_data, json_filename):
    with open(json_filename, 'a', encoding='utf-8') as jsonfile:  # Changed to 'a' to append to the file
        jsonfile.write(json.dumps(news_data, default=str, indent=4))

def save_article(news_data):
    conn = psycopg2.connect(
        dbname="noticias",
        user="postgres",
        password="34353435",
        host="localhost"
    ) 
        
    cur = conn.cursor()

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
    cur.execute(create_table_query)
    conn.commit()
        
    insert_query = '''
        INSERT INTO articles (title, description, image_url, article_url, author_name, content_date, section, revista)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        '''
    
    default_image_url = "https://www.lostiempos.com/sites/default/files/styles/noticia_detalle/public/media_imagen/2017/10/7/lt-tapa-ok.jpg?itok=zPFM4y59"

    for news in news_data:
        image_url = news['image_url'] if news['image_url'] else default_image_url
        cur.execute(insert_query, (
            news['title'],
            news['description'],
            image_url,
            news['article_url'],
            news['author_name'],
            news['content_date'],
            news['section'],
            'Los Tiempos'
        ))
    conn.commit()
    print("Conexión exitosa")
        
    cur.close()
    conn.close()

# Inicio del scraping

scrape_news("11/01/2023", "11/30/2023")
# scrape_news("12/01/2023", "12/31/2023")
#04/01/2024", "04/30/2024"
#03/01/2024", "03/31/2024"