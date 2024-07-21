import os
from time import sleep
import feedparser
import psycopg2
from psycopg2 import sql
from ollama import Client
import requests
from bs4 import BeautifulSoup
import PyPDF2
from urllib.parse import urlparse, parse_qs
from lxml import html

ollama_host = os.getenv('OLLAMA_HOST', 'http://localhost:11434')

def extract_pdf_from_url(url, id):
    response = requests.get(url)
    webpage_content = response.content

    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(webpage_content, 'html.parser')

    # find the value with xpath
    tree = html.fromstring(webpage_content)
    xpath = "/html/body/div/div/main/div/section[2]/div/div/div[2]/div[1]/div[1]/div/div/h3/a/span/img[2]"
    pdf_url = tree.xpath(xpath)

    # Make a request to download the PDF
    pdf_response = requests.get(pdf_url)

    # Überprüfen, ob der Inhaltstyp eine PDF-Datei ist
    if 'application/pdf' not in pdf_response.headers.get('Content-Type', ''):
        print("Der heruntergeladene Inhalt ist keine PDF-Datei.")
        return

    # Save the PDF to a file
    pdf_filename = "./files/" + str(id) + '.pdf'
    with open(pdf_filename, 'wb') as pdf_file:
        pdf_file.write(pdf_response.content)
    print(f"PDF heruntergeladen: {pdf_filename}")

    # Extrahieren des Textes aus der PDF-Datei
    with open(pdf_filename, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        text = ''
        for page_num in range(reader.numPages):
            page = reader.getPage(page_num)
            text += page.extractText()
    return text

def fetch_rss_feed(url):
    return feedparser.parse(url)

def insert_into_db(data, db_config):
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # Create table if not exists
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS rss_feed (
            id SERIAL PRIMARY KEY,
            title TEXT,
            link TEXT,
            description TEXT,
            typ TEXT,
            sachgebiet TEXT,
            published TEXT,
            ai_summary TEXT
        )
        '''
        cur.execute(create_table_query)

        # Iterate through feed entries
        for entry in data.entries:
            # insert only when <meta:typ>Gesetz</meta:typ>
            # if 'meta' in entry and 'typ' in entry.meta and entry.meta_typ != 'Gesetz':
            # if(entry.meta_typ != 'Gesetz'):
            #     continue

            # insert only when title does not exist in database yet
            check_query = sql.SQL('''
            SELECT COUNT(*) FROM rss_feed WHERE title = %s;
             ''')
            cur.execute(check_query, (entry.title,))
            count = cur.fetchone()[0]
            if count > 0:
                continue

            # INSERT IT TO DB
            insert_query = sql.SQL('''
            INSERT INTO rss_feed (title, link, description, typ, sachgebiet, published)
            VALUES (%s, %s, %s, %s, %s, %s)
            ''')
            cur.execute(insert_query, (
                entry.title,
                entry.link,
                entry.description,
                entry.meta_typ,
                entry.meta_sachgebiet if 'meta_sachgebiet' in entry else None,
                entry.pubDate if 'pubDate' in entry else None
            ))

        # Commit changes to DB
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            conn.close()

# RSS URL
rss_url = 'https://www.recht.bund.de/rss/feeds/rss_bgbl-1-2.xml?nn=87952'

# PostgreSQL Configuration
db_config = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'your_database'),
    'user': os.getenv('DB_USER', 'your_username'),
    'password': os.getenv('DB_PASS', 'your_password')
}

while(True):
    print("Fetching RSS Feed...")
    # RSS Feed calling and storing in DB
    feed_data = fetch_rss_feed(rss_url)
    insert_into_db(feed_data, db_config)
    print("RSS Feed fetched and stored in DB")

    # AI Summary
    print("Creating AI summaries...")
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    get_query = sql.SQL('''SELECT * FROM rss_feed WHERE ai_summary IS NULL;''')
    cur.execute(get_query)
    results = cur.fetchall()
    for(result) in results:
        print("Working on entry with id " + str(result[0]) + " ...")

        # print("Getting PDF from entry with id " + str(result[0]) + " ...")
        # website_url = result[2]
        # pdf = extract_pdf_from_url(website_url, result[0])
        # print("PDF extracted")

        print("Creating AI summary for id " + str(result[0]) + " ...")
        # AI Summary
        client = Client(host=ollama_host)
        response = client.chat(model='llama3', messages=[
        {
            'role': 'systems',
            'content': 'Du bist eine KI die Gesetzestexte auf deutsch zusammenfassen soll. Bitte halte dich kurz und verständlich.',
        },
        {
            'role': 'user',
            'content': result[1],
        },
        ])
        ai_summary = response['message']['content']
        print(ai_summary)
        update_query = sql.SQL('''UPDATE rss_feed SET ai_summary = %s WHERE id = %s;''')
        cur.execute(update_query, (ai_summary, result[0]))
        conn.commit()
    print("AI Summary fetched and stored in DB")
    
    # Sleep for an hour
    sleep(60 * 60) # sleep for 1 hour