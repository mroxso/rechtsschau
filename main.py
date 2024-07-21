import os
import feedparser
import psycopg2
from psycopg2 import sql

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
            sachgebiet TEXT,
            published TEXT
        )
        '''
        cur.execute(create_table_query)

        # Iterate through feed entries
        for entry in data.entries:
            # insert only when <meta:typ>Gesetz</meta:typ>
            # if 'meta' in entry and 'typ' in entry.meta and entry.meta_typ != 'Gesetz':
            if(entry.meta_typ != 'Gesetz'):
                continue

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
            INSERT INTO rss_feed (title, link, description, sachgebiet, published)
            VALUES (%s, %s, %s, %s, %s)
            ''')
            cur.execute(insert_query, (
                entry.title,
                entry.link,
                entry.description,
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

# RSS Feed calling and storing in DB
feed_data = fetch_rss_feed(rss_url)
insert_into_db(feed_data, db_config)
