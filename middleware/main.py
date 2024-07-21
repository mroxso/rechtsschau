# basic flask app
from flask import Flask, request, jsonify, render_template
import psycopg2
from psycopg2 import sql
import os


# PostgreSQL Configuration
db_config = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'your_database'),
    'user': os.getenv('DB_USER', 'your_username'),
    'password': os.getenv('DB_PASS', 'your_password')
}

conn = psycopg2.connect(**db_config)
cur = conn.cursor()

app = Flask(__name__)

@app.route('/')
def hello_world():
    get_query = sql.SQL('''SELECT * FROM rss_feed;''')
    cur.execute(get_query)
    results = cur.fetchall()
    return render_template("index.html", data=results)

@app.route('/api', methods=['GET'])
def getapi():
    get_query = sql.SQL('''SELECT * FROM rss_feed;''')
    cur.execute(get_query)
    results = cur.fetchall()
    print(results)
    return jsonify(results)
    # return results

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)