### Author: Greg Miller
### Class: Modern Software Concepts in Python
### File purpose: Call the function to initialize my website instance (flask), then run the website on 0.0.0.0

from flask import Flask, render_template #import the Flask package
import os
import psycopg
from query_data import connect, questions


def create_app(): #function to initialize the app
    app = Flask(__name__) #inititialize app as a flask instance
    
    return app

app = create_app() #call function to create app
@app.route('/')
def index():
    conn = connect()
    cur = conn.cursor()
    cur.execute('SELECT * FROM answers_table')
    query_answers = cur.fetchall()
    cur.close()
    conn.close()
    #query_answers = questions(conn)
    return render_template('index.html', applicant_data=query_answers)

# @app.route('/refresh', methods=['POST'])
# def refresh():
#     """Call the refresh_data function and then go back to the main page"""
#     refresh_data()  # this is your function that refreshes the DB or data
#     return redirect(url_for('index'))  # go back to index after refresh   
if __name__ == '__main__': #host app on 0.0.0.0 port 8080
    app.run(host='0.0.0.0', port = 8080, debug=True)