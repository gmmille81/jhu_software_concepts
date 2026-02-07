### Author: Greg Miller
### Class: Modern Software Concepts in Python
### File purpose: Call the function to initialize my website instance (flask), then run the website on 0.0.0.0

from flask import Flask, render_template, redirect, url_for  # import the Flask package
import psycopg
from query_data import connect, questions
import subprocess
import sys


def create_app():  # function to initialize the app
    app = Flask(__name__)  # inititialize app as a flask instance
    return app

app = create_app()  # call function to create app

db_process = None       # holds subprocess.Popen object
status_message = None   # message to show in the UI


def db_is_running():
    """Return True if the DB update subprocess is still running."""
    global db_process
    return db_process is not None and db_process.poll() is None

def check_db_completion():
    """
    Updates status_message based on the DB subprocess state.
    """
    global db_process, status_message

    if db_process is None:
        return

    retcode = db_process.poll()
    if retcode is None:
        status_message = "Database update in progress..."
    else:
        # Make sure process is fully terminated
        #db_process.wait()
        status_message = "Last requested database update complete."
        db_process = None

@app.route('/')
def index():
    global status_message
    check_db_completion()

    conn = connect()
    cur = conn.cursor()
    cur.execute('SELECT * FROM answers_table')
    query_answers = cur.fetchall()
    cur.close()
    conn.close()

    msg = status_message
    return render_template(
        'index.html',
        applicant_data=query_answers,
        message=msg,
        db_running=db_is_running(),
    )



@app.route("/update-db", methods=["POST"])
def update_db_route():
    global db_process, status_message
    check_db_completion()  # <-- important!

    if db_is_running():
        status_message = "Database update is already running."
        return redirect(url_for("index"))

    db_process = subprocess.Popen(
    [sys.executable, "update_db.py"],
    stderr=subprocess.STDOUT,
    stdin=subprocess.DEVNULL,
    close_fds=True,
    start_new_session=True
    )
    status_message = "Database update started."
    return redirect(url_for("index"))


@app.route("/update_analysis", methods=["POST"])
def update_analysis_route():
    global status_message
    check_db_completion()  # <-- important!

    if db_is_running():
        status_message = "Cannot run analysis while database update is running."
        return redirect(url_for("index"))

    conn = connect()
    questions(conn)
    status_message = "Analysis updated."
    return redirect(url_for("index"))



def run_update_analysis():
    print("Analysis updated!")


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True, use_reloader=False)
