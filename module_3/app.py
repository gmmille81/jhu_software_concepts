### Author: Greg Miller
### Class: Modern Software Concepts in Python
### File purpose: Initialize Flask and run the website

from flask import Flask, render_template, redirect, url_for
from query_data import connect, questions
import subprocess
import sys

def create_app():
    app = Flask(__name__)
    return app

app = create_app()

# Global state
db_process = None          # subprocess for DB update
status_message = None      # tuple: (text, type) for system messages
user_message = None        # tuple: (text, type) for user-triggered messages

def db_is_running():
    global db_process
    return db_process is not None and db_process.poll() is None

def check_db_completion():
    """Update system status_message based on DB subprocess."""
    global db_process, status_message

    if db_process is None:
        return

    retcode = db_process.poll()
    if retcode is None:
        status_message = ("Database update in progress...", "in-progress")
    else:
        status_message = ("Last requested database update complete.", "success")
        db_process = None

@app.route('/')
def index():
    global status_message, user_message
    check_db_completion()

    conn = connect()
    cur = conn.cursor()
    cur.execute('SELECT * FROM answers_table')
    query_answers = cur.fetchall()
    cur.close()
    conn.close()

    # Choose message to display: user takes priority
    msg_to_display = user_message or status_message

    # Unpack safely
    if isinstance(msg_to_display, tuple) and len(msg_to_display) == 2:
        message_text, message_type = msg_to_display
    else:
        message_text, message_type = None, None

    # Clear user message after showing once
    user_message = None

    return render_template(
        'index.html',
        applicant_data=query_answers,
        message=message_text,
        message_type=message_type,
        db_running=db_is_running()
    )
@app.route("/update-db", methods=["POST"])
def update_db_route():
    global db_process, status_message, user_message
    check_db_completion()

    if db_is_running():
        user_message = ("Database update is already running.", "error")
        return redirect(url_for("index"))

    db_process = subprocess.Popen(
        [sys.executable, "update_db.py"],
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        close_fds=True,
        start_new_session=True
    )
    status_message = ("Database update started.", "in-progress")
    return redirect(url_for("index"))

@app.route("/update_analysis", methods=["POST"])
def update_analysis_route():
    global status_message, user_message
    check_db_completion()

    if db_is_running():
        user_message = ("Cannot run analysis while database update is running.", "error")
        return redirect(url_for("index"))

    conn = connect()
    questions(conn)
    status_message = ("Analysis complete.", "success")
    return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True, use_reloader=False)
