### Author: Greg Miller
### Class: Modern Software Concepts in Python
### File purpose:
### Initialize the Flask web application, handle user actions,
### run long-running database updates via subprocess,
### and display analysis results on the website.

from flask import Flask, render_template, redirect, url_for
from query_data import connect, questions   # DB connection + analysis queries
import subprocess                           # Used to run update_db.py as a subprocess
import sys                                  # Used to reference the Python interpreter


def create_app():
    """
    Factory function to create and configure the Flask app.
    Using a factory makes the app easier to test and extend.
    """
    app = Flask(__name__)
    return app


# Create the Flask application instance
app = create_app()


# -------------------- Global State --------------------
# These variables persist across requests while the app is running

db_process = None        # Holds the subprocess running update_db.py (if any)
status_message = None   # System-generated status message (e.g., DB progress)
user_message = None     # User-triggered messages (e.g., button warnings)


def db_is_running():
    """
    Check whether the database update subprocess is still running.

    Returns:
        bool: True if update_db.py is currently running, False otherwise
    """
    global db_process
    # poll() returns None while the process is running
    return db_process is not None and db_process.poll() is None


def check_db_completion():
    """
    Check the status of the DB update subprocess and update the
    system status message accordingly.
    """
    global db_process, status_message

    # If no DB process has been started, do nothing
    if db_process is None:
        return

    # poll() checks the process without blocking
    retcode = db_process.poll()

    if retcode is None:
        # Process is still running
        status_message = "Database update in progress..."
    else:
        # Process has completed
        status_message = "Last requested database update complete."
        db_process = None  # Clear process so a new one can be started


@app.route('/')
def index():
    """
    Main page route.
    - Checks DB update status
    - Queries the database for analysis results
    - Displays messages and data in index.html
    """
    global status_message, user_message

    # Update status_message if DB subprocess finished
    check_db_completion()

    # Connect to database and fetch analysis results
    conn = connect()
    cur = conn.cursor()
    cur.execute('SELECT * FROM answers_table')
    query_answers = cur.fetchall()
    cur.close()
    conn.close()

    # Decide which message to show:
    # user_message takes priority over system status_message
    msg_to_display = user_message or status_message

    # Clear user_message after showing it once
    user_message = None

    return render_template(
        'index.html',
        applicant_data=query_answers,  # Data displayed on page
        message=msg_to_display,        # Status or user message
        db_running=db_is_running()     # Used for auto-refresh logic
    )


@app.route("/update-db", methods=["POST"])
def update_db_route():
    """
    Route triggered by the 'Pull Data' button.
    Starts update_db.py as a subprocess if not already running.
    """
    global db_process, status_message, user_message

    # Update status before deciding what to do
    check_db_completion()

    # Prevent multiple DB updates from running at the same time
    if db_is_running():
        user_message = "Database update is already running."
        return redirect(url_for("index"))

    # Start update_db.py as a background subprocess
    db_process = subprocess.Popen(
        [sys.executable, "update_db.py"],  # Use same Python interpreter
        stderr=subprocess.STDOUT,          # Combine stdout/stderr
        stdin=subprocess.DEVNULL,          # Prevent blocking on input
        close_fds=True,                    # Close file descriptors
        start_new_session=True             # Fully detach process
    )

    status_message = "Database update started."
    return redirect(url_for("index"))


@app.route("/update_analysis", methods=["POST"])
def update_analysis_route():
    """
    Route triggered by the 'Update Analysis' button.
    Runs SQL queries to refresh analysis results.
    """
    global status_message, user_message

    # Update DB status before running analysis
    check_db_completion()

    # Do not allow analysis while DB update is running
    if db_is_running():
        user_message = "Cannot run analysis while database update is running."
        return redirect(url_for("index"))

    # Run analysis queries against the database
    conn = connect()
    questions(conn)

    status_message = "Analysis complete."
    return redirect(url_for("index"))


if __name__ == "__main__":
    # Run the Flask development server
    # use_reloader=False is important so subprocesses don't run twice
    app.run(host="127.0.0.1", port=8080, debug=True, use_reloader=False)
