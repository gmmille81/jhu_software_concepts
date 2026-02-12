# src/pages.py
from flask import Blueprint, render_template, request
import subprocess
import sys
from query_data import connect, questions

# Blueprint definition
bp = Blueprint("main", __name__)

# -------------------- Global state --------------------
db_process = None
status_message = None
user_message = None

# -------------------- Helper functions --------------------
def db_is_running():
    global db_process
    return db_process is not None and db_process.poll() is None

def check_db_completion():
    global db_process, status_message
    if db_process is None:
        return
    retcode = db_process.poll()
    if retcode is None:
        status_message = "Database update in progress..."
    else:
        status_message = "Last requested database update complete."
        db_process = None


def _render_analysis_page():
    """
    Render the analysis page content shared by multiple routes.

    Centralizing this logic lets button routes return HTTP 200 directly while
    preserving the same page content and message behavior as /analysis.
    """
    global user_message

    # Keep state current before loading data for the page view.
    check_db_completion()

    # Fetch analysis results for display.
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM answers_table")
    query_answers = cur.fetchall()
    cur.close()
    conn.close()

    msg_to_display = user_message or status_message
    user_message = None

    return render_template(
        "index.html",
        applicant_data=query_answers,
        message=msg_to_display,
        db_running=db_is_running()
    )

# -------------------- Routes --------------------
@bp.route("/analysis", endpoint="index")
def index():
    """
    Main page route
    Displays analysis results and status/messages
    """
    # Route remains unchanged functionally; it now uses the shared renderer.
    return _render_analysis_page()

# -------------------- Update DB route --------------------
@bp.route("/update-db", methods=["POST"])
def update_db_route():
    """
    Triggered by 'Pull Data' button
    POST: start the DB update if not running
    Returns: 409 if DB update is already running, else analysis page (200)
    """
    global db_process, status_message, user_message

    check_db_completion()

    if db_is_running():
        # Return the analysis page (not plain text) so users keep the same UI
        # and auto-refresh behavior while still signaling conflict via 409.
        user_message = "Database update already running."
        return _render_analysis_page(), 409

    if request.method == "POST":
        # Start background DB update
        db_process = subprocess.Popen(
            [sys.executable, "update_db.py"],
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True
        )
        status_message = "Database update started."
    
    # Return the analysis page directly so this POST route responds with HTTP 200
    # while preserving the original page output and status message behavior.
    return _render_analysis_page(), 200

# -------------------- Update Analysis route --------------------
@bp.route("/update_analysis", methods=["GET", "POST"])
def update_analysis_route():
    """
    Triggered by 'Update Analysis' button
    POST: run analysis if DB update not running
    GET/POST: 409 if DB update is running, else analysis page (200)
    """
    global status_message, user_message

    check_db_completion()

    if db_is_running():
        user_message = "Cannot run analysis while database update is running."
        # Return the analysis page so the user stays on the normal UI while
        # still signaling conflict with HTTP 409.
        return _render_analysis_page(), 409

    if request.method == "POST":
        # Run analysis queries
        conn = connect()
        questions(conn)
        status_message = "Analysis complete."

    # Return the analysis page directly so this POST route responds with HTTP 200
    # while preserving the original page output and status message behavior.
    return _render_analysis_page(), 200
