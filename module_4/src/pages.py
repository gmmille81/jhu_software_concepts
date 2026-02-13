"""Flask route handlers and rendering helpers for analysis UI."""

from flask import Blueprint, jsonify, redirect, render_template, request, url_for
import subprocess
import sys
import re
from query_data import connect, questions

# Blueprint definition
bp = Blueprint("main", __name__)

# -------------------- Global state --------------------
db_process = None
status_message = None
user_message = None

# -------------------- Helper functions --------------------
def _wants_json_response():
    """
    Return True when the client explicitly prefers JSON over HTML.
    """
    return request.accept_mimetypes.best == "application/json"


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


def _format_percentages_in_text(value):
    """
    Normalize any percentage values in a string to two decimal places.

    Examples:
    - "52.3%" -> "52.30%"
    - "10%" -> "10.00%"
    """
    if not isinstance(value, str):
        return value

    def _repl(match):
        try:
            return f"{float(match.group(1)):.2f}%"
        except ValueError:
            return match.group(0)

    return re.sub(r"([-+]?\d+(?:\.\d+)?)\s*%", _repl, value)


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

    formatted_answers = []
    for row in query_answers:
        if len(row) < 2:
            formatted_answers.append(row)
            continue
        row_as_list = list(row)
        row_as_list[1] = _format_percentages_in_text(row_as_list[1])
        formatted_answers.append(tuple(row_as_list))

    msg_to_display = user_message or status_message
    user_message = None

    return render_template(
        "index.html",
        applicant_data=formatted_answers,
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
@bp.route("/pull-data", methods=["POST"])
def update_db_route():
    """
    Triggered by 'Pull Data' button
    POST: start the DB update if not running
    Returns JSON:
    - 409 with busy=true if DB update is already running
    - 200 with ok=true when DB update is started
    """
    global db_process, status_message, user_message

    check_db_completion()

    if db_is_running():
        user_message = "Database update already running."
        if _wants_json_response():
            return jsonify({"ok": False, "busy": True, "message": user_message}), 409
        return redirect(url_for("main.index"), code=303)

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

    if _wants_json_response():
        return jsonify({"ok": True, "busy": False, "message": status_message}), 200
    return redirect(url_for("main.index"), code=303)

# -------------------- Update Analysis route --------------------
@bp.route("/update_analysis", methods=["GET", "POST"])
def update_analysis_route():
    """
    Triggered by 'Update Analysis' button
    POST: run analysis if DB update not running
    Returns JSON:
    - 409 with busy=true if DB update is running
    - 200 with ok=true when analysis is updated
    """
    global status_message, user_message

    check_db_completion()

    if db_is_running():
        user_message = "Cannot run analysis while database update is running."
        if _wants_json_response():
            return jsonify({"ok": False, "busy": True, "message": user_message}), 409
        return redirect(url_for("main.index"), code=303)

    if request.method == "POST":
        # Run analysis queries
        conn = connect()
        questions(conn)
        status_message = "Analysis complete."
        if _wants_json_response():
            return jsonify({"ok": True, "busy": False, "message": status_message}), 200
        return redirect(url_for("main.index"), code=303)

    return _render_analysis_page(), 200
