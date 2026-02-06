### Author: Greg Miller
### Class: Modern Software Concepts in Python
### File purpose: Establishes the blueprint for the site and includes the functions for the three pages: home, contact and projects

from flask import Blueprint,render_template #import blueprint package and render_template function from Flask. 


bp = Blueprint("views",__name__,template_folder='templates') #Establish the blueprint, which is referenced in the init function when app created

@bp.route("/") #function to connect default url to home view
def home(): #defines homeview and points to home.html
    return render_template("pages/home.html")

@bp.route("/contact") #function to connect url to contact view
def contact(): #defines contact view and points to contact.html
    return render_template("pages/contact.html")

@bp.route("/projects") #function to connect url to projects view
def projects(): #defines projects view and points to projects.html
    return render_template("pages/projects.html")
