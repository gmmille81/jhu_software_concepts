### Author: Greg Miller
### Class: Modern Software Concepts in Python
### File purpose: Initializes the app as a Flask instance and links it to the blueprint

from flask import Flask #import the Flask package

from app import views #import views.py from the app folder. Note: VSCode compiler flags this as issue, but this line works when running the website via the run.py script

def create_app(): #function to initialize the app
    app = Flask(__name__) #inititialize app as a flask instance
    app.register_blueprint(views.bp) #register the blueprint from views.py to the flask instance
    return app
