### Author: Greg Miller
### Class: Modern Software Concepts in Python
### File purpose: Initializes the app as a Flask instance and links it to the blueprint

from flask import Flask #import the Flask package


def create_app(): #function to initialize the app
    app = Flask(__name__) #inititialize app as a flask instance
    return app
