### Author: Greg Miller
### Class: Modern Software Concepts in Python
### File purpose: Call the function to initialize my website instance (flask), then run the website on 0.0.0.0

from app.__init__ import create_app #import function to init app from other file

app = create_app() #call function to create app

if __name__ == '__main__': #host app on 0.0.0.0 port 8080
    app.run(host='0.0.0.0', port = 8080, debug=True)