import pytest
from flask import Flask,url_for
from src.app import create_app
from bs4 import BeautifulSoup

@pytest.mark.web
def test_create_app(app):
    print(app.blueprints['main'])
    assert app is not None
    assert app.blueprints['main'] is not None
    assert type(app) == type(Flask('__name__'))
@pytest.mark.web

def test_page_load(client, seeded_answers_table):
    response = client.get("/analysis")
    assert response.status_code == 200
    html = response.data.decode("utf-8")
    
    # Check that the page contains the word "Analysis"
    assert "Analysis" in html and "Answer:" in html
    
    soup = BeautifulSoup(response.data, "html.parser")

    buttons = [button.text.strip() for button in soup.find_all("button")]

    assert "Pull Data" in buttons
    assert "Update Analysis" in buttons
    assert soup.select_one('button[data-testid="pull-data-btn"]') is not None
    assert soup.select_one('button[data-testid="update-analysis-btn"]') is not None
# @pytest.mark.parametrize('page_name,expected_result',[
#     ('/',200),
#     ('/pull-data',200),
#     ('/update_analysis',200)
# ])

# def test_page_load(page_name,expected_result,client):
#     response = client.get(page_name)
#     assert response.status_code == expected_result


#test_create_app()
