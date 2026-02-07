from refresh_data import update_db
from module_2.scrape import scrape_data
if __name__ == "__main__":
    res = update_db()
    print('Res',res)
