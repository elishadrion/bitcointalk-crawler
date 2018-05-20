# coding=utf8
import cfscrape
from bs4 import BeautifulSoup
import requests
import dataset
import re
import time
from datetime import datetime
from threading import Thread


YEAR = "2018"

date = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September":9,
    "October": 10,
    "November": 11,
    "December": 12
}

class Crawler(Thread):
    def __init__(self, start_url, total_pages, month, token=False):
        Thread.__init__(self)
        self.start_url = start_url
        self.total_pages = total_pages
        self.db = dataset.connect("sqlite:///database.db")
        self.table = self.db["bitcointalk"]
        self.txt_file = month+".txt"
        self.scraper = cfscrape.create_scraper()
        if token:
            self.db = dataset.connect("sqlite:///database_tokens.db")
            self.table = self.db["bitcointalk_tokens"]
            self.txt_file = month+"_tokens.txt"

    def parse_main_page(self, board_number):
        link = self.scraper.get(self.start_url[:-2]+".{}".format(board_number))
        soup = BeautifulSoup(link.content, "html.parser")
        #lien précédent visité
        for span in soup.findChildren("span"):
            for a in soup.findChildren("a"):
                try:
                    #il faut qu"il y ait "topic=" car c"est un thread et pas un autre type de lien
                    #et les threads terminent par ".0"
                    if a["href"].find("topic=") != -1 and a["href"][-2] == ".":
                        #on vérifie que c"est un nouveau lien et qu"il n"est pas présent dans la base de donnée
                        if not self.table.find_one(thread_id=a["href"][40:]):
                            self.parse_thread(a["href"])
                except:
                    pass

    def parse_thread(self, url):
        try:
            thread_id = url[40:]
            link = self.scraper.get(url)
            soup = BeautifulSoup(link.content, "html.parser")
            title = soup.findChild("td",  {"id": "top_subject"})
            print (title.contents[0].split())
            print(url)
            print("------------------")
            with open(self.txt_file, "a", encoding="utf8") as text_file:
                txt = " ".join(title.contents[0].split())
                txt = txt[7:]
                text_file.write(txt+ "\n" + url+"\n ----- \n")
            #insertion dans la bdd
            self.table.insert(dict(thread_id=thread_id))
        except:
            pass


    def run(self):
        with open(self.txt_file, "a", encoding="utf8") as text_file:
            txt = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            text_file.write("\n#####\n" + txt +"\n#####\n")

        for i in range(0, self.total_pages, 40):
            print (i)
            self.parse_main_page(i)

def main():
    # Création des threads
    thread_1 = Crawler("https://bitcointalk.org/index.php?board=159.0", 26360, "may")
    thread_2 = Crawler("https://bitcointalk.org/index.php?board=240.0", 4200, "may", token=True)


    # Lancement des threads
    thread_1.start()
    time.sleep(5)
    thread_2.start()

if __name__ == "__main__":
    main()
