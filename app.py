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
        self.month = month.title()
        if token:
            self.db = dataset.connect("sqlite:///database_tokens.db")
            self.table = self.db["bitcointalk_tokens"]
            self.txt_file = month+"_tokens.txt"

    def parse_main_page(self, board_number):
        link = self.scraper.get(self.start_url[:-2]+".{}".format(board_number))
        soup = BeautifulSoup(link.content, "html.parser")
        for span in soup.find_all('span'):
            for result in span.find_all('a'):
                #Be sure that it's a link concerning a thread
                if len(result) and "topic" in result.attrs['href']:
                    try:
                        #Check if it's not in the database already
                        if not self.table.find_one(thread_id=result.attrs['href'][40:]):
                            self.parse_thread(result)
                    except:
                        pass

    def parse_thread(self, tag):
        try:
            link = self.scraper.get(tag.attrs['href'])
            soup = BeautifulSoup(link.content, "html.parser")
            #date_tag is None if the thread is too "old"
            month = self.month
            year = YEAR
            date_tag = None
            #We suppose the date of the thread's creation is in the first few tags from experience
            for result in soup.find_all('span')[3:10]:
                if "Today" in result.contents[0] or (str(YEAR) in result.contents[0] and self.month in result.contents[0]):
                    date_tag = result
                else:
                    #For data later, so we know which project was published which month
                    try:
                        for element in result.contents[0].split():
                            if date.get(element):
                                month = element
                                #year is always the third element
                                year = result.contents[0].split()[2]
                    except:
                        pass

            #if it's the correct year and month, we append to the files
            if date_tag:
                print (tag.contents[0])
                print (tag.attrs['href'])
                print ("------------------")
                with open(self.txt_file, "a", encoding="utf8") as text_file:
                    txt = " ".join(tag.contents[0].split())
                    text_file.write(txt+ "\n" + tag.attrs['href'] +"\n ----- \n")
            #We insert in both cases in the database
            self.table.insert(dict(thread_id=tag.attrs['href'][40:], time=time.time(), month=month, year=year))
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
    thread_1 = Crawler("https://bitcointalk.org/index.php?board=159.0", 30000, "may")
    thread_2 = Crawler("https://bitcointalk.org/index.php?board=240.0", 5000, "may", token=True)


    # Lancement des threads
    thread_1.start()
    time.sleep(5)
    thread_2.start()

if __name__ == "__main__":
    main()
