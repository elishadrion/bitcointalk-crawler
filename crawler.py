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

class Crawler(Thread):
    def __init__(self, start_url, token=False):
        Thread.__init__(self)
        self.start_url = start_url
        self.db = dataset.connect("sqlite:///database.db")
        self.table = self.db["bitcointalk"]
        #self.txt_file = month+".txt"
        self.scraper = cfscrape.create_scraper()
        if token:
            self.db = dataset.connect("sqlite:///database_tokens.db")
            self.table = self.db["bitcointalk_tokens"]
            #self.txt_file = month+"_tokens.txt"

    def parse_main_page(self, board_number):
        link = self.scraper.get(self.start_url.format(board_number))
        if link.status_code != 200:
            print ("parse_main_page response code error : ", link.status_code)
            return

        soup = BeautifulSoup(link.content, "html.parser")
        for span in soup.find_all('span'):
            for result in span.find_all('a'):
                #Be sure that it's a link concerning a thread
                if len(result) and "topic" in result.attrs['href']:
                    #Check if it's not in the database already
                    if not self.table.find_one(thread_id=result.attrs['href'][40:]):
                        #self.parse_thread(result)
                        print (result.contents[0])
                        print (result.attrs['href'])
                        print ("------------------")
                        self.table.insert(dict(thread_id=result.attrs['href'][40:], title=result.contents[0], month=datetime.now().month, year=datetime.now().year, thread_link=result.attrs['href']))

    def parse_thread(self, tag):
        link = self.scraper.get(tag.attrs['href'])
        if link.status_code != 200:
            print ("parse_thread response code error: ", link.status_code)
            print ("reason :", link.reason)
            time.sleep(8)
            return
        soup = BeautifulSoup(link.content, "html.parser")
        #date_tag is None if the thread is too "old"
        month = self.month
        year = YEAR
        day = 0

        date_tag = soup.select("[class*=edited]")[0]
        if date_tag:
            elements_list = date_tag.contents[0].split()
            month = elements_list[0]
            day = elements_list[1]
            year = elements_list[2]

        #if it's the correct year and month, we append to the files
        if date_tag and year == YEAR and self.month == month:
            print (tag.contents[0])
            print (tag.attrs['href'])
            print ("------------------")
            with open(self.txt_file, "a", encoding="utf8") as text_file:
                txt = " ".join(tag.contents[0].split())
                text_file.write(txt+ "\n" + tag.attrs['href'] +"\n ----- \n")
        #We insert in both cases in the database
        self.table.insert(
            dict(
                thread_id=tag.attrs['href'][40:],
                time=time.time(),
                month=month,
                year=int(year),
                day=int(day)
                )
            )


    def run(self):
        # with open(self.txt_file, "a", encoding="utf8") as text_file:
        #     txt = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        #     text_file.write("\n#####\n" + txt +"\n#####\n")

        while True:
            for i in range(0, 120, 40):
                print (i)
                time.sleep(5)
                self.parse_main_page(i)
            time.sleep(10)

def main():
    # CrÃ©ation des threads
    thread_1 = Crawler("https://bitcointalk.org/index.php?board=159.{};sort=first_post;desc")
    thread_2 = Crawler("https://bitcointalk.org/index.php?board=240.{};sort=first_post;desc", token=True)


    # Lancement des threads
    thread_1.start()
    time.sleep(5)
    thread_2.start()

if __name__ == "__main__":
    main()
