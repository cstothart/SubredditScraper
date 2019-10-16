""" SubredditScraper

Author:  Cary Stothart
Date:    16 Oct 2019

"""

import time
import random
import sys
import datetime

import praw
import mysql.connector
import numpy as np
from configparser import ConfigParser
from twilio.rest import Client

# Need to use a full path if automating the script with Windows Task 
# Scheduler.
CONFIG_PATH = "C:/Users/carys/Desktop/SubredditScraper/config.ini"

class SubredditScraper:

    def __init__(self):
        self.parser = ConfigParser()
        self.parser.read(CONFIG_PATH)
        
    def _log(self, msg):
        time = datetime.datetime.now()
        print(str(time) + ":  " + str(msg))
        
    def _createSubmissionTable(self):
        query = "CREATE TABLE IF NOT EXISTS submissions (\
                 submission_fullname VARCHAR(255) PRIMARY KEY, \
                 submission_id VARCHAR(255) NOT NULL, \
                 title VARCHAR(500) NOT NULL, \
                 author VARCHAR(255) NOT NULL, \
                 num_comments INT NOT NULL, \
                 score INT NOT NULL, \
                 upvote_ratio VARCHAR(255) NOT NULL, \
                 created_utc VARCHAR(255) NOT NULL, \
                 permalink VARCHAR(255) NOT NULL, \
                 body VARCHAR (40000) NOT NULL, \
                 time_entered_into_database TIMESTAMP NOT NULL DEFAULT NOW() \
                 );"
        self._log("Creating submissions table if it does not already " + 
                  "exist...")
        self.dbCursor.execute(query)
        
    def _createCommentsTable(self):
        query = "CREATE TABLE IF NOT EXISTS comments (\
                 comment_fullname VARCHAR(255) PRIMARY KEY, \
                 comment_id VARCHAR(255) NOT NULL, \
                 submission_fullname VARCHAR(255) NOT NULL, \
                 parent_id VARCHAR(255) NOT NULL, \
                 author VARCHAR(255) NOT NULL, \
                 score INT NOT NULL, \
                 created_utc VARCHAR(255) NOT NULL, \
                 edited VARCHAR(255) NOT NULL, \
                 permalink VARCHAR(255) NOT NULL, \
                 body VARCHAR(40000) NOT NULL, \
                 time_entered_into_database TIMESTAMP NOT NULL DEFAULT NOW(),\
                 CONSTRAINT fk1 FOREIGN KEY(submission_fullname) REFERENCES \
                 submissions(submission_fullname) ON DELETE CASCADE \
                 ON UPDATE CASCADE \
                 );"
        self._log("Creating comments table if it does not already exist...")
        self.dbCursor.execute(query)
    
    def _connectToDatabase(self):
        self.db = mysql.connector.connect(
            host = self.parser.get("MySQLDatabase", "hostname"),
            user = self.parser.get("MySQLDatabase", "username"),
            password = self.parser.get("MySQLDatabase", "password"),
            database = self.parser.get("MySQLDatabase", "database"),
            port = self.parser.get("MySQLDatabase", "port")
        )
        self._log("Connecting to mysql database...")
        self.dbCursor = self.db.cursor()
        
    def _setupDatabaseTables(self):
        self._createSubmissionTable()
        self._createCommentsTable()
        
    def _createReport(self, num_submissions):
        rep_msg = ""
        if num_submissions == 0:
            rep_msg = (rep_msg + "Did not find any submissions to scrape " + 
                       "in r/" + self.target_subreddit + ".\n")
        elif num_submissions == 1:
            rep_msg = (rep_msg + "Scraped " + str(num_submissions) + 
                       " submission from r/" + self.target_subreddit + ".\n")
        else:
            rep_msg = (rep_msg + "Scraped " + str(num_submissions) + 
                       " submissions from r/" + self.target_subreddit + ".\n")
        self.dbCursor.execute("SELECT COUNT(*) FROM submissions")
        rep_msg = (rep_msg + "Total # submissions:\n" + 
                   str(self.dbCursor.fetchall()[0][0]) + "\n")
        self.dbCursor.execute("SELECT COUNT(*) FROM comments")
        rep_msg = (rep_msg + "Total # comments:\n" + 
                   str(self.dbCursor.fetchall()[0][0]) + "\n")
        self.dbCursor.execute("SELECT COUNT(DISTINCT author) FROM comments")
        rep_msg = (rep_msg + "Total # authors:\n" + 
                   str(self.dbCursor.fetchall()[0][0]) + "\n")
        self.dbCursor.execute("SET @sub_num = (SELECT sum(LENGTH(body) - " +
                              "LENGTH(REPLACE(body, ' ', '')) + 1) " +
                              "FROM submissions)")
        self.dbCursor.execute("SET @com_num = (SELECT sum(LENGTH(body) - " +
                              "LENGTH(REPLACE(body, ' ', '')) + 1) " +
                              "FROM comments)")
        self.dbCursor.execute("SELECT @sub_num + @com_num")
        rep_msg = (rep_msg + "Total # words:\n" + 
                   str(round(self.dbCursor.fetchall()[0][0], 0)))
        return(rep_msg)
        
    def scrapeSubmission(self, submission_id):
        """ Scrape the given submission.
        
        Note: Does not scrape submission comments. To scrape submission
              comments, use scrapeSubmissionComments.
        """    
        submission = self.reddit.submission(submission_id)
        query = ("INSERT INTO submissions (" +
                 "submission_fullname, " +
                 "submission_id, " +
                 "title, " +
                 "author, " +
                 "num_comments, " +
                 "score, " +
                 "upvote_ratio, " +
                 "created_utc, " +
                 "permalink, " +
                 "body" +
                 ")\nVALUES (" +
                 "\"" + str(submission.fullname) + "\", " +
                 "\"" + str(submission.id) + "\", " +
                 "\"" + self.db.converter.escape(str(submission.title)) + 
                 "\", " +
                 "\"" + self.db.converter.escape(str(submission.author)) + 
                 "\", " +
                 str(submission.num_comments) + ", " +
                 str(submission.score) + ", " +
                 "\"" + str(submission.upvote_ratio) + "\", " +
                 "\"" + str(submission.created_utc) + "\", " +
                 "\"" + submission.permalink + "\", " +
                 "\"" + self.db.converter.escape(str(submission.selftext)) +
                 "\");")
        self._log("Scraping submission: " + submission.permalink)
        self.dbCursor.execute(query)
        self.db.commit()
        
    def scrapeSubmissionComments(self, submission_id):
        """ Scrapes all of the comments from a submission.
        
        Note: The submission must be scraped before the comments.
        """
        submission = self.reddit.submission(submission_id)
        if len(submission.comments.list()) == 0:
            return()
        self._log("Scraping comments from submission: " + 
                  submission.permalink)
        query = ("INSERT INTO comments (" +
                "comment_fullname, " +
                "comment_id, " +
                "submission_fullname, " +
                "parent_id, " +
                "author, " +
                "score, " +
                "created_utc, " +
                "edited, " +
                "permalink, " +
                "body" +
                ")\nVALUES ")
        submission.comments.replace_more(limit = None)
        i = 0
        for comment in submission.comments.list():
            i += 1
            query = (query + 
                    "\n(" +
                    "\"" + str(comment.fullname) + "\", " +
                    "\"" + str(comment.id) + "\", " +
                    "\"" + str(comment.link_id) + "\", " +
                    "\"" + str(comment.parent_id) + "\", " +
                    "\"" + self.db.converter.escape(str(comment.author)) +
                    "\", " +
                    "\"" + str(comment.score) + "\", " +
                    "\"" + str(comment.created_utc) + "\", " +
                    "\"" + str(comment.edited) + "\", " +
                    "\"" + comment.permalink + "\", " +
                    "\"" + self.db.converter.escape(str(comment.body)))
            if i == len(submission.comments.list()):
                query = query + "\")"
            else:
                query = query + "\"), "
        self.dbCursor.execute(query)
        self.db.commit()
        
    def findUnscrapedSubmissions(self, minSubmissionAge):
        """ Returns a list of IDs of unscraped submissions that have an age 
        equal to or older than minSubmissionAge.
        
        Note: A submission is considered scraped if it is in the submission
              table.
        """
        self._log("Searching for submissions to scrape...")
        reddit_ids = []
        mysql_ids = []
        for submission in self.subreddit.new(limit = 1000):
            submission_age = int(time.time()) - submission.created_utc
            if (submission_age >= minSubmissionAge):
                reddit_ids.append(submission.id)
        self.dbCursor.execute("SELECT submission_id FROM submissions")
        mysql_result = self.dbCursor.fetchall()
        for id in mysql_result:
            mysql_ids.append(id[0])
        to_scrape = np.setdiff1d(reddit_ids, mysql_ids)
        self._log("Found " + str(len(to_scrape)) + " submissions to scrape.")
        return(to_scrape)
        
    def sendSMS(self, sms_body):
        account_sid = self.parser.get("Twilio", "account_sid")
        auth_token = self.parser.get("Twilio", "auth_token")
        self.twilio_client = Client(account_sid, auth_token)
        self._log("Sending notification...")
        from_num = self.parser.get("Twilio", "from_num")
        to_num = self.parser.get("Twilio", "to_num")
        message = self.twilio_client.messages.create(
            to = to_num, 
            from_= from_num,
            body = sms_body)
            
    def setup(self):
        """ Setup the scraper.
        """
        self._log("Setting-up Reddit...")
        self.target_subreddit = self.parser.get("Reddit", "target_subreddit")
        client_id = self.parser.get("Praw", "client_id")
        client_secret = self.parser.get("Praw", "client_secret")
        user_agent = self.parser.get("Praw", "user_agent")
        self.reddit = praw.Reddit(
            client_id = client_id,
            client_secret = client_secret,
            user_agent = user_agent)
        self._log("Pointing to " + self.target_subreddit + " subreddit...")
        self.subreddit = self.reddit.subreddit(self.target_subreddit)
        self._connectToDatabase()
        self._setupDatabaseTables()
        
    def cleanup(self):
        """ Cleanup all running processes.
        """
        self._log("Closing mySQL connection...")
        self.dbCursor.close()
        self.db.close()
        
    def scrapeUnscrapedSubmissions(self):
        """ Scrape all unscraped submissions.
        
        Note: A submission is considered scraped if it is in the submission
              table.
        """
        min_submission_age = int(self.parser.get("Reddit", 
                                                 "min_submission_age"))
        submission_ids = self.findUnscrapedSubmissions(min_submission_age)
        i = 0
        num_submissions = len(submission_ids)
        for id in submission_ids:
            i += 1
            self._log("##### Scraping submission " + str(i) + " of " +
                      str(num_submissions) + " #####")
            self.scrapeSubmission(id)
            self.scrapeSubmissionComments(id)
            time.sleep(1)
        report_msg = self._createReport(num_submissions)
        self.sendSMS(report_msg)
        self._log("DONE!")
        
        
    def start(self):
        try:
            self.setup()
            self.scrapeUnscrapedSubmissions()
            self.cleanup()
        except:
            sms_msg = "Encountered a fatal error!"
            self.sendSMS(sms_msg)
            raise
        
        
def main():
    scpr = SubredditScraper()
    scpr.start()
        
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Error {}".format(e))
        time.sleep(15)