SubredditScraper
=============

A Python script that scrapes all submissions with a minimum age from a given 
subreddit (comments included) and uploads the scraped data to a given mySQL 
database. Sends an SMS notification to a given phone number when upload is 
complete. To avoid redundancy, the script checks the mySQL database to make
sure that each submission has not already been scraped.

(For example, if you set the minimum age to 1296000 (15 days in seconds) and
the target subreddit to datascience, the script will scrape all datascience 
submissions that are at least 15 days old.)

One limitation, however, is that Reddit limits the number of results 
obtainable to 1000. Thus, the script can only look at the most recent 1000
submissions of the given subreddit. Therefore, setting the minimum age too 
high will result in no submissions being scraped. For example, if you set the
minimum age to 30 days and more than 1000 submissions were posted on your
target subreddit within 30 days, then the script will not scrape any 
submissions.

### Usage ###

Before running the script, you need to set up the config.ini file by entering
your Twilio credentials, mySQL database credentials, praw credentials, target 
subreddit, and minimum submission age.

Note: You can automate this script by adding it to a scehduling program such
as Windows Task Scheduler.