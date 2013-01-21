twitter2sql
===========

Archive Twitter tweets to a SQL database.

Requires Python version 2.6 or 2.7, [sixohsix's twitter API](https://github.com/sixohsix/twitter) and [SQLObject](http://www.sqlobject.org/SQLObject.html), which supports many database types including: SQLite, MySQL, Microsoft SQL Server, and others.

Usage
=====

    # Archive search for tweets with Twitter picture URLs and store to SQLite database
    python twitter2sql.py -c sqlite:///home/username/twitter.db -q pic.twitter.com

    # Archive search for hashtag #FF (notice it is escaped for the shell)
    python twitter2sql.py -c sqlite:///home/username/twitter.db -q '#ff'

    # Archive nprnews timeline
    python twitter2sql.py -c sqlite:///home/username/twitter.db -s nprnews
