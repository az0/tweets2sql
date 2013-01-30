tweets2sql
===========

Archive Twitter tweets to a SQL database.

Requires Python version 2.6 or 2.7, [sixohsix's twitter API](https://github.com/sixohsix/twitter) and [SQLObject](http://www.sqlobject.org/SQLObject.html), which supports many database types including: SQLite, MySQL, and Microsoft SQL Server.

Usage
=====

These examples are for Linux and SQLite:

    # Archive search for tweets with Twitter picture URLs
    python tweets2sql.py -c sqlite:///home/username/twitter.db -s pic.twitter.com

    # Archive search for hashtag #FF (notice it is escaped for the shell)
    python tweets2sql.py -c sqlite:///home/username/twitter.db -s '#ff'

    # Archive nprnews timeline
    python tweets2sql.py -c sqlite:///home/username/twitter.db -u nprnews

This example is for Windows and Microsoft SQL Server:

    tweets2sql.py -c mssql://user:pass/db -u nprnews
