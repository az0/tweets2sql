twitter2sql
===========

Archive Twitter tweets to a SQL database.

Requires Python version 2.6 or 2.7, [sixohsix's twitter API](https://github.com/sixohsix/twitter) and [SQLObject](http://www.sqlobject.org/SQLObject.html), which supports many database types including: SQLite, MySQL, Microsoft SQL Server, and others.

Usage
=====

python twitter2sql.py -c sqlite:///home/username/twitter.db -q pic.twitter.com
python twitter2sql.py -c sqlite:///home/username/twitter.db -q '#ff'
python twitter2sql.py -c sqlite:///home/username/twitter.db -s nprnews
