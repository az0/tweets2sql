#!/usr/bin/env python
# vim: ts=4:sw=4:expandtab

## twitter2sql
## Copyright (C) 2013 Andrew Ziem
## https://github.com/az0/twitter2sql
##
## This program is free software: you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation, either version 3 of the License, or
## (at your option) any later version.
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with this program.  If not, see <http://www.gnu.org/licenses/>.


import os
import time
import twitter
from twitter.archiver import format_date
from sqlobject import *
from sqlobject.dberrors import IntegrityError

conection = None

class SearchTweet(SQLObject):
    """A tweet as found in the Twitter search API"""
    to_user = UnicodeCol(length=15, default=None)
    to_user_name = UnicodeCol(length=20, default=None)
    to_user_id = IntCol(default=None)
    from_user = UnicodeCol(length=15)
    from_user_name = UnicodeCol(length=20)
    from_user_id = IntCol()
    text = UnicodeCol(length=140)
    iso_language_code = StringCol(length=5) # examples: en, es, fil, zh-tw
    source = UnicodeCol()
    created_at = DateTimeCol()
    query = ForeignKey('Query')


class Query(SQLObject):
    """A query for the search API"""
    query = UnicodeCol()
    max_id = IntCol(default = None) # most recent seen


class TimelineTweet(SQLObject):
    """A tweet as found in the timeline"""
    # FIXME count retweets
    user_id = IntCol()
    user_screen_name = UnicodeCol(length=20)
    created_at = DateTimeCol()
    text = UnicodeCol(length=140)
    source = UnicodeCol()
    in_reply_to_status_id = IntCol(default=None)
    in_reply_to_user_id = IntCol(default=None)
    in_reply_to_screen_name = UnicodeCol(length=20, default=None)
    timeline = ForeignKey('Timeline')


class Timeline(SQLObject):
    """A Twitter timeline for a single user account"""
    # FIXME: user can change screen name
    screen_name = UnicodeCol(length=20, alternateID=True)
    max_id = IntCol(default=None)


def twitterdate(tdate):
    # Example: u'Sun, 20 Jan 2013 20:59:21 UTC'
    #return time.strptime(tdate, '%a, %d %b %Y %H:%M:%S %Z')
    import dateutil.parser
    return dateutil.parser.parse(tdate)


def search_sub(twitter_query_results, query):
    for tweet in twitter_query_results:
        created_at = twitterdate(tweet['created_at'])
        kwargs = { 'id' : tweet['id'], \
            'iso_language_code' : tweet['iso_language_code'], \
            'text' : tweet['text'], \
            'to_user' : tweet['to_user'], \
            'to_user_id' : tweet['to_user_id'], \
            'to_user_name' : tweet['to_user_name'], \
            'source' : tweet['source'], \
            'from_user' : tweet['from_user'], \
            'from_user_id' : tweet['from_user_id'], \
            'from_user_name' : tweet['from_user_name'],
            'created_at' : created_at, \
            'query' : query }
        try:
            SearchTweet(**kwargs)
        except IntegrityError:
            print 'DEBUG: tweet already in database', tweet['id']


def search(query_str):
    query_str = query_str.strip()

    print '**** QUERY: %s' % query_str

    # find the most recent ID
    results = Query.selectBy(query = query_str)
    if 0 == results.count():
        query = Query(query = query_str)
    else:
        query = results[0]

    ts = twitter.Twitter(domain = 'search.twitter.com')

    RPP = 200 # results per page
    kwargs = { 'q' : query_str, 'rpp' : RPP }
    while True:
        tquery = ts.search(**kwargs)
        search_sub(tquery['results'], query)
        query.max_id = tquery['max_id']
        kwargs['max_id'] = query.max_id
        print 'DEBUG: in this batch found %d tweets' % len(tquery['results'])
        if len(tquery['results']) < RPP:
            break


def timeline_sub(tl, timeline):
    for tweet in tl:
        created_at = twitterdate(tweet['created_at'])
        kwargs = { 'id' : tweet['id'], \
            'user_id' : tweet['user']['id'], \
            'user_screen_name' : tweet['user']['screen_name'], \
            'in_reply_to_status_id' : tweet['in_reply_to_status_id'], \
            'in_reply_to_user_id' : tweet['in_reply_to_user_id'], \
            'in_reply_to_screen_name' : tweet['in_reply_to_screen_name'], \
            'text' : tweet['text'], \
            'source' : tweet['source'], \
            'created_at' : created_at, \
            'timeline' : timeline }
        try:
            TimelineTweet(**kwargs)
        except IntegrityError:
            print 'DEBUG: tweet already in database', tweet['id']



def timeline(screen_name):
    print '*** SCREEN NAME: %s' % screen_name

    # find the most recent ID
    results = Timeline.selectBy(screen_name = screen_name)
    if 0 == results.count():
        # make a new timeline
        timeline = Timeline(screen_name = screen_name)
    else:
        # use the existing timeline
        timeline = results[0]

    # prepare to search
    t = twitter.Twitter(auth=twitter.NoAuth(), api_version='1', domain='api.twitter.com')
    kwargs = { 'screen_name' : screen_name }
    while True:
        tl = t.statuses.user_timeline(**kwargs)
        timeline_sub(tl, timeline)
        kwargs['max_id'] = min([tweet['id'] for tweet in tl])
    timeline.max_id = kwargs['max_id']


def main():
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option('-c', dest='connection_string', type="string", help='SQL connection URI such as sqlite:///full/path/to/database.db')
    parser.add_option('-q', dest='query', type="string", help='Archive search results such as #foo')
    parser.add_option('-s', dest='screen_name', type="string", help='Archive timeline for given screen name')
    (options, args) = parser.parse_args()
    connection = connectionForURI(options.connection_string)
    sqlhub.processConnection = connection
    Query.createTable(ifNotExists = True)
    SearchTweet.createTable(ifNotExists = True)
    TimelineTweet.createTable(ifNotExists = True)
    Timeline.createTable(ifNotExists = True)
    if options.query:
        search(options.query)
    if options.screen_name:
        timeline(options.screen_name)

main()
