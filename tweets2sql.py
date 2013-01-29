#!/usr/bin/env python
# vim: ts=4:sw=4:expandtab

## tweets2sql
## Copyright (C) 2013 Andrew Ziem
## https://github.com/az0/tweets2sql
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


from sqlobject import *
from sqlobject.dberrors import IntegrityError
from twitter.api import TwitterError
from twitter.util import Fail, err
import httplib
import os
import time
import twitter
import urllib2

Q_TIMELINE = 1
Q_SEARCH = 2

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


def SearchArchiver:
    """Archive search results"""

    def __init__(self, query_str, auth):
        self.query_str = query_str.strip()
        self.twitter = twitter.Twitter(auth = auth, domain = 'search.twitter.com')
        results = Query.selectBy(query = query_str)
        if 0 == results.count():
            # make a new query record
            self.query = Query(query = query_str)
        else:
            # use existing query record
            self.query = results[0]
        # counter
        self.new = 0
        self.dup = 0


    def query:
        """Make one API call and archive the results"""
        print 'SearchArchiver.query()'
        kwargs = { 'q' : self.query_str, 'rpp' : 200 }
        tquery = self.twitter.search(**kwargs)
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
                self.dup += 1
            else:
                self.new += 1


class TimelineArchiver:
    """Archive a user's timeline"""

    def __init__(self, screen_name, auth):
        self.screen_name = screen_name
        # find the most recent ID
        results = Timeline.selectBy(screen_name = screen_name)
        if 0 == results.count():
            # make a new timeline
            self.timeline = Timeline(screen_name = screen_name)
            self.max_id = None
        else:
            # use the existing timeline
            self.timeline = results[0]
            # find max ID
            self.max_id = Timeline.select(Timeline.q.screen_name==screen_name)[0].max_id

        # connection
        self.twitter = twitter.Twitter(auth=auth, api_version='1', domain='api.twitter.com')

        # stats
        self.new = 0
        self.dup = 0


    def query(self):
        """Make one API call and archive the results"""
        print 'TimelineArchiver.query()'
        kwargs = { 'screen_name' : self.screen_name, 'rpp': 200 }
        if self.max_id:
            kwargs['max_id'] = self.max_id
        tl = self.twitter.statuses.user_timeline(**kwargs)
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
                'timeline' : self.timeline }
            try:
                TimelineTweet(**kwargs)
            except IntegrityError:
                print 'DEBUG: tweet already in database', tweet['id']
                self.dup += 1
            else:
                self.new += 1




def archive(twitter, q_type, q_str):
    if q_type == Q_TIMELINE:
        return timeline(q_str, max_id)
    elif q_type == Q_SEARCH:
        return search(q_str, max_id)
    else:
        raise RuntimeError('unknown query type %d: %s' % (q_type, q_str))



def archive_loop(archiver):
    """Generic loop and handling for all kinds of archiving.
    Mostly copied from Mike Verdone's twitter.archiver."""
    fail = Fail()
    twitter = archiver.twitter
    last_new = 0
    # download one API call at a time until done while handling errors
    while True:
        try:
            archiver.query()
        except TwitterError as e:
            if e.e.code == 401:
                err("Fail: %i Unauthorized (tweets of that user are protected)"
                    % e.e.code)
                break
            elif e.e.code == 400:
                err("Fail: %i API rate limit exceeded" % e.e.code)
                rate = twitter.account.rate_limit_status()
                reset = rate['reset_time_in_seconds']
                reset = time.asctime(time.localtime(reset))
                delay = int(rate['reset_time_in_seconds']
                            - time.time()) + 5 # avoid race
                err("Hourly limit of %i requests reached, next reset on %s: "
                    "going to sleep for %i secs" % (rate['hourly_limit'],
                                                    reset, delay))
                fail.wait(delay)
                continue
            elif e.e.code == 404:
                err("Fail: %i This profile does not exist" % e.e.code)
                break
            elif e.e.code == 502:
                err("Fail: %i Service currently unavailable, retrying..."
                    % e.e.code)
            else:
                err("Fail: %s\nRetrying..." % str(e)[:500])
            fail.wait(3)
        except urllib2.URLError as e:
            err("Fail: urllib2.URLError %s - Retrying..." % str(e))
            fail.wait(3)
        except httplib.error as e:
            err("Fail: httplib.error %s - Retrying..." % str(e))
            fail.wait(3)
        except KeyError as e:
            err("Fail: KeyError %s - Retrying..." % str(e))
            fail.wait(3)
        else:
            this_new = archiver.new - last_new
            err('Browsing.  New:%d.  Dup:%d' % (archiver.new, archiver.dup))
            if this_new < 190:
                break
            last_new = archiver.new
            fail = Fail()


def main():
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option('-c', dest='connection_string', type="string", help='SQL connection URI such as sqlite:///full/path/to/database.db')
    parser.add_option('-q', dest='query', type="string", help='Archive search results such as #foo')
    parser.add_option('-s', dest='screen_name', type="string", help='Archive timeline for given screen name')
    parser.add_option('-o', dest='oauth', type="string", help='Authenticate using OAuth')
    (options, args) = parser.parse_args()

    # setup SQLObject
    connection = connectionForURI(options.connection_string)
    sqlhub.processConnection = connection
    Query.createTable(ifNotExists = True)
    SearchTweet.createTable(ifNotExists = True)
    TimelineTweet.createTable(ifNotExists = True)
    Timeline.createTable(ifNotExists = True)

    # authenticate
    if options.oauth:
        creds = os.path.expanduser('~/.tweets2sql-oauth')
        CONSUMER_KEY = 'mikYMFxbLhD1TAhaztCshA'
        CONSUMER_SECRET = 'Ys9VHBWLS5fX4cFnDHSVac52fl388JV19yJz1WMss'
        if not os.path.exists(creds):
            twitter.oauth_dance("tweets2sql", CONSUMER_KEY, CONSUMER_SECRET, creds)
        oauth_token, oauth_secret = twitter.read_token_file(creds)
        auth = twitter.OAuth(oauth_token, oauth_secret, CONSUMER_KEY, CONSUMER_SECRET)
    else:
        auth = twitter.NoAuth()

    # process command line
    if options.query:
        print '*** SEARCH: %s' % options.query
        sa = SearchArchiver(options.query, auth)
        archive_loop(sa)
    if options.screen_name:
        print '*** SCREEN NAME: %s' % options.screen_name
        ta = TimelineArchiver(options.screen_name, auth)
        archive_loop(ta)

main()
