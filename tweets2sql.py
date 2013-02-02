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


from sqlobject import SQLObject, DateTimeCol, StringCol, UnicodeCol, IntCol, \
    ForeignKey, connectionForURI, sqlhub, SQLObjectNotFound
from twitter.api import TwitterError
from twitter.util import Fail, err
import httplib
import os
import sys
import time
import twitter
import urllib2


class SearchTweet11(SQLObject):
    """A tweet as found in Twitter search/tweets 1.1"""
    created_at = DateTimeCol()
    in_reply_to_screen_name = UnicodeCol(length=20, default=None)
    in_reply_to_status_id = IntCol(default=None)
    in_reply_to_user_id = IntCol(default=None)
    iso_language_code = StringCol(length=5) # examples: en, es, fil, zh-tw
    source = UnicodeCol()
    text = UnicodeCol()
    user_id = IntCol()
    user_screen_name = UnicodeCol(length=20)
    search = ForeignKey('Search')


class Search(SQLObject):
    """A query for the search API"""
    query = UnicodeCol()
    since_id = IntCol(default = None) # most recent seen


class TimelineTweet(SQLObject):
    """A tweet as found in the timeline"""
    # FIXME count retweets
    user_id = IntCol()
    user_screen_name = UnicodeCol(length=20)
    created_at = DateTimeCol()
    text = UnicodeCol()
    source = UnicodeCol()
    in_reply_to_status_id = IntCol(default=None)
    in_reply_to_user_id = IntCol(default=None)
    in_reply_to_screen_name = UnicodeCol(length=20, default=None)
    timeline = ForeignKey('Timeline')


class Timeline(SQLObject):
    """A Twitter timeline for a single user account"""
    # FIXME: user can change screen name
    screen_name = UnicodeCol(length=20, alternateID=True)
    since_id = IntCol(default=None)


def twitterdate(tdate):
    # Example: u'Sun, 20 Jan 2013 20:59:21 UTC'
    #return time.strptime(tdate, '%a, %d %b %Y %H:%M:%S %Z')
    import dateutil.parser
    return dateutil.parser.parse(tdate)


class Archiver:
    """Base class for archiving X from Twitter to SQL"""

    def __init__(self, twitter_search):
        self.first_query = True
        self.min_id = None # for navigating backwards in this session
        self.new = 0 # counter
        self.dup = 0 # counter
        self.twitter_search = twitter_search # connection to Twitter


    def more(self):
        """Returns boolean whether more results are available"""
        assert(not self.first_query)
        # FIXME: improve this heuristic method
        # Twitter performs filters bad tweets after getting the requested
        # number of tweets, so there may be fewer
        return (self.rpp * 0.90) <= self.query_count


    def rate_limit_status(self):
        """Query the rate limit status"""
        r = self.twitter_search.application.rate_limit_status()
        sr = r['resources']['%s' % self.resource]['/%s/%s' % (self.resource, self.sub_resource)]
        return (sr['reset'], sr['limit'])


    def success(self):
        """Call once after successfully archiving the timeline"""
        self.record.since_id = self.since_id


class SearchArchiver(Archiver):
    """Archive search results"""

    def __init__(self, query_str, twitter_search):
        Archiver.__init__(self, twitter_search)
        self.query_str = query_str.strip()
        self.rpp = 100 # Twitter 1 and 1.1 API limits to 100 results per page
        self.resource = 'search'
        self.sub_resource = 'tweets'
        results = Search.selectBy(query = self.query_str)
        if 0 == results.count():
            # make a new query record
            self.record = Search(query = self.query_str)
            self.since_id = None
        else:
            # use existing query record
            self.record = results[0]
            self.since_id = Search.select(Search.q.query == self.query_str)[0].since_id


    def query(self):
        """Make one API call and archive the results"""
        kwargs = { 'q' : self.query_str, 'count' : self.rpp, \
            'contributor_details': 'true' }
        if self.first_query and self.since_id:
            kwargs['since_id'] = self.since_id
        if self.min_id:
            kwargs['max_id'] = self.min_id - 1
        print 'SearchArchiver.query(%s)' % kwargs
        tquery = self.twitter_search.search.tweets(**kwargs)
        statuses = tquery['statuses']
        found_any = False
        for tweet in statuses:
            created_at = twitterdate(tweet['created_at'])
            kwargs = { 'id' : tweet['id'], \
                'created_at' : created_at, \
                'in_reply_to_screen_name' : tweet['in_reply_to_screen_name'], \
                'in_reply_to_status_id' : tweet['in_reply_to_status_id'],
                'in_reply_to_user_id' : tweet['in_reply_to_user_id'],
                'iso_language_code' : tweet['metadata']['iso_language_code'], \
                'source' : tweet['source'], \
                'text' : tweet['text'], \
                'user_id' : tweet['user']['id'], \
                'user_screen_name' : tweet['user']['screen_name'], \
                'search' : self.record }
            found_any = True
            try:
                SearchTweet11.get(tweet['id'])
            except SQLObjectNotFound:
                SearchTweet11(**kwargs)
                self.new += 1
            else:
                print 'DEBUG: tweet already in database', tweet['id']
                self.dup += 1
        if not found_any:
            self.query_count = 0
            self.first_query = False
            return
        self.query_count = len(statuses)
        if self.first_query:
            self.first_query = False
            self.since_id = max([tweet['id'] for tweet in statuses])
        self.min_id = min([tweet['id'] for tweet in statuses])



class TimelineArchiver(Archiver):
    """Archive a user's timeline"""

    def __init__(self, screen_name, twitter_search):
        Archiver.__init__(self, twitter_search)
        self.screen_name = screen_name
        self.rpp = 200 # called count in this API
        self.resource = 'statuses'
        self.sub_resource = 'user_timeline'
        # find the most recent ID
        results = Timeline.selectBy(screen_name = screen_name)
        if 0 == results.count():
            # make a new timeline
            self.record = Timeline(screen_name = screen_name)
            self.since_id = None
        else:
            # use the existing timeline
            self.record = results[0]
            # find max ID
            self.since_id = Timeline.select(Timeline.q.screen_name==screen_name)[0].since_id


    def query(self):
        """Make one API call and archive the results"""
        kwargs = { 'screen_name' : self.screen_name, 'count': self.rpp }
        if self.first_query and self.since_id:
            kwargs['since_id'] = self.since_id
        if self.min_id:
            kwargs['max_id'] = self.min_id - 1
        print 'TimelineArchiver.query(%s)' % kwargs
        tl = self.twitter_search.statuses.user_timeline(**kwargs)
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
                'timeline' : self.record }
            try:
                TimelineTweet.get(tweet['id'])
            except SQLObjectNotFound:
                TimelineTweet(**kwargs)
                self.new += 1
            else:
                print 'DEBUG: tweet already in database', tweet['id']
                self.dup += 1
        if not tl:
            # no tweets
            self.query_count = 0
            self.first_query = False
            return
        self.query_count = len(tl)
        if self.first_query:
            self.first_query = False
            # for future sessions
            self.since_id = max([tweet['id'] for tweet in tl])
        # for this session, search backwards
        self.min_id = min([tweet['id'] for tweet in tl])


def archive_loop(archiver):
    """Generic loop and handling for all kinds of archiving.
    Mostly copied from Mike Verdone's twitter.archiver."""
    fail = Fail()
    twitter = archiver.twitter_search
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
                err("Fail: %i Bad Request" % e.e.code)
                break
            elif e.e.code == 404:
                err("Fail: %i Profile does not exist" % e.e.code)
                break
            elif e.e.code == 429:
                err("Fail: %i Too Many Requests" % e.e.code)
                (reset_unix, limit) = archiver.rate_limit_status()
                reset_str = time.asctime(time.localtime(reset_unix))
                delay = int(reset_unix - time.time()) + 5 # avoid race
                err("Limit of %i requests reached, next reset on %s: "
                    "going to sleep for %i secs" % (limit, reset_str, delay))
                fail.wait(delay)
                continue
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
            err('Browsing.  This batch: %d.  Cumulative new: %d.  Cumulative duplicate: %d' \
                % (archiver.query_count, archiver.new, archiver.dup))
            if not archiver.more():
                archiver.success()
                break
            last_new = archiver.new
            fail = Fail()


def connect_sql(connection_string):
    """Initialize SQL connection and database"""
    connection = connectionForURI(connection_string)
    sqlhub.processConnection = connection

    # When creating tables, make sure to start with those referenced as
    # foreign keys.
    Search.createTable(ifNotExists = True)
    SearchTweet11.createTable(ifNotExists = True)
    Timeline.createTable(ifNotExists = True)
    TimelineTweet.createTable(ifNotExists = True)


def print_sql():
    """Print SQL commands to create tables"""
    sql = Search.createTableSQL() + \
        SearchTweet11.createTableSQL() + \
        Timeline.createTableSQL() + \
        TimelineTweet.createTableSQL()
    sys.stdout.write('\n'.join(sql[0::2]))


def connect_twitter():
    """Initialize connection to Twitter"""
    # authenticate
    creds = os.path.expanduser('~/.tweets2sql-oauth')
    CONSUMER_KEY = 'mikYMFxbLhD1TAhaztCshA'
    CONSUMER_SECRET = 'Ys9VHBWLS5fX4cFnDHSVac52fl388JV19yJz1WMss'
    if not os.path.exists(creds):
        twitter.oauth_dance("tweets2sql", CONSUMER_KEY, CONSUMER_SECRET, creds)
    oauth_token, oauth_secret = twitter.read_token_file(creds)
    auth = twitter.OAuth(oauth_token, oauth_secret, CONSUMER_KEY, CONSUMER_SECRET)

    # connect
    return twitter.Twitter(domain='api.twitter.com', auth=auth, api_version = '1.1')


def main():
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option('-c', '--connnection', dest='connection_string', type='string', help='SQL connection URI such as sqlite:///full/path/to/database.db')
    parser.add_option('-s', '--search', dest='search', type='string', help='Archive search results such as #foo')
    parser.add_option('-u', '--user', dest='user', type='string', help='Archive user timeline')
    parser.add_option('--sql', action='store_true', help='Print backend-specific SQL commands to create database tables')
    (options, args) = parser.parse_args()

    if not options.connection_string:
        parser.print_usage()
        print 'Try the --help option'
        sys.exit(1)

    connect_sql(options.connection_string)

    if options.sql:
        print_sql()
        sys.exit(0)

    twitter_search = connect_twitter()

    # process command line
    if options.search:
        print '*** Archiving search: %s' % options.search
        sa = SearchArchiver(options.search, twitter_search)
        archive_loop(sa)
    if options.user:
        for user in options.user.split(','):
            print '*** Archiving user timelime: %s' % user
            ta = TimelineArchiver(user, twitter_search)
            archive_loop(ta)
            print ''

main()
