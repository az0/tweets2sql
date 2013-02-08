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

"""
Test tweets2sql using pytest <http://pytest.org/>
"""


from tweets2sql import *
import os
import pytest


@pytest.fixture()
def connection(tmpdir):
    tmpfile = tmpdir.join('archive.sqlite')
    connect_sql("sqlite:///%s" % tmpfile)


def test_TimelineTweet(connection):
    """Test TimelineTweet with mock data"""
    tl = Timeline(screen_name='test_user')

    # insert mock data
    tdate = twitterdate("Wed Aug 29 17:12:58 +0000 2012")
    text = u'\ucef4\ud328'
    tid = 240859602684612608
    tlt = TimelineTweet(id=tid, user_id=161651238,
        user_screen_name='test',
        text=text, created_at=tdate,
        source='Twitter', timeline=tl)
    assert(tlt.id == tid)

    # retrieve it
    tlt2 = TimelineTweet.get(tid)

    # compare
    assert(text == tlt.text)


def test_TimelineArchiver(connection):
    """Test TimelineArchiver with Twitter connection"""
    twitter_search = connect_twitter()
    ta = TimelineArchiver('bleachbit', twitter_search)
    archive_loop(ta)
    results = Timeline.selectBy(screen_name = 'bleachbit')
    assert(results.count() == 1)
    results = TimelineTweet.selectBy(user_screen_name = 'bleachbit')
    assert(results.count() > 0)

