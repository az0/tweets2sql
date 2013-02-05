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
Test tweets2sql using py.test <http://pytest.org/>
"""

from tweets2sql import *
import os

def test_timeline(tmpdir):
    """Test archiving timeline"""
    tmpfile = tmpdir.join('archive.sqlite')
    connect_sql("sqlite:///%s" % tmpfile)
    twitter_search = connect_twitter()
    ta = TimelineArchiver('bleachbit', twitter_search)
    archive_loop(ta)
    results = Timeline.selectBy(screen_name = 'bleachbit')
    assert(results.count() == 1)
    results = TimelineTweet.selectBy(user_screen_name = 'bleachbit')
    assert(results.count() > 0)

