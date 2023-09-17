#! /usr/bin/env python3
"""
Cleans up a collection of btrfs snapshots.

Assumes that each snapshot have a date suffix.
"""

from os import listdir
from datetime import datetime
import re
import subprocess
from argparse import ArgumentParser


def filter(name, dates):
    weeks = max_weeks
    days = max_days
    months = max_months

    cur_day = 0
    cur_week = 0
    cur_month = 0
    dates.sort(reverse=True)
    for datestr in dates:
        filter = ''

        d = datetime.strptime(datestr, '%Y-%m-%d')

        month = d.strftime('%Y-%m')
        week = d.strftime('%Y-%W')
        day = d.strftime('%Y-%m-%d')

        if month != cur_month:
            cur_month = month
            if months > 0:
                months -= 1
                filter += 'months'

        if week != cur_week:
            cur_week = week
            if weeks > 0:
                weeks -= 1
                filter += ' weeks'

        if day != cur_day:
            cur_day = day
            if days > 0:
                days -= 1
                filter += ' days'

        if filter == '':
            subprocess.check_call(
                [
                    'btrfs',
                    'subvolume',
                    'delete',
                    '{}/{}/{}-{}'.format(src, host, name, datestr)
                ]
            )
        else:
            print(datestr, filter.strip())

def cleanSrc(src):
    for host in listdir(src):
        print('###################################')
        print(host)
        snapshots = getSnapshots("/".join([src, host]))

        for name, dates in snapshots.items():
            print('\n' + name)
            filter(src, host, name, dates)


def getSnapshots(path):
    snapshots = {}
    for s in listdir(path):
        m = re.search('(.+)-(\\d{4}-\\d{2}-\\d{2})$', s)
        if m.group(1) in snapshots:
            snapshots[m.group(1)].append(m.group(2))
        else:
            snapshots[m.group(1)] = [m.group(2)]

    return snapshots


if __name__ == '__main__':
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('src', help='Source directory.', nargs='+')
    parser.add_argument(
        '-d',
        '--days',
        help='Number of daily snapshots to keep.',
        type=int,
        default=6
    )
    parser.add_argument(
        '-w',
        '--weeks',
        help='Number of weekly snapshots to keep.',
        type=int,
        default=6
    )
    parser.add_argument(
        '-m',
        '--months',
        help='Number of monthly snapshots to keep.',
        type=int,
        default=6
    )

    args = parser.parse_args()
    src = args.src
    max_days = args.days
    max_weeks = args.weeks
    max_months = args.months

    for src in args.src:
        print('*********{}*********'.format(src))
        cleanSrc(src)
