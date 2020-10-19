#! /usr/bin/env python3
"""
Sync btrfs snapshots between two locations.
Use host:dir for remote locations.
"""

import copy
import logging
import os
import subprocess
import sys
from argparse import ArgumentParser


class BackupNode:

    def __init__(self, base, verbose):
        self._base = base
        self._cmd = []
        self._log = logging.getLogger('BackupNode')
        self._verbose = verbose

    def mkdir(self, paths):
        for p in paths:
            os.makedirs('/'.join((self._base, p)), exist_ok=True)

    def ls(self, path):
        return set(os.listdir('/'.join((self._base, path))))

    def get_snapshot_dict(self, target):
        snapshots = {}

        for snapshot in self.ls(target):
            snapshotParts = snapshot.split('-')
            name = '-'.join(snapshotParts[:-3])
            date = '-'.join(snapshotParts[-3:])
            try:
                snapshots[name].add(date)
            except KeyError:
                snapshots[name] = {date}

        return snapshots

    def send(self, name, missingDate, parent=None):
        cmd = copy.copy(self._cmd)
        cmd.extend(['btrfs', 'send'])
        if parent is not None:
            cmd.extend(
                ['-p', self._base + '/' + name + '-' + parent]
            )
        cmd.append(self._base + '/' + name + '-' + missingDate)

        self._log.info(cmd)

        return subprocess.Popen(cmd, stdout=subprocess.PIPE)

    def receive(self, name, stream):
        cmd = copy.copy(self._cmd)
        cmd.extend(['btrfs', 'receive'])
        if self._verbose:
            cmd.append('-vv')
        cmd.append(self._base + '/' + name)

        subprocess.check_call(cmd, stdin=stream)

    def remove(self, name, dates):
        if not dates:
            return

        cmd = copy.copy(self._cmd)
        cmd.extend(
            ['btrfs', 'subvolume', 'delete']
        )
        cmd.extend([self._base + '/' + name + '-' + date for date in dates])
        subprocess.check_call(cmd)


class RemoteBackupNode(BackupNode):

    def __init__(self, host, base, verbose):
        self._cmd = ['ssh', host]
        self._base = base
        self._log = logging.getLogger('RemoteBackupNode')
        self._verbose = verbose

    def mkdir(self, paths):
        cmd = copy.copy(self._cmd)

        cmd.extend(['mkdir', '-p'])
        cmd.extend(['/'.join((self._base, p)) for p in paths])

        subprocess.check_call(cmd)

    def ls(self, path):
        cmd = copy.copy(self._cmd)
        cmd.append('ls')
        cmd.append('/'.join((self._base, path)))

        ps = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        out, err = ps.communicate()

        return set(filter(lambda l: l, out.decode('utf-8').split('\n')))


class Syncer:

    def __init__(self, src, dst, delete):
        self._src = src
        self._dst = dst
        self._delete = delete
        self._log = logging.getLogger('Syncer')

    def sync(self):
        targets = self._src.ls('')
        self._dst.mkdir(targets)

        for target in targets:
            self.sync_target(target)

    def sync_target(self, target):
        self._log.info('Syncing %s', target)
        src_snapshots = self._src.get_snapshot_dict(target)
        dst_snapshots = self._dst.get_snapshot_dict(target)

        for name, dates in src_snapshots.items():
            try:
                commonDates = dates & dst_snapshots[name]
                missingDates = dates - dst_snapshots[name]
            except KeyError:
                commonDates = set()
                missingDates = dates

            for missingDate in sorted(missingDates):
                parent = self.findParent(missingDate, commonDates)
                ps = self._src.send(target + '/' + name, missingDate, parent)
                self._dst.receive(target, ps.stdout)
                ps.wait()
                commonDates.add(missingDate)

            if self._delete:
                try:
                    self._dst.remove(
                        target + '/' + name,
                        dst_snapshots[name] - dates
                    )
                except KeyError:
                    pass

    @staticmethod
    def findParent(date, commonDates):
        parent = None
        for c in sorted(commonDates):
            if c > date:
                break
            else:
                parent = c

        return parent


def createBackupNode(path, verbose=False):
    parts = path.split(':')

    if len(parts) == 1:
        return BackupNode(path, verbose)
    elif len(parts) == 2:
        return RemoteBackupNode(parts[0], parts[1], verbose)
    else:
        raise ValueError('Unsupported path.')


def get_argument_parser():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('src', help='Source directory.')
    parser.add_argument('dst', help='Destination directory.')
    parser.add_argument(
        '-d',
        '--delete',
        help='Delete extraneous snapshots from destination.',
        action='store_true'
    )
    parser.add_argument(
        '-v', '--verbose',
        help='Verbose output.',
        action='store_true'
    )

    return parser


if __name__ == '__main__':
    parser = get_argument_parser()
    args = parser.parse_args()

    try:
        src = createBackupNode(args.src)
        dst = createBackupNode(args.dst, args.verbose)
    except AttributeError:
        parser.print_help()
        sys.exit(1)

    logging.basicConfig(level=logging.DEBUG)

    Syncer(src, dst, args.delete).sync()
