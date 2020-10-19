#! /usr/bin/env python3

import unittest
import os
import subprocess
from unittest.mock import call, patch, MagicMock
from snapshot_sync import BackupNode, RemoteBackupNode, Syncer


class BackupNodeTest(unittest.TestCase):

    def setUp(self):
        self._node = BackupNode('/tmp', False)

    def test_ls(self):
        with patch.object(
                os,
                'listdir',
                return_value=['unit', 'test']
        ) as mock_listdir:
            self.assertEqual({'unit', 'test'}, self._node.ls('snaps'))

        mock_listdir.assert_called_once_with('/tmp/snaps')

    def test_mkdir(self):
        with patch.object(os, 'makedirs') as mock_makedirs:
            self._node.mkdir(['www', 'mail'])

        mock_makedirs.assert_has_calls(
            [
                call('/tmp/www', exist_ok=True),
                call('/tmp/mail', exist_ok=True)
            ]
        )

    def test_get_snapshot_dict(self):
        dirlist = {
            'home-2015-09-30',
            'home-2015-10-31',
            'home-2015-11-30',
            'home-2015-12-31',
            'images-2015-09-30',
            'images-2015-10-31',
            'images-2015-11-30',
            'images-2015-12-31',
            'root-2015-09-30',
            'root-2015-10-31',
            'root-2015-11-30',
            'root-2015-12-31'
        }

        with patch.object(self._node, 'ls', return_value=dirlist) as mock_ls:
            result = self._node.get_snapshot_dict('obelix')
            dates = {'2015-09-30', '2015-10-31', '2015-11-30', '2015-12-31'}
            self.assertEqual(dates, result['home'])
            self.assertEqual(dates, result['images'])
            self.assertEqual(dates, result['root'])

        mock_ls.assert_called_once_with('obelix')

    def test_send_no_parent(self):
        with patch.object(subprocess, 'Popen') as mock_Popen:
            self._node.send('obelix/home', '2016-02-25')

        mock_Popen.assert_called_once_with(
            ['btrfs', 'send', '/tmp/obelix/home-2016-02-25'],
            stdout=subprocess.PIPE
        )

    def test_send_with_parent(self):
        with patch.object(subprocess, 'Popen') as mock_Popen:
            self._node.send('obelix/home', '2016-02-25', '2016-02-24')

        mock_Popen.assert_called_once_with(
            [
                'btrfs',
                'send',
                '-p',
                '/tmp/obelix/home-2016-02-24',
                '/tmp/obelix/home-2016-02-25'
            ],
            stdout=subprocess.PIPE
        )

    def test_receive(self):
        with patch.object(subprocess, 'check_call') as mock_check_call:
            self._node.receive('obelix', 'stdout')

        mock_check_call.assert_called_once_with(
            ['btrfs', 'receive', '/tmp/obelix'],
            stdin='stdout'
        )

    def test_receive_verbose(self):
        node = BackupNode('/tmp', True)
        with patch.object(subprocess, 'check_call') as mock_check_call:
            node.receive('obelix', 'stdout')

        mock_check_call.assert_called_once_with(
            ['btrfs', 'receive', '-vv', '/tmp/obelix'],
            stdin='stdout'
        )

    def test_remove(self):
        with patch.object(subprocess, 'check_call') as mock_check_call:
            self._node.remove('obelix/home', {'2016-02-26'})

        mock_check_call.assert_called_once_with(
            ['btrfs', 'subvolume', 'delete', '/tmp/obelix/home-2016-02-26']
        )

    def test_remove_no_dates(self):
        with patch.object(subprocess, 'check_call') as mock_check_call:
            self._node.remove('obelix/home', {})

        mock_check_call.assert_not_called()


class RemoteBackupNodeTest(unittest.TestCase):

    def setUp(self):
        self._node = RemoteBackupNode('host', '/tmp', False)

    def test_ls(self):
        ps = MagicMock()
        ps.communicate.return_value = [b'unit\ntest\n', None]
        with patch.object(subprocess, 'Popen', return_value=ps) as mock_Popen:
            self.assertEqual({'unit', 'test'}, self._node.ls('snaps'))

        mock_Popen.assert_called_once_with(
            [
                'ssh',
                'host',
                'ls',
                '/tmp/snaps'
            ],
            stdout=subprocess.PIPE
        )
        ps.communicate.assert_called_once_with()

    def test_mkdir(self):
        with patch.object(subprocess, 'check_call') as mock_check_call:
            self._node.mkdir(['mail', 'www'])

        mock_check_call.assert_called_once_with(
            ['ssh', 'host', 'mkdir', '-p', '/tmp/mail', '/tmp/www']
        )

    def test_send_no_parent(self):
        with patch.object(subprocess, 'Popen') as mock_Popen:
            self._node.send('obelix/home', '2016-02-25')

        mock_Popen.assert_called_once_with(
            ['ssh', 'host', 'btrfs', 'send', '/tmp/obelix/home-2016-02-25'],
            stdout=subprocess.PIPE
        )


class SyncerTest(unittest.TestCase):
    def setUp(self):
        self._src = MagicMock()
        self._dst = MagicMock()

    def test_sync(self):
        self._src.ls.return_value = {'obelix'}
        self._src.get_snapshot_dict.return_value = {
            'home': {'2015-02-03', '2015-02-04'},
            'root': {'2015-02-03'}
        }
        self._dst.get_snapshot_dict.return_value = {
            'home': {'2015-02-02', '2015-02-03'}
        }

        syncer = Syncer(self._src, self._dst, True)
        syncer.sync()

        self._src.ls.assert_called_once_with('')
        self._src.get_snapshot_dict.assert_called_once_with('obelix')
        self._dst.get_snapshot_dict.assert_called_once_with('obelix')

        self._src.send.assert_has_calls(
            [
                call('obelix/home', '2015-02-04', '2015-02-03'),
                call().wait(),
                call('obelix/root', '2015-02-03', None),
                call().wait()
            ],
            any_order=True
        )

        self._dst.receive.assert_has_calls(
            [
                call('obelix', self._src.send().stdout),
                call('obelix', self._src.send().stdout)
            ]
        )

        self._dst.remove.assert_called_once_with('obelix/home', {'2015-02-02'})

    def test_sync_without_delete(self):
        self._src.ls.return_value = {'obelix'}
        self._src.get_snapshot_dict.return_value = {
            'home': {'2015-02-03', '2015-02-04'},
            'root': {'2015-02-03'}
        }
        self._dst.get_snapshot_dict.return_value = {
            'home': {'2015-02-02', '2015-02-03'}
        }

        syncer = Syncer(self._src, self._dst, False)
        syncer.sync()

        self._dst.remove.assert_not_called()

    def test_sync_with_no_diff(self):
        self._src.ls.return_value = {'obelix'}
        self._src.get_snapshot_dict.return_value = {
            'home': {'2015-02-03', '2015-02-04'},
            'root': {'2015-02-03'}
        }
        self._dst.get_snapshot_dict.return_value = {
            'home': {'2015-02-03', '2015-02-04'},
            'root': {'2015-02-03'}
        }

        syncer = Syncer(self._src, self._dst, True)
        syncer.sync()

        self._src.send.assert_not_called()
        self._dst.receive.assert_not_called()
        self._dst.remove.assert_has_calls(
            [
                call('obelix/root', set()),
                call('obelix/home', set())
            ],
            any_order=True
        )


if __name__ == '__main__':
    unittest.main()
