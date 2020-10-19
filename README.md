btr-backup
===

`btr-backup` is some scripts to manage sets of [btrfs](https://btrfs.wiki.kernel.org/) snapshots. It's heavily influenced by [bontmia](https://github.com/mape2k/bontmia), but instead of using `cp` and `rsync` it's based on btrfs snapshots.

It can create daily snapshots of btrfs subvolumes, and sync them to a remote location. There it's possible to prune the number of snapshots, and only keep a configurable number of daily, weekly and monthly backups.

Scripts
---
- `backup-btrfs.sh`: Create daily snapshots, and sync to central location.
- `cleaner.py`: Remove old snapshots.
- `snapshot_sync.py`: Sync sets of snapshots between local or remote filesystems.
