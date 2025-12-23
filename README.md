btr-backup
===

> **This project has moved to [Codeberg](https://codeberg.org/kql/btr-backup).** This repository is archived and will no longer be updated.

`btr-backup` is a collection of scripts for managing sets of [btrfs](https://btrfs.wiki.kernel.org/) snapshots. It’s inspired by [bontmia](https://github.com/mape2k/bontmia), but instead of using `cp` and `rsync`, it relies entirely on btrfs snapshots for efficiency and consistency.

It can create daily snapshots of btrfs subvolumes and synchronize them to a remote location. At the destination, you can prune old snapshots according to a retention policy, keeping a configurable number of daily, weekly, and monthly backups.

Scripts
---
- `backup-btrfs.sh`: Creates daily snapshots and synchronizes them to a central location.
- `cleaner.py`: Removes old snapshots based on retention settings.
- `snapshot_sync.py`: Synchronizes snapshot sets between local and remote filesystems.
