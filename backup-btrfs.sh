#! /bin/bash
set -e -o pipefail

HOSTNAME=$(hostname -s)
BIN_DIR=$(realpath $(dirname $0))
DATE=$(date -I)
ALLOW_REMOTE=false
KEEP=7
[ -f /etc/btr-backup.conf ] && . /etc/btr-backup.conf

function error {
    echo $1 >&2
    return 1
}

function is_local {
    local host=$1

    for TRY in $(seq 5); do
        ADDR=$(dig +short $host A $host AAAA)
        if [ $? -eq 0 ]; then 
            for A in $ADDR; do
                if ROUTE=$(ip route get $A); then
                    grep -q ' via ' <<< $ROUTE || return 0
                fi
            done

            error "$host is not local."
        fi
        echo "Attempt to resolve $host failed, sleeping and trying again."
        sleep 10
    done

    error "Could not resolve $host"
}

function wait_for_online_host {
    local host=$1

    for TRY in $(seq 10); do
        if ping -4 -qc 1 $host > /dev/null 2>&1; then
            return 0
        elif ping -6 -qc 1 $host > /dev/null 2>&1; then
            return 0
        else
            echo "$host not available, sleeping..."
            sleep 10
        fi
    done

    error "Giving up, $host not available."
}

function check_params {
    [ -n "$DESTINATION" ] || error "Destination is not set, $DEST"
    [ -d "$SOURCE" ] || error "Source directory does not exist, $SOURCE"
    [ $KEEP -gt 0 ] || error "Number of snapshots to keep must be more than 0, $KEEP"
}

function check_prereq {
    for BIN in $*; do
        command -v $BIN > /dev/null 2>&1 || error "Required executable not found, install '$BIN' and try again."
    done
}

function create_snapshots {
    cd "$SOURCE"
    mkdir -p "snapshots/$HOSTNAME"

    for SUBVOL in *; do
	    [ $SUBVOL = 'snapshots' ] && continue

	    CURRENT_SNAPSHOT="snapshots/$HOSTNAME/$SUBVOL-$DATE"
	    btrfs subvolume snapshot -r $SUBVOL $CURRENT_SNAPSHOT
    done
}

function sync_snapshots {
    IFS=':'
    local dest_parts=($DESTINATION)
    unset IFS

    if [ ${#dest_parts[@]} -gt 1 ]; then
        $ALLOW_REMOTE || is_local ${dest_parts[0]}
        wait_for_online_host ${dest_parts[0]}
    fi

    $BIN_DIR/snapshot_sync.py "$SOURCE/snapshots" "$DESTINATION"
}

function remove_old_snapshots {
    for SUBVOL in *; do
        SNAPSHOTS=($(printf '%s\n' snapshots/$HOSTNAME/$SUBVOL-* | sort -r))
	    for ((I=$KEEP; I<${#SNAPSHOTS[@]}; I++)); do
		    btrfs sub delete ${SNAPSHOTS[$I]}
	    done
    done
}

ARGV=($@)
for ((I=0; I<$#; I++)); do
    case ${ARGV[$I]} in
        -d|--destination)
            DESTINATION=${ARGV[((++I))]}
            ;;
        -k|--keep)
            KEEP=${ARGV[((++I))]}
            ;;
        -r|--allow-remote-destination)
            ALLOW_REMOTE=true
            ;;
        -s|--source)
            SOURCE=${ARGV[((++I))]}
            ;;
        *)
            error Usage: $(basename $0) [-s SOURCE] [-d DESTINATION]
            ;;
    esac
done

check_params
check_prereq btrfs dig ip ping
create_snapshots
sync_snapshots
remove_old_snapshots
