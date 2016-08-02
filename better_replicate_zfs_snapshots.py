#!/usr/bin/env python
"""Usage: replicate_zfs_snapshots.py <src-host> <src-filesystem> <dest-host> <dest-filesystem> [-h | --help] [-v | --verbose | -q | --quiet] [-n | --dry-run] [--delete]

-n --dry-run
--delete        Delete snapshots not in source filesystem.
-h --help       Show this.
-v --verbose    Log more than default.
-q --quiet      Log less than default.

Example:
  replicate_zfs_snapshots.py sydney tank-microserver-0-mirror-2tb/share/kapsia localhost tank/sydney-tank-replica/share/kapsia
  replicate_zfs_snapshots.py localhost local-fast-tank-machine0/Virtual-Machines/VirtualBox/win7-new localhost local-4tb-tank-machine0/replication-mirrors/local-fast-tank-machine0/Virtual-Machines/VirtualBox/win7-new

This script synchronizes ZFS snapshots between two (possibly remote)
filesystems.

Snapshots present on source filesystem, but not on destination
filesystem will be copied to destination filesystem. If '--delete'
flag is passed, automatic ('zfs-auto-snap') snapshots not on source
filesystem will be removed from destination filesystem .

The script looks for the most recent snapshot in common, and does an
incremental send/receive from that to the most recent source snapshot.

If there is no snapshot in common between the two filesystems, the
script transfers the oldest snapshot from the destination filesystem,
and then recurses (to transfer an incremental snapshot from the that
snapshot to most recent source snapshots.)

If the source filesystems has child filesystems, they will also be
replicated to any corresponding children on the destination
filesystems. If a child filesystem exists on the source, but not the
destination, it will not be replicated, and a warning will be printed
to stderr.

If you run this script from the crontab, you may want to use cronic:
(http://habilis.net/cronic/) to monitor the output.

Script assumes that:
* passwordless ssh is set up between host running this script
  and the remote host.
* the user the ssh connection logs in to on the remote host is allowed
  password-less sudo on read-only commands (see /etc/sudoers.d/zfs).
* The user running this script is allowed to use destructive ZFS
  commands: destroy, zfs receive, etc.

This seems to work for me, but it could be improved/extended. Ideas:

* Another script to check that the two filesystems are actually
  synchronised correctly. Basically: compare/diff the sets of
  snapshots available on two different filesystems.

* Another script to trim auto-snapshots on destination.

  We no longer remove automatic snapshots from destination unless
  specifically asked to. This means we can delete a large snapshot on
  source without it being removed from destination ('backup').

  This will mean that automatic snapshots will tend to pile up on
  destination. Would be good to remove zfs-auto-snapshots where we
  have too many of them (e.g. > 8 weekly, etc).

* Another script to perfectly replicate the set of snapshots between
  two filesystems. This script just finds the last common snapshot and
  replicates snapshots after that one. We could make something that
  replicates all snapshots.

* Should investigate using the -R option to "zfs send" to transfer
  snapshots recursively instead of doing a non-recursive send/receive
  for each child of the source filesystem.

* This script should probably have an option to specify whether or not
  to attempt replication of any children of the source filesystem.

"""

from docopt import docopt

import subprocess, sys, fcntl

verbose = False
quiet = False

class ZfsReplicationNoRemoteSnapshots(Exception):
    pass

class ZfsReplicationNoSnapshotsInCommon(Exception):
    pass

def maybe_ssh(host):
    if (host == 'localhost'):
        ## no need to ssh host @ start of command - empty string
        return ""
    ##else
    ## will need the ssh in there
    return "ssh -C {}".format(host)

def run_query(cmd):
    """Run a shell command, return list of lines output."""
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True).split('\n')
    except subprocess.CalledProcessError as e:
        print >> sys.stderr, "    Exception running command: ", cmd
        print >> sys.stderr, "    Output from failed command:"
        for line in e.output.split('\n'):
            print >> sys.stderr, "      ", line
        return []

def snapshots_in_creation_order(filesystem, host='localhost'):
    "Return list of snapshots on FILESYSTEM in order of creation."
    result = []
    cmd = "{} sudo zfs list -o name -r -t snapshot -s creation {}".format(maybe_ssh(host), filesystem)
    lines = run_query(cmd)
    snapshot_prefix = filesystem + "@"
    for line in lines:
        if line.startswith(snapshot_prefix):
            result.append(line)
    return result

def strip_filesystem_name(snapshot_name):
    """Given the name of a snapshot, strip the filesystem part.

    We require (and check) that the snapshot name contains a single
    '@' separating filesystem name from the 'snapshot' part of the name.
    """
    assert snapshot_name.count("@")==1
    return snapshot_name.split("@")[1]

def maybe_run_command(cmd, dry_run=True):
    if dry_run:
        print "   Would execute: {}".format(cmd)
    else:
        if not quiet:
            print "   Executing: {}".format(cmd)
        text = run_query(cmd)
        if verbose:
            if not text:
                print "    <no output>"
            else:
                print "    output:"
                for line in text:
                    print "     {}".format(line)

def dependent_zfs_filesystems(filesystem, host='localhost'):
    "Return list of filsystems under FILESYSTEM recursively."
    result = []
    cmd = "{} sudo zfs list -r -o name {}".format(maybe_ssh(host), filesystem)
    lines = run_query(cmd)
    for line in lines:
        if line.startswith(filesystem+"/"):
            sub_filesystem = line[len(filesystem+"/"):]
            if sub_filesystem:
                result.append(sub_filesystem)
    return result

def replicate_snapshots(src_host, src_filesystem,
                        dest_host, dest_filesystem,
                        dry_run=True,
                        delete_snapshots_not_in_src=False):
    """Synchronise ZFS snapshots from source filesystem to a destination filesystem."""

    if verbose:
        print "   Started. source-host: {}, source-fs: {}, dest-host: {}, dest-fs: {}, dry-run: {}".format(
            src_host, src_filesystem,
            dest_host, dest_filesystem,
            dry_run)

    dest_snapshots = snapshots_in_creation_order(dest_filesystem, dest_host)
    src_snapshots = snapshots_in_creation_order(src_filesystem, src_host)

    if not src_snapshots:
        raise ZfsReplicationNoRemoteSnapshots("No source snapshots to replicate",
                                              "src-host: {}".format(src_host),
                                              "src-filesystem: {}".format(src_filesystem))

    src_set = set(map(strip_filesystem_name, src_snapshots))
    dest_set = set(map(strip_filesystem_name, dest_snapshots))

    last_common_snapshot = next((s for s in reversed(src_snapshots) if strip_filesystem_name(s) in dest_set), None)

    extra_snapshots_in_dest = [s for s in dest_snapshots if not strip_filesystem_name(s) in src_set]

    last_src_snapshot = src_snapshots[-1]

    if verbose:
        print "Source snapshots:"
        for snapshot in src_snapshots:
            print " {}".format(snapshot)
        print "Dest snapshots:"
        for snapshot in dest_snapshots:
            print " {}".format(snapshot)
        print "Last common snapshot: {}".format(last_common_snapshot)
        print "Last source snapshot: {}".format(last_src_snapshot)

    if extra_snapshots_in_dest:
        if verbose:
            print "Present in destination, but not in source:"
        for snapshot in extra_snapshots_in_dest:
            if verbose:
                print " {}".format(snapshot)
            snapshot_name = strip_filesystem_name(snapshot)
            if snapshot_name.startswith('zfs-auto-snap'):
                if delete_snapshots_not_in_src:
                    if not quiet:
                        print "Deleting expired auto-snapshot {} from destination.".format(snapshot)
                        maybe_run_command("{} sudo zfs destroy {}".format(maybe_ssh(dest_host), snapshot),
                                          dry_run)
                else:
                    if not quiet:
                        print "NOT deleting expired auto-snapshot {} from destination.".format(snapshot)
            else:
                if not quiet:
                    print "Leaving manual snapshot {} on destination.".format(snapshot)

    if not dest_snapshots:
        first_src_snapshot = src_snapshots[0]
        if not quiet:
            print "No snapshots exist on destination. Transferring oldest snapshot: '{}' from source.".format(
                strip_filesystem_name(first_src_snapshot))
        maybe_run_command(("{} sudo zfs send {} ".format(maybe_ssh(src_host),
                                                              first_src_snapshot)
                                + "| {} sudo zfs receive -F {}".format(maybe_ssh(src_host),
                                                                       dest_filesystem)),
                              dry_run)
        if dry_run:
            print "Would then call again recursively but will not show that output in dry run"
            return
        else:
            ## Live run - recurse after transferring initial snapshot
            if verbose:
                print "Have transferred initial snapshot {}. Will recurse to transfer remaining snapshots."
            return replicate_snapshots(src_host, src_filesystem,
                                       dest_host, dest_filesystem,
                                       dry_run = False,
                                       delete_snapshots_not_in_src = delete_snapshots_not_in_src)

    if not last_common_snapshot:
        raise ZfsReplicationNoSnapshotsInCommon("No snapshots in common. ",
                                                "src-host: {}".format(src_host),
                                                "src_filesystem: {}".format(src_filesystem),
                                                "dest-host: {}".format(dest_host),
                                                "dest_filesystem: {}".format(dest_filesystem))
    if last_src_snapshot == last_common_snapshot:
        if not quiet:
            print "    Destination up to date. Last source snapshot '{}' already on destination filesystem {}:{}.".format(
                strip_filesystem_name(last_src_snapshot), dest_host, dest_filesystem)
        return
    maybe_run_command(("{} sudo zfs send -I {} {} ".format(maybe_ssh(src_host),
                                                           last_common_snapshot, last_src_snapshot)
                       + "| {} sudo zfs receive -F {}".format(maybe_ssh(dest_host), dest_filesystem)),
                      dry_run)

def replicate_snapshots_recursively(src_host, src_filesystem,
                                    dest_host, dest_filesystem,
                                    dry_run=True,
                                    delete_snapshots_not_in_src=False):
    print "Copying ZFS snapshots from {}:{} to {}:{} recursively".format(src_host, src_filesystem,
                                                                         dest_host, dest_filesystem)
    replicate_snapshots(src_host, src_filesystem,
                        dest_host, dest_filesystem,
                        dry_run=dry_run,
                        delete_snapshots_not_in_src=delete_snapshots_not_in_src)

    src_subfilesystems = dependent_zfs_filesystems(src_filesystem, src_host)
    dest_subfilesystems = dependent_zfs_filesystems(dest_filesystem, dest_host)
    for filesystem in src_subfilesystems:
        if filesystem in dest_subfilesystems:
            try:
                print "  Copying ZFS snapshots from {}:{}/{} to {}:{}/{}".format(src_host,
                                                                                 src_filesystem,
                                                                                 filesystem,
                                                                                 dest_host,
                                                                                 dest_filesystem,
                                                                                 filesystem)
                replicate_snapshots(src_host, "{}/{}".format(src_filesystem, filesystem),
                                    dest_host, "{}/{}".format(dest_filesystem, filesystem),
                                    dry_run=dry_run,
                                    delete_snapshots_not_in_src=delete_snapshots_not_in_src)
            except ZfsReplicationNoRemoteSnapshots as e:
                print >> sys.stderr, "    Exception: ", e[0]
                print >> sys.stderr, "    ", e[1], e[2]
            except ZfsReplicationNoSnapshotsInCommon as e:
                print >> sys.stderr, "    Exception: ", e[0]
                print >> sys.stderr, "    ", e[1], e[2]
                print >> sys.stderr, "    ", e[3], e[4]
        else:
            print >> sys.stderr, "  destination filesystem {}:{}/{} does not exist. Not replicating {}:{}/{}".format(
                dest_host,
                dest_filesystem,
                filesystem,
                src_host,
                src_filesystem,
                filesystem,)

if __name__ == '__main__':
    arguments=docopt(__doc__)

    if arguments['--verbose']:
        verbose = True
        quiet = False
        print 'Arguments: {}'.format(arguments)
    if arguments['--quiet']:
        verbose = False
        quiet = True

    program_name = 'replicate_zfs_snapshots.py'

    try:
        if not quiet:
            print "{}".format(program_name)
            print "  src-host:       ", arguments['<src-host>']
            print "  src-filesystem: ", arguments['<src-filesystem>']
            print "  dest-host:      ", arguments['<dest-host>']
            print "  dest-filesystem:", arguments['<dest-filesystem>']
            print "  dry-run:        ", arguments['--dry-run']
            print "  delete:         ", arguments['--delete']

            ## Check we're the only copy running
            ## http://stackoverflow.com/questions/380870/python-single-instance-of-program/1265445#1265445
            pid_file = '/tmp/{}.pid'.format(program_name)
            fp = open(pid_file, 'w')
            try:
                fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError:
                # another instance is running
                print >> sys.stderr, "Another instance of {} is running. Exiting.".format(program_name)
                sys.exit(0)

        replicate_snapshots_recursively(arguments['<src-host>'], arguments['<src-filesystem>'],
                                        arguments['<dest-host>'], arguments['<dest-filesystem>'],
                                        dry_run=arguments['--dry-run'],
                                        delete_snapshots_not_in_src=arguments['--delete'])
    except Exception as e:

        print >> sys.stderr, "Unhandled exception: {}: {}".format(type(e), e)

    if not quiet:
        print "Finished."
