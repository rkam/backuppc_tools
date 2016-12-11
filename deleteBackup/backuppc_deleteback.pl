#!/bin/bash
#
#   http://sourceforge.net/apps/mediawiki/backuppc/index.php?title=How_to_delete_backups
#
#this script contributed by Matthias Meyer
#note that if your $Topdir seems to be wrong (because it is empty), the script will ask you
#the new location.
#
#10/2009 JJK: Modified by Jeffrey J. Kosowsky
#	add --nightly
#	check if user eguals root or backuppc
#11/2009 MMT: Modified by Matthias Meyer
#	add --remove
#
#10/2010 MMT: Modified by Matthias Meyer
#	check /etc/backuppc as well as /etc/BackupPC
#
usage="\
Usage: $0 [-c <client> [-d <backupnumber> -b <before data> -r [-f]] | [-l]] | [-n]

delete specified backups.
Attention!
 If a full backup will be deleted all depending incremantal backups
 will also be deleted.

	-c <client>	- client machine for which the backup was made
	-d <number>	- number of Backup which should be deleted
	-b <date>	- delete all backups before this date (YYYY-MM-DD)
	-r		- remove the entire client
	-f		- force to run Backuppc_nightly so the space got free afterwards
	-n		- only run Backuppc_nightly
	-l		- list the backups which made for <client>
	-n		- Just run BackupPC_nightly (even if no backups deleted)
	-h		- this help

Example:
list backups of <client>
 $0 -c <name of the client which was backed up> -l

remove backup #3 from <client>
 $0 -c <name of the client which was backed up> -d 3

remove all backups before 2007-07-02 from <client>
 $0 -c <name of the client which was backed up> -b 2007-07-02
"
declare -i len		# try "typeset" instead declare if you have problems
declare TopDir LogDir	# directories, evaluated by /etc/backuppc/config.pl
declare configDir

function run_BackupPC_nightly ()
{
#	path=${0%/BackupPC*}
	path=${0%/backuppc*}
	if [ ! -e $path/BackupPC_serverMesg ]; then path="/usr/share/backuppc/bin"; fi
	if [ ! -e $path/BackupPC_serverMesg ]; then path="/usr/share/BackupPC"; fi
	if [ ! -e $path/BackupPC_serverMesg ]; then path="/usr/share/BackupPC/bin"; fi
	echo "Running BackupPC_nightly..."
	$path/BackupPC_serverMesg BackupPC_nightly run
	$path/BackupPC_serverMesg log I honestly apologize for the inconvenience
	echo `date "+%Y-%m-%d %T"` BackupPC_deleteBackup BackupPC_nightly politely scheduled via daemon >> $LogDir/LOG
}

#JJK: NOTE NFS shares may only be accessible by 'backuppc' (not even by root if not 'no_root_squash') so run as user 'backuppc'
[ "`id -un`" != "backuppc" -a "`id -un`" != "root" ] && echo "Must be either 'root' or 'backuppc' to run" && exit
[ "`id -un`" != "backuppc" ] && exec su backuppc -s /bin/bash -c "$0 $*"

ANY=
while test $# -gt 0; do
	case "$1" in
	-c | --client )
		shift; client=`echo $1 | tr "[:upper:]" "[:lower:]"`; shift
		;;
	-b | --before )
		## Should have at least a basic date check!	VERY BAD (deletes all)
		if [ -z $ANY ]; then
			ANY="$1"
		else
			echo "Incompatable options $ANY and [ -b | --before ]"
		fi
		shift; bDate=$1; shift
		;;
	-d | --delete )
		## Should have at least a numerical check
		if [ -z $ANY ]; then
			ANY="$1"
		else
			echo "Incompatable options $ANY and [ -d | --delete ]"
		fi
		shift; bNumber=$1; shift
		;;
	-r | --remove )
		if [ -z $ANY ]; then
			ANY="$1"
		else
			echo "Incompatable options $ANY and [ -r | --remove ]"
		fi
		entire="true"; shift
		;;
	-f | --force )
		nightly="true"; shift
		;;
	-n | --nightly )
		if [ -z $ANY ]; then
			ANY="$1"
		else
			echo "Incompatable options $ANY and [ -n | --nightly ]"
		fi
		nightlyonly="true"; shift
		;;
	-l | --list )
		list="true"; shift
		;;
	* | -h | --help)
		echo "$usage"
		exit 0
		;;
	esac
done

if [ -z $nightlyonly ] && ( [ -z "$client" ] || [ -z $list ] && [ -z $bNumber ] && [ -z $bDate ] && [ -z $entire ] )
then
	echo "$usage"
	exit 0
fi

#MMT: both lines have to be deleted after test ;-)
#read -p "not tested yet. Do you want to continue anyway [Y/N]? " answer
#if [ "$answer" != "Y" ] && [ "$answer" != "y" ]; then exit 1; fi

WHAT=
if [ -z "$list$nightlyonly" ]; then
	DOUBLECHECK=
	if [ ! -z $bDate ]; then
		DOUBLECHECK=1
		WHAT="DELETE ALL BUILDS on $client before $bDate"
	elif [ ! -z $bNumber ]; then
		WHAT="Delete build #$bNumber on $client"
	elif [ ! -z $entire ]; then
		DOUBLECHECK=1
		WHAT="DELETE $client COMPLETELY"
	else
		echo "Don't understand (how to print confirmation for) options"
		exit 1
	fi

	if [ ! -z $nightly ]; then
		WHAT="$WHAT and force a run of nightly"
	fi
	read -p "$WHAT. Continue [Ny]? " answer
	if [ "$answer" != "Y" ] && [ "$answer" != "y" ]; then exit 1; fi

	if [ ! -z $DOUBLECHECK ]; then
		read -p "Are you sure [Ny]? " answer
		if [ "$answer" != "Y" ] && [ "$answer" != "y" ]; then exit 1; fi
	fi
fi

if [ -e /etc/backuppc/config.pl ]; then configDir="/etc/backuppc"
elif [ -e /etc/BackupPC/config.pl ]; then configDir="/etc/BackupPC"
else
	echo "BackupPCs config.pl not found"
	exit 1
fi

TopDir=`grep $Conf{TopDir} $configDir/config.pl | sed -e '/^#/d' | awk '{print $3}'`
len=${#TopDir}-3
TopDir=${TopDir:1:len}

ls $TopDir/pc > /dev/null 2>&1
while [ $? != 0 ]
do
	read -p "examined $TopDir seems wrong. What is TopDir ? " TopDir
	ls $TopDir/pc > /dev/null 2>&1
done

LogDir=`grep $Conf{LogDir} $configDir/config.pl | sed -e '/^#/d' | awk '{print $3}'`
len=${#LogDir}-3
LogDir=${LogDir:1:len}
if [ ! -e $LogDir/LOG ]; then LogDir="$TopDir/log"; fi

if [ ! -z $nightlyonly ]
then
	run_BackupPC_nightly
	exit 0
elif [ ! -z $entire ]
then
# the entire host should be removed. Therefore we will remove him
# from the host list as well as his client configuration
	while read CLine
	do
		host=`echo $CLine | awk '{print $1}' | tr "[:upper:]" "[:lower:]"`
		if [ "$host" != "$client" ]
		then
			echo "$CLine" >> $configDir/hosts.new
		fi
	done < $configDir/hosts
	chown --reference=$configDir/hosts $configDir/hosts.new
	chmod --reference=$configDir/hosts $configDir/hosts.new
	mv $configDir/hosts.new $configDir/hosts > /dev/null 2>&1
	rm -f $configDir/$client.pl* > /dev/null 2>&1
fi

ls $TopDir/pc/$client > /dev/null 2>&1
if [ $? != 0 ]
then
	echo "$client have no backups"
	exit 1
fi

if [ ! -z $list ]
then
	while read CLine
	do
		BackupNumber=`echo $CLine | awk '{print $1}'`
		BackupType=`echo $CLine | awk '{print $2}'`
		BackupTime=`stat -c "%y" $TopDir/pc/$client/$BackupNumber/backupInfo | awk '{print $1}'`
		echo "BackupNumber $BackupNumber - $BackupType-Backup from $BackupTime"
	done < $TopDir/pc/$client/backups
	exit 0
fi

if [ ! -z $bNumber ] && [ ! -e $TopDir/pc/$client/$bNumber ]
then
	echo "Backup Number $bNumber does not exist for client $client"
	exit 1
fi

if [ -z $entire ] && [ -e $TopDir/pc/$client/backups ]
then
	delete2full="false"
	rm -f $TopDir/pc/$client/backups.new > /dev/null 2>&1
	touch $TopDir/pc/$client/backups.new
	while read CLine
	do
		BackupNumber=`echo $CLine | awk '{print $1}'`
		BackupTime=`stat -c "%y" $TopDir/pc/$client/$BackupNumber/backupInfo | awk '{print $1}'`
		BackupType=`echo $CLine | awk '{print $2}'`
		if [ $BackupType == "full" ]
		then
			delete2full="false"
		fi
		if [ "$BackupTime" \< "$bDate" ] || [ $BackupNumber == "$bNumber" ] || [ $delete2full == "true" ]
		then
			if [ $BackupType == "full" ]
			then
				if [ $delete2full == "false" ]
				then
					delete2full="true"
				else
					delete2full="false"
				fi
			fi
			bNumber=$BackupNumber
			echo "remove $TopDir/pc/$client/$bNumber"
			rm -fr $TopDir/pc/$client/$bNumber > /dev/null 2>&1
			rm -f $TopDir/pc/$client/XferLOG.$bNumber > /dev/null 2>&1
			rm -f $TopDir/pc/$client/XferLOG.$bNumber.z > /dev/null 2>&1
			echo "`date +\"%Y-%m-%d %T\"` BackupPC_deleteBackup $TopDir/pc/$client/$bNumber deleted" >> $LogDir/LOG
			echo "`date +\"%Y-%m-%d %T\"` BackupPC_deleteBackup remove backup $bNumber" >> $TopDir/pc/$client/LOG.`date +%m%Y`
		fi
		if [ "$BackupNumber" != "$bNumber" ]
		then
			echo "$CLine" >> $TopDir/pc/$client/backups.new
		fi
	done < $TopDir/pc/$client/backups
	chown --reference=$TopDir/pc/$client/backups $TopDir/pc/$client/backups.new > /dev/null 2>&1
	chmod --reference=$TopDir/pc/$client/backups $TopDir/pc/$client/backups.new > /dev/null 2>&1
	mv $TopDir/pc/$client/backups.new $TopDir/pc/$client/backups > /dev/null 2>&1
	echo "`date +\"%Y-%m-%d %T\"` BackupPC_deleteBackup $TopDir/pc/$client/backups updated" >> $LogDir/LOG
elif [ ! -z $entire ]
then
	rm -fr $TopDir/pc/$client > /dev/null 2>&1
	echo "`date +\"%Y-%m-%d %T\"` BackupPC_deleteBackup $TopDir/pc/$client entirely removed" >> $LogDir/LOG
fi
if [ ! -z $nightly ]
then
	run_BackupPC_nightly
fi

exit $?
