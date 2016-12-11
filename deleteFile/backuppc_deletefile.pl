#!/usr/bin/perl
#============================================================= -*-perl-*-
# vim:et ts=4 sw=4 softtabstop=4
#
# http://sourceforge.net/apps/mediawiki/backuppc/index.php?title=BackupPC_DeleteFile
#
# BackupPC_deleteFile.pl: Delete one or more files/directories from
#                         a range of hosts, backups, and shares
#
# DESCRIPTION
#   See below for detailed description of what it does and how it works
#   
# AUTHOR
#   Jeff Kosowsky
#
# COPYRIGHT
#   Copyright (C) 2008, 2009  Jeff Kosowsky
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation; either version 2 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software
#   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#
#========================================================================
#
# Version 0.1.5, released Dec 2009
#
#========================================================================
# CHANGELOG
#     0.1 (Nov 2008)   - First public release
#     0.1.5 (Dec 2009) - Minor bug fixes
#                        Ability to abort/skip/force hard link deletion 
#========================================================================
# Program logic is as follows:
#
# 1. First construct a hash of hashes of 3 arrays and 2 hashes that
#    encapsulates the structure of the full and incremental backups
#    for each host. This hash is called:
#    %backupsHoHA{<hostname>}{<key>} 
#    where the keys are: "ante", "post", "baks", "level", "vislvl"
#    with the first 3 keys having arrays as values and the final 2
#    keys having hashes as values. This pre-step is done since this
#    same structure can be re-used when deleting multiple files and
#    dirs (with potential wilcards) across multiple shares, backups,
#    and hosts. The component arrays and hashes which are unique per
#    host are constructed as folows:
#     
#    - Start by constructing the simple hash %LevelH whose keys map
#      backup numbers to incremental backup levels based on the
#      information in the corresponding backupInfo file.
#
#    - Then, for each host selected, determine the list (@Baks) of
#      individual backups from which files are to be deleted based on
#      bakRange and the actual existing backups.
#  
#    - Based on this list determine the list of direct antecedent
#      backups (@Ante) that have strictly increasing backup levels
#      starting with the previous level 0 backup. This list thus
#      begins with the previous level zero backup and ends with the
#      last backup before @Baks that has a lower incremental level
#      than the first member of @Baks. Note: this list may be empty if
#      @Baks starts with a full (level 0) backup. Note: there is at
#      most one (and should in general be exactly one) incremental
#      backup per level in this list starting with level 0.
#
#    - Similarly, constuct the list of direct descendants (@Post) of
#      the elements of @Baks that have strictly decreasing backup
#      levels starting with the first incremental backup after @Baks
#      and continuing until we reach a backup whose level is less than
#      or equal to the level of the lowest incremental backup in @Baks
#      (which may or may not be a level 0 backup). Again this list may
#      be empty if the first backup after @Baks is lower than the
#      level of all backups in @Baks. Also, again, there is at most
#      one backup per level.
#
#    - Note that by construction, @Ante is stored in ascending order
#      and furthermore each backup number has a strictly ascending
#      incremental level. Similarly, @Post is stored in strictly
#      ascending order but its successive elements have monotonically
#      non-increasing incremental levels. Also, the last element of
#      @Ante has an incremental level lower than the first element of
#      @Baks and the the last element of @Post has an incremental
#      level higher than the lowest level of @Baks. This is all
#      because anything else neither affects nor is affected by
#      deletions in @Baks. In contrast, note that @Baks can have any
#      any pattern of increasing, decreasing, or repeated incremental
#      levels.
#   
#    - Finally, create the second hash (%VislvlH) which has keys equal
#      to levels and values equal to the most recent backup with that
#      level in @Baks or @Ante that could potentially still be visible
#      in @Post. So, since we need to keep @Post unchanged, we need to
#      make sure that whatever showed through into @Post before the
#      deletions still shows through after deletion. Specifically, we
#      may need to move/copy files (or directories) and set delete
#      attributes to make sure that nothing more or less is visible in
#      @Post after the deletions.
#
# 2. Second, for each host, combine the share names (and/or shell
#    regexs) and list of file names (and/or shell regexs) with the
#    backup ranges @Ante and @Baks to glob for all files that need
#    either to be deleted from @Baks or blocked from view by setting a
#    type=10 delete attribute type.  If a directory is on the list and
#    the remove directory flag (-r) is not set, then directories are
#    skipped (and an error is logged). If any of these files (or dirs)
#    are or contain hard links (either type hard link or a hard link
#    "target") then they are skipped and logged since hard links
#    cannot easily be deleted/copied/moved (since the other links will
#    be affected). Duplicate entries and entries that are a subtree of
#    another entry are rationalized and combined.
#
# 3. Third, for each host and for each relevant candidate file
#    deletion, start going successively through the @Ante, @Baks, and
#    @Post chains to determine which files and attributes need to be
#    deleted, cleared, or copied/linked to @Post.
#
#    - Start by going through, @Ante, in ascending order to construct
#      two visibility hashes. The first hash, %VisibleAnte, is used to
#      mark whether or not a file in @Ante may be visible from @Baks
#      from a higher incremental level. The presence of a file sets
#      the value of the hash while intervening delete type=10 or the
#      lack of a parent directory resets the value to invisible
#      (-1). Later, when we get to @Baks, we will need to make these
#      invisible to complete our deletion effect
#
#      The second hash, %VisibleAnteBaks, (whose construction
#      continues when we iterate through @Baks) determines whether or
#      not a file from @Ante or @Baks was originally visible from
#      @Post. And if a file was visible, then the backup number of
#      that file is stored in the value of the hash. Later, we will
#      use this hash to copy/link files from @Ante and @Baks into
#      @Post to preserve its pre-deletion state.
#
#      Note that at each level, there is at *most* one backup from
#      @Ante that is visible from @Baks (coded by %VisibleAnte) and
#      similarly there is at *most* one backup from @Ante and @Baks
#      combined that is visible from @Post (coded by
#      @VisibleAnteBaks).
#
#   - Next, go through @Baks to mark for deletion any instances of the
#     file that are present. Then set the attrib type to type=10
#     (delete) if %VisibleAnte indicates that a file from @Ante would
#     otherwise be visible at that level. Otherwise, clear the attrib
#     and mark it for deletion. Similarly, once the type=10 type has
#     been set, all higher level element of @Baks can have their file
#     attribs cleared whether they originally indicated a file type or
#     a delete type (i.e. no need for 2 layers of delete attribs).
#
#   - Finally, go through the list of @Post in ascending order. If
#     there is no file and no delete flag present, then use the
#     information coded in %VisibleAnteBaks to determine whether we
#     need to link/copy over a version of the file previously stored
#     in @Ante and/or @Baks (along with the corresponding file attrib
#     entry) or whether we need to set a type=10 delete
#     attribute. Conversely, if originally, there was a type=10 delete
#     attribute, then by construction of @Post, the delete type is no
#     longer needed since the deletion will now occur in one of its
#     antecedents in @Baks, so we need to clear the delete type from
#     the attrib entry.
#
# 4. Finally, after all the files for a given host have been marked
#    for deletion, moving/copying or attribute changes, loop through
#    and execute the changes. Deletions are looped first by host and
#    then by backup number and then alphabetically by filepath.
#
#     Files are deleted by unlinking (recursively via rmtree for
#    directories). Files are "copied" to @Post by first attempting to
#    link to pool (either using an existing link or by creating a new
#    pool entry) and if not successful then by copying. Directories
#    are done recursively. Attributes are either cleared (deleted) or
#    set to type=10 delete or copied over to @Post. Whenever an
#    attribute file needs to be written, first an attempt is made to
#    link to pool (or create a new pool entry and link if not
#    present). Otherwise, the attribute is just written. Empty
#    attribute files are deleted. The attribute writes to filesystem
#    are done once per directory per backup (except for the moves).
#
# 5. As a last step, optionally BackupPC_nightly is called to clean up
#    the pool, provided you set the -c flag and that the BackupPC
#    daemon is running. Note that this routine itself does NOT touch
#    the pool.

# Debugging & Verification:

# This program is instrumented to give you plenty of "output" to see
# all the subtleties of what is being deleted (or moved) and what is
# not. The seemingly simple rules of "inheritance" of incrementals
# hide a lot of complexity (and special cases) when you try to delete
# a file in the middle of a backup chain.
#
# To see what is happening during the "calculate_deletes" stage which
# is the heart of the algorithm in terms of determining what happens
# to what, it is best to use DEBUG level 2 or higher (-d 2). Then for
# every host and for every (unique) top-level file or directory
# scheduled for deletion, you will see the complete chain of how the
# program walks sequentially through @Ante, @Baks, and @Post.
# For each file, you first see a line of form:
#    LOOKING AT: [hostname] [@Ante chain] [@Baks chain] [@Post chain] <file name>
#
# Followed by a triad of lines for each of the backups in the chain of form:
#     ANTE[baknum](baklevel) <file path including host> [file code] [attribute code]
#     BAKS[baknum](baklevel) <file path including host> [file code] [attribute code] [action flag]
#     POST[baknum](baklevel) <file path including host> [file code] [attribute code] [action flag]
#
#  where the file code is one of:
#     F = file present at that baklevel and to be deleted (if in @Baks)
#         (or f if in @Ante or @Post and potentially visible)
#     D = Dnir present at that baklevel and to be deleted (if in @Baks)
#	      (or f if in @Ante or @Post and potentially visible)
#     - = File not present at that baklevel
#     X = Parent directory not present at that baklevel 
#         (or x if in @Ante or @Post)
#  and the attribute code is one of:
#     n = Attribute type key (if present)
#     - = If no attribute for the file (implies no file)
#  and the action flag is one of the following: (only applies to @Baks & @Post)
#     C = Clear attribute (if attribute was previously present)
#     D = Set to type 10 delete (if not already set)
#     Mn = Move file/dir here from level 'n' (@Post only)
#
# More detail on the individual actions can be obtained by increasing
# the debugging level.
#
# The other interesting output is the result of the "execute_deletes"
# stage which shows what actually happens. Here, for every host and
# every backup of that host, you see what happens on a file by file
# level. The output is of form:
#   [hostname][@Ante chain] [@Baks chain] [@Post chain]
#   **BACKUP: [hostname][baknum](baklevel)
#       [hostname][baknum] <file name> [file code][attribute code]<move>
#
#  where the file code is one of:
#     F = Single file deleted
#     D(n) = Directory deleted with total of 'n' file/dir deletes
#             (including the directory)
#     - = Nothing deleted
#  and the attribute code is one of:
#     C = Attribute cleared
#     D = Attribute set to type 10 delete
#     d = Attribute left alone with type 10 delete
#     - = Attrib (otherwise) unchanged [shouldn't happen]
#  and the (optional) move code is: (applies only to @Post)
#     n->m  = File/dir moved by *linking* to pool from backup 'n' to 'm'
#     n=>   = File/dir moved by *copying* from backup 'n' to 'm'
# Finally, since the files are sorted alphabetically by name and
# directory, we only need to actually write the attribute folder after
# we finish making all the delete/clear changes in a directory.
# This is coded as:
#       [hostname][baknum] <dir>/attrib [-][attribute code]
#
#  where the attribute code is one of:
#     W = Attribute file *linked* to pool successfully
#     w = Attribute file *copied* to filesystem successfully
#     R = Empty attribute file removed from filesystem
#     X = Error writing attribute file
#========================================================================

use strict;
use warnings;

use File::Find;
use File::Glob ':glob';
use Data::Dumper;  #Just used for debugging...

#use lib "/usr/share/BackupPC/lib";
use lib "/usr/share/backuppc/lib";
use BackupPC::Lib;
use BackupPC::jLib;
use BackupPC::Attrib qw(:all);
use BackupPC::FileZIO;
use Getopt::Std;

use constant S_HLINK_TARGET => 0400000;    # this file is hardlink target

my $DeleteAttribH = {  #Hash reference to attribute entry for deleted file
	type  => BPC_FTYPE_DELETED,  #10
	mode  => 0,
	uid   => 0,
	gid   => 0,
	size  => 0,
	mtime => 0,
};

my %filedelsHoH;
# Hash has following structure:
# $filedelsHoH{$host}{$baknum}{$file} = <mask for what happened to file & attribute>
#                                       where the mask is one of the following elements

use constant FILE_ATTRIB_COPY  => 0000001;  # File and corresponding attrib copied/linked to new backup in @Post
use constant FILE_DELETED       => 0000002;  # File deleted (not moved)
use constant ATTRIB_CLEARED     => 0000010;  # File attrib cleared
use constant ATTRIB_DELETETYPE  => 0000020;  # File attrib deleted


my $DEBUG; #Note setting here will override options value

die("BackupPC::Lib->new failed\n") if ( !(my $bpc = BackupPC::Lib->new) );
my $TopDir = $bpc->TopDir();
chdir($TopDir); #Do this because 'find' will later try to return to working
            #directory which may not be accessible if you are su backuppc


(my $pc = "$TopDir/pc") =~ s|//*|/|g;
%Conf   = $bpc->Conf();  #Global variable defined in jLib.pm

my %opts;
if ( !getopts("h:n:s:lrH:mF:qtcd:u", \%opts) || defined($opts{u}) ||
	 !defined($opts{h}) || !defined($opts{n}) || 
	 (!defined($opts{s}) && defined($opts{m})) || 
	 (defined $opts{H} && $opts{H} !~ /^(0|abort|1|skip|2|force)$/) ||
	 (!$opts{l} && !$opts{F} && @ARGV < 1)) {
    print STDERR <<EOF;
usage: $0 [options] files/directories...

  Required options:
    -h <host>     Host (or - for all) from which path is offset
    -n <bakRange> Range of successive backup numbers to delete.
                    N   delete files from backup N (only)
                    M-N delete files from backups M-N (inclusive)
                    -M  delete files from all backups up to M (inclusive)
                    M-  delete files from all backups up from M (inlusive)
                    -   delete files from ALL backups
                   {N}  if one of the numbers is in braces, then  interpret
                        as the N\'th backup counting from the *beginning*
                   [N]  if one of the numbers is in braces, then  interpret
                        as the N\'th backup counting from the *end*
    -s <share>    Share name (or - for all) from which path is offset
                  (don\'t include the 'f' mangle)
                  NOTE: if unmangle option (-m) is not set then the share name
                  is optional and if not specified then it must instead be 
                  included in mangled form as part of the file/directory names.

  Optional options:
    -l            Just list backups by host (with level noted in parentheses)
    -r            Allow directories to be removed too (otherwise skips over directories)
    -H <action>   Treatment of hard links contained in deletion tree:
                    0|abort  abort with error=2 if hard links in tree [default]
                    1|skip   Skip hard links or directories containing them
                    2|force  delete anyway (BE WARNED: this may affect backup
                             integrity if hard linked to files outside tree)
    -m            Paths are unmangled (i.e. apply mangle to paths; doesn\'t apply to shares)
    -F <file>     Read files/directories from <file> (or stdin if <file> = -)
    -q            Don\'t show deletions
    -t            Trial run -- do everything but deletions
    -c            Clean up pool - schedule BackupPC_nightly to run (requires server running)
                  Only runs if files were deleted
    -d level      Turn on debug level
    -u            Print this usage message...
EOF
exit(1);
}

my $hostopt = $opts{h};
my $numopt = $opts{n};
my $shareopt = $opts{s} || '';
my $listopt = $opts{l} || 0;
my $mangleopt = $opts{m} || 0;
my $rmdiropt = $opts{r} || 0;
my $fileopt = $opts{F} || 0;
my $quietopt = $opts{q} || 0;
$dryrun = $opts{t} || 0; #global variable jLib.pm
my $runnightlyopt = $opts{c} || 0;

my $hardopt = $opts{H} || 0;
my $hardaction;
if($hardopt =~ /^(1|skip)$/) {
	$hardopt = 1;
	$hardaction = "SKIPPING";
}
elsif($hardopt =~ /^(2|force)$/) {
	$hardopt = 2;
}
else{
	$hardopt = 0;
	$hardaction = "ABORTING";
}

$DEBUG = ($opts{d} || 0 ) unless defined $DEBUG; #Override hard-coded definition unless set explicitly
#$DEBUG && ($dryrun=1);  #Uncomment if you want DEBUG to imply dry run
#$dryrun=1; #JJK: Uncomment to hard-wire to always dry-run (paranoia)
my $DRYRUN = ($dryrun == 0 ? "" : " DRY-RUN");


# Fill hash with backup structure by host
my %backupsHoHA;
get_allhostbackups($hostopt, $numopt, \%backupsHoHA);
if($listopt) {
	print_backup_list(\%backupsHoHA);
	exit;
}

my $shareregx_sh = my $shareregx_pl = $shareopt;
if($shareopt eq '-') {
	$shareregx_pl = "f[^/]+";
	$shareregx_sh = "f*"; # For shell globbing
}
elsif($shareopt ne '') {
	$shareregx_pl =~ s|//*|%2f|g; #Replace (one or more) '/' with %2f
    $shareregx_sh = $shareregx_pl = "f" . $shareregx_pl;
}

#Combine share and file arg regexps
my (@filelist, @sharearglist);
if($fileopt) {
	@filelist = read_file($fileopt);
}
else {
	@filelist = @ARGV;
}
foreach my $file (@filelist) {
	$file = $bpc->fileNameMangle($file) if $mangleopt; #Mangle filename
	my $sharearg = "$shareregx_sh/$file";
	$sharearg =~ s|//*|/|g;  $sharearg =~ s|^/*||g; $sharearg =~ s|/*$||g;
	    # Remove double, leading, and trailing slashes
	die "Error: Can't delete root share directory: $sharearg\n"
		if ("$sharearg" =~ m|^[^/]*$|); #Avoid because dangerous...
	push(@sharearglist, $sharearg);
}

my $filesdeleted = my $totfilesdeleted = my $filescopied = 0;
my $attrsdeleted = my $attrscleared = my $atfilesdeleted = 0;

my $hrdlnkflg;
foreach my $Host (keys %backupsHoHA) { #Loop through each host
	$hrdlnkflg=0;
	unless(defined @{$backupsHoHA{$Host}{baks}}) { #@baks is empty
		print "[$Host] ***NO BACKUPS FOUND IN DELETE RANGE***\n" unless $quietopt;
		next;
	}
	my @Ante = @{$backupsHoHA{$Host}{ante}};
	my @Baks = @{$backupsHoHA{$Host}{baks}};
	my @Post = @{$backupsHoHA{$Host}{post}};

	print "[$Host][" . join(" ", @Ante) . "][" . 
		join(" ", @Baks) . "][" . join(" ", @Post) . "]\n" unless $quietopt;

$DEBUG > 1 && (print "  ANTE[$Host]: " . join(" ", @Ante) ."\n");
$DEBUG > 1 && (print "  BAKS[$Host]: " . join(" ", @Baks) ."\n");
$DEBUG > 1 && (print "  POST[$Host]: " . join(" ", @Post) ."\n");

	#We need to glob files that occur both in the delete list (@Baks) and
	#in the antecedent list (@Ante) since antecedents affect presence of
	#later incrementals.
	my $numregx_sh = "{" . join(",", @Ante, @Baks) . "}";
	my $pcHost = "$pc/$Host";
	my @filepathlist;

	foreach my $sharearg (@sharearglist) {
		#Glob for all (relevant) file paths for host across @Baks & @Ante backups
#JJK		@filepathlist = (@filepathlist, <$pcHost/$numregx_sh/$sharearg>);
		@filepathlist = (@filepathlist, bsd_glob("$pcHost/$numregx_sh/$sharearg"));
	}
    #Now use a hash to collapse into unique file keys (with host & backup number stripped off)
	my %fileH;
	foreach my $filepath (@filepathlist) {
		next unless -e $filepath; #Skip non-existent files (note if no wildcard in path, globbing
		                          #will always return the file name even if doesn't exist)
		$filepath =~ m|^$pcHost/[0-9]+/+(.*)|;
		$fileH{$1}++;  #Note ++ used to set the keys
	}
	unless(%fileH) {
$DEBUG && print "  LOOKING AT: [$Host] [" . join(" ", @Ante) . "][" . join(" ", @Baks) . "][" . join(" ", @Post) . "] **NO DELETIONS ON THIS HOST**\n\n";
			next;
	}
	my $lastfile="///"; #dummy starting point since no file can have this name since eliminated dup '/'s
	foreach my $File (sort keys %fileH) { #Iterate through sorted files
		# First build an array of filepaths based on ascending backup numbers in
		# @Baks. Also, do a quick check for directories.
		next if $File =~ m|^$lastfile/|; # next if current file is in a subdirectory of previous file
        $lastfile = $File;
		#Now create list of paths to search for hardlinks
		my @Pathlist = ();
		foreach my $Baknum (@Ante) { #Need to include @Ante in hardlink search
			my $Filepath = "$pc/$Host/$Baknum/$File";
			next unless -e $Filepath;
			push (@Pathlist, $Filepath);
		}
		my $dirflag=0;
		foreach my $Baknum (@Baks) {
			my $Filepath = "$pc/$Host/$Baknum/$File";
			next unless -e $Filepath;
			if (-d $Filepath && !$rmdiropt) {
				$dirflag=1; #Only enforce directory check in @Baks because only deleting there
				printerr "Skipping directory `$Host/*/$File` since -r flag not set\n\n";
				last;
			}
			push (@Pathlist, $Filepath);
		}
		next if $dirflag;
		next unless(@Pathlist); #Probably shouldn't get here since by construction a path should exist 
		                        #for at least one of the elements of @Ante or @Baks
		#Now check to see if any hard-links in the @Pathlist
		find(\&find_is_hlink, @Pathlist ) unless $hardopt == 2; #Unless force
		exit 2 if $hrdlnkflg && $hardopt == 0; #abort
		next if $hrdlnkflg;
$DEBUG && print "  LOOKING AT: [$Host] [" . join(" ", @Ante) . "][" . join(" ", @Baks) . "][" . join(" ", @Post) . "] $File\n";
		calculate_deletes($Host, $File, \$backupsHoHA{$Host}, \$filedelsHoH{$Host}, !$quietopt);
$DEBUG && print "\n";
	}
	execute_deletes($Host, \$backupsHoHA{$Host}, \$filedelsHoH{$Host}, !$quietopt);
}

print "\nFiles/directories deleted: $filesdeleted($totfilesdeleted)     Files/directories copied: $filescopied\n" unless $quietopt;
print "Delete attrib set: $attrsdeleted                Attributes cleared: $attrscleared\n" unless $quietopt;
print "Empty attrib files deleted: $atfilesdeleted       Errors: $errorcount\n" unless $quietopt;
run_nightly($bpc) if (!$dryrun && $runnightlyopt);
exit;

#Set $hrdlnkflg=1 if find a hard link (including "targets")
# Short-circuit/prune find as soon as hard link found.
sub find_is_hlink
{
	if($hrdlnkflg) {
		$File::Find::prune = 1; #Prune search if hard link already found
        #i.e. don't go any deeper (but still will finish the current level)
	}
	elsif($File::Find::name eq $File::Find::topdir  #File
		  && -f && m|f.*|
		  &&( get_jtype($File::Find::name) & S_HLINK_TARGET)) {
	# Check if file has type hard link (or hard link target) Note: we
	# could have used this test recursively on all files in the
	# directory tree, but it would be VERY SLOW since we would need to
	# read the attrib file for every file in every
	# subdirectory. Instead, we only use this method when we are
	# searching directly for a file at the top leel
	# (topdir). Otherwise, we use the method below that just
	# recursively searches for the attrib file and reads that
	# directly.
		$hrdlnkflg = 1;
		print relpath($File::Find::name) . ": File is a hard link. $hardaction...\n\n";
	}
	elsif (-d && -e  attrib($File::Find::name)) { #Directory
    # Read through attrib file hash table in each subdirectory in tree to
	# find files that are hard links (including 'targets'). Fast
	# because only need to open attrib file once per subdirectory to test
	# all the files in the directory.
		read_attrib(my $attr, $File::Find::name);
		foreach my $file (keys (%{$attr->get()})) { #Look through all file hash entries
			if (${$attr->get($file)}{type} == 1 || #Hard link
				(${$attr->get($file)}{mode} & S_HLINK_TARGET)) { #Hard link target
				$hrdlnkflg = 1;
				$File::Find::topdir =~ m|^$pc/([^/]+)/([0-9]+)/(.*)|;
#				print relpath($File::Find::topdir) .
#				print relpath($File::Find::name) .
#					": Directory contains hard link: $file'. $hardaction...\n\n";
				print "[$1][$2] $3: Directory contains hard link: " .
					substr($File::Find::name, length($File::Find::topdir)) .
				    "/f$file ... $hardaction...\n\n";

				last; #Stop readin attrib file...hard link found
			}
		}
	}
}		

# Main routine for figuring out what files/dirs in @baks get deleted
# and/or copied/linked to @post along with which attrib entries are
# cleared or set to delete type in both the @baks and @post backupchains.
sub calculate_deletes
{
	my ($hostname, $filepath, $backupshostHAref, $filedelsHref, $verbose) = @_;
	my @ante = @{$$backupshostHAref->{ante}};
	my @baks = @{$$backupshostHAref->{baks}};
	my @post = @{$$backupshostHAref->{post}};
	my %Level = %{$$backupshostHAref->{level}};
	my %Vislvl = %{$$backupshostHAref->{vislvl}};
	my $pchost = "$pc/$hostname";

	#We first need to look down the direct antecedent chain in @ante
	#to determine whether earlier versions of the file exist and if so
	#at what level of incrementals will they be visible. A file in the
	#@ante chain is potentially visible later in the @baks chain at
	#the given level (or higher) if there is no intervening type=10
	#(delete) attrib in the chain. If there is already a type=10
	#attrib in the @ante chain then the file will be invisible in the
	#@baks chain at the same level or higher of incrmental backups.

	#Recall that the elements of @ante by construction have *strictly*
	#increasing backup levels. So, that the visibility scope decreases
	#as you go down the chain.

    #We first iterate up the @ante chain and construct a hash
	#(%VisibleLvl) that is either 1 or 0 depending on whether there is
	#a file or type=10 delete attrib at that level. For any level at
	#which there is no antecedent, the corresponding entry of
	#%VisibleLvl remains undef

	my %VisibleAnte;  # $VisibleAnte{$level} is equal to -1 if nothing visible from @Ante at the given level.
	                  # i.e. if either there was a type=delete at that level or if that level was blank but 
	                  # there was a type=delete at at a lower level without an intervening file.
	                  # Otherwise, it is set to the backup number of the file that was visible at that level.
	                  # This hash is used to determine where we need to add type=10 delete attributes to 
	                  # @baks to keep the files still present in @ante from poking through into the 
                      # deleted @baks region.

	my %VisibleAnteBaks;  # This hash is very similar but now we construct it all the way through @Baks to 
                     # determine what was ORIGINALLY visible to the elements of @post since we may
                     # need to copy/link files forward to @post if they have been deleted from @baks or
                     # if they are now blocked by a new type=delete attribute in @baks.

	$VisibleAnte{0} = $VisibleAnteBaks{0} = -1; #Starts as invisible until first file appears
	$filepath =~ m|(.*)/|;
	foreach my $prevbaknum (@ante) {	
		my $prevbakfile = "$pchost/$prevbaknum/$filepath";
		my $level = $Level{$prevbaknum};
		my $type = get_attrib_type($prevbakfile);
		my $nodir = ($type == -3 ? 1 : 0);	#Note type = -3 if dir non-existent
		printerr "Attribute file unreadable: $prevbaknum/$filepath\n" if $type == -4;

		#Determine what is visible to @Baks and to @Post
		if($type == BPC_FTYPE_DELETED || $nodir) {  #Not visible if deleted type or no parent dir
			$VisibleAnte{$level} = $VisibleAnteBaks{$level} = -1; #always update
			$VisibleAnteBaks{$level} = -1 
				if defined($Vislvl{$level}) && $Vislvl{$level} == $prevbaknum; 
			#only update if this is the most recent backup at this level visible from @post
		}
		elsif (-r $prevbakfile) { #File exists so visible at this level
			$VisibleAnte{$level} = $prevbaknum; # always update because @ante is strictly increasing order
			$VisibleAnteBaks{$level} = $prevbaknum 
				if defined($Vislvl{$level}) && $Vislvl{$level} == $prevbaknum;
				#Only update if this will be visible from @post (may be blocked later by @baks)
		}

$DEBUG > 1 && print "    ANTE[$prevbaknum]($level) $hostname/$prevbaknum/$filepath [" . (-f $prevbakfile ? "f" : (-d $prevbakfile ? "d": ($nodir ? "x" : "-"))) . "][" . ($type >=0 ? $type : "-") . "]\n";
	}

    #Next, iterate down @baks to schedule file/dirs for deletion
    #and/or for clearing/changing file attrib entry based on the
    #status of the visibility flag at that level (or below) and the
    #presence of $filepath in the backup.
	#The status of what we do to the file and what we do to the attribute is stored in
	#the hash ref %filedelsHref
	my $minbaklevel = $baks[0];
	foreach my $currbaknum (@baks) {
		my $currbakfile = "$pchost/$currbaknum/$filepath";
		my $level = $Level{$currbaknum};
		my $type = get_attrib_type($currbakfile); 
		my $nodir = ($type == -3 ? 1 : 0);	#Note type = -3 if dir non-existent
		printerr "Attribute file unreadable: $currbaknum/$filepath\n" if $type == -4;
		my $actionflag = "-"; my $printstring = "";#Used for debugging statements only 

		#Determine what is visible to @Post; also set file for deletion if present
		if($type == BPC_FTYPE_DELETED || $nodir) {  #Not visible if deleted type or no parent dir
			$VisibleAnteBaks{$level} = -1 
				if defined $Vislvl{$level} && $Vislvl{$level} == $currbaknum;  #update if visible from @post
		}
        elsif (-r $currbakfile ) {
			$VisibleAnteBaks{$level} = $currbaknum 
				if defined($Vislvl{$level}) && $Vislvl{$level} == $currbaknum; #update if visible
			$$filedelsHref->{$currbaknum}{$filepath} |= FILE_DELETED;
$DEBUG > 2 && ($printstring .= "      [$currbaknum] Adding to delete list: $hostname/$currbaknum/$filepath\n");
		}

		#Determine whether deleted file attribs should be cleared or set to type 10=delete
		if(!$nodir && $level <= $minbaklevel && last_visible_backup($level, \%VisibleAnte) >= 0) {
			#Existing file in @ante will shine through since nothing in @baks is blocking
			#Note if $level > $minbaklevel then we will already be shielding it with a previous @baks element
			$minbaklevel = $level;
			if ($type != BPC_FTYPE_DELETED) { # Set delete type if not already of type delete
				$$filedelsHref->{$currbaknum}{$filepath} |= ATTRIB_DELETETYPE;
				$actionflag="D";
$DEBUG > 2 &&  ($printstring .=  "      [$currbaknum] Set attrib to type=delete: $hostname/$currbaknum/$filepath\n");
			}
		}
		elsif ($type >=0) { #No antecedent from @Ante will shine through since already blocked.
			                #So if there is an attribute type there, we should clear the attribute since
			                #nothing need be there
			$$filedelsHref->{$currbaknum}{$filepath} |= ATTRIB_CLEARED;
			$actionflag="C";
$DEBUG > 2 && ($printstring .= "      [$currbaknum] Clear attrib file entry: $hostname/$currbaknum/$filepath\n");
		}
$DEBUG > 1 && print "    BAKS[$currbaknum]($level) $hostname/$currbaknum/$filepath [" . (-f $currbakfile ? "F" : (-d $currbakfile ? "D": ($nodir ? "X" : "-"))) . "][" . ($type>=0 ? $type : "-") . "][$actionflag]\n";
$DEBUG >3 && print $printstring;
	}

#Finally copy over files as necessary to make them appropriately visible to @post
#Recall again that successive elements of @post are strictly lower in level.
#Therefore, each element of @post either already has a file entry or it
#inherits its entry from the previously deleted backups.
	foreach my $nextbaknum (@post) { 
		my $nextbakfile = "$pchost/$nextbaknum/$filepath";
		my $level = $Level{$nextbaknum};
		my $type = get_attrib_type($nextbakfile);
		my $nodir = ($type == -3 ? 1 : 0);	#Note type = -3 if dir non-existent
		printerr "Attribute file unreadable: $nextbaknum/$filepath\n" if $type == -4;
		my $actionflag = "-"; my $printstring = ""; #Used for debugging statements only 

		#If there is a previously visible file from @Ante or @Post that used to shine through (but won't now
        #because either in @Ante and blocked by @Post deletion or deleted from @Post) and if nothing in @Post
		# is blocking (i.e directory exists, no file there, and no delete type), then we need to copy/link
		#the file forward
		if ((my $delnum = last_visible_backup($level, \%VisibleAnteBaks)) >= 0 &&
			$type != BPC_FTYPE_DELETED && !$nodir &&  !(-r $nextbakfile)) {
			#First mark that last visible source file in @Ante or @Post gets copied
			$$filedelsHref->{$delnum}{$filepath} |= FILE_ATTRIB_COPY;
            #Note still keep the FILE_DELETED attrib because we may still need to delete the source 
            #after moving if the source was in @baks
			#Second tell the target where it gets its source
			$$filedelsHref->{$nextbaknum}{$filepath} = ($delnum+1) << 6; #
			#Store the source in higher bit numbers to avoid overlapping with our flags. Add 1 so as to
			#be able to distinguish empty (non stored) path from backup #0.
$DEBUG > 2 && ($printstring .= "      [$nextbaknum] Moving file and attrib from backup $delnum: $filepath\n");
			$actionflag = "M$delnum";
		}
		elsif ($type == BPC_FTYPE_DELETED) {
			# File has a delete attrib that is now no longer necessary since
			# every element of @post by construction has a deleted immediate predecessor in @baks
			$$filedelsHref->{$nextbaknum}{$filepath} |= ATTRIB_CLEARED;
$DEBUG > 2 && ($printstring .= "      [$nextbaknum] Clear attrib file entry:  $hostname/$nextbaknum/$filepath\n");
			$actionflag = "C";
		}
$DEBUG >1 && print "    POST[$nextbaknum]($level) $hostname/$nextbaknum/$filepath [" . (-f $nextbakfile ? "f" : (-d $nextbakfile ? "d": ($nodir ? "x" : "-"))) . "][" . ($type >= 0 ? $type : "-") . "][$actionflag]\n";
$DEBUG >3 && print $printstring;
	}
}

sub execute_deletes
{
	my ($hostname, $backupshostHAref, $filedelsHref, $verbose) = @_;
	my @ante = @{$$backupshostHAref->{ante}};
	my @baks = @{$$backupshostHAref->{baks}};
	my @post = @{$$backupshostHAref->{post}};
	my %Level = %{$$backupshostHAref->{level}};

	my $pchost = "$pc/$hostname";
	foreach my $backnum (@ante, @baks, @post) {
        #Note the only @ante action is copying over files
        #Note the only @post action is clearing the file attribute
		print "**BACKUP: [$hostname][$backnum]($Level{$backnum})\n";
		my $prevdir=0;
		my ($attr, $dir, $file);
		foreach my $filepath (sort keys %{$$filedelsHref->{$backnum}}) {
			my $VERBOSE = ($verbose ? "" : "[$hostname][$backnum] $filepath:");
			my $delfilelist;
			my $filestring = my $attribstring = '-';
			my $movestring = my $printstring = '';
			$filepath =~ m|(.*)/f(.*)|;
			$dir = "$pchost/$backnum/$1";
			my $dirstem = $1;
			$file = $2;
			if($dir ne $prevdir) { #New directory - we only need to read/write the atrrib file once per dir
				write_attrib_out($bpc, $attr, $prevdir, $verbose)
					if $prevdir; #Write out previous $attr
				die "Error: can't write attribute file to directory: $dir" unless -w $dir;
				read_attrib($attr, $dir); #Read in new attribute
				$prevdir = $dir;
			}

			my $action = $$filedelsHref->{$backnum}{$filepath};
			if($action & FILE_ATTRIB_COPY) {
				my %sourceattr;
				get_file_attrib("$pchost/$backnum/$filepath", \%sourceattr);
				my $checkpoollinks = 1; #Don't just blindly copy or link - make sure linked to pool
				foreach my $nextbaknum (@post) {
					my ($ret1, $ret2);
					next unless (defined($$filedelsHref->{$nextbaknum}{$filepath}) &&
								 ($$filedelsHref->{$nextbaknum}{$filepath} >> 6) - 1 == $backnum);
						#Note: >>6 followed by decrement of 1 recovers the backup number encoding
						#Note: don't delete or clear/delete source attrib now because we may need to move
						#several copies - so file deletion and attribute clear/delete is done after moving

					
					if(($ret1=link_recursively_topool ($bpc, "$pchost/$backnum/$filepath", 
													  "$pchost/$nextbaknum/$filepath",
													   $checkpoollinks, 1)) >= 0
					   && ($ret2=write_file_attrib($bpc, "$pchost/$nextbaknum/$dirstem", $file, \%sourceattr, 1)) > 0){
						#First move files by linking them to pool recursively and then copy attributes
						$checkpoollinks = 0 if $ret1 > 0; #No need to check pool links next time if all ok now
						$movestring .= "," unless $movestring eq '';
						$movestring .= "$backnum" . ($ret1 == 1 ? "->" :  "=>") . "$nextbaknum\n";
						$filescopied++;
					}
					else {
						$action = 0; #If error moving, cancel the subsequent file and attrib deletion/clearing
						junlink("$pchost/$nextbaknum/$filepath"); #undo partial move
						if($ret1 <0) {
							$printstring .= "$VERBOSE      FAILED TO MOVE FILE/DIR: $backnum-->$nextbaknum -- UNDOING PARTIAL MOVE\n";
						}
						else {
							$printstring .= "$VERBOSE      FAILED TO WRITE NEW ATTRIB FILE IN $nextbaknum AFTER MOVING FILE/DIR: $backnum-->$nextbaknum FROM $backnum -- UNDOING MOVE\n";
						}
						next; # Skip to next move
					}
				}
			}
			if ($action & FILE_DELETED) { #Note delete follows moving
				my $isdir = (-d "$pchost/$backnum/$filepath" ? 1 : 0);
				my $numdeletes = delete_files("$pchost/$backnum/$filepath", \$delfilelist);
				if($numdeletes > 0) {
					$filestring = ($isdir ? "D$numdeletes" : "F" );
					$filesdeleted++;
					$totfilesdeleted +=$numdeletes;
					if($delfilelist) {
						$delfilelist =~ s!(\n|^)(unlink|rmdir ) *$pchost/$backnum/$filepath(\n|$)!!g; #Remove top directory
						$delfilelist =~ s!^(unlink|rmdir ) *$pc/!       !gm; #Remove unlink/rmdir prefix
					}
				}
				else {
					$printstring .= "$VERBOSE      FILE FAILED TO DELETE ($numdeletes)\n";
				}
			}
	 		if ($action & ATTRIB_CLEARED) { #And attrib changing follows file moving & deletion...
				$attr->delete($file);
				$attribstring = "C";
				$attrscleared++;

			}
			elsif($action & ATTRIB_DELETETYPE) {
				 if (defined($attr->get($file)) && ${$attr->get($file)}{type} == BPC_FTYPE_DELETED) {
					 $attribstring = "d";
				 }
				 else {
					 $attr->set($file, $DeleteAttribH);  # Set file to deleted type (10)
					 $attribstring = "D";
					 $attrsdeleted++;
				 }
			}
			print "    [$hostname][$backnum]$filepath [$filestring][$attribstring] $movestring$DRYRUN\n" 
				if $verbose && ($filestring ne '-' || $attribstring ne '-' || $movestring ne '');
			print $delfilelist . "\n" if $verbose && $delfilelist;
			print $printstring;
		}
		write_attrib_out($bpc, $attr, $dir, $verbose)
			if $prevdir; #Write out last attribute
	}
}

sub write_attrib_out 
{
	my ($bpc, $attr, $dir, $verbose) = @_;
	my $ret;
	my $numattribs = count_file_attribs($attr);
	die "Error writing to attrib file for $dir\n" 
		unless ($ret =write_attrib ($bpc, $attr, $dir, 1, 1)) > 0;
	$dir =~ m|^$pc/([^/]*)/([^/]*)/(.*)|;
	$atfilesdeleted++ if $ret==4;
	print "    [$1][$2]$3/attrib [-]" . 
		($ret==4 ? "[R]" : ($ret==3 ? "[w]" : ($ret > 0 ? "[W]" : "[X]")))
		 ."$DRYRUN\n" if $verbose;
	return $ret;
}

#If earlier file is visible at this level, return the backup number where a file was last present
#Otherwise return -1 (occurs if there was an intervening type=10 or if a file never existed)
sub last_visible_backup
{
	my ($numlvl, $Visiblebackref) = @_;
	my $lvl = --$numlvl; #For visibility look at one less than current level and lower

	return -1 unless $lvl >= 0;
	do {
		return ($Visiblebackref->{$numlvl} = $Visiblebackref->{$lvl}) #Set & return
			if defined($Visiblebackref->{$lvl});
	} while($lvl--);
	return -1;  #This shouldn't happen since we initialize $Visiblebackref->{0} = -1;
}

# Get the modified type from the attrib file.
# Which I define as:
#    type + (type == BPC_FTYPE_HARDLINK => 1; ? S_HLINK_TARGET : (mode & S_HLINK_TARGET) )
# i.e. you get both the type and whether it is either an hlink 
# or an hlink-target
sub get_jtype
{
	my ($fullfilename) = @_;
	my %fileattrib;

	return 100 if  get_file_attrib($fullfilename, \%fileattrib) <= 0;
	my $type = $fileattrib{type};
	my $mode = $fileattrib{mode};
	$type + ($type == BPC_FTYPE_HARDLINK ? 
			 S_HLINK_TARGET : ($mode & S_HLINK_TARGET));
}

#Set elements of the hash backupsHoHA which is a mixed HoHA and HoHoH
#containing backup structure for each host in hostregex_sh

# Elements are:
#   backupsHoHA{$host}{baks} - chain (array) of consecutive backups numbers
#         whose selected files we will be deleting
#   backupsHoHA{$host}{ante} - chain (array) of backups directly antecedent
#         to those in 'baks' - these are all "parents" of all elemenst 
#         of 'baks' [in descending numerical order and strictly descending
#         increment order]
#   backupsHoHA{$host}{post} - chain (array) of backups that are incremental
#         backups of elements of 'baks' - these must all be "children" of 
#         all element of 'baks' [in ascending numerical order and strictly
#         descending increment order]
#   backupsHoHA{$host}{level}{$n}  - level of backup $n
#   backupsHoHA{$host}{vislvl}{$level}  - highest (most recent) backup number in (@ante, @baks) with $level
#         Note: this determines which backups from (@ante, @baks) are potentially visible from @post

sub get_allhostbackups
{
	my ($hostregx_sh, $numregx, $backupsHoHAref) = @_;


	die "$0: bad host name '$hostregx_sh'\n"
		if ( $hostregx_sh !~ m|^([-\w\.\s*]+)$| || $hostregx_sh =~ m{(^|/)\.\.(/|$)} );
	$hostregx_sh = "*" if ($hostregx_sh eq '-'); # For shell globbing

	die "$0: bad backup number range '$numopt'\n" 
		if ( $numregx !~ m!^((\d*)|{(\d+)}|\[(\d+)\])-((\d*)|{(\d+)}|\[(\d+)\])$|(\d+)$! );

	my $startnum=0;
	my $endnum = 99999999;
	if(defined $2 && $2 ne '') {$startnum = $2;}
	elsif(defined $9) {$startnum = $endnum = $9;}
	if(defined $6 && $6 ne ''){$endnum=$6};
	die "$0: bad dump range '$numopt'\n"
		if ( $startnum < 0 || $startnum > $endnum);
	my $startoffsetbeg = $3;
	my $endoffsetbeg = $7;
	my $startoffsetend = $4;
	my $endoffsetend = $8;

	my @allbaks = bsd_glob("$pc/$hostregx_sh/[0-9]*/backupInfo");
       #Glob for list of valid backup paths
	for (@allbaks) { #Convert glob to hash of backups and levels
		m|.*/(.*)/([0-9]+)/backupInfo$|; # $1=host $2=baknum
		my $level = get_bakinfo("$pc/$1/$2", "level");
		$backupsHoHAref->{$1}{level}{$2} = $level 
			if defined($level) && $level >=0; # Include if backup level defined
	}

	foreach my $hostname (keys %{$backupsHoHAref}) { #Loop through each host
		#Note: need to initialize the following before we assign reference shortcuts
		#Note {level} already defined
		@{$backupsHoHAref->{$hostname}{ante}} = ();
		@{$backupsHoHAref->{$hostname}{baks}} = ();
		@{$backupsHoHAref->{$hostname}{post}} = ();
		%{$backupsHoHAref->{$hostname}{vislvl}} = ();

		#These are all references
		my $anteA= $backupsHoHAref->{$hostname}{ante};
		my $baksA= $backupsHoHAref->{$hostname}{baks};
		my $postA= $backupsHoHAref->{$hostname}{post};
		my $levelH= $backupsHoHAref->{$hostname}{level};
		my $vislvlH= $backupsHoHAref->{$hostname}{vislvl};

		my @baklist =  (sort {$a <=> $b} keys %{$levelH}); #Sorted list of backups for current host
		$startnum = $baklist[$startoffsetbeg-1] || 99999999 if defined $startoffsetbeg;
		$endnum = $baklist[$endoffsetbeg-1] || 99999999 if defined $endoffsetbeg;
		$startnum = $baklist[$#baklist - $startoffsetend +1] || 0 if defined $startoffsetend;
		$endnum = $baklist[$#baklist - $endoffsetend +1] || 0 if defined $endoffsetend;

		my $minbaklevel = my $minvislevel = 99999999;
		my @before = my @after = ();
		#NOTE: following written for clarity, not speed
		foreach my $baknum (reverse @baklist) { #Look backwards through list of backups
        #Loop through reverse sorted list of backups for current host
			my $level = $$levelH{$baknum};
			if($baknum <= $endnum) {
				$$vislvlH{$level} = $baknum if $level < $minvislevel;
				$minvislevel = $level if $level < $minvislevel;
			}
			if($baknum >= $startnum && $baknum <= $endnum) {
				unshift(@{$baksA}, $baknum); #sorted in increasing order
				$minbaklevel = $level if $level < $minbaklevel;
			}
			push (@before, $baknum) if $baknum < $startnum; #sorted in decreasing order
			unshift(@after, $baknum) if $baknum > $endnum; #sorted in increasing order
		}
		next unless defined @{$baksA}; # Nothing to backup on this host

		my $oldlevel = $$levelH{$$baksA[0]}; # i.e. level of first backup in baksA
		for (@before) { 
			#Find all direct antecedents until the preceding level 0 and push on anteA
			if ($$levelH{$_} < $oldlevel) { 
				unshift(@{$anteA}, $_);	#Antecedents are in increasing order with strictly increasing level
				last if $$levelH{$_} == 0;
				$oldlevel = $$levelH{$_};
			}
		}
		$oldlevel = 99999999;
		for (@after) {
			# Find all successors that are immediate children of elements of @baks
			if ($$levelH{$_} <= $oldlevel) { # Can have multiple descendants at the same level
				last if $$levelH{$_} <= $minbaklevel; #Not a successor because dips below minimum
				push(@{$postA}, $_); #Descendants are increasing order with non-increasing level
				$oldlevel = $$levelH{$_};
			}
		}
	}
}

# Print the @Baks list along with the level of each backup in parentheses
sub print_backup_list
{
	my ($backupsHoHAref) = @_;	

	foreach my $hostname (sort keys %{$backupsHoHAref}) { #Loop through each host
		print "$hostname: ";
		foreach my $baknum (@{$backupsHoHAref->{$hostname}{baks}}) {
			print "$baknum($backupsHoHAref->{$hostname}{level}{$baknum}) ";
		}
		print "\n";
	}
}

#Read in external file and return list of lines of file
sub read_file
{
	my ($file) = @_;
	my $fh;
	my @lines;

	if($file eq '-') {
		$fh = *STDIN;
	}
	else {
		die "ERROR: Can't open: $file\n" unless open($fh, "<", $file);
	}
	while(<$fh>) {
		chomp;
		next if m|^\s*$| || m|^#|;
		push(@lines, $_);
	}
	close $fh if $file eq '-';
	return @lines;

}
		
		
# Strip off the leading $TopDir/pc portion of path
sub relpath
{
	substr($_[0],1+length($pc));
}


sub min
{
	$_[0] < $_[1] ? $_[0] : $_[1];
}
