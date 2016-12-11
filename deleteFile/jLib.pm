#============================================================= -*-perl-*-
#
# BackupPC::jLib package
#
# DESCRIPTION
#
#   This library includes various supporting subroutines for use with BackupPC
#   functions used by BackupPC.
#   Some of the routines are variants/extensions of routines originally written
#   by Craig Barratt as part of the main BackupPC release.
#
# AUTHOR
#   Jeff Kosowsky
#
# COPYRIGHT
#   Copyright (C) 2008-2013  Jeff Kosowsky
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
# Version 0.4.3, released October 2014

# CHANGELOG
# 0.4.0 (Jan 2011)
# 0.4.1 (Jan 2013) Added MD52RPath, is_poollink
#                  Added jchmod, jchown
# 0.4.2 (Feb 2013) Made write_attrib more robust
# 0.4.3 (Oct 2014) Fixed bug in write_attrib
#
#========================================================================

package BackupPC::jLib;

use strict;
use vars qw($VERSION);
$VERSION = '0.4.3';

use warnings;
use File::Copy;
use File::Glob ':glob';
use File::Path;
use File::Temp;
use Fcntl;  #Required for RW I/O masks

use BackupPC::Lib;
use BackupPC::Attrib;
use BackupPC::FileZIO;
use Data::Dumper;  #Just used for debugging...

no utf8;

use constant _128KB               => 131072;
use constant _1MB                 => 1048576;
use constant LINUX_BLOCK_SIZE     => 4096;
use constant TOO_BIG              => 2097152; # 1024*1024*2 (2MB)

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT = qw(
       %Conf $dryrun $errorcount
       LINUX_BLOCK_SIZE TOO_BIG
       printerr warnerr firstbyte
       zFile2MD5 zFile2FullMD5
       link_recursively_topool
       jfileNameUnmangle
       getattr read_attrib count_file_attribs
       get_bakinfo get_file_attrib get_attrib_value get_attrib_type
       write_attrib write_file_attrib
       attrib
       MD52RPath
       is_poollink GetPoolLink jMakeFileLink
       poolname2number renumber_pool_chain delete_pool_file
       run_nightly
       jcompare zcompare zcompare2
       delete_files jtouch
       jchmod jchown jcopy jlink junlink jmkdir jmkpath jmake_path 
       jrename jrmtree jutime
);

#Global variables
our %Conf;
our $errorcount=0;
our $dryrun=1;  #global variable - set to 1 to be safe -- should be set to
				#0 in actual program files if you don't want a dry-run
                #None of the routines below will change/delete/write actual
                #file data if this flag is set. The goal is to do everything but
                #the final write to assist with debugging or cold feet :)

sub printerr
{
	print "ERROR: " . $_[0];
	$errorcount++;
}

sub warnerr
{
	$|++; # flush printbuffer first
	warn "ERROR: " . $_[0];
	$errorcount++;
}

# Returns the firstbyte of a file.
# If coding $coding undefined or 0, return as unpacked 2 char hexadecimal
# string. Otherwise, return as binary byte.
# Return -1 on error.
# Useful for checking the type of compressed file/checksum coding.
sub firstbyte {
	my ($file, $coding) = @_;
	my $fbyte='';
	sysopen(my $fh, $file, O_RDONLY) || return -1;
	$fbyte = -1 unless sysread($fh, $fbyte, 1) == 1;
	close($fh);
	if (! defined($coding) || $coding == 0) {
		$fbyte = unpack('H*',$fbyte); # Unpack as 2 char hexadecimal string
	}
	else {
		$fbyte = vec($fbyte, 0, 8);  # Return as binary byte
	}
	return $fbyte;
}

# Compute the MD5 digest of a compressed file. This is the compressed
# file version of the Lib.pm function File2MD5.
# For efficiency we don't use the whole file for big files
#   - for files <= 256K we use the file size and the whole file.
#   - for files <= 1M we use the file size, the first 128K and
#     the last 128K.
#   - for files > 1M, we use the file size, the first 128K and
#     the 8th 128K (ie: the 128K up to 1MB).
# See the documentation for a discussion of the tradeoffs in
# how much data we use and how many collisions we get.
#
# Returns the MD5 digest (a hex string) and the file size if suceeeds.
# (or "" and error code if fails).
# Note return for a zero size file is ("", 0).
#
# If $size < 0 then calculate size of file by fully decompressing
# If $size = 0 then first try to read corresponding attrib file
#    (if it exists), if doesn't work then calculate by fully decompressing
# IF $size >0 then use that as the size of the file
#
# If compreslvl is undefined then use the default compress level from 
# the config file

sub zFile2MD5
{
    my($bpc, $md5, $name, $size, $compresslvl) = @_;
	
	my ($fh, $rsize, $filesize, $md5size);

	return ("", -1) unless -f $name;
	return ("", 0) if (stat(_))[7] == 0;  #Zero size file
	$compresslvl = $Conf{CompressLevel} unless defined $compresslvl;
	unless (defined ($fh = BackupPC::FileZIO->open($name, 0, $compresslvl))) {
		printerr "Can't open $name\n";
		return ("", -1);
	}

	my ($datafirst, $datalast);
	my @data;
	#First try to read up to the first 128K (131072 bytes)
	if ( ($md5size = $fh->read(\$datafirst, _128KB)) < 0 ) { #Fist 128K
		printerr "Can't read & decompress $name\n";
		return ("", -1);
	}

	if ($md5size == _128KB) { # If full read, continue reading up to 1st MB
		my $i=0;
		#Read in up to 1MB (_1MB), 128K at a time and alternate between 2 data buffers
		while ( ($rsize = $fh->read(\$data[(++$i)%2], _128KB)) == _128KB
				&&  ($md5size += $rsize) < _1MB ) {}
		$md5size +=$rsize if $rsize < _128KB; # Add back in partial read
	    $datalast = ($i > 1 ? 
					 substr($data[($i-1)%2], $rsize, _128KB-$rsize) : '')
			. substr($data[$i%2], 0 ,$rsize); #Last 128KB (up to 1MB)
    }

	$size = 0 unless defined $size;
	if($size > 0) {  #Use given size
		$filesize = $size;
	}elsif($md5size < _1MB) { #Already know the size because read it all (note don't do <=)
		$filesize = $md5size;
	} elsif($compresslvl == 0) { #Not compressed, so: size = actual size
		$filesize = -s $name;
	}elsif($size == 0) { # Try to find size from attrib file
		$filesize = get_attrib_value($name, "size");
		if(!defined($filesize)) {
			warn "Can't read size of $name from attrib file so calculating manually\n";
		}
	}
	if(!defined($filesize)) { #No choice but continue reading to find size
		$filesize = $md5size;
		while (($rsize = $fh->read(\($data[0]), _128KB)) > 0) {
			$filesize +=$rsize;
        }
   }
   $fh->close();

   $md5->reset();
   $md5->add($filesize);
   $md5->add($datafirst);
   $md5->add($datalast) if defined($datalast);
   return ($md5->hexdigest, $filesize);
}

#Compute md5sum of the full data contents of a file.
#If the file is compressed, calculate the md5sum of the inflated
#version (using the zlib routines to uncompress the stream). Note this
#gives the md5sum of the FULL file -- not just the partial file md5sum
#above.
sub zFile2FullMD5
{
    my($bpc, $md5, $name, $compresslvl) = @_;

	my $fh;
	my $data;

	$compresslvl = $Conf{CompressLevel} unless defined $compresslvl;
	unless (defined ($fh = BackupPC::FileZIO->open($name, 0, $compresslvl))) {
		printerr "Can't open $name\n";
		return -1;
	}

	$md5->reset();	
	while ($fh->read(\$data, 65536) > 0) {
		$md5->add($data);
	}

    return $md5->hexdigest;
}

# Like MakeFileLink but for existing files where we don't have the
# digest available. So compute digest and call MakeFileLink after
# For each file, check if the file exists in $bpc->{TopDir}/pool.
# If so, remove the file and make a hardlink to the file in
# the pool.  Otherwise, if the newFile flag is set, make a
# hardlink in the pool to the new file.
#
# Returns 0 if a link should be made to a new file (ie: when the file
#    is a new file but the newFile flag is 0).
# Returns 1 if a link to an existing file is made,
# Returns 2 if a link to a new file is made (only if $newFile is set)
# Returns negative on error.
sub zMakeFileLink
{
    my($bpc, $md5, $name, $newFile, $compress) = @_;

	$compress = $Conf{CompressLevel} unless defined $compress;

	my ($d,$ret) = zFile2MD5($bpc, $md5, $name, 0, $compress);
	return -5 if $ret < 0;
	$bpc->MakeFileLink($name, $d, $newFile, $compress);
}


# Use the following to create a new file link ($copy) that links to
# the same pool file as the original ($orig) file does (or
# should). i.e. this doesn't assume that $orig is properly linked to
# the pool. This is computationally more costly than just making the
# link, but will avoid further corruption whereby you get archive
# files with multiple links to each other but no link to pool.

# First, check if $orig links to the pool and if not create a link
# via MakeFileLink.  Don't create a new pool entry if newFile is zero.
# If a link either already exists from the original to the pool or if
# one was successfully created, then simply link $copy to the same
# pool entry.  Otherwise, just copy (don't link) $orig to $copy
# and leave it unlinked to the pool.  

# Note that checking whether $orig is linked to the pool is
# cheaper than running MakeFileLink since we only need the md5sum
# checksum.
# Note we assume that the attrib entry for $orig (if it exists) is
# correct, since we use that as a shortcut to determine the filesize 
# (via zFile2MD5)
# Returns 1 if a link to an existing file is made,
# Returns 2 if a link to a new file is made (only if $newFile is set)
# Returns 0 if file was copied (either because MakeFileLink failed or 
#   because newFile=0 and no existing pool match
# Returns negative on error.
sub zCopyFileLink 
{
	my ($bpc, $orig, $copy, $newFile, $compress) = @_;
	my $ret=1;
	$compress = $Conf{CompressLevel} unless defined $compress;
	my $md5 = Digest::MD5->new;
	my ($md5sum, $md5ret) = zFile2MD5($bpc, $md5, $orig, 0, $compress);

    #If $orig is already properly linked to the pool (or linkable to pool after running 
	#MakeFileLink on $orig) and HardLinkMax is not exceeded, then just link to $orig.
	if($md5ret > 0) { #If valid md5sum and non-zero length file (so should be linked to pool)...
		if((GetPoolLinkMD5($bpc, $orig, $md5sum, $compress, 0) == 1 || #If pool link already exists
			($ret = $bpc->MakeFileLink($orig, $md5sum, $newFile, $compress))> 0) #or creatable by MakeFileLink
		   && (stat($orig))[3] < $bpc->{Conf}{HardLinkMax}) {     # AND (still) less than max links
			return $ret if link($orig, $copy); # Then link from copy to orig
		}
	}
	if(copy($orig, $copy) == 1) { #Otherwise first copy file then try to link the copy to pool
		if($md5ret > 0 && ($ret = $bpc->MakeFileLink($copy, $md5sum, $newFile, $compress))> 0) {
			return 2; #Link is to a new copy
		}
		printerr "Failed to link copied file to pool: $copy\n";	
		return 0; #Copy but not linked
	}
	die "Failed to link or copy file: $copy\n";	
	return -1;
}

# Copy $source to $target, recursing down directory trees as
# needed. The 4th argument if non-zero, means (for files) use
# zCopyFileLink to make sure that everything is linked properly to the
# pool; otherwise, just do a simple link. The 5th argument $force,
# will erase a target directory if already present, otherwise an error
# is signalled. The final 6th argument is the CompressionLevel which
# can be left out and will the be calculated from
# bpc->Conf{CompressLevel}

# Note the recursion is set up so that the return is negative if
# error, positive if consistent with a valid pool link (i.e. zero
# length file or directories are consistent too), and zero if
# successful but not consistent with a valid pool.  The overall result
# is -1 if there is any error and otherwise the AND'ed result of the
# operations. That means if the overall result is positive then the
# whole tree is successfully linked to the pool, so the next time
# around we can use a simple linking (i.e $linkcheck=0).  Note $source
# and $target must be full paths from root.  Note: if files are not
# compressed properly then you won't be able to link them to pool.
sub link_recursively_topool
{
    my ($bpc, $source, $target, $linkcheck, $force, $compress) = @_;
	my $ret=1;
	die "Error: '$source' doesn't exist" unless -e $source;
	if (-e $target) {
		die "Error can't overwrite: $target (unless 'force' set)\n" unless $force;
		die "Error can't remove: $target ($!)\n"  unless rmtree($target, 0, 0);
	}
	if (-d $source) {
		die "Error: mkdir failed to create new directory: $target ($!)\n"
			unless jmkdir($target);
		opendir( my $dh, $source) || die "Error: Could not open dir $source ($!)\n";
		foreach my $elem (readdir($dh)) {
            next if /^\.\.?$/;     # skip dot files (. and ..)
			my $newsource = "$source/$elem";
			my $newtarget = "$target/$elem";
            my $newret = link_recursively_topool($bpc, $newsource, $newtarget, $linkcheck, $force, $compress);
			if ($newret < 0) { #Error so break out of loop & return
				closedir $dh;
				return -1 
			}
			$ret = 0 if $newret == 0; #note $ret stays at 1 only if no elements return -1 or 0
        }
		closedir $dh;
		return $ret;
	}
	elsif ($dryrun) {return 1} # Return before making changes to filesystem
	elsif ( ! -s $source) { # zero size
		copy($source, $target); #No link since zero size
	}
	elsif ($linkcheck) { #Makes sure first that source properly linked to pool
		return(zCopyFileLink($bpc, $source, $target, 1, $compress));
	}
	else {#Otherwise, OK to perform simple link to source
		return (link($source, $target) == 1 ? 1 : -1)
	}
}

sub get_bakinfo
{
	my ($bakinfofile, $entry) = @_;
	our %backupInfo = ();

	$bakinfofile .= "/backupInfo";
	warn "Can't read $bakinfofile\n" unless -f $bakinfofile;

	unless (my $ret = do $bakinfofile) { # Load  the backupInfo file
		if ($@) {
			warn "couldn't parse $bakinfofile: $@\n";
		}
		elsif (!defined $ret) {
			warn "couldn't do $bakinfofile: $!\n";
		}
		elsif (! $ret) {
			warn "couldn't run $bakinfofile\n";
		}
	}
	my $value = $backupInfo{$entry};
	warn "$bakinfofile is empty or missing entry for '$entry'\n"  
		unless defined $value;
	return $value;
}

# Note: getattr($attr) =$attr->{files} 
#       getattr($attr, $file) =$attr->{files}{$file}
#       getattr($attr, $file, $attribute)  =$attr->{files}{$file}{$attribute}

sub getattr
{
    my($attr, $fileName, $Attribute) = @_;
    return $attr->{files}{$fileName}{$Attribute} if ( defined($Attribute) );
    return $attr->{files}{$fileName} if ( defined($fileName) );
    return $attr->{files};
}


#Reads in the attrib file for directory $_[1] and (optional alternative 
#attrib file name $_[2]) and #stores it in the hashref $_[0] passed to 
#the function
#Returns -1 and a blank $_[0] hash ref if attrib file doesn't exist 
#already (not necessarily an error)
#Dies if attrib file exists but can't be read in.
sub read_attrib
{ 
#Note: $_[0] = hash reference to attrib object
#SO DO NOT USE LOCAL VARIABLE FOR IT (i.e. don't do my $attr=$_[0]
	$_[0] = BackupPC::Attrib->new({ compress => $Conf{CompressLevel} });

#	unless (defined $_[1]) { #JJK: DEBUGGING
#		print "read_attrib: \$_[1] undefined\n";
#		print Dumper @_;
#	}
	return -1 unless -f attrib($_[1], $_[2]);
    #This is not necessarily an error because dir may be empty

	$_[0]->read($_[1],$_[2]) or
		die "Error: Cannot read attrib file: " . attrib($_[1],$_[2]) . "\n";

	return 1;
}

#Same as Lib.pm fileNameUnmangle but doesn't require
#unneccessary '$bpc'
sub jfileNameUnmangle {
    my($name) = @_;

    $name =~ s{/f}{/}g;
    $name =~ s{^f}{};
    $name =~ s{%(..)}{chr(hex($1))}eg;
    return $name;
}
	
sub count_file_attribs
{
	my ($attrib) = @_;
	return( scalar (keys (%{$attrib->get()})));
}

# Get attrib entry for $fullfilname. The corresponding hash is both returned and
# also fills the hash reference (optionally) passed via $fileattrib.
# If attrib file not present, return -1 (which may not be an error)
# Returns -2 if not a mangled file
# Dies if error
sub get_file_attrib
{
	my ($fullfilename, $fileattrib) = @_;
	$fullfilename =~ m{(.+)/(.+)};  #1=dir; $2=file
	return -2 unless defined $2;

	return -1 if read_attrib(my $attr, $1) < 0;

	%{$fileattrib} =  %{$attr->{files}{jfileNameUnmangle($2)}};
	#Note unmangling removes initial 'f' AND undoes char mapping
}

# Returns value of attrib $key for $fullfilename (full path)
# If not a mangled file or attrib file not present or there is not an
# entry for the specificed key for the given file, then return 'undef'
sub get_attrib_value
{
	my ($fullfilename, $key) = @_;
	$fullfilename =~ m{(.+)/(.+)};  #1=dir; $2=file

	return undef unless defined $2;
	return undef if read_attrib(my $attr, $1) < 0;
	return $attr->{files}{jfileNameUnmangle($2)}{$key}; 
    #Note this returns undefined if key not present
	#Note unmangling removes initial 'f' AND undoes char mapping
}

# Returns value of attrib type key for $fullfilename (full path)
# If attrib file present but filename not an entry, return -1 [not an error if file nonexistent]
# If no attrib file (but directory exists), return -2 [not an error if directory empty]
# If directory non-existent, return -3
# If attrib file present but not readble, return -4 [true error]
# Note there may an entry even if file non-existent (e.g. type 10 = delete)
sub get_attrib_type
{
	my ($fullfilename) = @_;
	$fullfilename =~ m{(.+)/(.+)};  #1=dir; $2=file

#	unless (defined $1) { #JJK: DEBUGGING
#		print "get_attrib_type: \$1 undefined\n";
#		print Dumper @_;
#	}

	return -3 unless -d $1;
	return -2 unless -f attrib($1);
	return -4 unless read_attrib(my $attr, $1) >= 0;
	my $type = $attr->{files}{jfileNameUnmangle($2)}{type};
	#Note unmangling removes initial 'f' AND undoes char mapping
	return (defined($type) ? $type : -1);
}

# Write out $attrib hash to $dir/attrib (or $dir/$attfilename) and
# link appropriately to pool.
# Non-zero 4th argument $poollink creates link to pool (using MakeFileLink)
# after writing the attrib file.
# 5th argument tells what to do if no files in $attrib 
# (0=return error, 1=delete attrib file and return success)
# 6th argument is an optional alternative name for the attrib file itself
# Note does an unlink first since if there are hard links, we don't want
# to modify them
# Returns positive if successful, 0 if not
# Specifically, 1 if linked to existing, 2 if linked to new, 
# 3 if written without linking, 4 if (empty) attrib file deleted/not-written

sub write_attrib
{
	my ($bpc, $attrib, $dir, $poollink, $delempty, $attfilename) = @_; 
	die "Error: Cannot write to directory: $dir\n" unless -w $dir;

#	unless (defined $dir) { #JJK: DEBUGGING
#		print "write_attrib: \$dir undefined";
#		print Dumper @_;
#	}

	my $attfilepath = attrib($dir, $attfilename);

	if(count_file_attribs($attrib) == 0) { #No new attrib file to write
		if(-e $attfilepath && $delempty) { #Delete existing attrib file
			#Delete existing attrib file
			die "Error: could not unlink existing attrib file: $attfilepath\n"  
				unless(junlink($attfilepath));
		}
		return 4;
	}

	#First (safely) write out new attrib file to directory
	my $tempname = mktemp("attrib.XXXXXXXXXXXXXXXX");
	unless($attrib->write($dir, $tempname) == 1 &&
		   (! -e $attfilepath || junlink($attfilepath)) &&
		   jrename("$dir/$tempname", $attfilepath)) {
		unlink("$dir/$tempname") if -e "$dir/$tempname";
		die "Error: could not write attrib file: $attfilepath\n";
	}
	unlink("$dir/$tempname") if $dryrun;

	my $ret=3;
	if ($poollink) {
		my $data = $attrib->writeData;
		my $md5 = Digest::MD5->new;
		my $digest;
		if(($digest = $bpc->Buffer2MD5($md5, length($data), \$data)) eq -1
		   || ($ret = jMakeFileLink($bpc, $attfilepath, $digest, 2, 
									$Conf{CompressLevel})) <= 0) {
			printerr "Can't link attrib file to pool: $attfilepath ($ret)\n";
		}
	}
	return $ret;
}

# Write out $fileattrib for $file (without the mangle) to $dir/$attfilename (or
# to the default attribute file for $dir if $attfilename is undef)
# Reads in existing attrib file if pre-existing
# 4th argument $poollink says whether to write to file (0) or link to
# pool (using MakeFileLink).
# Returns positive if successful, 0 if not
# Specifically, 1 if linked to existing, 2 if linked to new,
# 3 if written without linking
sub write_file_attrib
{
    my ($bpc, $dir, $file, $fileattrib, $poollink, $attfilename) = @_; #Note $fileattrib is a hashref
	my $ret=0;

	read_attrib(my $attr, $dir, $attfilename); #Read in existing attrib file if it exists
	$ret = write_attrib($bpc, $attr, $dir, $poollink, 0, $attfilename) 
		if $attr->set($file, $fileattrib) > 0;

#	unless (defined $dir) { #JJK: DEBUGGING
#		print "write_file_attrib: \$dir undefined\n";
#		print Dumper @_;
#	}

	die "Error writing to '$file' entry to attrib file: " . attrib($dir, $attfilename) . "\n" unless $ret > 0;
	return $ret;
}

sub attrib
{
	return (defined($_[1]) ? "$_[0]/$_[1]" : "$_[0]/attrib");
}

#
# Given an MD5 digest $d and a compress flag, return the relative
# path (to TopDir)
#
sub MD52RPath
{
    my($bpc, $d, $compress, $poolDir) = @_;

    return if ( $d !~ m{(.)(.)(.)(.*)} );
    $poolDir = ($compress ? "cpool" : "pool")
		    if ( !defined($poolDir) );
    return "$poolDir/$1/$2/$3/$1$2$3$4";
}

# Modified version of MakeFileLink including:
# 1. Efficiency/clarity improvements
# 2. Calls GetPoolLink to find candidate link targets.
# 2. For non-compressed files, uses my improved jcompare comparison algorithm
# 3. For compressed files, uses zcompare2 which compares only the compressed
#    data sans first-byte header & potential rsync digest trailer. This allows
#    for matches even if one file has rsync digest and other does not
# 4. Moves file before unlinking in case subsequent link fails and needs to be 
#    undone
# 5. Added 6th input parameter to return pointer to the pool link name
# 6. Extended meaning of newFile flag
#      0 = Don't creat new pool file (as before)
#      1 = Create new pool file IF no other links to source file
#          (this was the previous behavior for whenever newFile was set)
#      >2 = Create new pool file EVEN if source file has more than one link
#          (this will by extension link other things linked to the source
#           also to the pool -- which means that the pool might not clean
#           if it links to things outside of the pc directory -- so 
#           use carefully
#  7. Includes 'j' versions of file routines to allow dryrun
#  8. Added check to see if already in pool and if so returns 3

# For each file, check if the file exists in $bpc->{TopDir}/pool.
# If so, remove the file and make a hardlink to the file in
# the pool.  Otherwise, if the newFile flag is set, make a
# hardlink in the pool to the new file.
#
# Returns 0 if a link should be made to a new file (ie: when the file
#    is a new file but the newFile flag is 0).
#    JJK: of if newFile =1 and $name has nlinks >1
#    JJK: This is to allow erroring on the case where the pc file is linked
#    JJK: to something other than the pool
# Returns 1 if a link to an existing file is made,
# Returns 2 if a link to a new file is made (only if $newFile is set)
# Return 3 if first finds that already linked to the appropriate pool chain
# Returns negative on error.

sub jMakeFileLink
{
	my($bpc, $name, $d, $newFile, $compress, $linkptr) = @_;

	return -7 unless -s $name; #Zero size file shouldn't be linked to pool

	my $poollink;
	my $result=GetPoolLinkMD5($bpc, $name, $d, $compress, 1, \$poollink);
	$$linkptr = $poollink if defined($linkptr) && $result > 0;

	if($result == 1){ #Already linked to the $d pool chain
		return 3;
	}elsif($result == 2) { #Matches existing, linkable pool file
		my $tempname = mktemp("$name.XXXXXXXXXXXXXXXX");
		return -5 unless jrename($name, $tempname); #Temorarily save
		if(!jlink($poollink, $name)) { #Link $name to existing pool file
			jrename($tempname, $name); #Restore if can't link
			return -3;
		}
		junlink($tempname); #Safe to remove the original
		return 1;
	}elsif($result == 3) { #No link or match
		if(defined($newFile) &&
		   ($newFile > 1 || ($newFile == 1 && (stat($name))[3] == 1 ))) {
			$poollink =~ m|(.*)/|;
			jmkpath($1, 0, 0777) unless -d $1 ;
			return -4 unless jlink($name, $poollink);
			return 2;
		} else { #New link should have been made but told not to
			return 0;
		}
	}else {
		return -6; #Error from GetPoolLink call
	}
}

# GetPoolLink
# GetPoolLinkMD5
#Find the pool/cpool file corresponding to file $name.
#1. First iterates entire chain to see if *same inode* is present. I.e. if 
#   already linked to the chain. If so, it returns the first instance. 
#   Return = 1 and $Poolpathptr = name of the hard-linked match
#2. If $compareflg is set, then iterate through again this time looking for
#   file *content* matches (this is much slower). 
#   If so, it returns the first instance with Nlinks < HardLinkMax
#   Return = 2 and $Poolpathptr = name of the content match
#3. Finally, if not linked (and also not matched if $compareflg set)
#   Return=3 and $$poolpathptr = first empty chain
#Note: Return zero if zero size file
#      Return negative if error.
#Note: if chain has multiple copies of the file, then it returns the first linked
#match if present and if none and $compareflag set then the first content match
sub GetPoolLink
{
	my($bpc, $md5, $name, $compress, $compareflg, $poolpathptr) = @_;

	$compress = $bpc->{Conf}{CompressLevel} unless defined $compress;

	my ($md5sum , $ret) = defined($compress) && $compress > 0 ?
		zFile2MD5($bpc, $md5, $name, 0, $compress) : 
		$bpc->File2MD5($md5, $name);

	return 0 if $ret == 0; #Zero-sized file
	return -3 unless $ret >0;

	GetPoolLinkMD5($bpc, $name, $md5sum, $compress, $compareflg, $poolpathptr);
}

sub GetPoolLinkMD5
{
	my($bpc, $name, $md5sum, $compress, $compareflg, $poolpathptr) = @_;
	my($poolbase, $i);

	return -1 unless -f $name;
	my $inode = (stat(_))[1];  #Uses stat from -f
	return 0 if (stat(_))[7] == 0; #Zero-size (though shouldn't really happen since
                                   #md5sum input not defined for zero sized files

	$compress = $bpc->{Conf}{CompressLevel} unless defined $compress;

	return -2 unless 
		defined($poolbase = $bpc->MD52Path($md5sum, $compress));

	#1st iteration looking for matching inode
	$$poolpathptr = $poolbase;
	for($i=0; -f $$poolpathptr; $i++) { #Look for linked copy (inode match)
		return 1 if ((stat(_))[1] == $inode);
		$$poolpathptr = $poolbase . '_' . $i; #Iterate
	}

	return 3 unless $compareflg; #No inode match

	#Optional 2nd iteration looking for matching content
	my $compare = defined($compress) && $compress > 0 ? \&zcompare2 : \&jcompare;
	$$poolpathptr = $poolbase;
	for(my $j=0; $j<$i; $j++ ) { #Look for content match
		return 2 if (stat($$poolpathptr))[3] < $bpc->{Conf}{HardLinkMax} &&
			!$compare->($name, $$poolpathptr);
		$$poolpathptr = $poolbase . '_' . $j; #Iterate
	}
	# No matching existing pool entry - $$poolpathptr is first empty chain element
	return 3;
}

#Test if $file is a valid link to the pool tree determined by $compresslvl.
#Returns the name of the pool file link if it is valid, 0 otherwise.
#Size is optional and is as described for zFile2MD5
#Note this is similar to GetPoolLink but faster though less powerful
sub is_poollink
{
	my ($bpc, $md5, $file, $size, $compresslvl)= @_;

	return 0 unless -f $file;

    $compresslvl = $Conf{CompressLevel} unless defined $compresslvl;
	my ($filemd5, $ret) = zFile2MD5($bpc, $md5, $file, $size, $compresslvl);
	return 0 unless $ret > 0;

	my $finode = (stat($file))[1];
	my $poolpath = $bpc->MD52Path($filemd5, $compresslvl);

	my %poolhash = map {(stat($_))[1] => $_} bsd_glob($poolpath . "_*");
	$poolhash{(stat(_))[1]} = $poolpath if -f $poolpath; #Add poolpath
	#Note we can't add $poolpath to the glob in case it doesn't exist
	#since glob will return a non-existing non-pattern match entry as-is

	foreach (keys %poolhash) {
		if($_ == $finode) {
			$poolhash{$_} =~ m|.*/(.*)|;
			return $1;
		}
	}
	return 0;
}


#Convert pool name to constant length string consisting
#of 32 hex digits for the base plus 6 (left) zero padded digits for
#the chain suffix (suffixes are incremented by 1 so that no suffix 
#records as 0). Note this accomodates chains up to 10^6 long.
#Use a number bigger than 6 if you have longer chains
#Useful if you want to order (e.g., sort) pool file names numerically
sub poolname2number
{
	$_[0] =~ m|(.*/)?([^_]*)(_(.*))?|;
	my $suffix = defined($4) ? $4+1 : 0;
	return sprintf("%s%06s", $2, $suffix)
}

# Renumber pool chain holes starting at $firsthole and continue up the chain 
# to fill up # to $maxgap holes (which need not be contiguous).
# If $maxgap is not specified it defaults to 1 (sufficient to cover one 
# deletion - i.e. hole -- which may be $file itself)
# If $firsthole exists, it is an error. Use delete_pool_file instead,
# if you want to delete first before renumbering.
# Return 1 on success; Negative on failure
sub renumber_pool_chain
{
	my ($firsthole, $maxholes) = @_;
	
	$maxholes = 1 unless defined $maxholes;
	
	my ($base, $sufx);
	if($firsthole =~ m|(.*_)([0-9]+)|) {
		$base = $1;
		$sufx = $2;
	}else{
		$base = $firsthole . "_";
		$sufx = -1; #Corresponds to no suffix
	}

	my $nextsufx = $sufx+1;
	my $nextfile = $base .  $nextsufx;
	while($nextsufx - $sufx <= $maxholes) {
		if(-e $nextfile) { #Non-hole so move/renumber
			if(-e $firsthole || ! jrename($nextfile, $firsthole)) {
				warn "Error: Couldn't renumber pool file: $nextfile --> $firsthole\n";
				return -2;
			}
#			print "Renumbering: $nextfile --> $firsthole\n";
			$firsthole = $base . (++$sufx); #Move hole up the chain
		}
		$nextfile = $base . (++$nextsufx);
	}
	return 1;
}

# Delete pool file (if it exists) and regardless renumber 
# chain to fill hole left by file. Fill up to $maxholes
# above $file (including the hole left by $file) where
# $maxholes defaults to 1.
# If $file doesn't exist, then it just fills the hole
# left by $file.
# Return 1 on success; Negative on failure
sub delete_pool_file
{
	my ($file, $maxholes) = @_;

	if(-e $file && !junlink($file)) { #Try to delete if exists
		warn "Error: Couldn't unlink pool file: $file\n";
		return -3;
	}
	return(renumber_pool_chain($file,$maxholes)); #Returns -1/-2 on fail
}

# Run BackupPC_nightly
sub run_nightly
{
	my ($bpc) = @_;
    my $err = $bpc->ServerConnect($Conf{ServerHost}, $Conf{ServerPort});
    if ($err) {
        printerr "BackupPC_nightly: can't connect to server ($err)...\n";
        return($err);
    }
    if ((my $reply = $bpc->ServerMesg("BackupPC_nightly run")) eq "ok\n" ) {
        $bpc->ServerMesg("log $0: called for BackupPC_nightly run...");
        print "BackupPC_nightly scheduled to run...\n";
        return 0;
    }
    else {
        printerr "BackupPC_nightly ($reply)...\n";
        return $reply;
    }
}


# Rewrite of compare function (in File::Compare) since it messes up on
# weird filenames with spaces and things (since it combines the "<"
# and the filename in a single string). Also streamline the code by
# getting rid of of extra fluff and removing code for comparing text
# files while we are at. The basic algorithm remains the classic one
# for comparing 2 raw files.
# Returns 0 if equal, 1 if not equal. And negative if error.
sub jcompare {
    my ($file1,$file2,$size) = @_;
	my ($fh1open, $fh2open, $fh1size);
	my $ret=0;

    local (*FH1, *FH2);
	unless (($fh1open = open(FH1, "<", $file1)) &&
			($fh2open = open(FH2, "<", $file2))) {
		$ret = -1;
		goto compare_return;
	}
	binmode FH1;
	binmode FH2;
	if (($fh1size = -s FH1) != (-s FH2)) {
		$ret=1;
		goto compare_return;
	}

	unless (defined($size) && $size > 0) {
	    $size = $fh1size;
	    $size = LINUX_BLOCK_SIZE if $size < LINUX_BLOCK_SIZE;
	    $size = TOO_BIG if $size > TOO_BIG;
	}

	my $data1 = my $data2 = '';
	my ($r1,$r2);
	while(defined($r1 = read(FH1,$data1,$size)) && $r1 > 0) {
	    unless (defined($r2 = read(FH2,$data2,$r1)) && $r2 == $r1
				&& $data2 eq $data1) {
			$ret=1;
			goto compare_return;
	    }
	}
	$ret=1  if defined($r2 = read(FH2,$data2,LINUX_BLOCK_SIZE)) && $r2 > 0;
	$ret =-2 if $r1 < 0 || $r2 < 0; # Error on read

  compare_return:
	close(FH1) if $fh1open;
	close(FH2) if $fh2open;
	return $ret;
}

# Version of compare for BackupPC compressed files. Compares inflated
# (uncompressed) versions of files. The BackupPC::FileZIO subroutine
# is used to read in the files instead of just raw open & read.  Note
# this version of compare is relatively slow since you must use zlib
# to decompress the streams that are being compared. Also, there is no
# shortcut to just first compare the filesize since the size is not
# known until you finish reading the file.
sub zcompare {
	my ($file1, $file2, $compress)=@_;
	my ($fh1, $fh2);
	my $ret=0;

	$compress =1 unless defined $compress;
	unless ((defined ($fh1 = BackupPC::FileZIO->open($file1, 0, $compress))) &&
			(defined ($fh2 = BackupPC::FileZIO->open($file2, 0, $compress)))) {
		$ret = -1;
		goto zcompare_return;
	}
	my $data1 = my $data2 = '';
	my ($r1, $r2);
	while ( ($r1 = $fh1->read(\$data1, 65536)) > 0 ) {
		unless ((($r2 = $fh2->read(\$data2, $r1)) == $r1)
				&& $data1 eq $data2) {
			$ret=1;
			goto zcompare_return;
		}
	}
	$ret =1 if ($r2 = $fh2->read(\$data2, LINUX_BLOCK_SIZE)) > 0; #see if anything left...
	$ret =-1 if $r1 < 0 || $r2 < 0; # Error on read

  zcompare_return:
	$fh1->close() if defined $fh1;
	$fh2->close() if defined $fh2;
	return $ret;
}

# Second alternative version that combines the raw speed of jcompare
# with the ability to compare compressed files in zcompare.  This
# routine effectively should compare two compressed files just as fast
# as the raw jcompare. The compare algorithm strips off the first byte
# header and the appended rsync checksum digest info and then does a
# raw compare on the intervening raw compressed data. Since no zlib
# inflation (decompression) is done it should be faster than the
# zcompare algorithm which requires inflation. Also since zlib
# compression is well-defined and lossless for any given compression
# level and block size, the inflated versions are identical if and
# only if the deflated (compressed) versions are identical, once the
# header (first byte) and trailers (rsync digest) are stripped
# off. Note that only the data between the header and trailers have
# this uniqe mapping. Indeed, since BackupPC only adds the checksum
# the second time a file is needed, it is possible that the compressed
# value will change with time (even though the underlying data is
# unchanged) due to differences in the envelope. Note, to some extent
# this approach assumes that the appended digest info is correct (at
# least to the extent of properly indicating the block count and hence
# compressed data size that will be compared)
sub zcompare2 {
    my ($file1,$file2,$size) = @_;
	my ($fh1, $fh2, $fh1open, $fh2open);
	my $Too_Big = 1024 * 1024 * 2;
	my $ret=0;
	
	unless (($fh1open = open($fh1, "<", $file1)) &&
			($fh2open = open($fh2, "<", $file2))) {
		$ret = -1;
		goto zcompare2_return;
	}
	binmode $fh1;
	binmode $fh2;
	
	my $fh1size = -s $fh1;
	my $fh2size = -s $fh2;

	my $data1 = my $data2 = '';
	unless (read($fh1, $data1, 1) == 1 &&  # Read first byte
			read($fh2, $data2, 1) == 1) {
		$ret = -1;
		goto zcompare2_return;
	}
	if (vec($data1, 0, 8) == 0xd6 || vec($data1, 0, 8) == 0xd7) {
		return -2 unless ( defined(seek($fh1, -8, 2)) ); 
		return -3 unless ( read($fh1, $data1, 4) == 4 );
		$fh1size -= 20 * unpack("V", $data1) + 49;
		# [0xb3 separator byte] + 20 * NUM_BLOCKS + DIGEST_FILE(32) + DIGEST_INFO (16)
		# where each of NUM_BLOCKS is a 20 byte Block digest consisting of 4 bytes of
		# Adler32 and the full 16 byte (128bit) MD4 file digest (checksum).
		# DIGEST_FILE is 2 copies of the full 16 byte (128bit) MD4 digest 
		# (one for protocol <=26 and the second for protocol >=27)
		# DIGEST_INFO is metadata consisting of a pack("VVVV") encoding of the
		# block size (should be 2048), the checksum seed, length of the
		# block digest (NUM_BLOCKS), and the magic number (0x5fe3c289).
		# Each is a 4 byte integer.
	}

	if (vec($data2, 0, 8) == 0xd6 || vec($data2, 0, 8) == 0xd7) {
		return -2 unless ( defined(seek($fh2, -8, 2)) ); 
		return -3 unless ( read($fh2, $data2, 4) == 4 );
		$fh2size -= 20 * unpack("V", $data2) + 49;
	}

	if ($fh1size != $fh2size) {
		$ret=1;
		goto zcompare2_return;
	}

	seek($fh1,1,0) ; #skip first byte
	seek($fh2,1,0) ; #skip first byte


	my $bytesleft=--$fh1size; #Reduce by one since skipping first byte
	$size = $fh1size unless defined($size) && $size > 0 && $size < $fh1size;
	$size = $Too_Big if $size > $Too_Big;

	my ($r1,$r2);
	while(defined($r1 = read($fh1,$data1,$size))
		  && $r1 > 0) {
	    unless (defined($r2 = read($fh2,$data2,$r1))
				&& $data2 eq $data1) { #No need to test if $r1==$r2 since should be same size
			$ret=1;        #Plus if it doesn't, it will be picked up in the $data2 eq $data1 line
			goto zcompare2_return;
	    }
		$bytesleft -=$r1;
		$size = $bytesleft if $bytesleft < $size;
	} 
	$ret=1  if defined($r2 = read($fh2,$data2,$size)) && $r2 > 0;
    #Note: The above line should rarely be executed since both files same size
	$ret =-2 if $r1 < 0 || $r2 < 0; # Error on read

  zcompare2_return:
	close($fh1) if $fh1open;
	close($fh2) if $fh2open;
	return $ret;
}

#Routine to remove files (unlink) or directories (rmtree)
# $fullfilename is full path name to file or directory
# Returns number of files deleted in the listref $dirfileref if defined;
sub delete_files
{
	my ($fullfilename, $dirfileref) = @_;
	my $listdeleted = defined($dirfileref);
	my $ret = -1;
	my $output;
	die "Error: '$fullfilename' doesn't exist or not writeable\n" 
		unless  -w $fullfilename;
	if(-f $fullfilename) {
		$ret = junlink($fullfilename);
	}
	elsif(-d $fullfilename) {
		$$dirfileref = "" if $listdeleted;
		open(local *STDOUT, '>', $dirfileref) #redirect standard out to capture output of rmtree
			if $listdeleted;
		$ret = jrmtree($fullfilename, $listdeleted, 1);
	}
	return $ret;
}

#Touch command (modified version of Unix notion)
#First argument is the (full)filepath.
#Zero-length file is created if non-existent.
#If existent and second argument zero or  not defined then access 
# and modification times are updated.
#Returns 1 on success, -1 on error;
sub jtouch
{
	return 1 if $dryrun;
	if(! -e $_[0]) { #Create if non-existent
		return -1 unless defined(sysopen(my $fh, $_[0], O_CREAT));
		close $fh;
	}elsif(!$_[1]) {
		my $time = time();
		utime($time, $time, $_[0]);
	}
	return 1;
}

#Simple wrappers to protect filesystem when just doing dry runs
sub jchmod
{
	return 1 if $dryrun;
	chmod @_;
}

sub jchown
{
	return 1 if $dryrun;
	chown @_;
}

sub jcopy
{
	return 1 if $dryrun;
	copy @_;
}

sub jlink
{
	return 1 if $dryrun;
	link $_[0], $_[1];
}

sub junlink
{
	return 1 if $dryrun;
	unlink @_;
}

sub jmkdir
{
	return 1 if $dryrun;
	mkdir @_;
}

sub jmkpath
{
    return 1 if $dryrun;
    mkpath @_;
}

sub jmake_path
{
    return 1 if $dryrun;
    mkpath @_;
}

sub jrename
{
	return 1 if $dryrun;
	rename $_[0], $_[1];
}

sub jrmtree
{
	return 1 if $dryrun;
	rmtree @_;
}

sub jutime
{
	return 1 if $dryrun;
	utime @_;
}

1;
