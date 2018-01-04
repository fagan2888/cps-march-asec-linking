#!/usr/bin/perl -w

# This script takes two file names as arguments.
# It compares the files line by line, ignoring the CPSID (columns 12-25).
# Any lines that disagree are reported.

use warnings;
use strict;

die "Usage: $0 FILE1 FILE2\n"
    unless @ARGV == 2
    and -f $ARGV[0]
    and -f $ARGV[1];

my ($f1, $f2) = @ARGV;

open(my $fh1, '<', $f1) or die $!;
open(my $fh2, '<', $f2)  or die $!;

my $n;

until (eof($fh1) or eof($fh2)){
    my $line1 = <$fh1>;
    my $line2 = <$fh2>;
    $n ++;
    print "\tProblem on line $n.\n"
        unless substr($line1, 0, 11) eq substr($line2, 0, 11)
        and    substr($line1, 25)    eq substr($line2, 25)
    ;
}

print "\tFiles have differing N of records.\n" unless eof($fh1) and eof($fh2);

