#!/usr/bin/perl -w
use strict;
$| = 1;

use File::Basename;
use lib dirname($0);

use Spreadsheet::DataDictionary;
use Spreadsheet::ParseExcel;
use Spreadsheet::XLSX;

my $ddData = {};
my $year  = $ARGV[0] || die "usage: marbasec.pl <year>";

my $cpsDataPath = "data/years/";

my $outputDir   = "cpsid_work_area/BASIC_ASEC/output/";

my $ddFileB   = $cpsDataPath . $year . "/data/data_dict_cps" . $year . "_03b.xls";
my $ddFileS   = $cpsDataPath . $year . "/data/data_dict_cps" . $year . "_03s.xls";

# when CPS switches to xlsx, this covers the filename change
$ddFileB = $ddFileB . "x" if -e $ddFileB . "x";
$ddFileS = $ddFileS . "x" if -e $ddFileS . "x";

my $dataFileB = $cpsDataPath . $year . "/data/cps" . $year . "_03b.dat";
my $dataFileS = $cpsDataPath . $year . "/data/cps" . $year . "_03s.dat";

my $files = {
	b => {
		dd   => $ddFileB,
		data => $dataFileB,
	},
	s => {
		dd   => $ddFileS,
		data => $dataFileS,
	},
};

my $merge = {};
my $varLocations = {};

print "---------------------------------------------------------------------------\n";
print "MARBASEC CPS linking script\n";
print "---------------------------------------------------------------------------\n";
print "First pass:\n";
print "Process 03b and 03s samples to identify record link across samples...\n";

# Read in the 03b and 03b samples, identify unique persons/household with a combination
# of the household hrhhid1 and hrhhid2 values, plus pulineno for person records
# (household records get a pulineno of 00)
# Use the unique value as a hash key, and then generate a marbasec ID for that key
$merge = generateMarbasecIds($merge);

# match up the keys across b and s samples, and add the marbasec IDs (hash values) 
# generated in the B sample to the S sample. After this is done, we're ready to write
# the new .dat file
$merge = mergeKeys($merge);

# Show how many records we found in the b sample, in the s sample, 
# and how many are common between them
for my $sample ('b', 's', 'common') {
	print $year . " " . $sample . ":\t";
	print scalar(keys %{ $merge->{$sample} });
	print "\n";
}

# The $merge hash is fully populated, now take a second pass on the input
# data and write it out with the marbasec IDs appended to the end
#
print "\nSecond pass:\nWriting output files with MARBASECID to $outputDir\n";
writeOutput();

# Do a reverse match as a quality assurance check
# Pick 1000 marbasec IDs at random, find those lines in both the
# b and s samples, extract the hrhhid/hrhhid2/pulineno values from the line
# and verify they are the same between matched records
#
print "\nThird Pass:\nQuality check of output files\n";
spotCheckLinkedRecords();

##########################################################
sub spotCheckLinkedRecords {

	# using only MARBASECID, link records across b and s samples and 
	# verify they have identical aggregate keys of hrhhid, hrhhid, and (if P) pulineno
	
	# how many ids did we generate?
	my $numberOfLinks = scalar(keys %{ $merge->{common} });

	my $allgood = 1;

	# generate 10 MARBASECIDs at random somewhere between 1..$numberOfLinks;
	my $check = {};
    my $generated = {};
	for my $i (1..1000) {
		my $n = int(rand($numberOfLinks)) + 1;
		my $num = sprintf("%06d", $n);

		# prepend with k because perl doesn't want hash keys starting with numbers
		my $marbasecID = 'k11' . substr($year, 2, 2) . $num; 

		$generated->{$marbasecID} = {};
	}

	for my $sample ( 'b', 's' ) {
		$varLocations = getTargetFields($sample, $year);
		print "Checking $sample sample:\n";

		my $file = $outputDir . "cps" . $year . "_03" . $sample . ".dat";
		my ($hrhhid, $hrhhid2, $pulineno);
		my $inc = 0;
		open(IN,  "< " . $file  ) or die "could not read $file $!";
		while(<IN>) {
			chomp;
			my $line = $_;
			my $id = substr($line, -10, 10);
			my $key = 'k' . $id;
			if ($line =~ /^H/) {
				$hrhhid  = getSubstr($line, 'hrhhid');
				$hrhhid2 = getSubstr($line, 'hrhhid2');
			}
			if (defined $generated->{$key}) {
				if ($line =~ /^H/) {
					$check->{$key}{$sample}{'hrhhid'} = $hrhhid;
					$check->{$key}{$sample}{'hrhhid2'} = $hrhhid2;
					$check->{$key}{$sample}{'pulineno'} = '00';
				}
				elsif ($line =~ /^P/) {
					$check->{$key}{$sample}{'hrhhid'} = $hrhhid;
					$check->{$key}{$sample}{'hrhhid2'} = $hrhhid2;
					$pulineno = getSubstr($line, 'pulineno');
					$check->{$key}{$sample}{'pulineno'} = $pulineno;
				}
			}
			print "." if $inc % 10000 == 0;
			$inc++;
		}
		close(IN);
		print "\n";
	}

	print "Checking across b and s samples:\n";
	for my $checkMe ( sort keys %$check ) {
		for my $field ( 'hrhhid', 'hrhhid2', 'pulineno' ) {
			if ( defined($check->{$checkMe}{b}{$field}) and defined($check->{$checkMe}{s}{$field}) and
                 ($check->{$checkMe}{b}{$field} eq $check->{$checkMe}{s}{$field}) ) {
				print ".";
			}
			else {
				print "\n!!! $checkMe between b and s samples did not match\n";
				$allgood = 0;
			}
		}
	}

	if ($allgood) {
		print "\nEverything looks good\n";
	}
	else {
		print "\nSomething doesn't look right in the output data. Please investigate further.\n";
	}

}

sub generateMarbasecIds {
	my $merge = shift;
	for my $sample ( 'b', 's' ) {
		print $year . "_03" .  $sample . ":\n";
		# find column locations of interest
		$varLocations = getTargetFields($sample, $year);
		# digest data file and compile hash of hid1/hid2/pulineno
		$merge->{$sample} = generateMergeKeys($files->{$sample}{data}, $varLocations, $year, $sample);
	}
	return $merge;
}

sub writeOutput {
	for my $sample ( 'b', 's' ) {
		print $year . "_03" .  $sample . ":\n";
		my $outfile = $outputDir . "cps" . $year . "_03" . $sample . ".dat";
		my $infile  = $files->{$sample}{data};
		my $inc;
		my $abbr_year = substr($year, 2, 2);
		# find column locations of interest
		$varLocations = getTargetFields($sample, $year);

		if ($sample eq 'b') {
			$inc = 400001;
		}
		else {
			$inc = 1;
		}
		my ($hrhhid1, $hrhhid2, $pulineno);
		open(OUT, "> " . $outfile ) or die "could not write $outfile: $!";
		open(IN,  "< " . $infile  ) or die "could not read $infile: $!";

        my $loc = {};

		# incrementers
		my $hits = 0;
		my $misses = 0;
		my $hh_hits = 0;
		my $hh_misses = 0;
		my $lineCount = 0;

		while(<IN>) {
			chomp;
			my $key;
			if (/^H/) {
				$hrhhid1 = getSubstr($_, 'hrhhid');
				$hrhhid2 = getSubstr($_, 'hrhhid2');
				$key = get_key($hrhhid1, $hrhhid2, '00');
			    if ( exists $merge->{common}{$key} ) {
			    	$hh_hits++;
                }
                else {
                    $hh_misses++;
                }
                if (! defined($loc->{'H'})) {
                    $loc->{'H'} = length($_) + 1;
                }
			}
			elsif (/^P/) {
				$pulineno = getSubstr($_, 'pulineno');
				$key = get_key($hrhhid1, $hrhhid2, $pulineno);
                if (! defined($loc->{'P'})) {
                    $loc->{'P'} = length($_) + 1;
                }
			}
			if ( exists $merge->{common}{$key} ) {
				$hits++;
				print OUT $_ . $merge->{$sample}{$key} . "\n";
			}
			else {
				$misses++;
				my $noMergeCount = sprintf("%06d", $inc);
				print OUT $_ . '00' . $abbr_year . $noMergeCount . "\n";
				$inc++;
			}
			$lineCount++;
			print "." if $lineCount % 10000 == 0;
		}
		close(IN);
		close(OUT);

		if ($sample eq 'b') {
    		print "\n\nBasic Sample--\n";
        }
        else {
		    print "\n\nASEC Sample--\n";
        }
        for my $rectype ('H', 'P') {
            print "Col and Wid of marbasecid in $rectype records: " .
                    $loc->{$rectype} . ", 10\n";
        }
		print "Merge Keys Written:\t$hits ($hh_hits households)\n";
		print "No Merge Key Found:\t$misses ($hh_misses households)\n";
		my $total = $hits + $misses;
		my $hh_total = $hh_hits + $hh_misses;
		print "Total records:     \t$total ($hh_total households)\n\n";

	}
	return 1;
}


sub getSubstr {
	my $line = shift;
	my $field = shift;
    if ( ! defined $varLocations->{$field}{col} || ! defined $varLocations->{$field}{wid} ) {
        die "FATAL: There is no information known via this program about $field for this year.";
    }
	return substr($line, $varLocations->{$field}{col}, $varLocations->{$field}{wid});
}

sub mergeKeys {
	my $merge = shift;
	for my $key (keys %{ $merge->{s} } ) {
		if (exists $merge->{b}{$key}) {
			$merge->{common}{$key}++;
			$merge->{s}{$key} = $merge->{b}{$key}; # slurp in the marbasec ID
		}
	}
	return $merge;
}

sub generateMergeKeys {
	my $infile = shift;
	my $varLocations = shift;
	my $year = shift;
	my $sample = shift;

	my $keys = {};
	my $hrhhid1;
	my $hrhhid2;
    my $hrintsta;
    my $h_hhtype;
	my $pulineno;
	my $inc = 1;

	open(IN, "< " . $infile ) or die "could not read $infile: $!";
	my $key;
    my $hh_totals = 0;
    my $p_totals = 0;
    my $eligible_household = 0;
	while(<IN>) {
		chomp;
		if (/^H/) {
            $eligible_household = 0;
			$hrhhid1 = getSubstr($_, 'hrhhid');
			$hrhhid2 = getSubstr($_, 'hrhhid2');
			$key = get_key($hrhhid1, $hrhhid2, '00');
			if ($sample eq 'b') {
				my $count = sprintf("%06d", $inc);
				my $marbasecID = '11' . substr($year, 2, 2) . $count;
                $hrintsta = getSubstr($_, 'hrintsta');
                if ($hrintsta == 1) {
				    $keys->{$key} = $marbasecID;
                    $hh_totals++;
                    $eligible_household = 1;
                }
			}
			else {
                $h_hhtype = getSubstr($_, 'h_hhtype');
                if ($h_hhtype == 1) {
    				$keys->{$key}++;
                    $hh_totals++;
                    $eligible_household = 1;
                }
			}
		}
		elsif (/^P/ and $eligible_household) {
			$pulineno = getSubstr($_, 'pulineno');
			$key = get_key($hrhhid1, $hrhhid2, $pulineno);
			if ($sample eq 'b') {
				my $count = sprintf("%06d", $inc);
				my $marbasecID = '11' . substr($year, 2, 2) . $count;
				$keys->{$key} = $marbasecID;
			}
			else {
				$keys->{$key}++;
			}
            $p_totals++;
		}
		$inc++;
		print "." if $inc % 10000 == 0;
	}
	close(IN);
	print "\nDone with $infile: $inc total records analyzed\n";
    print "Linkable households:\t$hh_totals\n";
    print "Linkable people from those households\t$p_totals\n\n";
	return $keys;
}

sub get_key {
	my $key = join '', @_;
	return "k" . $key;
}

sub getVarNames {
	my ($sample, $year) = @_;
	my $yearKey = 'y' . $year;

	# write up a translation table for expected var name and what it actually is in the DD
	# so far b samples are almost identical across years except for 2013 hrhhid
	# s samples are identical across years

    # XXX this is what gets sent back for 2014 and on
    my $default = {
        b => {
            hrhhid => 'hrhhid', 
            hrhhid2 => 'hrhhid2', 
            pulineno => 'pulineno',
            hrintsta => 'hrintsta',
        },
        s => {
            hrhhid => 'h_idnum1', 
            hrhhid2 => 'h_idnum2', 
            pulineno => 'a_lineno',
            h_hhtype => 'h_hhtype',
        },
    };

    # for years in which the $default isn't correct
	my $varNames = { 
		y2013 => {
		    b => {
				hrhhid => 'hhid', 
				hrhhid2 => 'hrhhid2', 
				pulineno => 'pulineno',
                hrintsta => 'hrintsta', # correct ?
			},
		    s => {
				hrhhid => 'h_idnum1', 
				hrhhid2 => 'h_idnum2', 
				pulineno => 'a_lineno',
                h_hhtype => 'h_hhtype', # correct ?
			},
        },
	};
    if ($year != 2013) {
        return $default->{$sample};
    }
    else {
        return $varNames->{$yearKey}{$sample};
    }

}

sub getTargetFields {
	my $sample = shift;
	my $year = shift;

	my $dd        = Spreadsheet::DataDictionary->new($files->{$sample}{dd});

	my $targets   = getVarNames($sample, $year);
	my $return = {};
	for my $target (sort keys %$targets) {
		my ($colStart, $colWid) = $dd->startAndWidth($targets->{$target});

		if ( ! defined $colStart || ! defined $colWid ) {
			die "Could not find colStart and colWidth for $target";
		}

		# column counts start with 1, so make the 0-based transform here on $colStart
		$return->{$target} = {
			col => $colStart - 1,
			wid => $colWid,
		};
	}
	return $return;
}
