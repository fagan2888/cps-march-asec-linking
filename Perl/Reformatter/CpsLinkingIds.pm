#! /usr/bin/perl

$| = 1;
package Reformatter::CpsLinkingIds;

use warnings;
use strict;

use IO::File qw();
use File::Basename qw(dirname);
use Data::Dumper;
use Getopt::Long;
use YAML qw(LoadFile);

use General_utils qw(
    Record_segments
    Reduce_column_list
    Start_width_to_skip_width
    Zero_padded
);

=pod

A household is asked to particate in the CPS for 8 months. This module creates IDs
that uniquely identify households and persons for longitudinal analysis.

The module can be run directly as a script. The program takes many parameters, which
can be supplied via a YAML file or via command-line options -- or both. If both 
methods are used command-line options will trump YAML specifications. See default_opts()
for the complete list of paramenters.

The identifer created by this program (called CPSID) consists of 4 elements that
uniquely identify each household and person across the CPS project.

    CPSID = YEAR + MONTH + HHNUM + PERNUM

    where HHNUM and PERNUM are the serial numbers 
    created during the CPS reformatting process.

When a household and its members first appear in the CPS -- in other words, when
their "month in sample" (MIS) equals 1 -- the program assigns them the CPSID based on
their HHNUM and PERNUM in the CPS data file for that YEAR and MONTH.

The CPSIDs obtained by the program are inserted into the the CPS or ATUS data file
being processed. In addition, any new CPSID values that arise from the file being
processed are persisted to a CPSID data file.

More specifically, the program operates like this:

    - Given a CPS or ATUS data file to process.

    - Load into memory the persisted CPSID data files that will be needed.

        - See notes_cps_atus_linking.xlsx for examples from CPS and ATUS
          illustrating the files that need to be loaded.

    - Process the current CPS or ATUS data file.

        - For CPS:

            - If MIS=1, create a new CPSID, based on the current HHNUM and PERNUM.

            - Otherwise, look up the CPSID previously assigned, based on the survey rotation
              pattern. For details, see the %ROTATION_PATTERN variable and the Excel files noted
              above.

            - The lookup is based on the household and person identifiers from the source data,
              referred to as HH_ID and PER_ID in this program.

            - If a match is found, the CPSID is inserted into the data record.

            - Otherwise, a new CPSID is spawned, even though MIS does not equal 1.

        - For ATUS:

            - Look up the CPSID and insert it into the ATUS data file.
            
            - If a match is not found, the CPSID field in the data file is left as it is (zero
              filled).

    - Persist any new CPSID values that were generated (applies only to CPS).

The persisted CPSID data files have the structure shown below. Effectly, a CPSID file for
a given YEAR and MONTH is a concordance between the identifiers in the source data (HH_ID
and PER_ID) and the serial numbers created during the CPS reformatting process (HHNUM and
PERNUM).

    CPSID directory/
        YEAR_MONTH.dat
        YEAR_MONTH.dat
        etc.

    Where the .dat files have the following structure:

        H|MIS|HH_ID|PER_ID|YEAR|MONTH|HHNUM|00|BRIDGE_HH_ID
        P|MIS|HH_ID|PER_ID|YEAR|MONTH|HHNUM|PERNUM|BRIDGE_HH_ID
        P|MIS|HH_ID|PER_ID|YEAR|MONTH|HHNUM|PERNUM|BRIDGE_HH_ID
        P|MIS|HH_ID|PER_ID|YEAR|MONTH|HHNUM|PERNUM|BRIDGE_HH_ID
        H|etc.

The persisted CPSID files can be treated as fixed-width or delimited, depending the purpose, because
fields that might vary in length (such as MONTH) are zero-padded to a consistent width.

Note that CPSIDs are persisted to the YEAR-MONTH when the ID was spawned -- which is usually, but
not always, the YEAR-MONTH when the household's MIS equals 1.

=cut


####
# Globals.
####

my $mis_differences;
my $bridge_match = 0;
my $bridge_dup   = 0;

# Record type values.
my $RT_HH  = 'H';
my $RT_PER = 'P';

# Verbosity levels.
my %VERBOSITY = (
    QUIET   => 0,
    DEFAULT => 1,
    HIGH    => 2,
);
my $INDENT = '    - ';

# MatchResults constants.
my $MR_MIS1  = 'MIS1';
my $MR_MATCH = 'MATCH';
my $MR_AGE   = 'AGE';
my $MR_SEX   = 'SEX';
my $MR_RACE  = 'RACE';
my $MR_FAIL  = 'FAIL';

# Hard-coded info about the format of the CPSID.
my %ID_SPEC = (
    init_year    => '0000',
    init_month   => '00',
    init_pernum  => '00',
    init_age     => '00',
    init_sex     => '00',
    init_race    => '00',
    init_hhnum   => '000000',
    cpsid_width  => 14,
    join_char    => '|',
);

# Declare the metadata items that consist of lists of column locations, by project.
my %COL_LOC = (
    cps  => [ qw(hh_id per_id mis hhnum pernum age sex race bridge_hh_id ) ],
    atus => [ qw(hh_id per_id mis              cps8_year cps8_month) ],
);
$COL_LOC{all}{$_} = 1 for map { @$_ } values %COL_LOC;
$COL_LOC{all} = [ keys %{$COL_LOC{all}} ];

# Metadata to express the CPS rotation pattern, which works like this:
#    4 months in survey
#    8 months out
#    4 months in survey
#
# For example:
#    A respondent household first appears in Dec 2004 CPS.
#
#    MIS=1   Dec 2004
#        2   Jan 2005
#        3   Feb 2005
#        4   Mar 2005
#            8 months out of the survey.
#        5   Dec 2005
#        6   Jan 2006
#        7   Feb 2006
#        8   Mar 2006
#
# In %ROTATION_PATTERN, the hash keys represent MIS, as read from the CPS data
# file being processed. The corresponding hash value indicates the number of
# months ago that the respondent first appeared in the CPS.
my %ROTATION_PATTERN = (
    1 => 0,
    2 => 1,
    3 => 2,
    4 => 3,
    5 => 12,
    6 => 13,
    7 => 14,
    8 => 15,
);


####
# Create reformatter object.
# Load metadata -- from command line and YAML file.
# Validate metadata.
####

sub new {
    my $class = shift;
    my %opt = get_options_and_yaml(@_);
    validate(%opt);
    my $self = bless { %opt }, $class;
    return $self;
}

sub get_options_and_yaml {
    my %DEFAULTS = default_opts();

    # Get command-line options.
    local @ARGV = @_;
    my %opt;
    $SIG{__WARN__} = \&usage;
    GetOptions(
        \%opt,
        map {
            ref     $DEFAULTS{$_} ? "$_=s\@{,}" :
            defined $DEFAULTS{$_} ? "$_=s"      : $_
        } keys %DEFAULTS,
    );
    $SIG{__WARN__} = '';

    # Provide help if requested.
    usage() if $opt{help};

    # Load YAML options, if any.
    my $yaml = exists $opt{yaml} ? LoadFile($opt{yaml}) : {};

    # Merge defaults, YAML, and command-line so that command-line takes precedence.
    return %DEFAULTS, %$yaml, %opt;
}

sub default_opts {
    # Options can be supplied via the YAML configuration file or via command line.
    # Command-line options will override values from the YAML file.
    # Parameters shown with a default of [] take a list of start columns and widths.
    return
        # User-configurable items.
        help          => undef,      # If true, provide help and exit.
        yaml          => '',         # YAML file name.
        data          => '',         # File name of data being processed.
        year          => '',         # Year and month of that data file.
        month         => '',
        project       => '',         # cps or atus.
        hh_id         => [],         # The CPS household and person IDs from the source data.
        bridge_hh_id  => [],         # An alternate hh_id that bridges across to a previous sample batch
        bridge_start_yr => 0,        # bridge_start_yr and bridge_start_mo are used only when we need to bridge
        bridge_start_mo => 0,        # across to a previous sample batch
        inspect_hhid   => 0,         # debug option that will print extra debug info for a specific hhid

		# an empty hash for putting hh_ids that are not to be used on "crossing the bridge"
		do_not_cross_bridge => {},
        per_id        => [],
        mis           => [],         # Month in sample.
        hhnum         => [],         # The household and person serial numbers that 
        pernum        => [],         # were created during CPS reformatting.
		age           => [],
		sex           => [],
		race          => [],
        cps8_year     => [],         # Needed for ATUS: the year and month the person was at
        cps8_month    => [],         # mis==8 in the CPS.
        cpsid_dir     => '',         # Directory where CPSIDs are persisted.
        cpsid_start   => '',         # Start column of the CPSID in the data file.
        no_data       => 0,          # If true, do not write output data.
        verbosity     => 1,          # 0=quiet 1=default 2=high.
        output        => '',         # Output data file name.
        progress      => '',         # File name for progress (default: STDERR).
        progress_mode => 'a',        # a=append  w=write.

        # Other items -- not user configurable.
        # Atributes holding CpsIdSet objects. See init_idsets().
        idsets        => {},      # All.
        search_lists  => {},      # Organized by YEAR-MONTH-MIS.
        current_set   => undef,   # The CpsIdSet corresponding to the current year-month.
    ;
}

sub usage {
    my $usage = "Usage: $0 [OPTIONS] [--help]";
    my $help  = $usage;
    die $help, "\n" unless @_;
    my @msg = @_;
    chomp @msg;
    warn $_, "\n" for @msg, '', $usage;
    exit;
}

sub validate {
    my $msg;
    my %opt = @_;

    # Project and paths.
    usage("Invalid project: '$opt{project}'.") unless $opt{project} =~ /^(atus|cps)$/;
    usage("Data not found: '$opt{data}'.") unless -f $opt{data};
    usage("CPSID directory not found: '$opt{cpsid_dir}'.") unless -d $opt{cpsid_dir};

    # Integer arguments and their limits.
    my %LIMIT = (
        year        => { min => 1000, max => 9999  },
        month       => { min => 1,    max => 12    },
        cpsid_start => { min => 1,    max => 99999 },
    );
    for my $k (keys %LIMIT){
        my $msg = "Invalid $k: '$opt{$k}'.";
        usage($msg) unless is_int($opt{$k})
                     and $opt{$k} >= $LIMIT{$k}{min}
                     and $opt{$k} <= $LIMIT{$k}{max}
        ;
    }

    # Lists of column locations: check for presence.
    for my $k ( @{$COL_LOC{$opt{project}}} ){
        usage("Odd number of values for $k.") if @{$opt{$k}} % 2;
        usage("Values required for $k.") unless @{$opt{$k}} or $k eq 'bridge_hh_id'; #bridge_hh_id is optional
    }

    # Lists of column locations: make sure they are positive integers.
    for my $k ( @{$COL_LOC{all}} ){
        for my $v ( @{$opt{$k}} ){
            my $msg = "Invalid $k value: '$v'.";
            usage($msg) unless is_int($v) and $v > 0;
        }
    }
}

sub is_int {
    return 1 if $_[0] =~ /^-?\d+$/;
    return;
}

sub adjust_metadata {
    my $self = shift;

    # Metadata items that consist of lists of column locations as start-width pairs:
    #   - reduce them to a minimum set
    #   - convert to skip-with pairs
    for my $k ( @{$COL_LOC{all}} ){
        my @cols = Reduce_column_list( @{$self->{$k}} );
        @cols = Start_width_to_skip_width(@cols);
        $self->{$k} = [@cols];
    }

    # Convert start location to a skip value.
    $self->{cpsid_start} --;
}

sub open_output_files 
{
	# Set up the file handles for output.
	my $self = shift;

	$self->{output_fh}   = *STDOUT;
	$self->{progress_fh} = *STDERR;

	if (length $self->output)
	{
		$self->{output_fh} = IO::File->new($self->output, 'w');

		if (!defined($self->{output_fh}))
		{
			print STDERR "Unable to open " . $self->output . ": $!\n";
		}
	}

	if (length $self->progress)
	{
        	$self->{progress_fh} = IO::File->new($self->progress, $self->progress_mode);

		if (!defined($self->{progress_fh}))
		{
			print STDERR "Unable to open " . $self->progress . ": $!\n";
		}
		
       		$self->{progress_fh}->autoflush(1);
    	
	}
}

sub init_idsets {
    # The reformatter needs a number of CpsIdSet objects, which it
    # stores by year and month.
    #
    #    $self->{idsets}{YEAR}{MONTH} = CpsIdSet for that year-month
    #
    # This method initializes the idsets data structure.
    #
    # In addition, the same CpsIdSet objects are stored in another data
    # structure, organized by year, month, and MIS.
    #
    #    $self->{search_lists}{YEAR}{MONTH}{MIS} = [ CpsIdSet objects ]
    #
    # Each such list indicates the CpsIdSet objects to consult when
    # searching for matching CPSIDs.
    #
    # The search_lists data structure varies between CPS and ATUS.
    #     - For CPS, the YEAR and MONTH are constant: they
    #       correspond to whatever CPS data file is being processed.
    #       And MIS ranges from 1 through 8.
    #     - For ATUS, the YEAR and MONTH vary: the values are read
    #       from the data file (the cps8_year and cps8_month variables).
    #       And MIS is always 8.

    my $self = shift;
    my @ym_list = $self->project_is_atus ?
                  $self->preprocess_atus_data :
                  ( [$self->year, $self->month] );
    my @mis_list = $self->project_is_atus ? (8) : (1..8);
    my $last_month = $ym_list[-1][1]; # For ATUS, use the last available month.

    for my $ym ( @ym_list ){
        my ($year, $month) = @$ym;

        for my $mis (@mis_list){
            my $slr = $self->{search_lists}{$year}{$month}{$mis} = [];

            for my $n (1 .. $mis){
                my ($yr, $mo) =
                    $self->project_is_atus ?
                    $self->yrmo_cps($year,       $month,       $mis, $n) :
                    $self->yrmo_cps($self->year, $self->month, $mis, $n);

                $self->init_idset($yr, $mo);

				if ( $self->project_is_atus or $n != $mis ) {
                	push @$slr, { 
						idset => $self->idset($yr, $mo),
						yr    => $yr,
						mo    => $mo,
					};
				}
            }
        }
    }

    # The CpsIdSet corresponding to the current year-month.
    $self->{current_set} = $self->{idsets}{$self->year}{$last_month};
    die "The current CpsIdSet is undefined: cannot proceed.\n"
        unless defined $self->{current_set};
}

sub preprocess_atus_data {
    # Preprocess the ATUS data file to determine all unique
    # combinations of the cps8_year and cps8_month variables.
    # This determines which persisted CPSID files we will need
    # to load. Returns those combinations as a list of array refs:
    #    [YEAR, MONTH], [YEAR, MONTH], etc.

    my $self = shift;
    my %ym;

    $self->show_progress(
        $VERBOSITY{DEFAULT},
        "\nPre-processing ATUS data to determine which CPSID data files to load.",
    );

    open(my $fh, '<', $self->data) or die $!;
    while (my $line = <$fh>){
        next unless substr($line, 0, 1) eq $RT_HH;
        my $yr = $self->read_cps8_year($line);
        my $mo = $self->read_cps8_month($line);
        $mo += 0;
        $ym{"$yr/$mo"} = [$yr, $mo] unless exists $ym{"$yr/$mo"};
    }

    return sort { $a->[0] <=> $b->[0] or $a->[1] <=> $b->[1] } values %ym;
}

sub init_idset {
    # Initialize a CpsIdSet for a given year and month.
    my ($self, $yr, $mo) = @_;
    $self->{idsets}{$yr} = {} unless exists $self->{idsets}{$yr};
    $self->{idsets}{$yr}{$mo} =
        CpsIdSet->new(type => $RT_HH, year => $yr, month => $mo)
        unless exists $self->{idsets}{$yr}{$mo};
}

sub idset {
    # Returns the CpsIdSet for a given year and month.
    my ($self, $yr, $mo) = @_;
    return $self->{idsets}{$yr}{$mo};
}

sub search_list {
    # Takes a year, month, and MIS. Returns a reference to the
    # corresponding list of CpsIdSet objects to be checked when
    # looking for matching CPSIDs.
    my ($self, $yr, $mo, $mis) = @_;
    my $r = $self->{search_lists}{$yr}{$mo}{$mis};
    return $r if defined $r;
    my $msg = "CpsLinkingIds.pm error: search_list() returned undef: yr='$yr', mo='$mo', mis='$mis'.";
    die $msg, "\n";
}


####
# Run the reformatter.
####

sub run {
    my ($self);
    chomp(my $current_time = `date`);
    $self = shift;
    $self->open_output_files;
    $self->show_progress(
        $VERBOSITY{DEFAULT},
        join('', "\nRunning ", __PACKAGE__, ': ', $current_time, '.'),
    );
    $self->adjust_metadata;
    $self->init_idsets;
    $self->load_persisted_cpsids;
    $self->process_data_file;
	$self->persist_cpsids unless $self->project_is_atus;
}

sub show_progress {
    my ($self, $level, @msg) = @_;
    return unless $self->verbosity >= $level;
    print { $self->progress_fh } $_, "\n" for @msg;
}

sub load_persisted_cpsids {
    my $self = shift;

    $self->show_progress(
        $VERBOSITY{DEFAULT},
        "\nLoading persisted CPSIDs.",
    );

    my ($current_hh_id);
    for my $idset ($self->all_idsets){
        # For the current year-month, we don't need the persisted IDs (CPS only).
        next if  $self->year == $idset->year
             and $self->month == $idset->month
             and not $self->project_is_atus;

        my $file_name = $self->cpsid_file_name($idset->year, $idset->month);

        my $skip = 1;
        $skip = 0 if -f $file_name and -s $file_name;
        $self->show_progress(
            $VERBOSITY{DEFAULT},
            $INDENT . $file_name . ($skip ? ': skipped' : ''),
        );
        next if $skip;

        open(my $fh, '<', $file_name) or die $!;
        while (my $line = <$fh>){
            chomp $line;
            my ($rt, $mis, $hh_id, $per_id, $yr, $mo, $hhnum, $pernum, $age, $sex, $race, $bridge_hh_id, $dup_flag)
                = split quotemeta($ID_SPEC{join_char}), $line;
            $mo += 0;

            if (substr($line, 0, 1) eq $RT_HH){
                $current_hh_id = $idset->add(
                    CpsIdH->new(
                        mis          => $mis,
                        year         => General_utils::Zero_padded($yr,4),
                        month        => General_utils::Zero_padded($mo,2),
                        hh_id        => General_utils::Zero_padded($hh_id,6),
                        bridge_hh_id => defined($bridge_hh_id) ? $bridge_hh_id : 0,
                        hhnum        => $hhnum,
                        persons      => CpsIdSet->new(
                                            type => $RT_PER,
                                            year => $yr,
                                            month => $mo,
                                        ),
                    )
                );
            }
            else {
				# because we already mitigate for it by creating unique keys for per_id even with dups,
				# there are no dups by definition in persisted data, thus the dup_flag here is
				# hard-coded to 0
                $current_hh_id->add_person($mis, $per_id, $pernum, $age, $sex, $race, 0);
            }
        }
        close $fh;
    }
}

sub process_data_file {
    my $self = shift;
    my (
        $mis, $hh_id, $per_id, $hhnum, $pernum, $age, $sex, $race,
        $rt, @cpsid, $id_src, $current_hh_id, $current_per_id,
        $line_n, $cps8_year, $cps8_month, $bridge_hh_id, $dup_flag,
    );

	my $age_match = 0;
	my $sex_match = 0;
	my $race_match = 0;

    my ($yr, $mo) = ($self->year, $self->month);
    my $no_data   = $self->no_data;
    my $atus      = $self->project_is_atus;
    my $fh_output = $self->output_fh;
    my $mr        = MatchingResults->new;

    $self->show_progress(
        $VERBOSITY{DEFAULT},
        "\nProcessing data file: ",
        $INDENT . $self->data,
    );

	# if we've got configuration for a bridge_hh_id we need to take a first
	# pass on $self->data to find duplicate bridge_hh_id's and put those in
	# the do_not_cross_bridge hash
	# bridge_start_yr defaults to 0, so that's a decent way to check whether
	# this is necessary
	if ( $self->{bridge_start_yr} ) {
		my $seen = {};
    	open(DATA, '<', $self->data) or die $!;
	    while (<DATA>) {
			my $line = $_;
        	next unless substr($line, 0, 1) eq $RT_HH;
            my $hh_id        = $self->read_hh_id($line);
            my $bridge_hh_id = $self->read_bridge_hh_id($line);
			my $mis          = $self->read_mis($line);
			if ( $seen->{$bridge_hh_id} ) {
				my $key = "K_" . $bridge_hh_id; #stringify just to be safe
				$self->{do_not_cross_bridge}{$key}++ unless $mis == 1;
			}
			$seen->{$bridge_hh_id}++;
		}
		close(DATA);
	}

	# Do a first pass looking for duplicate per_ids within households,
	# and track them so they have new cpsids spawned for them.
	my $seen = {};
	my $check = {};
	my $dups = 0;
	open(DATA, '<', $self->data) or die $!;
	while (<DATA>) {
		my $line = $_;
		my $rectype = substr($line, 0, 1);
		if ($rectype eq $RT_HH) {
			$check->{hh_id} = $self->read_hh_id($line);
			$check->{mis}   = $self->read_mis($line);
		}
		elsif ($rectype eq $RT_PER) {
            my $per_id = $self->read_per_id($line);
			# if we have blanks for per_id, make it 99 so it feeds into the pernum incrementing code without bombing
			$per_id = '99' if $per_id eq 'BB';
			my $key = "K_" . $check->{hh_id};
			if ( exists $seen->{$key}{$per_id} ) {
				$self->{duplicate_per_ids}{$check->{hh_id}}{$per_id}++;
				$dups++;
			}
			$seen->{$key}{$per_id}++;
		}
	}
	close(DATA);
	print STDERR "Duplicate per_ids flagged in file: $dups\n";
	
	my %seen_it;
    open(my $fh, '<', $self->data) or die $!;
    while (my $line = <$fh>){
        $line_n ++;
        $self->show_progress(
            $VERBOSITY{HIGH},
            $INDENT . "line $line_n",
        ) unless $line_n % 10000;

        $rt = substr($line, 0, 1);
        my $is_hh_record  = $rt eq $RT_HH  ? 1 : 0;
        my $is_per_record = $rt eq $RT_PER ? 1 : 0;

        # Get needed values from the data record.
        if ($is_hh_record){
            if ($atus){
                $cps8_year  = $self->read_cps8_year($line);
                $cps8_month = $self->read_cps8_month($line) + 0;
                $mis        = 8;
            }
            else {
                $mis = $self->read_mis($line);
            }
            $hh_id        = $self->read_hh_id($line);
            $bridge_hh_id = $self->read_bridge_hh_id($line);
            $hhnum        = $self->read_hhnum($line);
            $per_id       = $ID_SPEC{init_pernum};
            $pernum       = $ID_SPEC{init_pernum};
            $age          = $ID_SPEC{init_age};
            $sex          = $ID_SPEC{init_sex};
            $race         = $ID_SPEC{init_race};
			$mis += 0;
        }
        elsif ($is_per_record) {
            $per_id = $self->read_per_id($line);
			# if we have blanks for per_id, make it 99 so it feeds into the pernum incrementing code without bombing
			$per_id = '99' if $per_id eq 'BB';
            $pernum = $self->read_pernum($line);
			if (! $atus ) {
				$age    = $self->read_age($line);
				$sex    = $self->read_sex($line);
				$race   = $self->read_race($line);
			}
        }

		my $new_cpsid = 0;
        # Find matching CPSID or, for CPS, spawn a new ID.
        if ($is_hh_record){
            my @find_hh_yrmo = $atus ? ($cps8_year,  $cps8_month ) : 
                                       ($self->year, $self->month) ;
            $current_hh_id = $self->find_hh_id( @find_hh_yrmo, $mis, $hh_id, $bridge_hh_id );
			# found an existing CPSID
            if ($current_hh_id){
                $mr->tally($RT_HH, $MR_MATCH);
            }
			# did not find a CPSID
            else {
                $mr->tally($RT_HH, $mis == 1 ? $MR_MIS1 : $MR_FAIL);
                $current_hh_id = $self->add_new_hh_id($mis, $hh_id, $hhnum, $bridge_hh_id);
            }
        }
        elsif ($is_per_record) {
           	$current_per_id = $current_hh_id->persons->get({ key => $per_id, hh_id => $current_hh_id });

			# if this is an already used per_id, we need to spawn a new cpsid
			if ( $self->{duplicate_per_ids}{$hh_id}{$per_id} ) {
				$dup_flag = 1;
				$current_per_id = undef;
			}
			else {
				$dup_flag = 0;
			}

            if ($current_per_id) {
				if ( ! $current_per_id->{'age'} =~ /\D/ ) {
					if ( $current_per_id->{'age'} eq $age) {
								$mr->tally($RT_PER, $MR_AGE);
						$age_match = 1;
					}
					elsif ( $current_per_id->{'age'} - $age <= 5 || $current_per_id->{'age'} - $age <= -5 ) {
						# Fuzzy matching, if we are within five years
								$mr->tally($RT_PER, $MR_AGE);
						$age_match = 2;
					}
				}

				if ( ! $current_per_id->{'sex'} =~ /\D/ ) {
					if ( $current_per_id->{'sex'} == $sex ) {
								$mr->tally($RT_PER, $MR_SEX);
						$sex_match = 1;
					}
				}
				
				if ( ! $current_per_id->{'race'} =~ /\D/ ) {
					if ( $current_per_id->{'race'} == $race ) {
								$mr->tally($RT_PER, $MR_RACE);
						$race_match = 1;
					}
				}

				$mr->tally($RT_PER, $MR_MATCH);
			}
			else {
				$new_cpsid = 1;
				$mr->tally($RT_PER, $mis == 1 ? $MR_MIS1 : $MR_FAIL);
				$current_per_id = $current_hh_id->add_person($mis, $per_id, $pernum, $age, $sex, $race, $dup_flag);
			}
        }

        # Create the full CPSID as a string.
        # Note for ATUS: A and W records inherit info from most recent person.
		my @cpsid_parts = (
            Zero_padded($current_hh_id->year,4),
            Zero_padded($current_hh_id->month,2),
            Zero_padded($current_hh_id->hhnum,6),
            $is_hh_record ? $ID_SPEC{init_pernum} : Zero_padded($current_per_id->pernum, 2),
		);
        my $id_str = join '', @cpsid_parts;

		 # having an incorrect length cpsid is grounds for exiting the program
         die "Incorrect length for CPSID: " . join('|', @cpsid_parts) . "\nID_SPEC{cpsid_width}, is " . length $id_str
            unless $ID_SPEC{cpsid_width} == length $id_str;

		# print some debug if inspect_hhid flag is present
        if ( $current_hh_id->hh_id eq $self->{inspect_hhid} && $is_per_record ) {
			dump_persisted_person_data($new_cpsid, $id_str, $mis, $current_per_id);
			dump_new_person_data($age, $sex, $mis, $per_id, $pernum, $line_n);
			dump_people_in_household($current_hh_id);
		}

		# we should never get here producing a duplicate cpsid
		# die if we do, because re-using cpsids in a file IS MUY BAD
		# ATUS has child rectypes to P records, so skip those
		if ( $is_per_record || $is_hh_record ) {
			if ($seen_it{$id_str}) {
				my $fatal = "FATAL: THIS PERSON RECORD IS A DUP $id_str\n";
				$fatal .= "per_id of $per_id for this household returned a stored person record:\n";
				$fatal .= join("\n\t",
					"hh_id: " . $current_hh_id->hh_id,
					"mis: " . $current_per_id->{'mis'},
					"per_id: " .  $current_per_id->per_id,
					"age: " .  $current_per_id->{'age'},
					"sex: " . $current_per_id->{'sex'},
					"pernum: " . $current_per_id->pernum,
					"\n");
				$fatal .= "From the data file, this person record had values of:\n";
				$fatal .= join("\n\t",
					"hh_id: " . $current_hh_id->hh_id,
					"mis: $mis",
					"per_id: " .  $per_id,
					"age: " .  $age,
					"sex: " . $sex,
					"pernum: " . $pernum,
					"\n");
					die $fatal if $current_hh_id->hh_id eq $self->{inspect_hhid};
			}
			$seen_it{$id_str}++;
		}

        # Insert the CPSID into the data record.
        substr($line, $self->cpsid_start, $ID_SPEC{cpsid_width}, $id_str);

	# Insert age/sex/race match
#	my $first_line_piece = substr($line, 0, $self->cpsid_start + $ID_SPEC{cpsid_width});
#	my $second_line_piece = substr($line, $self->cpsid_start + $ID_SPEC{cpsid_width});

	unless ($no_data)
	{
		#print $fh_output $first_line_piece . $age_match . $sex_match . $race_match . $second_line_piece;
		print $fh_output $line;
	}

	$age_match = 0;
	$sex_match = 0;
	$race_match = 0;
    }
    close $fh;

	# audit step: look through output_fh (if exists) and confirm no dup cpsids
	if ( length $self->output ) {
		close $fh_output;
		my %seen;
		my $cpsid;
		print "Auditing " . $self->output . " for duplicate cpsids within the file...\n";
		open(OUTPUT, "<", $self->output) or die "$!";
		while(<OUTPUT>) {
			my $rectype = substr($_, 0, 1);
			next unless $rectype eq $RT_HH or $rectype eq $RT_PER;
			$cpsid = substr($_, $self->cpsid_start, $ID_SPEC{cpsid_width});
			die "Duplicate cspid found! $cpsid" if $seen{$cpsid};
			$seen{$cpsid}++;
		}
		print "No duplicates in " . $self->output . "\n";
		close(OUTPUT);
	}

    $self->show_progress(
        $VERBOSITY{DEFAULT},
        "\nMatching results: ",
        $mr->results_table,
    );

	if ( $bridge_match > 0 ) {
		$self->show_progress(
			$VERBOSITY{DEFAULT},
			"\nHousehold Records successfully bridged across varying width CPSID batches: ",
			$bridge_match,
		);
	}
	if ( $bridge_dup > 0 ) {
		$self->show_progress(
			$VERBOSITY{DEFAULT},
			"\nHousehold Records not bridged across varying width CPSID batches due to duplication in bridge linking keys: ",
			$bridge_dup,
		);
	}
	
	my @mis_message;
	for my $key ( sort keys %{ $mis_differences } ) {
		push @mis_message, join(': ', $key, $mis_differences->{$key});
	}
	if ( scalar(@mis_message) ) {
		$self->show_progress(
			$VERBOSITY{DEFAULT},
			"\nMonth In Sample Issues: ",
			@mis_message,
		);
	}
}
sub dump_people_in_household {
	my $current_hh_id = shift;
	print STDERR "\nhh_id:" . $current_hh_id->hh_id . "\n";
	print STDERR "----------------------------------------------\n";
	print STDERR "Current Persons stored in HH object:\n";
	for my $pid ( sort keys %{ $current_hh_id->persons->{ids} } ) {
		my $p = $current_hh_id->persons->{ids}{$pid};
		print STDERR join(" ",
			'key', $pid,
			'mis', $p->{'mis'},
			'age', $p->{'age'},
			'sex', $p->{'sex'},
			'per_id', $p->{'per_id'},
			'pernum', $p->{'pernum'},
			"\n",
		);
	}
	print STDERR "----------------------------------------------\n\n";
}

sub dump_new_person_data {
	my $age = shift;
	my $sex = shift;
	my $mis = shift;
	my $per_id = shift;
	my $pernum = shift;
	my $line_n = shift;
	print STDERR join(" ",
		"From this data file:", 
		"age", $age, 
		"sex", $sex, 
		"mis", $mis, 
		"per_id", $per_id, 
		"pernum", $pernum, 
		'line#', $line_n, 
		"\n"
	);
}

sub dump_persisted_person_data {
	my $new_cpsid = shift;
	my $id_str = shift;
	my $mis = shift;
	my $current_per_id = shift;
	my $new = $new_cpsid ? "Yes": "No";
	print STDERR "New cpsid? $new cpsid: " . $id_str . "\n";
	print STDERR join(" ", 
		'From persisted data:',
		"age", $current_per_id->{'age'}, 
		"sex", $current_per_id->{'sex'}, 
		"mis", $mis, 
		"per_id", $current_per_id->per_id, 
		"pernum", $current_per_id->pernum, 
		"\n"
	);
}

sub add_new_hh_id {
    # Add a new household-level ID to the current CpsIdSet.
    my ($self, $mis, $hh_id, $hhnum, $bridge_hh_id) = @_;

	my $padded_yr    = General_utils::Zero_padded($self->year, 4);
	my $padded_mo    = General_utils::Zero_padded($self->month, 2);
	my $padded_hhnum = General_utils::Zero_padded($hhnum, 6);

    $self->current_set->add(
        CpsIdH->new(
            mis     => $mis,
			# year, month and hhnum are all part of cpsid and need to be fixed width
            year    => $padded_yr,
            month   => $padded_mo,
            hhnum   => $padded_hhnum,
            hh_id   => $hh_id,
            bridge_hh_id => $bridge_hh_id,
            persons => CpsIdSet->new(type => $RT_PER, year => $padded_yr, month => $padded_mo),
        )
    );
}

sub find_hh_id {
    # Tries to find a matching CPSID among the relevant CpsIdSets.
    my ($self, $yr, $mo, $mis, $hh_id, $bridge_hh_id) = @_;
	my $expected_mis = $self->project_is_atus ? undef : 0;
	my $dup_found;
    for my $href ( @{$self->search_list($yr, $mo, $mis)} ){
		my $cpsid_set = $href->{idset};
		$expected_mis++ unless $self->project_is_atus;
		# find expected mis for a given month we're looking at
		my $i;
		# use the bridge_hh_id if we need to cross the bridge
		my $cross_the_bridge = $self->cross_the_bridge($href);
		if ( $cross_the_bridge ) {
			my $key = "K_" . $bridge_hh_id; #stringify just to be safe
			if ( $self->{do_not_cross_bridge}{$key} ) {
				$dup_found++;
			}
			else {
	        	$i = $cpsid_set->get({key => $bridge_hh_id, expected_mis => $expected_mis });
				$bridge_match++ if $i;
			}
		}
		else {
        	$i = $cpsid_set->get({ key => $hh_id, expected_mis => $expected_mis });
		}
		
        return $i if $i;
    }

   	# increment $bridge_dup if we both hit a) do_not_cross_bridge *and*
	# b) we never found a match on the non-bridge-crossing months
	# we won't know b) until all of the search_list is done, which is
	# why we wait until we exit the for loop above before deciding this
	$bridge_dup++ if $dup_found;
	return;
}

sub cross_the_bridge {
	my $self = shift;
	my $href = shift;

	# crossing the bridge is not an ATUS thing
	return 0 if $self->project_is_atus;

	if (
		( $href->{yr} < $self->{bridge_start_yr} ) or
		( $href->{yr} == $self->{bridge_start_yr} and $href->{mo} <= $self->{bridge_start_mo} )
	) {
		return 1;
	}
	return 0;
}

sub yrmo_cps {
    # Takes a YEAR, MONTH, MIS, and N, where the first three arguments
    # correspond to a CPS interview during the given year-month.
    # Returns the calendar year and month corresponding to the
    # respondent's Nth interview in the CPS, based on the rotation pattern.
    my ($self, $yr, $mo, $mis, $n) = @_;
    $mo = $mo - $ROTATION_PATTERN{$mis} + $ROTATION_PATTERN{$n};
    while ($mo < 1){
        $mo += 12;
        $yr --;
    }
    return $yr, $mo;
}

sub cpsid_file_name {
	my $self = shift;
	my $yr = shift;
	my $mo = shift;

	my $file_name = sprintf '%s/%s_%02d.dat', $self->cpsid_dir, $yr, $mo;
    return $file_name;
}

sub persist_cpsids {
    my $self = shift;
    my ($fh, $rt, $mis, $hh_id, $per_id, $yr, $mo, $hhnum, $pernum, $age, $sex, $race, $bridge_hh_id );

    $self->show_progress(
        $VERBOSITY{DEFAULT},
        "\nPersisting CPSIDs.",
    );

    my $write_record = sub {
		print $fh join($ID_SPEC{join_char},
			$rt, $mis,
			$hh_id, $per_id,
			$yr, Zero_padded($mo, 2),
			$hhnum, $pernum, $age, $sex, $race, $bridge_hh_id,
		), "\n";
    };

    for my $idset ($self->all_idsets){
        my $file_name = $self->cpsid_file_name($idset->year, $idset->month);
        $yr = $idset->year;
        $mo = $idset->month;

        my $skip = $idset->count > 0 ? 0 : 1;
        $self->show_progress(
            $VERBOSITY{DEFAULT},
            $INDENT . $file_name . ($skip ? ': skipped' : ''),
        );
        next if $skip;

        undef $fh;

	if (!open($fh, '>', $file_name))
	{
		print STDERR "Unable to open $file_name: $!\n";
	}

        for my $hh ( @{$idset->all_ids} ){
            $rt     = $RT_HH;
            $mis    = $hh->mis;
            $hh_id  = $hh->hh_id;
            $hhnum  = $hh->hhnum;
            $per_id = $ID_SPEC{init_pernum};
            $pernum = $ID_SPEC{init_pernum};
            $age    = $ID_SPEC{init_age};
            $sex    = $ID_SPEC{init_sex};
            $race   = $ID_SPEC{init_race};
            $bridge_hh_id = defined($hh->bridge_hh_id) ? $hh->bridge_hh_id : 0;

            $write_record->();

            $rt = $RT_PER;
            for my $per ( @{$hh->persons->all_ids} ){
                $pernum = $per->pernum;
				# persist the key not the per_id (not the same in the case of bad-data duplicate per_ids)
                $per_id = $per->key;
				$age = $per->age;
				$sex = $per->sex;
				$race = $per->race;
                $write_record->();
            }
        }

        close $fh;
    }
}


####
# Simple getters and variable readers.
####

sub yaml          { $_[0]->{yaml          } }
sub data          { $_[0]->{data          } }
sub year          { $_[0]->{year          } }
sub month         { $_[0]->{month         } }
sub project       { $_[0]->{project       } }
sub hh_id         { $_[0]->{hh_id         } }
sub bridge_hh_id  { $_[0]->{bridge_hh_id  } }
sub per_id        { $_[0]->{per_id        } }
sub mis           { $_[0]->{mis           } }
sub hhnum         { $_[0]->{hhnum         } }
sub pernum        { $_[0]->{pernum        } }
sub age           { $_[0]->{pernum        } }
sub sex           { $_[0]->{sex           } }
sub race          { $_[0]->{race          } }
sub is_dup        { $_[0]->{is_dup        } }
sub cps8_year     { $_[0]->{cps8_year     } }
sub cps8_month    { $_[0]->{cps8_month    } }
sub cpsid_dir     { $_[0]->{cpsid_dir     } }
sub cpsid_start   { $_[0]->{cpsid_start   } }
sub no_data       { $_[0]->{no_data       } }
sub verbosity     { $_[0]->{verbosity     } }

sub output        { $_[0]->{output        } }
sub output_fh     { $_[0]->{output_fh     } }
sub progress      { $_[0]->{progress      } }
sub progress_mode { $_[0]->{progress_mode } }
sub progress_fh   { $_[0]->{progress_fh   } }

sub current_set   { $_[0]->{current_set   } }

sub read_hh_id      { Record_segments($_[1], $_[0]->{hh_id          }) }
sub read_bridge_hh_id { 
	if ( $_[0]->{bridge_hh_id} ) {
		return Record_segments($_[1], $_[0]->{bridge_hh_id });
	}
	return;
}
sub read_per_id     { Record_segments($_[1], $_[0]->{per_id         }) }
sub read_hhnum      { Record_segments($_[1], $_[0]->{hhnum          }) }
sub read_pernum     { Record_segments($_[1], $_[0]->{pernum         }) }
sub read_age        { Record_segments($_[1], $_[0]->{age            }) }
sub read_sex        { Record_segments($_[1], $_[0]->{sex            }) }
sub read_race       { Record_segments($_[1], $_[0]->{race           }) }
sub read_cps8_year  { Record_segments($_[1], $_[0]->{cps8_year      }) }
sub read_cps8_month { Record_segments($_[1], $_[0]->{cps8_month     }) }
sub read_mis        { Record_segments($_[1], $_[0]->{mis            }) }

sub project_is_atus {
    my $self = shift;
    return 1 if $self->project eq 'atus';
    return;
}

sub all_idsets {
    my $self = shift;
    my @idsets;
    for my $yr ( sort keys %{ $self->{idsets} } ){
        for my $mo ( sort {$a <=> $b} keys %{ $self->{idsets}{$yr} } ){
            push @idsets, $self->{idsets}{$yr}{$mo};
        }
    }
    return @idsets;
}


####
# Allow the module to be run directly as a script.
####

__PACKAGE__->main(@ARGV) unless caller;

sub main {
    my $class = shift;
    my $reformatter = $class->new(@ARGV);
    $reformatter->run;
}


####
# CpsIdSet
#   - A CpsIdSet is a collection of CPSIDs, either household-level or person-level.
#   - For each CPS year-month, we have a CpsIdSet of household-level IDs.
#   - Person-level IDs are children of a household-level ID.
#   - The set of IDs is implemented as a hash.
#   - In that hash, household IDs are keyed using HH_ID, person IDs using PER_ID.
#   - The add() method prevents the adding of duplicates.
#   - The get() method returns false when trying to retrieve a non-existent key.
####

package CpsIdSet;

sub new {
    my ($class, %args) = @_;
    my $self = bless {
        type  => undef, # H or P.
        year  => undef,
        month => undef,
        ids   => {},
        %args,
    }, $class;
    return $self;
}

sub add {
    my ($self, $cpsid) = @_;
    my $key  = $cpsid->key;
    my $type = $self->type;

	my $dup_inc = 1;

	# create a special key for detected duplicate per_ids
	if ( $cpsid->is_dup ) {
		my $dupkey = $key;
		$dupkey = $key . 'D' . $dup_inc;
		while (exists $self->{ids}{$dupkey}) {
			$dup_inc++;
			$dupkey = $key . 'D' . $dup_inc;
		}
		$key = $dupkey;
    	$cpsid->{key} = $key;
	}

    if (exists $self->{ids}{$key}){
        die "FATAL CpsIdSet : add(key = '$key') : CpsId$type with key already exists.\n";
    }
    $self->{ids}{$key} = $cpsid;
}

sub get {
	my $self = shift;
	my $args = shift;
	my $key = $args->{key};
	my $expected_mis  = $args->{expected_mis} || undef;
	my $current_hh_id = $args->{hh_id} || undef;

    return unless exists $self->{ids}{$key};
	if ( defined ($expected_mis) ) {
		if ( $self->{ids}{$key}{mis} != $expected_mis ) {
			my $diff = $self->{ids}{$key}{mis} - $expected_mis;
			if ( $diff > 0 ) {
				$diff = "mis_too_high_by_" . "$diff";
			}
			else {
				$diff = "mis_too_low_by_" . "$diff";
			}
			$mis_differences->{$diff}++;

			# if the month-in-sample doesn't match up with what 
			# is expected, this is not a cpsid match so bail
			return;
		}
		else {
			$mis_differences->{correct_mis}++;
		}
	}
    return $self->{ids}{$key};
}

sub type  { $_[0]->{type } }
sub year  { $_[0]->{year } }
sub month { $_[0]->{month} }
sub ids   { $_[0]->{ids  } }

sub count {
    my $self = shift;
    return scalar keys %{ $self->ids };
}

sub all_ids {
    my $self = shift;
    return [ sort { $a->{key} cmp $b->{key} } values %{$self->ids} ];
}


####
# CpsIdH: household-level CPSIDs.
####

package CpsIdH;

sub new {
    my ($class, %args) = @_;
    my $self = bless {
        mis          => undef,
        year         => undef,
        month        => undef,
        hh_id        => undef,
        hhnum        => undef,
        persons      => undef,
        key          => undef,
        used_pernums => {},
        %args,
    }, $class;
    $self->{key} = $self->{hh_id};
    return $self;
}

sub mis          { $_[0]->{mis          } }
sub year         { $_[0]->{year         } }
sub month        { $_[0]->{month        } }
sub hh_id        { $_[0]->{hh_id        } }
sub bridge_hh_id { $_[0]->{bridge_hh_id } }
sub hhnum        { $_[0]->{hhnum        } }
sub persons      { $_[0]->{persons      } }
sub key          { $_[0]->{key          } }
sub is_dup       { return '0'; } # hardcoded to 0 for H records

sub add_person {
    my ($self, $mis, $per_id, $pernum, $age, $sex, $race, $dup_flag) = @_;

    # Ensure that PERNUM is unique.
	my $padded_pernum = General_utils::Zero_padded($pernum, 2);
	while ( exists $self->{used_pernums}{$padded_pernum} ) {
		$pernum++;
		$padded_pernum = General_utils::Zero_padded($pernum, 2);
		die "FATAL: pernum cannot be three digits long" if $pernum > 99;
	}
    $self->{used_pernums}{$padded_pernum}++;
    my $new_person = CpsIdP->new(
        mis    => $mis,
        year   => $self->year,
        month  => $self->month,
        per_id => $per_id,
        pernum => $padded_pernum,
		age    => $age,
		sex    => $sex,
		race   => $race,
		is_dup => $dup_flag,
    );

    $self->persons->add($new_person);
}


####
# CpsIdP: person-level CPSIDs.
####

package CpsIdP;

sub new {
    my ($class, %args) = @_;
    my $self = bless {
        mis     => undef,
        year    => undef,
        month   => undef,
        per_id  => undef,
        pernum  => undef,
        key     => undef,
        is_dup  => undef,
        %args,
    }, $class;
    $self->{key} = $self->{per_id};
    return $self;
}

sub mis     { $_[0]->{mis    } }
sub year    { $_[0]->{year   } }
sub month   { $_[0]->{month  } }
sub per_id  { $_[0]->{per_id } }
sub pernum  { $_[0]->{pernum } }
sub age     { $_[0]->{age    } }
sub sex     { $_[0]->{sex    } }
sub race    { $_[0]->{race   } }
sub is_dup  { $_[0]->{is_dup } }
sub key     { $_[0]->{key    } }


####
# An object to track the success of the matching.
####

package MatchingResults;

sub new {
    my $class = shift;
    my $self = bless {
        $RT_HH => {
            $MR_MIS1  => 0,
            $MR_MATCH => 0,
            $MR_FAIL  => 0,
            FAILURES  => [],
        },
        $RT_PER => {
            $MR_MIS1  => 0,
            $MR_MATCH => 0,
            $MR_FAIL  => 0,
            $MR_AGE   => 0,
            $MR_SEX   => 0,
            $MR_RACE  => 0,
            FAILURES  => [],
        },
    }, $class;
    return $self;
}

sub tally {
    my ($self, $rt, $type, $id) = @_;
    $self->{$rt}{$type} ++;
    push @{ $self->{$rt}{FAILURES} }, $id if $type eq $MR_FAIL;
}

sub results_table {
    my $self = shift;
    return
        sprintf('           %6s %6s', $RT_HH,                     $RT_PER                     ),
        sprintf('  MIS=1    %6d %6d', $self->{$RT_HH}{$MR_MIS1},  $self->{$RT_PER}{$MR_MIS1}  ),
        sprintf('  Matches  %6d %6d', $self->{$RT_HH}{$MR_MATCH}, $self->{$RT_PER}{$MR_MATCH} ),
        sprintf('      Age         %6d', $self->{$RT_PER}{$MR_AGE}                               ),
        sprintf('      Sex         %6d', $self->{$RT_PER}{$MR_SEX}                               ),
        sprintf('     Race         %6d', $self->{$RT_PER}{$MR_RACE}                              ),
        sprintf('  Failures %6d %6d', $self->{$RT_HH}{$MR_FAIL},  $self->{$RT_PER}{$MR_FAIL}  ),
    ;
}


####
# Module return.
####

1;
