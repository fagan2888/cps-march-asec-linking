#!/usr/bin/perl -w

$| = 1;
use warnings;
use strict;
use File::Basename;
use Getopt::Long;
use IO::Compress::Gzip qw(gzip $GzipError) ;

use lib dirname($0);
use Reformatter::CpsLinkingIds;

my %opt;
main();

sub main {
    # Get command-line args and options and set up %opt.
    set_options(@ARGV);

    # Load batch configuration information, and set up
    # the needed info on the samples to be run.
    require $opt{config_file};
    $opt{run_these} = samples_to_be_run(%opt);

    # Make some settings depending on the mode we are running:
    my %mode_info = (
        move  => [ 'Moving',   \&move_args  ],
        check => [ 'Checking', \&check_args ],
        cpsid => [ 'Running',  \&cpsid_args ],
    );
    my ($msg, $args_method) = @{ $mode_info{$opt{mode}} };

	# if we're moving, we need to archive data first
	if ($opt{mode} eq 'move') {
		archive_data($opt{run_these});
	}

    # Run the samples.
    run_this( "$msg: $_->{sample}\n", $args_method->($_) ) for @{$opt{run_these}};
}

sub archive_data {
	my $run_these = shift;
	my ($day, $month, $year) = (localtime)[3,4,5];
	$year += 1900;
	$day = sprintf("%02d",$day);
	$month = sprintf("%02d",$month+1);
	my $archive_folder = 'archive_cpsid_' . $month . $day . $year ;
	for my $this (@$run_these) {
		# note that this relative path means that the expectation is that the years folder is one level above where this script is being invoked
		my $data_dir = "../years/" . $this->{year} . "/data/";
		my $archive_dir = $data_dir . $archive_folder;

		# make sure archive_dir exists, and create it if it does not
		if (! -d $archive_dir ) {
			print "Creating $archive_dir directory for storing gzipped copies of pre-move data\n";
			mkdir($archive_dir) or die "Could not create $archive_dir!";
		}

		my $file_to_be_archived = $data_dir . $this->{sample} . ".dat";
		if ( -e $file_to_be_archived ) {
			my $archive_file   = $archive_dir . "/" . $this->{sample} . ".dat.gz";
			my $inc = 0;
			# don't overwrite any existing archive file
			while ( -e $archive_file ) {
				$inc++;
				print "Archive file exists: $file_to_be_archived\n" if -e $file_to_be_archived;
				$archive_file   = $archive_dir . "/" . $this->{sample} . "." . $inc . ".dat.gz";
				print "Trying $archive_file\n";
			}
			print "Archiving\n\t$file_to_be_archived to\n\t$archive_file\n";
			gzip $file_to_be_archived => $archive_file or die "gzip failed: $GzipError\n";
		}
		else {
			print "! $file_to_be_archived does not exist, so no need to archive\n";
		}
	}
}

sub help {
    my ($usage_message, $help_message, $error_message);
    $usage_message = join('',
        "Usage: ",
        basename($0),
        " [ --run_all ] [--batches X Y ...] [--samples X Y ...] [--move] [--check] [--help] [--debug] [--continue]",
        "\n",
    );

    $help_message = "
        ---------------
        CPSID front-end
        ---------------

        This utility serves as a front-end for common tasks related to CPSIDs.
        It operates in various modes:

            Default mode: Invoke the CPSLinkingIDs module for one or more
                          samples or batches of samples, or optionally all batches.
            
            Check mode:   Run some basic checks on data recently generated
                          by the CpsLinkingIDs module.

            Move mode:    Move recently generated data to the project's main
                          data area.

        The script must be run within a CPSID work area because it depends on
        the existence of various subdirectories and files.

        Command-line arguments:
            At least one value for the --batches or --samples options, or --run_all.

        Output is written to these subdirectories:
            logs
            output
            persisted_cpsid_data

        Options:
            --check             Check mode.
            --move              Move mode.

            --batches X Y ...   Batches to run. Example: 2005_08.
            --samples X Y ...   Samples to run. Example: 2006_12s.
            --run_all           Run all batches in alpha-sorted order.

            --persist_local     Persist the CPSID data locally rather than in project area.
            --inspect_hhid      Print debug information while running on a particular hhid
            --debug             Do not run anything; just print what would be run.
            --continue          Continue running even when an individual run fails.
            --help              Display this help message.

        To pass other options to the CpsLinkingIds module, put them at the end of 
        the command line, following a '--' marker, like this:
        
            $0 -- OTHER OPTIONS
        ";
    $help_message =~ s/\n {8}/\n/g;
    if (@_){
        $error_message = shift;
        chomp $error_message;
        die $error_message, "\n", $usage_message;
    } else {
        die $help_message;
    }
}

####
# Process the command-line arguments and options.
####

sub set_options {
    my (%DEFAULT_OPT);

    %DEFAULT_OPT = (
        move    => 0,
        check   => 0,
        help    => 0,
        debug   => 0,
        persist_local => 0,
        inspect_hhid => 0,
        run_all => undef,
        batches => [],
        samples => [],
    );

    # Get options.
    $SIG{__WARN__} = \&help;
    GetOptions (
        \%opt,
        'move',
        'check',
        'help',
        'debug',
        'continue',
        'persist_local',
        'inspect_hhid=s',
        'run_all',
        'batches=s@{,}',
        'samples=s@{,}',
    );
    $SIG{__WARN__} = '';

    # Provide help if requested.
    help() if $opt{help};

    # Check for needed files and subdirectories.
    my $config_file = 'batch_config.pl';
    my @subdirs = qw(persisted_cpsid_data logs output yaml_files);
    help("Did not find batch configuration file: $config_file\n") unless -f $config_file;
    for my $sd (@subdirs){
        help("Expected directory not found: $sd\n") unless -d $sd;
    }

    # Set the program mode.
    $opt{mode} = $opt{check} ? 'check' :
                 $opt{move}  ? 'move'  : 'cpsid';

    # Merge user options and arguments with default option values.
    %opt = (
        %DEFAULT_OPT,
        %opt,
        config_file   => $config_file,
        run_these     => [],
        cpsid_module  => dirname($0) . 'Reformatter/CpsLinkingIds.pm',
        cpsid_checker => dirname($0) . 'cpsid_checker.pl',
        cpsid_dir     => 'cpsid_work_area/persisted_cpsid_data',
        other_options => [@ARGV],
    );

    # If working in a local dev area, we don't want to write to any project dirs,
    # so remove the path from the cpsid_dir.
    $opt{cpsid_dir} = basename($opt{cpsid_dir}) if $opt{persist_local};

    # Check for required arguments.
    help("You must supply at least one value for --batches or --samples, or use the --run_all option.")
        unless $opt{run_all} or @{$opt{batches}} or @{$opt{samples}};
}

sub samples_to_be_run {
    # Returns a sorted list of run-items
    my @run_these;
    my %batch_info = batch_config();

	if ( $opt{run_all} ) {
		$opt{batches} = [];
		for my $batch ( sort keys %batch_info ) {
			push @{ $opt{batches} }, $batch;
		}
	}

    # Get samples in each batch requested by user.
    for my $b ( @{$opt{batches}} ){
        die "Did not find configuration information for batch: $b\n"
            unless exists $batch_info{$b};
        push @run_these, run_item($_, $b) for @{$batch_info{$b}};
    }

    # Look up the batch for each sample requested by user.
    for my $s ( @{$opt{samples}} ){
        my $batch;
        BATCH: for my $b (keys %batch_info){
            for my $ss ( @{$batch_info{$b}} ){
                next unless $ss eq $s;
                $batch = $b;
                last BATCH;
            }
        }
        die "Did not find configuration information for sample: $s\n"
            unless defined $batch;
        push @run_these, run_item($s, $batch);
    }

    # Return the run items, sorted by the sample key.
    return [sort { $a->{sample} cmp $b->{sample} } @run_these];
}

sub run_item {
    # Takes a sample (2004_12s) and a batch (2004_05).
    # Returns a hash ref containing all items needed to invoke
    # a system() call -- for example, to run CpsLinkingIds.pm.
    my ($sample, $batch) = @_;
    my ($year, $month_zp) = $sample =~ /^(\d\d\d\d)_(\d\d)[bs]$/;
    my $month = $month_zp + 0; # Remove zero padding.
    $sample = 'cps' . $sample;
    return {
        sample   => $sample,
        batch    => $batch,
        year     => $year,
        month    => $month,
        month_zp => $month_zp,
    };
}

sub cpsid_args {
    # Takes a run item, and returns the arguments needed for a system() call.
    my $ri = shift;
    return (
        $opt{cpsid_module},
        '--yaml',      "yaml_files/batch_$ri->{batch}.yaml",
        '--data',      "data/years/$ri->{year}/data/$ri->{sample}.dat", 
        '--year',      $ri->{year},
        '--month',     $ri->{month},
        '--output',    "output/$ri->{sample}.dat",
        '--cpsid_dir', $opt{cpsid_dir},
        '--progress',  "logs/$ri->{sample}.txt",
        '--inspect_hhid',  $opt{inspect_hhid},
        @{$opt{other_options}},
    );
}

sub move_args {
    # Takes a run item, and returns the arguments needed for a system() call.
    my $ri = shift;
    return (
        'cp',
        "output/$ri->{sample}.dat",
        "data/years/$ri->{year}/data/$ri->{sample}.dat", 
    );
}

sub check_args {
    # Takes a run item, and returns the arguments needed for a system() call.
    my $ri = shift;
    return (
        $opt{cpsid_checker},
        "data/years/$ri->{year}/data/$ri->{sample}.dat", 
        "output/$ri->{sample}.dat",
    );
}

sub run_this {
    # Takes a message and some arguments.
    # In debug mode, the arguments are simply printed.
    # Otherwise, the message is printed and the arguments are passed to system().
    my ($msg, @args) = @_;
    if ($opt{debug}){
        print join(' ', @args), "\n";
    }
    else {
        print $msg;
        my $return = system @args;
		if ($return > 0 && ! $opt{'continue'} ) {
			die "cpsid.pl is bailing out because last command of:\n\n" . join(' ', @args) . "\n\nreturned an error code\n";
		}
    }
}

