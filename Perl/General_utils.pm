package General_utils;

use strict;
use warnings;
use base qw(Exporter);
use File::Basename;
use Carp;

use vars qw($VERSION @EXPORT_OK);

$VERSION = 1.0;

@EXPORT_OK = qw(
    HH_reader
    Is_file
    Chomp_all
    Trimmed
    Right_filled
    Zero_padded
    Is_positive_integer
    Is_even_integer
    Hash_to_records
    Record_segments
    Reduce_column_list
    Start_width_to_skip_width
    Start_width_to_start_end
    Spaces_to_Bs_selective
    Left_justify_fields
    Next_free_file_name
    Next_free_dir_name
    Open_file
    System_command
    Merge_hashes
    Get_password
    Public_password
);

# Arguments : FILE_NAME for an IPUMS-style data file (H and P in first column).
# Behavior  : Opens file for reading.
# Returns   : An iterator that will read households from the file, returning each
#             set of records as an array reference. Returns undefined when the
#             file is exhausted.
sub HH_reader {
    my $file_name = shift;
    open my $fh, '<', $file_name or die $!;
    my $record;

    return sub {
        my (@hh);
        push @hh, $record if defined $record;

        while ($record = <$fh>){
            my $rt = substr $record, 0, 1;
            last if $rt eq 'H' and @hh;
            push @hh, $record;
        }

        return unless @hh;
        return \@hh;
    }
}

# Arguments : SCALAR.
# Returns   : T/F indicating whether SCALAR is a file.
sub Is_file {
    my $f = shift;
    return unless defined $f;
    return unless -f      $f;
    return 1;
}


# Arguments : SCALAR.
# Behavior  : Removes all trailing newline characters from SCALAR: both \r and \n.
# Returns   : T/F indicating whether any characters were removed.
sub Chomp_all {
    $_[0] =~ s/[\r\n]+$//;
}


# Arguments : STRING.
# Returns   : A copy of STRING without leading or trailing whitespace.
sub Trimmed {
    my $s = $_[0];
    $s =~ s/^\s+//;
    $s =~ s/\s+$//;
    return $s;
}

# Arguments : STRING and INTEGER.
# Returns   : A copy of STRING with sufficient right-padding (spaces)
#             to achieve a length of INTEGER.
# Behavor   : Trailing whitespace is removed before padding. If STRING
#             is aleady long enough, padding does not occur, but whitespace
#             removal does.
sub Right_filled {
    my ($s, $w);
    $s = $_[0];
    $s =~ s/\s+$//;
    $w = $_[1] - length($s);
    $s = $s . (' ' x $w) if $w > 0;
    return $s;
}


# Arguments : STRING, WIDTH.
# Returns   : A copy of STRING left-padded with zeroes to the desired WIDTH.
# Notes     : If STRING is already wide enough (or wider), returns STRING unaltered.
sub Zero_padded {
    my ($s, $w);
    $s = $_[0];
    $w = $_[1];
    return ('0' x ($w - length $s)) . $s;
}


# Arguments : SCALAR.
# Returns   : True if SCALAR is a positive integer in ordinary notation; false otherwise.
sub Is_positive_integer {
    my ($n) = $_[0];
    return 1 if  $n =~ /^\d+$/ 
             and $n > 0;
    return 0;
}


# Arguments : SCALAR.
# Returns   : True if SCALAR is an even integer; false otherwise.
sub Is_even_integer {
    my ($n) = $_[0];
    return 1 if  $n =~ /^\d+$/
             and $n % 2 == 0;
    return 0;
}


# Arguments : HASH_REF.
# Returns   : A list of records, each consisting of a hash key and the corresponding hash value.
#             The records are tab-delimited, newline terminated, and sorted by the hash keys.
sub Hash_to_records {
    my ($hash_ref, @lines);
    $hash_ref = shift;
    for my $k (sort keys %$hash_ref){
        push @lines, join('', $k, "\t", $hash_ref->{$k}, "\n");
    }
    return @lines;
}


# Arguments : A STRING and a ref to a list of column locations (skip values and widths).
# Returns   : A new string created by concatenating the segments of STRING
#             defined by the skip-width pairs.
sub Record_segments {
    my ($rec, $cols, $rec_new);
    $rec = shift;
    $cols = shift;
    for (my $i = 0; $i < @$cols; $i += 2){
		if ( length($rec) < ( $cols->[$i] + $cols->[$i + 1]) ) {
			die "Substring at " . $cols->[$i] . " with width " . $cols->[$i + 1] . " exceeds length of record: " . length($rec);
		}
        $rec_new .= substr($rec, $cols->[$i], $cols->[$i + 1]);
    }
    return $rec_new;
}


# Arguments : A list of column locations, specified as pairs of start columns and widths.
# Returns   : A "reduced" list -- the smallest list that still defines the
#             same column locations. Reduction occurs whenever two start-width
#             pairs are contiguous and can be expressed more compactly as one pair.
sub Reduce_column_list {
    my (@full, @reduced);

    # Check arguments.
    return unless @_;
    confess "Reduce_column_list(): N of arguments must be even.\n" if @_ % 2;
    @full = @_;
    for (@full){
        confess "Reduce_column_list(): values must be positive integers: $_.\n"
            unless Is_positive_integer($_);
    }

    # Reduce the list.
    while (@full){
        # If the last start-width pair in @reduced is contiguous with the
        # first pair in @full, reduce; otherwise, store. 
        if (@reduced and $full[0] == $reduced[-2] + $reduced[-1]){
            # Reduce.
            shift @full;
            $reduced[-1] += shift(@full);
        } else {
            # Store.
            push @reduced, shift(@full) for 1 .. 2;
        }
    }
    return @reduced;
}


# Arguments : A list of start columns and widths.
# Returns   : A list of skip values and widths.
sub Start_width_to_skip_width {
    my (@cols);
    return unless @_;
    confess "Start_width_to_skip_width(): N of arguments must be even.\n" if @_ % 2;
    @cols = @_;
    for my $i (0 .. $#cols){
        confess "Start_width_to_skip_width(): values must be positive integers: $cols[$i].\n"
            unless Is_positive_integer($cols[$i]);
        $cols[$i] -- if $i % 2 == 0;
    }
    return @cols;
}


# Arguments : A list of start columns and widths.
# Returns   : A list of start and end columns.
sub Start_width_to_start_end {
    my (@cols);
    return unless @_;
    confess "Start_width_to_start_end(): N of arguments must be even.\n" if @_ % 2;
    @cols = @_;
    for my $i (0 .. $#cols){
        confess "Start_width_to_start_end(): values must be positive integers: $cols[$i].\n"
            unless Is_positive_integer($cols[$i]);
        $cols[$i] = $cols[$i - 1] + $cols[$i] - 1 if $i % 2;
    }
    return @cols;
}


# Arguments : A STRING and a ARRAY REFERENCE, with the latter containing
#             a list of column locations (as skip values and widths).
# Returns   : A copy of STRING, with all spaces converted to Bs for the
#             specified columns.
# Notes     : Does not validate the integrity of the 2nd argument.
sub Spaces_to_Bs_selective {
    my ($record, $cols, $field, $s, $w);
    $record = shift;
    $cols = shift;
    
    # Process each field specified by the list of column locations.
    for (my $i = 0; $i < @$cols; $i += 2){
        # Get the field's skip value and width.
        $s = $cols->[$i];
        $w = $cols->[$i + 1];

        # Get the field and try to convert spaces to Bs. If any are
        # converted, insert the edited field back into the full string.
        $field = substr($record, $s, $w);
        substr($record, $s, $w, $field) if $field =~ tr/ /B/;
    }
    
    return $record;
}


# Arguments : A STRING and a ARRAY REFERENCE, with the latter containing
#             a list of column locations (as skip values and widths).
# Returns   : A copy of STRING, with the fields left justified.
# Notes     : Does not validate the integrity of the 2nd argument.
sub Left_justify_fields {
    my ($record, $cols, $field, $s, $w);
    $record = shift;
    $cols = shift;
    
    # Process each field specified by the list of column locations.
    for (my $i = 0; $i < @$cols; $i += 2){
        # Get the field's skip value and width.
        $s = $cols->[$i];
        $w = $cols->[$i + 1];

        # Left justify the field:
        #   - try to remove leading spaces from the field
        #   - if any were removed, right-fill with spaces to the original width
        #   - put the edited field back into the full string
        $field = substr($record, $s, $w);
        if ($field =~ s/^ +//){
            $field = $field . (' ' x ($w - length($field)));
            substr($record, $s, $w, $field);
        }
    }
    
    return $record;
}


# Arguments : One or two strings -- a STEM and an optional SUFFIX.
# Returns   : The next available file name, using this pattern.
#                 STEM_0SUFFIX
#                 STEM_1SUFFIX
#                 etc. until a free file is found
sub Next_free_file_name {
    my ($stem, $suffix, $n, $file);
    $stem = shift;
    $suffix =shift;
    $suffix = '' unless defined $suffix;
    while (1){
        $n ++;
        $file = join '', $stem, '_', $n, $suffix;
        last unless -f $file;
    }
    return $file;
}


# Arguments : A string -- STEM.
# Returns   : The next available directory name, using this pattern.
#                 STEM_0
#                 STEM_1
#                 etc. until a free directory is found
sub Next_free_dir_name {
    my ($stem, $n, $dir);
    $stem = shift;
    while (1){
        $n ++;
        $dir = join '', $stem, '_', $n;
        last unless -d $dir;
    }
    return $dir;
}


# Arguments : A file name and a mode.
# Returns   : The file handle after opening the file.
sub Open_file {
    my ($file_name, $mode, $handle, %valid_mode);
    %valid_mode = (
        '<'  => '<',
        'r'  => '<',
        '>'  => '>',
        'w'  => '>',
        '>>' => '>>',
        'a'  => '>>',
    );
    ($file_name, $mode) = @_;
    confess "Open_file(): incorrect N of arguments.\n" unless @_ == 2;
    confess "Open_file(): invalid mode.\n" unless exists $valid_mode{$mode};
    $mode = $valid_mode{$mode};
    open($handle, $mode, $file_name) or confess "Open_file('$file_name', '$mode'): failed open.\n";
    return $handle;
}


# Arguments : A command or a list of key-value pairs (see %opt).
# Behavior  : Calls system(COMMAND), exec(COMMAND), or print(COMMAND).
sub System_command {
    my (%opt);
    %opt = (
        cmd  => undef,
        run  => 1,
        exec => 0,
    );

    # Check arguments.
    if (@_ == 1){
        # One argument: the command.
        $opt{cmd} = shift;
    }
    elsif (@_ and @_ % 2 == 0) {
        # Multiple arguments: use to populate %opt.
        Merge_hashes(\%opt, [@_]);
    }
    else {
        confess "System_command(): invalid N of arguments.\n";    
    }
    confess "System_command(): undefined command.\n" unless defined $opt{cmd};
    confess "System_command(): invalid command `$opt{cmd}`.\n" unless length $opt{cmd};

    # Run or print the command.
    if ($opt{run} and $opt{exec}){
        exec $opt{cmd};
    }
    elsif ($opt{run} and ! $opt{exec}){
        system $opt{cmd};    
    }
    else {
        chomp $opt{cmd};
        print $opt{cmd}, "\n";
    }
}


# Arguments : (1) Reference to a TARGET hash, and (2) reference to a SOURCE hash
#             or list. The items in SOURCE are expected to be key-value pairs
#             and each key must exist in TARGET.
# Behavior  : Uses the pairs of items from SOURCE to set values in TARGET.
sub Merge_hashes {
    my ($target, $source, @source_items);

    # Validate arguments.
    confess "Merge_hashes(): invalid N of arguments.\n" unless @_ == 2;
    ($target, $source) = @_;
    confess "Merge_hashes(): invalid argument; expected hash ref.\n"
        unless ref($target) eq 'HASH';
    if (ref($source) eq 'HASH'){
        @source_items = %$source;
    }
    elsif (ref($source) eq 'ARRAY'){
        @source_items = @$source;
        confess "Merge_hashes(): odd number of items in source array.\n"
            if @source_items % 2;
    }
    else {
        confess "Merge_hashes(): invalid argument; expected hash ref or array ref.\n";
    }

    # Use SOURCE to set values in TARGET.
    while (@source_items){
        my $k = shift @source_items;
        my $v = shift @source_items;
        confess "Merge_hashes(): invalid key in source ($k).\n" unless exists $target->{$k};
        $target->{$k} = $v;
    }
    
    # We modified the target hash directly. Nonetheless, return it as well.
    return $target;
}

sub Get_password {
    my $pw;
    my $prompt = shift;
    $prompt = 'Enter password: ' unless defined $prompt;
    print STDERR $prompt;
    system "stty -echo";
    chomp ($pw = <STDIN>);
    print STDERR "\n";
    system "stty echo";
    return $pw;
}

sub Public_password {
    my $pw = 'HV4TA4R!';
    $pw = lc $pw;
    $pw =~ tr/a-mn-z/n-za-m/;
    return $pw;
}

# Module return.
1;
