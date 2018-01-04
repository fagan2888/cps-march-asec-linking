package Spreadsheet::DataDictionary;

##############################################################################
#
# Spreadsheet::DataDictionary - MPC-specific methods for getting data from Data Dictionaries
#
# initial version
# bklaas 07.2014
#
# enhanced to include var value data
# bklaas 01.2015
#
##############################################################################

use strict;
use warnings;

our $VERSION = '0.02';

use Spreadsheet::ParseExcel;
use Spreadsheet::ParseExcel::SaveParser;
use Spreadsheet::XLSX;
use Storable;

#------------------------------------------------------------------------------
# Spreadsheet::DataDictionary->new
#------------------------------------------------------------------------------
sub _newFromWorkbook {
    my ( $class, $workbook, $sheet ) = @_;

	$sheet = 0 unless $sheet;

	my $worksheet = $workbook->worksheet($sheet);

	if ( ! defined($worksheet) ) {
		die "usage: Object of type Spreadsheet::ParseExcel::SaveParser::Worksheet or Spreadsheet::ParseExcel::Worksheet required as argument to new()\n";
	}

	if (
		$worksheet->isa('Spreadsheet::ParseExcel::SaveParser::Worksheet') ||
		$worksheet->isa('Spreadsheet::ParseExcel::Worksheet')
	) {
		my $self = {};

		$self = {
			workbook       => $workbook,
			worksheet      => $worksheet,
			columns        => {},
			debug          => 0,
			dieOnCheckFail => 1,
			vals           => {},
			quiet          => 0,
		};

		($self->{cmin}, $self->{cmax}) = $self->{worksheet}->col_range();
		($self->{rmin}, $self->{rmax}) = $self->{worksheet}->row_range();
		# skip row headings
		$self->{rmin}++;
 
		# store column numbers for all labels
		for my $col ( $self->{cmin}..$self->{cmax} ) {
			my $cell = $self->{worksheet}->get_cell(0, $col);
			if ( defined $cell ) {
				my $val = $self->{worksheet}->get_cell(0, $col)->value();
				$val =~ s/^\s+//;
				$val =~ s/\s+$//;
				$val = uc($val);
				$self->{columns}{$val} = $col;
			}
		}

		# store row numbers for all Vars as well as a concatenated RecType_ColStart_ColWidth key
		my ( $sVar, $rowRecType, $rowVar, $col_location_key, $rt_var_key );
		for my $row ($self->{rmin}..$self->{rmax}) {
			my $thisRowRecType  = data($self, $row, $self->{columns}{RECORDTYPE}) || '';
			my $thisRowVar      = data($self, $row, $self->{columns}{VAR}) || '';
			my $thisRowSvar        = data($self, $row, $self->{columns}{SVAR}) || '';
			last if $thisRowRecType eq '<end>';

			if ( defined($thisRowRecType) and defined($thisRowVar) and ($thisRowRecType ne '' and $thisRowVar ne '' ) ) {
				$rowRecType  = uc($thisRowRecType);
				$rowVar      = uc($thisRowVar);
				$sVar        = uc($thisRowSvar);
				my $start       = $self->{worksheet}->get_cell($row, $self->{columns}{COL} ) && 
									$self->{worksheet}->get_cell($row, $self->{columns}{COL} )->unformatted();
				my $width       = $self->{worksheet}->get_cell($row, $self->{columns}{WID} ) && 
									$self->{worksheet}->get_cell($row, $self->{columns}{WID} )->unformatted();
				# store rows with Vars
				if ($rowVar ne "") {

					# if C, go ahead and make H and P keys for these as well
					if ($rowRecType eq 'C') {
						$col_location_key = "H_" . $start . "_" . $width;
						$self->{rows}{$col_location_key} = $row;
						$col_location_key = "P_" . $start . "_" . $width;
						$self->{rows}{$col_location_key} = $row;

						# a few mnemonics (e.g., city and puma in usa, repeat themselves in H and P records. Create a rt x var combo key
						$rt_var_key = "H__" . $rowVar;
						$self->{rows}{$rt_var_key} = $row;
						$rt_var_key = "P__" . $rowVar;
						$self->{rows}{$rt_var_key} = $row;
					}

					$col_location_key  = $rowRecType . "_" . $start . "_" . $width;
					$rt_var_key        = $rowRecType . "__" . $rowVar; # two __ is intentional
					$self->{rows}{$sVar}             = $row if defined($sVar);
					$self->{rows}{$rowVar}           = $row;
					$self->{rows}{$col_location_key} = $row;
					$self->{rows}{$rt_var_key}       = $row;
				}
			}
			# this is a value row, get the value data and push it into values arrays for
			# $sVar, $rowVar, and $col_location_key
			else {
				# only harvest info on rows with Value column non-empty
				my $values = {};
				for my $col ( 'VALUE', 'VALUELABEL', 'FREQ', 'VALUELABELORIG',
                               'VALUELABELSVAR', 'VALUESVAR' ) {
					my $val = $self->{worksheet}->get_cell($row, $self->{columns}{$col}) &&
						$self->{worksheet}->get_cell($row, $self->{columns}{$col} )->unformatted();
					$values->{$col} = $val;
				}

				if ( defined($values->{VALUE}) and $values->{VALUE} ne "" ) {
					if ( defined( $sVar ) ) {
						push @{ $self->{vals}{$sVar} }, $values;
					}
					if ( defined( $rowVar ) ) {
						push @{ $self->{vals}{$rowVar} }, $values;
					}
				}
			}
		}

		bless $self, $class;
		return $self;
	}
	else {
		die "$worksheet object is not of type Spreadsheet::ParseExcel::SaveParser::Worksheet or Spreadsheet::ParseExcel::Worksheet\n";
	}

}

sub new {
	my ($class, $file, $params) = @_;

	$params = {} unless $params;
	$params->{sheet} = 0 unless $params->{sheet};
	$params->{use_cache} = 1 unless defined($params->{use_cache});
	my ($worksheet, $workbook);

	my $cache_dir  = 'storable';
	my $cache_file = $file;
	my @projects   = qw/ ahtus atus cps dhs ihis ipumsi mtus napp highered usa brfss nyts yrbss nsduh mock fullusa ipumsi_pre_merge pma /; 
	$params->{use_cache} = 0 if $params->{sheet} > 0;

	# if use_cache, find the storable file, and flip the use_cache flag to 0 if we don't
	if ( $params->{use_cache} ) {
		for my $proj (@projects) {
			if ( $cache_file =~ /\/$proj\// ) {
				$cache_file =~ s/.*?\/$proj\///;
				$cache_file = $cache_dir . "/" . $proj . "/" . $cache_file . ".storable";
				$params->{use_cache} = -f $cache_file ? 1 : 0;
				last;
			}
		}
	}
	# semi-kludgy workaround: if we have a $cache_file that does not end in .storable, turn use_cache to 0
	if ( $cache_file !~ /\.storable$/ ) {
		$params->{use_cache} = 0;
	}

	# if use_cache and the cache is more recently modified than the xls file, get the cache and return the cached and blessed object
	if ( $params->{use_cache} and ( -M $file > -M $cache_file ) ) {
		print STDERR "INFO: Using cached DD data from $cache_file\n" if $params->{debug};
		my $cache_data = retrieve($cache_file);
		my $self = $$cache_data;
		return $self;
	}
	# otherwise parse it
	else {
		print STDERR "INFO: Parsing DD data directly from excel: $file\n" if $params->{debug};
		if ($file =~ /xlsx$/) {
			$workbook  = Spreadsheet::XLSX->new( $file ) or die "$!: $file";
		}
		elsif ($file =~ /xls$/) {
			my $parser = Spreadsheet::ParseExcel::SaveParser->new();
			$workbook  = $parser->parse( $file ) or die "$!: $file";
		}
		else {
			die "Argument to new must be the location of an excel workbook";
		}
		return _newFromWorkbook($class, $workbook, $params->{sheet});
	}

}

sub checkDD {
	my $self = shift;
	$self->checkSourceVarsUnique();
	$self->checkVarsUnique();

	return 1;
}

sub checkVarsUnique {
	my $self = shift;
	return $self->_checkUnique('Var');
}
sub checkSourceVarsUnique {
	my $self = shift;
	return $self->_checkUnique('Svar');
}

sub quiet {
	my $self = shift;
	my $quiet = shift;
	if ( $quiet ) {
		$self->{quiet} = 1;
	}
	return $self;
}


sub _checkUnique {
	my $self = shift;
	my $column = shift;
	my $errors = [];

	# store row numbers for all Vars as well as a concatenated RecType_ColStart_ColWidth key
	my %unique;
	for my $row ($self->{rmin}..$self->{rmax}) {
		my $cellRecType     = $self->{worksheet}->get_cell($row, $self->{columns}{RecordType} );
		my $targetCell      = $self->{worksheet}->get_cell($row, $self->{columns}{$column} );
		if ( defined $cellRecType && $targetCell ) {
			my $rowRecType  = $cellRecType->unformatted();
			my $targetVal   = uc($targetCell->unformatted()); # uppercase for consistency
			my $key = $rowRecType . '_' . $targetVal;
			if ($targetVal ne '' && $unique{$key} ) {
				push @$errors, "DD Format Error: $column $targetVal, recType $rowRecType repeated on row $unique{$key} and $row of spreadsheet";
			}
			$unique{$key} = $row;
		}
	}
	if ($errors->[0]) {
		for my $error (@$errors) {
			print "$error\n";
		}
		die "Died because of above errors" if $self->{dieOnCheckFail};
	}
	return $errors;
}
	
sub insertRowAbove {
	my $self      = shift;	
	my $rowNumber = shift;

	if ( ! $self->{worksheet}->isa('Spreadsheet::ParseExcel::SaveParser::Worksheet')) {
		warn "Spreadsheet manipulation only allowed when using the Read-Write module Spreadsheet::ParseExcel::SaveParser";
		return 0;
	}
	
	# from bottom row up to $rowNumber, copy row contents down on to next row
	my ($rowMax, $colMax) = $self->rowColMax();
	for ( my $row = $rowMax; $row >= $rowNumber; $row--) {
		my @colvals;
		# copy $row into $row+1
		for my $col (0..$colMax) {
			my $cell = $self->{worksheet}->get_cell($row, $col);
			if (defined($cell) and defined($cell->value()) and $cell->value() ne '') {
				my $val = $cell->value();
				# remove commas in numbers to force Type => Numeric for these numbered values
				if ($val =~ /^[\d,\.]+$/) {
						$val =~ s/,//;
				}
				$self->{worksheet}->AddCell( ($row +1), $col, $val, $cell->{FormatNo});
			}
		}
		# blank out $row
		for my $col (0..$colMax) {
			$self->{worksheet}->AddCell( $row, $col, '');
		}
	}
}

sub allVals {
	my $self = shift;
	return $self->{vals};
}

sub freqs {
	my $self = shift;
	my $key  = shift;
	return _valsForKey($self, $key, 'FREQ');
}

# values is a reserved Perl word and not usable here (also would be confusing)
sub varValues {
	my $self = shift;
	my $key  = shift;
	return _valsForKey($self, $key, 'VALUE');
}

# XXX needs completion
#sub varLabelOrig {
#	my $self = shift;
#	my $key  = shift;
#
#}

sub valueLabelOrigs {
	my $self = shift;
	my $key  = shift;
	return _valsForKey($self, $key, uc('ValueLabelOrig') );
}

sub valueLabels {
	my $self = shift;
	my $key  = shift;
	return _valsForKey($self, $key, uc('ValueLabel') );
}

sub valueLabelSvars {
	my $self = shift;
	my $key  = shift;
	return _valsForKey($self, $key, uc('ValueLabelSvar') );
}

sub varLabelSvar {
	my $self = shift;
	my $svar = shift;
	my $row  = $self->rowBySvar($svar);
	my $col  = $self->colByLabel('VARLABELSVAR');
	if ( defined($row) and defined($col) ) {
		return $self->cell($row, $col);
	}
	else {
		warn "Row and Column pair not found for VARLABELSVAR, $svar" unless $self->{quiet};
	}
	return undef;
}

sub cell {
    my $self = shift;
    my $row = shift;
    my $col = shift;
    my $val = ''; 
    my $cell = $self->{worksheet}->get_cell($row, $col);
    if ( defined $cell ) { 
        $val = $self->{worksheet}->get_cell($row, $col)->value();
    }   
    return $val;
}

sub _valsForKey {
	my $self   = shift;
	my $key    = shift;
	my $valKey = shift;
	$key = uc($key);
	my $return = [];
	if ( $self->{vals}{$key} ) {
		for my $href ( @{ $self->{vals}{$key} } ) {
			push @{ $return }, $href->{$valKey} ? $href->{$valKey} : '';
		}
	}
	else {
		warn "No values found for $key" unless $self->{quiet};
	}
	return $return;
}

sub vals {
	my $self = shift;
	my $key  = shift;
	$key = uc($key);
	if ( $self->{vals}{$key} ) {
		return $self->{vals}{$key};
	}
	else {
		warn "No vals found for $key" unless $self->{quiet};
		return [];
	}
}

sub colMin {
	my $self = shift;
	return $self->{cmin};
}

sub colMax {
	my $self = shift;
	return $self->{cmax};
}

sub rowMin {
	my $self = shift;
	return $self->{rmin};
}

sub rowMax {
	my $self = shift;
	return $self->{rmax};
}

sub rowColMax {
	my $self = shift;
	return $self->{rmax}, $self->{cmax};
}

sub dieOnCheckFail {
	my $self = shift;

	# set
	if (defined($_[0])) {
		$self->{dieOnCheckFail} = $_[0];
	}
	# get
	return $self->{dieOnCheckFail};
}


sub debug {
	my $self = shift;

	# set
	if ($_[0]) {
		$self->{debug} = $_[0];
	}
	# get
	return $self->{debug};
}

sub colRecType {
	my $self = shift;
	return $self->colByLabel('RECORDTYPE');
}

sub allRecTypes {
	my $self     = shift;
	my $col      = $self->colRecType();
	my $rows     = $self->varRows();
	my $recTypes = [];
	my $found    = {};
	my $vars     = $self->varRows();
	for my $var ( keys %$vars ) {
		my $rt = $self->cell($vars->{$var}, $col);
		push @$recTypes, $rt unless $found->{$rt};
		$found->{$rt}++;
	}
	return $recTypes;
}

sub colVar {
	my $self = shift;
	return $self->colByLabel('VAR');
}

sub colWid {
	my $self = shift;
	return $self->colByLabel('WID');
}

sub colStart {
	my $self = shift;
	return $self->colByLabel('COL');
}

sub colSvar {
	my $self = shift;
	return $self->colByLabel('SVAR');
}

# requires recType, colStart and colWid
sub rowByCol {
	my $self     = shift;
	my $recType  = shift;
	my $colStart = shift;
	my $colWid   = shift;
	my $key = $recType . "_" . $colStart . "_" . $colWid;
	return $self->_rowNumber($key);
}

#
# requires SVar
sub rowBySvar {
	my $self      = shift;
	my $svar      = shift;
	return $self->_rowNumber($svar);
}


# requires Var
sub rowByVar {
	my $self     = shift;
	my $var      = shift;
	return $self->_rowNumber($var);
}

# 
# returns a hashref of var names and the rows in which they appear
sub varRows {
	my $self = shift;
	my $return = {};

	if (! $self->{varRows} )  {
		for my $row ($self->{rmin}..$self->{rmax}) {
			my $cellVar     = $self->{worksheet}->get_cell($row, $self->{columns}{VAR} );
			if ( defined $cellVar ) {
				my $rowVar = uc($cellVar->unformatted()); # uppercase for consistency
				if ($rowVar ne '') {
					$self->{varRows}{$rowVar} = $row;
				}
			}
		}
	}

	return $self->{varRows};

}

# returns a hashref with vars as keys, a hashref as values which includes:
#  {
#  		rectype	=> 
#  		start	=> 
#		zStart  =>
#		key_find_ord  =>
#  		wid		=> 
#  		svar 	=>
# }
sub allVarsData {

	my $self = shift;
	my $varData = {};
	my $vars = $self->varRows();
	for my $var ( keys %$vars ) {
		my ($start, $width) = $self->startAndWidth($var);
		my $recTypeCol      = $self->colRecType();
		my $recType         = defined($recTypeCol) ? $self->cell($vars->{$var}, $recTypeCol) : '';
		my $svarCol         = $self->colSvar();
		my $svar            = defined($svarCol) ? $self->cell($vars->{$var}, $svarCol) : '';
		my $kfoCol          = $self->colByLabel('KEY_FIND_ORD');
		my $key_find_ord    = defined($kfoCol) ? $self->cell($vars->{$var}, $kfoCol) : 'x';

		$varData->{$var} = {
			start         => $start,
			zStart        => $start - 1, # zStart counts from 0
			key_find_ord  => $key_find_ord,
			width         => $width,
			recType       => $recType,
			svar          => $svar,
		};

	}
	
	return $varData;
}

# takes an svar as an argument and responds with the col start and width
sub startAndWidth {
	my $self = shift;
	my $val  = shift;
	my $colStart  = $self->colStart();
	my $colWid    = $self->colWid();
	my $targetRow = $self->_rowNumber($val);
	return ( $self->data($targetRow, $colStart), $self->data($targetRow, $colWid) );

}

# requires row number and column number
sub data {
	my $self = shift;
	my $row  = shift;
	my $col  = shift;
	if (! defined $row || ! defined $col ) {
		warn "You must specify a row and column";
		return undef;
	}
	else {
		my $cell = $self->{worksheet}->get_cell($row, $col);
		if (! defined $cell) {
			warn "Row $row Col $col of worksheet did not return a value" if $self->{debug};
			return undef;
		}
		else {
			return $cell->value();
		}
	}
}

sub _rowNumber {
	my $self = shift;
	my $var  = shift;
	$var = uc($var);
	if ( ! $self->{rows}{$var} ) {
		warn "No row number found for $var: $!\n" unless $self->{quiet};
		return undef;
	}
	return $self->{rows}{$var};
}

sub colByLabel {
	my $self = shift;
	my $label = shift;
	$label = uc($label);
	defined($self->{columns}{$label}) ? return $self->{columns}{$label} : return undef;
}

sub hasVar {
	my $self = shift;
	my $var  = shift;
	$var     = uc($var);
	if ( $self->{rows}{$var} ) {
		return $self->{rows}{$var};
	}
	return undef;
}

sub svar2Var {
	my $self = shift;
	my $svar = shift;
	$svar = uc($svar);
	my $varRow = $self->{rows}{$svar};

	if ( $varRow ) {
		my $cellVar = $self->{worksheet}->get_cell($varRow, $self->{columns}{VAR} );
		my $var = defined($cellVar) ? $cellVar->unformatted() && uc($cellVar->unformatted()) : undef; 
		return $var;
	}

	return undef;
}

sub row2Svar {
	my $self = shift;
	my $row  = shift;
	return data($self, $row, $self->{columns}{SVAR});
}


sub row2Var {
	my $self = shift;
	my $row  = shift;
	return data($self, $row, $self->{columns}{VAR});
}

sub var2Svar {
	my $self = shift;
	my $var  = shift;
	my $rt   = shift || undef;
	$var     = uc($var);
	my $varRow = $self->{rows}{$var};
	if ( $rt ) {
		my $key = uc($rt) . "__" . $var;
		$varRow = $self->{rows}{$key};
	}

	if ( $varRow ) {
		my $cellSvar = $self->{worksheet}->get_cell($varRow, $self->{columns}{SVAR} );
		my $sVar = defined($cellSvar) ? $cellSvar->unformatted() && uc($cellSvar->unformatted()) : undef; 
		return $sVar;
	}

	return undef;
}

# attempted workaround for using (IHIS only, right now) origRT column
# instead of the regular record type column
sub varAndOrigRT2Svar {
    my $self = shift;
    my $var  = shift;
    my $origrt = shift || undef;
    if (! $origrt ) {
        return undef;
    }

    # go through every row looking for var + origrt
    # there's no real efficient way of doing this
    for my $row ($self->{rmin}..$self->{rmax}) {
        my $thisRowOrigRT  = data($self, $row, $self->{columns}{ORIGRT}) || '';
        my $thisRowVar      = data($self, $row, $self->{columns}{VAR}) || '';
        my $thisRowSvar        = data($self, $row, $self->{columns}{SVAR}) || '';
        if (uc($thisRowOrigRT) eq uc($origrt) and uc($thisRowVar) eq uc($var)) {
            return $thisRowSvar;
        }
    }

    return undef;
}


1;
__END__

=pod

=head1 NAME

Spreadsheet::DataDictionary - Custom MPC module for delivering methods for extracting data from a data dictionary represented as a Spreadsheet::ParseExcel object

=head1 SYNOPSIS

use Spreadsheet::ParseExcel;

use Spreadsheet::DataDictionary;

my $dd        = Spreadsheet::DataDictionary->new($worksheet);

=head1 METHODS

=over 2

=item new()

new( _file_, { optional_params } )

# file is a path to an xls or xlsx data dictionary

# optional params are sheet and use_cache, which default to 0 and 1, respectively.

example:
my $dd = Spreadsheet::DataDictionary('/path/to/data_dict_sample.xls', { sheet => 0, use_cache => 0 });

The new method will create an object with class Spreadsheet::DataDictionary, and populate an initial data structure full of e.g. column and row locations for headings and variables, respectively. These are used extensively by the methods below.

If use_cache is set to 1 (it will default to this), the script will look for a .storable cache file first, then determine if that cache is newer than the last modified time of the spreadsheet. If it is, it will load that in with Storable instead of the more time-consuming parsing of the spreadsheet.

=item checkDD()

Runs all spreadsheet check methods in sequence, currently checkVarsUnique and checkSourceVarsUnique

=item checkVarsUnique()

Checks that all RecType/Var tuples across DD are unique. Prints error messages when they are not, and dies if dieOnCheckFail is set to a non-zero value.

=item checkSourceVarsUnique()

Checks that all RecType/SourceVar tuples across DD are unique. Prints error messages when they are not, and dies if dieOnCheckFail is set to a non-zero value.

=item quiet()

quiet(1)

When set to a non-zero value, quiet will suppress some chatty warnings found in various DataDictionary methods.

=item dieOnCheckFail()

If set to non-zero, will die when any of the check functions are run and detect an error. Object default is 1 (die on error).

example
$dd->dieOnCheckFail(0); # will not die on error check failure
$dd->dieOnCheckFail(1); # die on error check failure

=item debug()

If set to non-zero, will print more debug info on failures

=item insertRowAbove()

insertRowAbove(RowNumber)

Inserts a blank row above RowNumber of the spreadsheet. Only available when using Spreadsheet::ParseExcel::SaveParser

=item rowMin()

rowMin()

Returns first row number that contains data (excludes heading row)

=item rowMax()

rowMax()

Returns highest row number that contains data

=item colMin()

colMin()

=item colMax()

colMax()

Returns highest column number that contains data

=item rowColMax()

rowColMax()

Returns highest row number and highest column number that contain data

=item colByLabel()

colByLabel(_label_)

Takes a string as an argument and returns the column number in the data dictionary for that string, if it's found. Returns undef on failure.

=item colRecType()

colRecType()

Returns the column number for the column heading RecordType. This is a convenience method to colByLabel('RecordType')

=item colVar()

Returns the column number for the column heading Var. This is a convenience method to colByLabel('Var')

=item colWid()

Returns the column number for the column heading Wid. This is a convenience method to colByLabel('Wid')

=item colStart()

Returns the column number for the column heading Col. This is a convenience method to colByLabel('Col')

=item colSvar()

Returns the column number for the column heading Svar. This is a convenience method to colByLabel('Svar')

=item rowByCol()

rowByCol( _rectype_, _colstart_, _colwidth_ )

Returns the row number for the row given by the combination of a RecType (RecordType), a Column Start (Start) and a Column Width (Wid).  Returns undef on failure, along with spitting a warning.

=item rowBySvar()

rowBySvar( _svar_ )

Returns the row number for the row that contains a given Svar. Svar's should be completely unique. By contrast, the Var isn't technically constrained in this manner.  Returns undef on failure, along with spitting a warning.

=item rowByVar()

rowByVar( _var_ )

Returns the row number for the row that contains a given Var. Note: Var comes from the source data and should not be counted on to be unique (though it probably is) or consistent across data dictionaries.  Returns undef on failure, along with spitting a warning.

=item allVals()

Returns the entire hashref of arrayrefs of hashrefs of the data found for Value, ValueLabel, ValueLabelOrig, Freq, and SvarLabelOrig across all vars and svars. This is probably best used only for debugging, as vals is a more intuitive method for extracting these data

=item freqs()

freqs( _var_ ) or freqs( _svar_ )

Pass an input var name or a svar name and method returns an array reference of all Freqs for that var.

=item varValues()

varValues( _var_ ) or varValues( _svar_ )

Pass an input var name or a svar name and method returns an array reference of all Values for that var.

=item varLabelOrig()

XXX this method not written yet
varLabelOrig( _var_ ) or varLabelOrig( _svar_ )

Pass an input var name or a svar name and method returns an the VarLabelOrig for that var.

=item vals()

vals( _var_ ) or vals( _svar_ )

Returns an arrayref of hashrefs of the data found for Value, ValueLabel, ValueLabelOrig, Freq, and SvarLabelOrig

=item valueLabels()

valueLabels( _var_ ) or valueLabels( _svar_ )

Pass an input var name or a svar name and method returns an array reference of all ValueLabels for that var.

=item valueLabelOrigs()

valueLabelOrigs( _var_ )

Pass an input var name or an svar name and method returns an array reference of all valueLabelOrig cells for that var

=item valueLabelSvars()

valueLabelSvars( _var_ )

Pass an input var name or an svar name and method returns an array reference of all valueLabelSvar cells for that var

=item varLabelSvar()

varLabelSvar( _var_ )

Pass an input var name or svar name and method returns the value in the cell "VarLabelSvar"

=item varRows()

Returns a hashref of all Var names with the var name as the hash key and row number of that var as the hash value

=item cell()

cell( row, col)

Pass a row and col number to this method and method returns the value contained in that cell. It will return an empty string (and not an error) if that cell is empty.

Note: this is a shortcut to getting the cell value via the worksheet method get_cell(row, col)->value(), which can be cumbersome because if the cell is empty ->value() will throw an undefined error. This gets around that issue and returns just the value in the cell, if any. It will return an empty string if the cell is empty.

=item startAndWidth()

startAndWidth( _value_ )

Takes a string value as an argument, typically the value from the Svar column, and returns the column start and column width from that row of the data dictionary. This can also be used with values from the Var column, though the consistency of that column's strings across data dictionaries cannot be depended upon.

For example:

my ($start, $width) = $datadictionary->startAndWidth('hrhhid') 

Would return the start and width for the row in the spreadsheet identified by the value of 'hrhhid' in the Var column. 

Whereas this example:

my ($start, $width) = $datadictionary->startAndWidth('CPS2011_03B_0009');

Would return the start and width for the row in the spreadsheet identified by the value of 'CPS2011_03B_0009' in the SVar column. 

=item allVarsData()

Returns a hashref with vars as keys and a hashref as values which includes rectype, start, width, and svar

=item data()

data( _row_, _col_ )

Takes a row and column number as arguments and returns the data from the worksheet's cell.

If there is no value in the cell it will return undef. This is consistent with the worksheet method ->get_cell() but in contrast to the DataDictionary cell(), which will return an empty string when the cell is empty.

=item var2Svar()

var2Svar( _var_ )

Takes an input variable name and returns its associated svar. Returns undef if nothing found.

=item svar2Var()

svar2Var( _svar_ )

Takes an source variable and returns its associated input var name. Returns undef if nothing found.

=item allRecTypes()

Returns an arrayref of all rec types found in the Data Dictionary.

=item hasVar()

hasVar(_var_) or hasVar( _svar_)

Takes a var or svar and returns the row number of where that var/svar exists in the Data Dictionary. Returns undef when it is not found.

=item row2Svar()

row2Svar(_row)

Takes a row and returns the Svar found on that row. Will return undef if nothing is found.

=item row2Var()

row2Var(_row)

Takes a row and returns the input var found on that row. Will return undef if nothing is found.


=back

=head1 EXAMPLES

use Spreadsheet::DataDictionary;

my $dd        = Spreadsheet::DataDictionary->new($worksheet);

# get the column number found for the Start and Wid columns

my $colStart = $dd->colStart();

my $colWid   = $dd->colWid();
 
# get the row number for Svar of CPS2011_03B_0009

my $targetRow = $dd->('CPS2011_03B_0009');

# get the column start and column width values for the targeted row

my $start = $dd->data($targetRow, $colStart);

my $width = $dd->data($targetRow, $colWid);

# OR, do everything above in one line with startAndWidth()

my ($start, $width) = $dd->startAndWidth('CPS2011_03B_0009');


=cut

