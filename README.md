# cps-march-asec-linking

## Perl marbasec.pl Basic to Supplement record linking
Respondents in the CPS March Basic and Supplement samples are not explicitly linked. Using the Minnesota Population Center's cpsid method of identifying individuals, a Perl script was written to link records between the Basic and Supplement samples within a given year's March CPS sample.

For running the Perl script, a few CPAN modules are necessary (along with any dependencies those modules require, which should resolve if you install via a CPAN client)
* Spreadsheet::ParseExcel
* Spreadsheet::XLSX
* Spreadsheet::WriteExcel

Year 2013 was selected as an example for demonstrating the marbasec.pl script. 

The script requires two sets of information: Data Dictionaries (codebooks) in xls form for each sample, and fixed width data files. 

The March 2013 Data Dictionaries are included in this repository, but the data files are too large to store in GitHub. Instead, they should be downloaded at the following URLs: TBD

After downloading the .dat files, they should be saved to the same data/years/2013/data directory the xls files reside.

Once these files are in the proper directory, from the top level of the git checkout the command to execute is

    ./marbasec.pl 2013

