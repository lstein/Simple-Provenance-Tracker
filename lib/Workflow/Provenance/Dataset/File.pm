package Workflow::Provenance::Dataset::File;

use warnings;
use DBI;

sub new {
    my $self = shift;
    my ($oid,$dbh) = @_;
    return bless { oid=>$oid,
		   dbh=>$dbh },ref $self || $self;
}

1;
