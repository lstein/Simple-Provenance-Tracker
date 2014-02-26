package Workflow::Provenance::Base;
use strict;
use warnings;

use Workflow::Provenance;
use Data::UUID;
use DBI;

sub new {
    my $class = shift;
    my $dbh   = shift;
    if ($dbh) {
	local $dbh->{autocommit} = 1;
	my @statements = split /;/,$class->schema;
	$_=~/\S/ && $dbh->do($_) foreach @statements;
    }
    return bless {dbh=>$dbh},ref $class || $class;
}

sub dbh { shift->{dbh} }

sub new_oid { Data::UUID->new->create_str }

sub is_oid {
    my $self = shift;
    my $str  = shift;
    return $str =~ /^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$/;
}

1;

