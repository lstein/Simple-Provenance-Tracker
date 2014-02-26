package Workflow::Provenance::Dataset;
use strict;
use warnings;

use base 'Workflow::Provenance::Base';
use Workflow::Provenance;
use Workflow::Provenance::Filetype;
use DBI;
use Carp 'croak';

sub schema {
    return <<END;
create table if not exists dataset (   # should have an "owner"
    id        int(10)    auto_increment primary key,
    oid       char(36)   not null,
    format    int(10),                    # foreign key into file_format table
    path      varchar(65536) not null,    # can be a local path or a URL
    unique index (oid)
) engine=innodb;

create table if not exists file_format (
   id          int(10) auto_increment primary key,
   oid         char(36)     not null,
   ftype       varchar(255) not null,
   suffix      varchar(32),
   description text,
   version     varchar(32),
   unique index(oid)
) engine=innodb;
END
}

sub register_dataset {
    my $self = shift;
    my ($format,$path) = @_;
    my $format = $self->filetype($format)
	or croak "unknown format $format";
    my $dbh = $self->dbh;
    if (my ($oid) = $dbh->selectrow_array("select oid from dataset where path=".DBI->quote($path))) {
	croak "$path already exists under OID $oid";
    }

    local $dbh->{autocommit} = 1;
    my $sth = $dbh->prepare_cached('insert into dataset (oid,format,path) values (?,?,?)');
    my $oid = $self->new_oid;
    $sth->execute($oid,$format,$path);
    $sth->finish;
    return $oid;
}

sub dataset {
    my $self = shift;
    my $path = shift;
    my $sth = $self->dbh->prepare_cached(
	$self->is_oid($path) ? 'select oid from dataset where oid=?'
	                     : 'select oid from dataset where path=?'
	);
    $sth->execute($type);
    my ($oid) = $sth->fetchrow_array or return;
    $sth->finish;
    return Workflow::Provenance::Dataset::File->new($oid,$self->dbh);
}

sub register_filetype {
    my $self = shift;
    my ($type,$suffix,$description,$version) = @_;
    my $dbh = $self->dbh;
    
    local $dbh->{autocommit}=1;
    my $sth = $dbh->prepare_cached('select oid from file_format where ftype=?');
    if ($sth->execute($type)>0) {
	my ($oid) = $sth->fetchrow_array;
	$sth->finish;
	return $oid;
    }
    $sth->finish;

    $sth = $dbh->prepare_cached('insert into file_format (oid,ftype,suffix,description,version) values(?,?,?,?,?)');
    my $oid = $self->new_oid;
    $sth->execute($oid,$type,$suffix,$description,$version);
    $sth->finish;
    return $oid;
}

sub filetype {
    my $self = shift;
    my $type = shift;
    my $sth = $self->dbh->prepare_cached(
	$self->is_oid($type) ? 'select id,oid,ftype,suffix,description,version from file_format where oid=?'
	                     : 'select id,oid,ftype,suffix,description,version from file_format where ftype=?'
	);
    $sth->execute($type);
    my ($id,$oid,$t,$suffix,$description,$version) = $sth->fetchrow_array or return;
    $sth->finish;
    return Workflow::Provenance::Filetype->new($id,$oid,$t,$suffix,$description,$version);
}


1;

__DATA__
