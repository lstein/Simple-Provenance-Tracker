package Workflow::Provenance;
use strict;
use warnings;

our $VERSION = '0.001';

use base 'Workflow::Provenance::Base';
use Workflow::Provenance::Filetype;
use Workflow::Provenance::Dataset;
use DBI;
use Carp 'croak';

sub register_dataset {
    my $self = shift;
    my ($format,$path) = @_;
    my $f = $self->filetype($format)
	or croak "unknown format $format";
    my $dbh = $self->dbh;
    if (my ($oid) = $dbh->selectrow_array("select oid from dataset where path=".$dbh->quote($path))) {
	croak "$path already exists under OID $oid";
    }

    local $dbh->{autocommit} = 1;
    my $sth = $dbh->prepare_cached('insert into dataset (oid,format,path) values (?,?,?)');
    my $oid = $self->new_oid;
    $sth->execute($oid,$f->id,$path);
    $sth->finish;
    return $self->dataset($oid);
}

sub dataset {
    my $self = shift;
    my $path = shift;
    my $sth = $self->dbh->prepare_cached(
	$self->is_oid($path) ? 'select oid from dataset where oid=?'
	                     : 'select oid from dataset where path=?'
	);
    $sth->execute($path);
    my ($oid) = $sth->fetchrow_array or return;
    $sth->finish;
    return Workflow::Provenance::Dataset->new($oid,$self->dbh);
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
    return $self->filetype($type);
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

sub register_analysis_step {
    my $self = shift;
    my ($protocol,$started,$completed) = @_;
    my $p    = $self->protocol($protocol)
	or croak "unknown protocol $protocol";
    local $dbh->{autocommit} = 1;
    my $sth = $dbh->prepare_cached('insert into analysis_step (oid,protocol,started,completed) values (?,?,?,?)');
    my $oid = $self->new_oid;
    $sth->execute($oid,$p->id,$started,$completed);
    $sth->finish;
    return $self->analysis_step($oid);
}

sub register_protocol {
    my $self = shift;
    my ($title,$version,$description,$url) = @_;
    $title       ||= 'untitled protocol';
    $description ||= 'protocol with no description';
    my $dbh = $self->dbh;
    
    local $dbh->{autocommit}=1;
    my $sth = $dbh->prepare_cached(defined $version ? 'select oid from protocol where title=? and version=?')
	                                            : 'select oid from protocol where title=?';
    my @args = defined $version ? ($title,$version) : ($title);
    if ($sth->execute(@args)>0) {
	my ($oid) = $sth->fetchrow_array;
	$sth->finish;
	return $oid;
    } else {
	$sth->finish;
    }

    $sth = $dbh->prepare_cached('insert into protocol (oid,title,url,description,version) values(?,?,?,?,?)');
    my $oid = $self->new_oid;
    $sth->execute($oid,$title,$url,$description,$version);
    $sth->finish;
    return $self->protocol($oid);
}

sub protocol {
    my $self = shift;
    my @args = @_;

    my ($sql,@bind);
    if (@args == 2) { # protocol title and version
	$sql = 'select oid from protocol where title=? and version=?';
	@bind   = @args;
    } elsif ($self->is_oid($args[0])) {
	$sql = 'select oid from protocol where oid=?';
	@bind   = $args[0];
    } else {
	$sql = 'select oid from protocol where title=? order by id desc';
	@bind  = $args[0];
    }
    my $sth = $self->dbh->prepare_cached($sql);
    $sth->execute(@bind);
    my ($oid) = $sth->fetchrow_array;
    $sth->finish;
    return Workflow::Provenance::Protocol->new($oid,$self->dbh);
}

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

create table if not exists protocol (
   id          int(10) auto_increment primary key,
   oid         char(36)       not null,
   title       varchar(255)   not null,
   url         varchar(1024),
   description text,
   version     varchar(32),
   unique index(oid)
) engine=innodb;

create table if not exists analysis_step (
   id          int(10) auto_increment primary key,
   oid         char(36) not null,
   protocol    int(10) not null,
   started     timestamp default current_timestamp,
   completed   timestamp default current_timestamp,
   unique index(oid)
) engine=innodb;

create table if not exists analysis_io {
   analysis_oid char(36) not null,
   dataset_oid  char(36) not null,
   role         enum('input','output','configuration','metadata'),
   index (analysis_oid),
   index (dataset_oid)
}

END
}

1;

