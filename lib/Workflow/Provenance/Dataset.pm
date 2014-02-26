package Workflow::Provenance::Dataset;

use warnings;
use base 'Workflow::Provenance::Base';

use overload '""' => 'oid',
         fallback => 1;

sub new {
    my $class = shift;
    my ($oid,$dbh) = @_;
    $class->SUPER::new();
    return bless { oid=>$oid,
		   dbh=>$dbh },ref $class || $class;
}

sub dbh { shift->{dbh} }
sub oid { shift->{oid} }
sub id  { 
    my $self = shift;
    return $self->{id} ||= $self->_id();
}
sub format {
    my $self = shift;
    return $self->{format} ||= $self->_format();
}
sub path {
    my $self = shift;
    if (@_) {
	my $newpath = shift;
	my $sth     = $self->dbh->prepare('update dataset set path=? where oid=?');
	$sth->execute($newpath,$self->oid);
	$sth->finish;
	$self->{path} = $newpath;
    }
    return $self->{path} ||= $self->_path();
}
sub _id  {
    my $self = shift;
    my $sth = $self->dbh->prepare_cached('select id from dataset where oid=?');
    my @h   = $sth->execute($self->{oid});
    $sth->finish;
    return $h[0];
}
sub _format {
    my $self = shift;
    my $sth  = $self->dbh->prepare_cached(<<END);
select f.id,f.oid,ftype,suffix,description,version 
       from file_format as f, dataset as d
where f.id=d.format and d.oid=?
END
;
    $sth->execute($self->oid);
    my @h = $sth->fetchrow_array();
    $sth->finish;
    return unless @h;
    return Workflow::Provenance::Filetype->new(@h);
}
sub _path {
    my $self = shift;
    my $sth  = $self->dbh->prepare('select path from dataset where oid=?');
    $sth->execute($self->oid);
    my ($p) = $sth->fetchrow_array;
    $sth->finish;
    return $p;
}



1;
