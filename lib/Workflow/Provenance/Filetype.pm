package Workflow::Provenance::Filetype;
use strict;
use warnings;

use overload '""' => 'oid',
    fallback      => 1;
    

sub new {
    my $class = shift;
    my ($id,$oid,$type,$suffix,$description,$version) = @_;
    return bless {id  => $id,
		  oid=>$oid,
		  type=>$type,
		  suffix=>$suffix,
		  description=>$description,
		  version=>$version,
    },ref $class || $class;
}

sub  id    { shift->{id}     }
sub oid    { shift->{oid}    }
sub type   { shift->{type}   }
sub description { shift->{description} }
sub version     { shift->{version}     }
sub suffix { shift->{suffix} }

1;
