#!/usr/bin/perl

use strict;
use warnings;
use Module::Build;

my $build = Module::Build->new(
    module_name       => 'Workflow::Provenance',
    license           => 'perl',
    dist_version_from => 'lib/Workflow/Provenance.pm',
    dist_abstract     => 'Track the provenance of files produced/consumed by an analytic workflow',
    dist_author       => 'Lincoln Stein <lincoln.stein@gmail.com>',
    configure_requires => { 'Module::Build' => 0 },
    requires          => {
	'DBI'         => 0,
	'DBD::mysql'  => 0,
	'Data::UUID'  => 0,
    },
    );

$build->create_build_script;
exit 0;
