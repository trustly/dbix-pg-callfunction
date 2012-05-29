use strict;
use warnings;

use ExtUtils::MakeMaker 6.11;

my %WriteMakefileArgs = (
    'test' => {
        'TESTS' => 't/*.t'
    },
    'NAME' => 'DBIx::Pg::CallFunction',
    'DISTNAME' => 'DBIx-Pg-CallFunction',
    'CONFIGURE_REQUIRES' => {
        'ExtUtils::MakeMaker' => '6.11'
    },
    'AUTHOR' => 'Joel Jacobson <joel@trustly.com>',
    'BUILD_REQUIRES' => {},
    'ABSTRACT' => 'DBIx::Pg::CallFunction',
    'EXE_FILES' => [],
    'VERSION' => '0.002',
    'PREREQ_PM' => {
        'DBI' => '1.605'
    },
    'LICENSE' => 'mit'
);


delete $WriteMakefileArgs{LICENSE}
    unless eval { ExtUtils::MakeMaker->VERSION(6.31) };

WriteMakefile(%WriteMakefileArgs);



