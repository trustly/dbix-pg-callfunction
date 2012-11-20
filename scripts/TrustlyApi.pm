#!/usr/bin/perl

use strict;
use warnings;

use DBI;
use DBD::Pg;
use DBIx::Connector;
use JSON;

package TrustlyApi;

BEGIN
{
    #require Exporter;
    require TrustlyApi::DBConnection;
    #our $VERSION = 1.00;
    #our @ISA = qw(Exporter);
    #our @EXPORT = qw(a);
}

sub sign_object
{
    my ($unsigned, $uuid) = shift;

    return 
        {
            signature => $signature,
            uuid => $uuid;
        };
}

sub create_result_object
{

}

sub create_error_object
{
}

END
{
}

1;
