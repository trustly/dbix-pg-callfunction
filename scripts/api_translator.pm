#!perl

use strict;
use warnings;

use DBI;
use DBD::Pg;
use DBIx::Pg::CallFunction;
use DBIx::Connector;

use Data::Dumper;

my %api_cache = ();

my $callback = sub {
    my ($method, $params, $dbconn) = @_;

    # replace parameter names; we always do this
    my $new_params = {};
    my @old_param_list = keys(%{$params});
    foreach my $old_param (@old_param_list)
    {
        if ($old_param !~ "^_")
        {
            my $new_param = "_".$old_param;
            $params->{$new_param} = $params->{$old_param};
            delete $params->{$old_param};
        }
    }

    # if this API method is cached, return it now
    return $api_cache{$method} if (exists($api_cache{$method}));

    # not cached, see if we can map it to a function
    my $dbh = $dbconn->dbh;
    die "could not connect to database: $DBI::errstr\n" if (!defined($dbh));

    my $sth = $dbh->prepare("SELECT DISTINCT proname FROM pg_catalog.pg_proc WHERE lower(regexp_replace(proname, E'([^\\\\^])_', E'\\\\1', 'g')) = lower(?)");
    $sth->execute($method);

    die "unknown API call \"$method\"" if ($sth->rows == 0);
    die "could not unambiguously map API call \"$method\" to a function" if ($sth->rows > 1);

    my $data = $sth->fetchrow_hashref;
    # make sure there are no more rows
    die "internal error" if defined($sth->fetchrow_hashref);

    my $func = $data->{proname};
    $api_cache{$method} = $func;
    return $func;
};
