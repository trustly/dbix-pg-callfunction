#!/usr/bin/perl

use strict;
use warnings;

use DBI;
use DBD::Pg;
use DBIx::Connector;
use Storable qw();
use MIME::Base64 qw(decode_base64);

package TrustlyApiMapper;

BEGIN
{
    require Exporter;
    require TrustlyApiMapper::SqlQueries;
    our $VERSION = 1.00;
    our @ISA = qw(Exporter);
    our @EXPORT = qw(api_method_call_mapper api_method_call_postprocessing);
}

# cache for external API method calls
my %external_api_call_cache = ();

# cache for API method -> function call mapping
my %api_method_cache = ();

sub _get_special_mapping
{
    my ($method) = @_;

    my $simple_mapping = {
                            "GetViewParams"     => "get_view_json",
                            "GetView"           => "get_view_json",
                            "NewBankWithdrawal" => "new_bankwithdrawal_json",
                            "NewBankWithdrawalFromSpecificAccount"
                                                => "new_bankwithdrawal_json",
                            "GetServerRequest"  => "get_server_request_json"
                         };

    if (defined((my $value = $simple_mapping->{$method})))
    {
        return $value;
    }

    return $method;
}

sub api_method_call_postprocessing
{
    my ($method_call, $result) = @_;

    my $method = $method_call->{method};
    my $params = $method_call->{params};

    # no special handler, just return whatever we got from the database
    return $result;
}

sub _has_external_api_call_signature
{
    my $external_signature = join(',', sort qw(Signature UUID Data));

    my $method_params = shift;
    my $method_signature = join(',', sort keys %{$method_params});

    return $method_signature eq $external_signature;
}

# Return 1 if the method matches an external API call, false otherwise
sub _matches_external_api_call
{
    my ($dbconn, $method, $data) = @_;

    my $dbh = $dbconn->dbh;
    die "could not connect to database: $DBI::errstr\n" if (!defined($dbh));

    my $sth = $dbh->prepare($TrustlyApiMapper::SqlQueries::sql_map_external_method_call);
    $sth->execute($method, [keys %{$data}]);

    return 0 if $sth->rows == 0;

    my $result = $sth->fetchrow_hashref;
    # make sure there are no more rows
    die "internal error" if defined($sth->fetchrow_hashref);

    return 1;
}

sub _map_external_api_call
{
    my ($dbconn, $method, $params, $host) = @_;

    # check that the call has the external API method call signature
    return undef if (!_has_external_api_call_signature($params));
    # now do a lookup in the database to see if this is actually an API method
    return undef if (!_matches_external_api_call($dbconn, $method, $params->{Data}));

    return {
                proname         => 'api_call',
                nspname         => 'public',
                returns_json    => 1,
                params          => $params
           };
}

sub api_method_call_mapper
{
    my ($method_call, $dbconn, $host) = @_;

    my $method = $method_call->{method};
    my $params = $method_call->{params};

    # check whether this is an external API call
    if (defined (my $api_call = _map_external_api_call($dbconn, $method, $params)))
    {
        # It looks like an external API call, so treat it as such.  At this
        # point there's no going back to a "regular" function call.
        $params = _convert_parameter_list($params);
        $params->{_host} = $host;
        $params->{_method} = $method;
        $api_call->{params} = $params;

        return $api_call;
    }

    # convert the parameter list
    $params = _convert_parameter_list($params);

    # allow special mapping required by some API calls
    $method = _get_special_mapping($method);

    # if this API method is cached, return it now
    my $cache_key = _calculate_api_method_cache_key($method, [keys %{$method_call->{params}}]);
    if (exists($api_method_cache{$cache_key}))
    {
        my $cache_entry = $api_method_cache{$cache_key};
        # inject host if necessary
        $params->{_host} = $host if ($cache_entry->{requirehost});

        return {
                    proname         => $cache_entry->{proname},
                    nspname         => $cache_entry->{nspname},
                    returns_json    => $cache_entry->{returns_json},
                    params          => $params
               };
    }

    # not cached, see if we can map it to a function
    my $dbh = $dbconn->dbh;
    die "could not connect to database: $DBI::errstr\n" if (!defined($dbh));

    my @method_arguments = keys %{$params};
    my $sth;
    if ((scalar @method_arguments) == 0)
    {
        $sth = $dbh->prepare($TrustlyApiMapper::SqlQueries::sql_map_function_call_noparams);
        $sth->execute($method);
    }
    else
    {
        $sth = $dbh->prepare($TrustlyApiMapper::SqlQueries::sql_map_function_call);
        $sth->execute($method, \@method_arguments);
    }

    die "unknown API call \"".$method_call->{method}."(".join(",", keys %{$method_call->{params}}).")\"" if ($sth->rows == 0);
    die "could not unambiguously map API call \"".$method_call->{method}."\" to a function" if ($sth->rows > 1);

    my $data = $sth->fetchrow_hashref;
    # make sure there are no more rows
    die "internal error" if defined($sth->fetchrow_hashref);

    my $requirehost = $data->{requirehost};
    $api_method_cache{$cache_key} =
        {
            proname         => $data->{proname},
            nspname         => $data->{nspname},
            returns_json    => $data->{returns_json},
            requirehost     => $requirehost
        };

    # inject host if necessary
    $params->{_host} = $host if ($requirehost);

    return {
                proname         => $data->{proname},
                nspname         => $data->{nspname},
                returns_json    => $data->{returns_json},
                params          => $params
           };
}

sub _convert_parameter_list
{
    my $old_params = shift;

    my $new_params = {};
    foreach my $old_param (keys %{$old_params})
    {
        # lowercase it and prepend an underscore if it doesn't begin with one
        my $new_param = lc($old_param);
        $new_param = "_".$new_param if ($new_param !~ "^_");

        $new_params->{$new_param} = $old_params->{$old_param};
    }

    return $new_params;
}

# Calculate a cache key for a method call, given its signature.
sub _calculate_api_method_cache_key
{
    my ($proname, $argnames) = @_;
    return $proname."(".join(",", sort @{$argnames}).")";
}

END
{
}

1;
