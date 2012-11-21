#!/usr/bin/perl

use strict;
use warnings;

use DBI;
use DBD::Pg;
use DBIx::Connector;
use JSON;

package TrustlyApi::Mapper;

BEGIN
{
    require Exporter;
    require TrustlyApi::MapperSqlQueries;
    require TrustlyApi::DBConnection;
    our $VERSION = 1.00;
    our @ISA = qw(Exporter);
    our @EXPORT = qw(api_method_call_mapper);
}

# cache for API method -> function call mapping
my %api_method_cache = ();

sub _get_special_mapping
{
    my ($method) = @_;

    my $simple_mapping = {
                            "GetViewParamsCSVDelimiter"
                                                => "get_view_csv",
                            "GetViewParamsCSV"  => "get_view_csv",
                            "GetViewParams"     => "get_view_json",
                            "GetView"           => "get_view_json",
                            "NewBankWithdrawal" => "new_bankwithdrawal_json",
                            "NewBankWithdrawalFromSpecificAccount"
                                                => "new_bankwithdrawal_json",
                            "GetUserDailyStatement"
                                                => "get_user_daily_statement_json",
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

sub _has_v1_api_call_signature
{
    my $external_signature = join(',', sort qw(Signature UUID Data));

    my $method_params = shift;
    my $method_signature = join(',', sort keys %{$method_params});

    return $method_signature eq $external_signature;
}

sub _map_v1_api_call
{
    my ($dbc, $method, $params, $host) = @_;

    # check that the call has the external API method call signature
    if (!_has_v1_api_call_signature($params))
    {
        # XXX maybe there's a better error code for this
        die "ERROR_INVALID_PARAMETERS v1 API call does not have the correct parameters" 
    }
    # now do a lookup in the database to see if this is actually an API method
    my $result = $dbc->execute($TrustlyApi::MapperSqlQueries::sql_map_v1_method_call,
                               $method, [keys %{$params->{Data}}]);
    my $num_rows = scalar @{$result->{rows}};
    # if the signature matches, it has to match an API call
    die "ERROR_INVALID_FUNCTION unknown external API call \"".$method."(".join(",", keys %{$params->{Data}}).")\"" if ($num_rows == 0);

    return {
                proname         => 'api_call',
                nspname         => 'public',
                proretset       => 0,
                returns_json    => 1,
                params          => $params->{Data}
           };
}

sub api_method_call_mapper
{
    my ($method_call, $dbc, $host) = @_;

    my $method = $method_call->{method};
    my $params = $method_call->{params};

    # check whether this is an external API call
    if ($method_call->{is_v1_api_call})
    {
        my $api_call = _map_v1_api_call($dbc, $method, $params);

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
                    proretset       => $cache_entry->{proretset},
                    returns_json    => $cache_entry->{returns_json},
                    params          => $params
               };
    }

    # not cached, see if we can map it to a function
    my @method_arguments = keys %{$params};
    my $result;
    if ((scalar @method_arguments) == 0)
    {
        $result = $dbc->execute($TrustlyApi::MapperSqlQueries::sql_map_function_call_noparams, $method);
    }
    else
    {
        $result = $dbc->execute($TrustlyApi::MapperSqlQueries::sql_map_function_call, $method, \@method_arguments);
    }

    my $num_rows = @{$result->{rows}};

    die $result->{errstr} if (!defined $result->{rows});
    die "ERROR_INVALID_FUNCTION unknown API call \"".$method_call->{method}."(".join(",", keys %{$method_call->{params}}).")\"" if ($num_rows == 0);
    die "ERROR_INVALID_PARAMETERS could not unambiguously map API call \"".$method_call->{method}."\" to a function" if ($num_rows > 1);

    my $data = $result->{rows}->[0];
    my $requirehost = $data->{requirehost};
    $api_method_cache{$cache_key} =
        {
            proname         => $data->{proname},
            nspname         => $data->{nspname},
            proretset       => $data->{proretset},
            returns_json    => $data->{returns_json},
            requirehost     => $requirehost
        };

    # inject host if necessary
    $params->{_host} = $host if ($requirehost);

    return {
                proname         => $data->{proname},
                nspname         => $data->{nspname},
                proretset       => $data->{proretset},
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
