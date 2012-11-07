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
    our $VERSION = 1.00;
    our @ISA = qw(Exporter);
    our @EXPORT = qw(api_method_call_mapper api_method_call_postprocessing);
}

my $sql_query = q{
-- Get a list of IN and INOUT arguments from a function
WITH FunctionArgs AS
(
    SELECT
        proname, oid, proargname
    FROM
    (
        SELECT
            proname, oid, unnest(proargnames) AS proargname, unnest(proargmodes) AS proargmode
        FROM
        (
            SELECT
                proname, oid, proargnames,
                -- proargmodes can be NULL, meaning all arguments are IN               
                COALESCE(proargmodes, array_fill('i'::"char", ARRAY[array_length(proargnames,1)])) AS proargmodes
            FROM
                pg_proc
            WHERE
                lower(regexp_replace(proname, E'([^\\\\^])_', E'\\\\1', 'g')) = lower(?)
        ) ss
    ) ss2
    WHERE
        proargmode = 'i' OR proargmode = 'b'
),
-- Now we need to unnest() the arguments to the method call, once for each
-- function that potentially could match the method call.  That way we can
-- easily tell which arguments are missing from which list by using a FULL
-- JOIN.
MatchedArgs AS
(
    SELECT
        COALESCE(FunctionArgs.oid, MethodArgs.oid) AS oid,
        FunctionArgs.proargname AS FunctionArgument, MethodArgs.proargname AS MethodArgument
    FROM
        FunctionArgs
    FULL JOIN
    (
        SELECT
            proargname, oid
        FROM
        (
            SELECT
                DISTINCT oid
            FROM
                FunctionArgs
        ) ss
        CROSS JOIN
            unnest(?::name[]) proargname
    ) MethodArgs
        ON (FunctionArgs.proargname = MethodArgs.proargname AND FunctionArgs.oid = MethodArgs.oid)
)

SELECT
    pg_proc.proname, pg_namespace.nspname, requirehost
FROM
(
    SELECT
        oid,
        -- If any function argument is missing from the method argument list, it
        -- must be _host (see the WHERE clause below).  In that case, let the API
        -- code know that it needs to supply a value for the _host.
        bool_or(EXISTS(SELECT * FROM MatchedArgs m2 WHERE m1.oid = m2.oid AND m2.MethodArgument IS NULL)) AS requirehost
    FROM
        MatchedArgs m1
    WHERE
        -- If there are any arguments that are not present in the function's
        -- argument list, there's no way we can call that function
        NOT EXISTS (SELECT * FROM MatchedArgs m2
                        WHERE m1.oid = m2.oid AND m2.FunctionArgument IS NULL)

            AND

        -- We allow one function to be missing from the method's argument list:
        -- _host.  In that case the API code puts in the correct host.
        --
        -- Also see "requirehost" in the SELECT list above.
        NOT EXISTS (SELECT * FROM MatchedArgs m2
                        WHERE m1.oid = m2.oid AND m2.MethodArgument IS NULL AND m2.FunctionArgument <> '_host')
    GROUP BY
        oid
) MappedFunctions
JOIN
    pg_proc
        ON (pg_proc.oid = MappedFunctions.oid)
JOIN
    pg_namespace
        ON (pg_namespace.oid = pg_proc.pronamespace)
;
};

sub _get_special_handler
{
    my ($method, $params, $host) = @_;
    my @paramlist = keys %{$params};

    if (_compare_signature($method, \@paramlist,
                           'GetViewParams', [ qw(_username _password _viewname
                                                 _offset _datestamp _dateorder
                                                 _limit _sortby _sortorder
                                                 _filterkeys _params) ]))
    {
        # Convert the params from a hashref into a list.  DO NOT set it to an
        # empty list or postgres will go crazy.  Use "undef" instead of there
        # are no params.
        my $converted_params = undef;
        foreach my $k (keys %{$params->{_params}} ) {
            my $converted_params = [] unless $converted_params;
            push @$converted_params, [$k, $params->{_params}->{$k}];
        }
        $params->{_params} = $converted_params;

        $params->{_host} = $host;
        return {
                    proname => 'get_view',
                    nspname => undef,
                    params  => $params 
               };
    }

    return undef;
}

sub api_method_call_postprocessing
{
    my ($method_call, $result) = @_;

    my $method = $method_call->{method};
    my $params = $method_call->{params};

    if ($method eq 'GetViewParams')
    {
        my $datestamp;
        my @report;

        eval 
        {
            my $ret = Storable::thaw(MIME::Base64::decode_base64($result));
            return undef unless ref $ret eq "ARRAY";

            $datestamp = shift @$ret;
            my $keys = shift @$ret;
            foreach my $row (@$ret) {
                my %pack;
                my $i = 0;
                foreach my $col ( @$row ) {
                    $pack{$keys->[$i]} = $col;
                    $i++;
                }
                push @report, \%pack;
            }
        };

        die $@ if $@;
            
        return {
            now  => $datestamp,
            data => \@report,
        };            
    }

    # no special handler, just return whatever we got from the database
    return $result;
}

my %api_method_cache = ();

sub api_method_call_mapper
{
    my ($method_call, $dbconn, $host) = @_;

    my $method = $method_call->{method};
    my $params = $method_call->{params};

    # replace parameter names; we always do this
    my $new_params = {};
    my @old_param_list = keys(%{$params});
    foreach my $old_param (@old_param_list)
    {
        my $param = lc($old_param);
        $param = "_".$param if ($param !~ "^_");
        $params->{$param} = $params->{$old_param};
        delete $params->{$old_param};
    }

    # see if there's a special handler for this method call
    if ((my $function_call = _get_special_handler($method, $params, $host)))
    {
        return $function_call;
    }

    # if this API method is cached, return it now
    my $cache_key = _calculate_api_method_cache_key($method, \@old_param_list);
    if (exists($api_method_cache{$cache_key}))
    {
        my $cache_entry = $api_method_cache{$cache_key};
        # inject host if necessary
        $params->{_host} = $host if ($cache_entry->{requirehost});

        return {
                    proname => $cache_entry->{proname},
                    nspname => $cache_entry->{nspname},
                    params  => $params
               };
    }

    # not cached, see if we can map it to a function
    my $dbh = $dbconn->dbh;
    die "could not connect to database: $DBI::errstr\n" if (!defined($dbh));

    my $sth = $dbh->prepare($sql_query);
    my @method_arguments = keys %{$params};
    $sth->execute($method, \@method_arguments);

    die "unknown API call \"$method(".join(",", @old_param_list).")\"" if ($sth->rows == 0);
    die "could not unambiguously map API call \"$method\" to a function" if ($sth->rows > 1);

    my $data = $sth->fetchrow_hashref;
    # make sure there are no more rows
    die "internal error" if defined($sth->fetchrow_hashref);

    my $requirehost = $data->{requirehost};
    $api_method_cache{$cache_key} =
        {
            proname     => $data->{proname},
            nspname     => $data->{nspname},
            requirehost => $requirehost
        };

    # inject host if necessary
    $params->{_host} = $host if ($requirehost);

    return {
                proname => $data->{proname},
                nspname => $data->{nspname},
                params  => $params
           };
}


# Calculate a cache key for a method call, given its signature.
sub _calculate_api_method_cache_key
{
    my ($proname, $argnames) = @_;
    return $proname."(".join(",", sort @{$argnames}).")";
}

# Compare signatures of two method calls.  Uses _calculate_api_method_cache_key
sub _compare_signature
{
    my ($a, $aparams, $b, $bparams) = @_;

    return _calculate_api_method_cache_key($a, $aparams) eq
           _calculate_api_method_cache_key($b, $bparams);
}

END
{
}

1;
