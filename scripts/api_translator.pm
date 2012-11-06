#!perl

use strict;
use warnings;

use DBI;
use DBD::Pg;
use DBIx::Pg::CallFunction;
use DBIx::Connector;

use Data::Dumper;

my $sql_query = q{
-- Get a list of IN and INOUT arguments from a function
WITH function_args AS
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
matched_args AS
(
    SELECT
        COALESCE(function_args.oid, method_args.oid) AS oid,
        function_args.proargname AS functionarg, method_args.proargname AS methodarg
    FROM
        function_args
    FULL JOIN
    (
        SELECT
            proargname, oid
        FROM
        (
            SELECT
                DISTINCT oid
            FROM
                function_args
        ) ss
        CROSS JOIN
            unnest(?::name[]) proargname
    ) method_args
        ON (function_args.proargname = method_args.proargname AND function_args.oid = method_args.oid)
)
SELECT
    -- There should only be one result so a scalar subquery will work perfectly here
    (SELECT proname FROM pg_proc WHERE pg_proc.oid = m1.oid) AS proname,

    -- If any function argument is missing from the method argument list, it
    -- must be _host (see the WHERE clause below).  Let the API code know that
    -- it needs to supply a value for the _host.
    bool_or(EXISTS(SELECT * FROM matched_args m2 WHERE m1.oid = m2.oid AND m2.methodarg IS NULL)) AS requirehost
FROM
    matched_args m1
WHERE
    -- If there are any arguments that are not present in the function's
    -- argument list, there's no way we can call that function
    NOT EXISTS (SELECT * FROM matched_args m2
                    WHERE m1.oid = m2.oid AND m2.functionarg IS NULL)

        AND

    -- We allow one function to be missing from the method's argument list:
    -- _host.  In that case the API code puts in the correct host.
    --
    -- Also see "requirehost" in the SELECT list above.
    NOT EXISTS (SELECT * FROM matched_args m2
                    WHERE m1.oid = m2.oid AND m2.methodarg IS NULL AND m2.functionarg <> '_host')

GROUP BY
    oid
;
};

my %api_cache = ();

my $callback = sub {
    my ($method, $params, $dbconn, $host) = @_;

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
    if (exists($api_cache{$method}))
    {
        my $cache_entry = $api_cache{$method};
        # inject host if necessary
        $params->{_host} = $host if ($cache_entry->{requirehost});
        return $cache_entry->{proname};
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

    my $func = $data->{proname};
    my $requirehost = $data->{requirehost};
    $api_cache{$method} = { proname => $func, requirehost => $requirehost };

    # inject host if necessary
    $params->{_host} = $host if ($requirehost);
    return $func;
};
