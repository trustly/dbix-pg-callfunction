#!/usr/bin/perl

use strict;
use warnings;

package TrustlyApiMapper::SqlQueries;

BEGIN
{
}

our $sql_map_function_call = q{
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
                COALESCE(proargmodes, array_fill('i'::"char", ARRAY[COALESCE(array_length(proargnames,1), 0)])) AS proargmodes
            FROM
                pg_proc
            WHERE
                lower(regexp_replace(proname, E'([^\\\\^])_', E'\\\\1', 'g')) = lower($1) OR
                lower(proname) = lower($1)
        ) ss
    ) ss2
    WHERE
        proargmode IN('i', 'b')
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
            unnest($2::name[]) proargname
    ) MethodArgs
        ON (FunctionArgs.proargname = MethodArgs.proargname AND FunctionArgs.oid = MethodArgs.oid)
)

SELECT
    pg_proc.proname, pg_namespace.nspname, requirehost, prorettype = 'json'::regtype AS returns_json
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

our $sql_map_function_call_noparams = q{
SELECT
    proname, pg_namespace.nspname, FALSE AS requirehost,
    prorettype = 'json'::regtype AS returns_json
FROM
    pg_proc
JOIN
    pg_namespace
        ON (pg_namespace.oid = pg_proc.pronamespace)
WHERE
    (lower(regexp_replace(proname, E'([^\\\\^])_', E'\\\\1', 'g')) = lower($1) OR lower(proname) = lower($1)) AND
    proargnames IS NULL

    UNION ALL

SELECT
    proname, pg_namespace.nspname, TRUE AS requirehost,
    prorettype = 'json'::regtype AS returns_json
FROM
(
    SELECT
        oid, array_agg(proargname) AS proargnames
    FROM
    (
        SELECT
            oid,
            unnest(proargnames) AS proargname,
            -- proargmodes can be NULL, meaning all arguments are IN               
            unnest(COALESCE(proargmodes, array_fill('i'::"char", ARRAY[COALESCE(array_length(proargnames,1), 0)]))) AS proargmode
        FROM
            pg_proc
        WHERE
            (lower(regexp_replace(proname, E'([^\\\\^])_', E'\\\\1', 'g')) = lower($1) OR lower(proname) = lower($1)) AND
            '_host' = ANY(proargnames)
    ) unnested
    WHERE
        proargmode IN ('i', 'b')
    GROUP BY
        oid
) aggregated
JOIN
    pg_proc
        ON (pg_proc.oid = aggregated.oid)
JOIN
    pg_namespace
        ON (pg_namespace.oid = pg_proc.pronamespace)
WHERE
    aggregated.proargnames = ARRAY['_host']
;
};

our $sql_map_external_method_call = q{
SELECT
    Name
FROM
    Functions
WHERE
    ApiMethod = $1 AND
    (ApiParams || text 'Password') <@ $2 AND (ApiParams || text 'Password') @> $2
};

END
{
}

1;
