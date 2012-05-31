package DBIx::Pg::CallFunction;
our $VERSION = '0.004';

=head1 NAME

DBIx::Pg::CallFunction - Simple interface for calling PostgreSQL functions from Perl

=head1 VERSION

version 0.004

=head1 SYNOPSIS

    use DBI;
    use DBIx::Pg::CallFunction;

    my $dbh = DBI->connect("dbi:Pg:dbname=joel", 'joel', '');
    my $pg = DBIx::Pg::CallFunction->new($dbh);

Returning single-row single-column values:

    my $userid = $pg->get_userid_by_username({'username' => 'joel'});
    # returns scalar 123

Returning multi-row single-column values:

    my $hosts = $pg->get_user_hosts({userid => 123});
    # returns array ref ['127.0.0.1', '192.168.0.1', ...]

Returning single-row multi-column values:

    my $user_details = $pg->get_user_details({userid => 123});
    # returns hash ref { firstname=>..., lastname=>... }

Returning multi-row multi-column values:

    my $user_friends = $pg->get_user_friends({userid => 123});
    # returns array ref of hash refs [{ userid=>..., firstname=>..., lastname=>...}, ...]

=head1 DESCRIPTION

This module provides a simple efficient way to call PostgreSQL functions
with from Perl code. It only support functions with named arguments, or
functions with no arguments at all. This limitation reduces the mapping
complexity, as multiple functions in PostgreSQL can share the same name,
but with different input argument types.

=head1 SEE ALSO

This module is built on top of L<DBI>, and
you need to use that module (and the appropriate DBD::xx drivers)
to establish a database connection.

There is another module providing about the same functionality,
but without support for named arguments. Have a look at this one
if you need to access functions without named arguments,
or if you are using Oracle:

L<DBIx::ProcedureCall|DBIx::ProcedureCall>

=head1 LIMITATIONS

Requires PostgreSQL 9.0 or later.
Only supports stored procedures / functions with
named input arguments.

=head1 AUTHORS

Joel Jacobson L<http://www.joelonsql.com>

=head1 COPYRIGHT

Copyright (c) Joel Jacobson, Sweden, 2012. All rights reserved.

This software is released under the MIT license cited below.

=head2 The "MIT" License

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.

=cut

use strict;
use warnings;

use Carp;
use DBI;

our $AUTOLOAD;

sub new
{
    my $class = shift;
    my $self =
    {
        dbh => shift
    };
    bless $self, $class;
    return $self;
}

sub AUTOLOAD
{
    my $self   = shift;
    my $args   = shift;
    my $name   = $AUTOLOAD;
    return if ($name =~ /DESTROY$/);
    $name =~ s!^.*::([^:]+)$!$1!;
    return $self->call($name, $args);
}

sub _proretset
{
    # Returns the value of pg_catalog.pg_proc.proretset for the function.
    # "proretset" is short for procedure returns set.
    # If 1, the function returns multiple rows, or zero rows.
    # If 0, the function always returns exactly one row.
    my ($self, $name, $argnames) = @_;

    my $get_proretset = $self->{dbh}->prepare_cached("
        WITH
        -- Unnest the proargname and proargmode
        -- arrays, so we get one argument per row,
        -- allowing us to select only the IN
        -- arguments and build new arrays.
        NamedInputArgumentFunctions AS (
            -- For functions with only IN arguments,
            -- proargmodes IS NULL
            SELECT
                oid,
                proname,
                proretset,
                unnest(proargnames) AS proargname,
                'i'::text AS proargmode
            FROM pg_catalog.pg_proc
            WHERE proargnames IS NOT NULL
            AND proargmodes IS NULL
            UNION ALL
            -- For functions with INOUT/OUT arguments,
            -- proargmodes is an array where each
            -- position matches proargname and
            -- indicates if its an IN, OUT or INOUT
            -- argument.
            SELECT
                oid,
                proname,
                proretset,
                unnest(proargnames) AS proargname,
                unnest(proargmodes) AS proargmode
            FROM pg_catalog.pg_proc
            WHERE proargnames IS NOT NULL
            AND proargmodes IS NOT NULL
        ),
        OnlyINandINOUTArguments AS (
            -- Select only the IN and INOUT
            -- arguments and build new arrays
            SELECT
                oid,
                proname,
                proretset,
                array_agg(proargname) AS proargnames
            FROM NamedInputArgumentFunctions
            WHERE proargmode IN ('i','b')
            GROUP BY
                oid,
                proname,
                proretset
        )
        -- Find any function matching the name
        -- and having identical argument names
        SELECT * FROM OnlyINandINOUTArguments
        WHERE proname = ?::text
        -- The order of arguments doesn't matter,
        -- so compare the arrays by checking
        -- if A contains B and B contains A
        AND ?::text[] <@ proargnames
        AND ?::text[] @> proargnames
    ");
    $get_proretset->execute($name, $argnames, $argnames);

    my $proretset;
    my $i = 0;
    while (my $h = $get_proretset->fetchrow_hashref()) {
        $i++;
        $proretset = $h;
    }
    if ($i == 0)
    {
        croak "no function matches the input arguments, function: $name";
    }
    elsif ($i == 1)
    {
        return $proretset->{proretset};
    }
    else
    {
        croak "multiple functions matches the same input arguments, function: $name";
    }
}

sub call
{
    my ($self,$name,$args) = @_;

    my $validate_name_regex = qr/^[a-zA-Z_][a-zA-Z0-9_]*$/;

    croak "dbh and name must be defined" unless defined $self->{dbh} && defined $name;
    croak "invalid format of name" unless $name =~ $validate_name_regex;
    croak "args must be a hashref" unless ref $args eq 'HASH';

    my @arg_names = sort keys %{$args};
    my @arg_values = @{$args}{@arg_names};

    foreach my $arg_name (@arg_names)
    {
        if ($arg_name !~ $validate_name_regex)
        {
            croak "invalid format of argument name: $arg_name";
        }
    }

    my $placeholders = join ",", map { "$_ := ?" } @arg_names;

    my $sql = 'SELECT * FROM ' . $name . '(' . $placeholders . ');';

    my $proretset = $self->_proretset($name, \@arg_names);

    my $query = $self->{dbh}->prepare($sql);
    $query->execute(@arg_values);

    my $output;
    my $num_cols;
    my @output_columns;
    for (my $row_number=0; my $h = $query->fetchrow_hashref(); $row_number++)
    {
        if ($row_number == 0)
        {
            @output_columns = sort keys %{$h};
            $num_cols = scalar @output_columns;
            croak "no columns in return" unless $num_cols >= 1;
        }
        if ($proretset == 0)
        {
            # single-row
            croak "function returned multiple rows" if defined $output;
            if ($num_cols == 1)
            {
                # single-column
                $output = $h->{$output_columns[0]};
            }
            elsif ($num_cols > 1)
            {
                # multi-column
                $output = $h;
            }
        }
        elsif ($proretset == 1)
        {
            # multi-row
            if ($num_cols == 1)
            {
                # single-column
                push @$output, $h->{$output_columns[0]};
            }
            elsif ($num_cols > 1)
            {
                # multi-column
                push @$output, $h;
            }
        }
    }
    return $output;
}

1;

=begin Pod::Coverage

call

=end Pod::Coverage

# vim: ts=8:sw=4:sts=4:et