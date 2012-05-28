package DBIx::Pg::CallFunction;
our $VERSION = '0.001';

=head1 NAME

DBIx::Pg::CallFunction - Simple interface for calling PostgreSQL functions from Perl

=head1 VERSION

version 0.001

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

=head1 OTHER INFORMATION

=head2 Limitations and Caveats

Requires PostgreSQL 9.0 or later.

=head2 Author and Copyright

Joel Jacobson L<http://www.joelonsql.com>

Copyright (c) Joel Jacobson, Sweden, 2012. All rights reserved.
You may use and distribute on the same terms as Perl 5.10.1.

=cut

use strict;
use warnings;
use Carp;
use DBI;

our $AUTOLOAD;

sub new {
    my $class = shift;
    my $self = {
        dbh => shift
    };
    bless $self, $class;
    return $self;
}

sub AUTOLOAD {
    my $self   = shift;
    my $args   = shift;
    my ($name) = $AUTOLOAD;
    return if ($name =~ /DESTROY$/);
    $name =~ s!^.*::([^:]+)$!$1!;
    return $self->call($name, $args);
}

sub _proretset {
    my ($self, $name, $argnames) = @_;
    my $get_proretset = $self->{dbh}->prepare("
        SELECT proretset
        FROM pg_catalog.pg_proc
        WHERE proname = ?::text
        AND ?::text[] <@ proargnames
    ");
    $get_proretset->execute($name, $argnames);
    my ($proretset) = $get_proretset->fetchrow_array();
    return $proretset;
}

sub call
{
    my ($self,$name,$args) = @_;

    my $validate_name_regex = qr/^[a-zA-Z_]+[a-zA-Z0-9_]*$/;

    croak "dbh and name must be defined" unless defined $self->{dbh} && defined $name;
    croak "invalid format of name" unless $name =~ $validate_name_regex;
    croak "args must be a hashref" unless ref $args eq 'HASH';

    my @arg_names = sort keys %{$args};
    my @arg_values = @{$args}{@arg_names};

    $_ =~ $validate_name_regex || croak "invalid format of argument name: $_" for @arg_names;

    my $placeholders = join ",", map { "$_ := ?" } @arg_names;

    my $sql = 'SELECT * FROM ' . $name . '(' . $placeholders . ');';

    # proretset=0 : Function returns 1 row
    # proretset=1 : Function returns >=0 rows
    my $proretset = $self->_proretset($name, \@arg_names);

    my $query = $self->{dbh}->prepare($sql);
    $query->execute(@arg_values);

    my $output;
    my $num_cols;
    my @output_columns;
    for (my $row_number=0; my $h = $query->fetchrow_hashref(); $row_number++) {
        if ($row_number == 0) {
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