#!/usr/bin/perl

use strict;
use warnings;

use DBI;
use DBD::Pg;
use DBIx::Connector;
use JSON;

package TrustlyApi::DBConnection;

# new(): Create a handle to a new DBConnection object
sub new
{
    my $class = shift;
    my ($dbconn) = @_;

    my $self = 
    {
        dbconn      => $dbconn,
        timed_out   => 0
    };

    bless $self, $class;
    return $self;
}

# call_function(): Call a function.  The parameter is a hashref which should
# contain the following elements:
#   - nspname: The namespace of the function.  May be "undef", in which case
#              the first schema in search_path containing the function is used.
#   - proname: The name of the function to call.
#   - proretset: Boolean, 1 if the functions returns a set, 0 otherwise
#   - params: A hashref containg the parameters.
sub call_function
{
    my ($self, $function_call) = @_;

    my $nspname = $function_call->{nspname};
    my $proname = $function_call->{proname};
    my $params = $function_call->{params};

    my @param_names = sort keys %{$params};
    my @param_values = ();
    foreach my $key (@param_names)
    {
        my $value = $params->{$key};

        # If any of the parameters is a hashref, replace it with a json
        # representation of that hash.
        if (ref($value) eq 'HASH')
        {
            push @param_values, JSON::encode_json($value);
        }
        else
        {
            push @param_values, $value;
        }
    }

    my $placeholders = join ",", map { $self->_quote_identifier($_)." := ?" } @param_names;
    my $statement = 'SELECT * FROM ' . $self->_quote_identifier($nspname, $proname) . '(' . $placeholders . ');'; 

    my $result = $self->execute($statement, @param_values);

    return $result; 
}

# execute(): Execute a query with 0 or more parameters.  Returns a hashref with
# with the keys "rows", "state" and "errstr".  If "rows" is defined, the command
# succeeded.  If "rows" is undefined, "errstr" is set to an error message
# providing additional information.  "state" is the SQLSTATE, and is always set
# to an SQLSTATE describing the error.
sub execute
{
    my ($self, $statement, @parameters) = @_;

    my $dbh;
    my $result;

    # $dbconn->dbh calls $dbh->ping() every time, and there's no reason to do
    # that, so we do the following instead: we keep getting the connection from
    # _dbh so long as queries work correctly on that connection.  If, for some
    # reason, a query does not work on that connection and we get back an
    # SQLSTATE we can't recognize (i.e. there *might* be a problem with the
    # connection), we call ->dbh() to go through the entire ping/reconnect
    # procedure.
    #
    # .. UNLESS the previous execute() call timed out.  In that case, just try
    # ->dbh() once, and if it fails, die immediately.
    if ($self->{timed_out})
    {
        $dbh = $self->{dbconn}->dbh;
        if (!defined $dbh)
        {
            # could not connect to the server
            die "Server connection failure: ".$DBI::errstr;
        }
    }
    else
    {
        $dbh = $self->{dbconn}->_dbh;
        # no need to check whether dbh is valid; that case is handled in the
        # loop below
    }

    my $retries = 0;
    while (!defined $dbh ||
           !defined($result = _execute($dbh, $statement, @parameters)))
    {
        if ($retries > 3)
        {
            # timed out
            $self->{timed_out} = 1;
            last;
        }

        #
        # This gets a bit ugly: if the database connection was lost between
        # requests, DBD::Pg reports SQLSTATE "22000" (with a rather unhelpful
        # errmsg of "7", WTF).  So if on the first attempt we get that state
        # back, make sure we try at least once more, through ->dbh() and not
        # ->_dbh().  That should catch any connection problems.
        #
        if ($dbh &&
            !($retries == 0 && $dbh->state eq '22000') &&
               ($dbh->state =~ '22[0-9A-Z]{3}' ||
                $dbh->state =~ '40[0-9A-Z]{3}' ||
                $dbh->state =~ '42[0-9A-Z]{3}' ||
                $dbh->state =~ 'P0[0-9A-Z]{3}'))
        {
            # no need to retry; the connection is fine, the query just failed
            last;
        }

        $dbh = $self->{dbconn}->dbh;
        sleep($retries * 3);
        $retries += 1;
    }

    if (!defined $dbh)
    {
        # could not connect to the server
        die "Server connection failure: ".$DBI::errstr;
    }

    # we have a connection, so reset the "timed out" variable
    $self->{timed_out} = 0;

    if (!defined $result)
    {
        my $errstr;

        $errstr = $dbh->errstr // "unknown error (SQLSTATE ".($dbh->state // "unknown").")";
        return { rows => undef, num_rows => -1, state => $dbh->state, errstr => $errstr };
    }
    return { rows => $result, num_rows => scalar @{$result}, state => '00000', errstr => undef };
}

# _execute(): Actually executes the query.  Should only be called from execute()
# and NEVER from the outside.
sub _execute
{
    my ($dbh, $statement, @parameters) = @_;

    my $sth = $dbh->prepare_cached($statement);
    return undef if (!$sth);    

    my $rv = $sth->execute(@parameters);
    return undef if (!defined $rv);

    my $result = $sth->fetchall_arrayref({});
    return undef if (defined $sth->err && $sth->err != 2);

    return $result;
}

#
# Quote an SQL identifier for safe use in a query.
#
# This is a slightly simpler version of what DBI->quote_identifier() does.  The
# reason we're reinventing the wheel here is that DBI's version always requires
# a connection handle, which greatly complicated things in some cases.  And,
# because we know we're always connecting to a PostgreSQL database, there is
# absolutely no need for a connection.
#
# Note that DBI's version also does some caching, which we don't do.  I would
# be so bold to claim that there's no need to, but I haven't tested it.
#
sub _quote_identifier
{
    my ($self, @id) = @_;

    foreach (@id)
    {
        s/"/""/g;    # escape embedded quotes
        $_ = qq{"$_"};
    }

    return join '.', @id;
}

1;
