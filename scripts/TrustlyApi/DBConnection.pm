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
        dbconn => $dbconn
    };

    bless $self, $class;
    return $self;
}

# call_function(): Call a function.  The parameter is a hashref which should
# contain the following elements:
#   - nspname: The namespace of the function.  May be "undef", in which case
#              the first schema in search_path containing the function is used.
#   - proname: The name of the function to call.
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

    my $dbh = $self->_dbh;
    my $placeholders = join ",", map { $dbh->quote_identifier($_)." := ?" } @param_names;
    my $statement = 'SELECT * FROM ' . $dbh->quote_identifier(undef, $nspname, $proname) . '(' . $placeholders . ');'; 

    my $result = $self->execute($statement, @param_values);

    return $result; 
}

# _dbh(): Get a database handle without pinging the database
sub _dbh
{
    my $self = shift;
    my $dbh = $self->{dbconn}->_dbh;
    die $DBI::errstr if (!defined $dbh);
    return $dbh;
}

# dbh(): Get a database handle (always pings the database)
sub dbh
{
    my $self = shift;
    my $dbh = $self->{dbconn}->dbh;
    die $DBI::errstr if (!defined $dbh);
    return $dbh;
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
    # that, so we do the following instead: we keep getting the connection
    # from _dbh so long as queries work correctly on that connection.  If,
    # for some reason, a query does not work on that connection and we get
    # back an SQLSTATE we can't recognize (i.e. there *might* be a problem
    # with the connection), we call dbh to go through the entire ping/reconnect
    # procedure and retry immediately.
    $dbh = $self->_dbh;

    my $retried = 0;
    while (!defined($result = _execute($dbh, $statement, @parameters)))
    {
        if ($retried ||
            ($dbh->state =~ '22[0-9A-Z]{3}' ||
             $dbh->state =~ '40[0-9A-Z]{3}' ||
             $dbh->state =~ '42[0-9A-Z]{3}' ||
             $dbh->state =~ 'P0[0-9A-Z]{3}'))
        {
            # no need to retry
            last;
        }

        $dbh = $self->dbh;
        $retried = 1;
    }

    if (!defined $result)
    {
        my $errstr;

        $errstr = $dbh->errstr;
        # make sure errstr is set to something
        $errstr = "unknown error" if (!defined $errstr);

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


1;
