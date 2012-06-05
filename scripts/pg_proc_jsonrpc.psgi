#!perl

use strict;
use warnings;

use DBI;
use DBD::Pg;
use DBIx::Pg::CallFunction;
use JSON;
use Plack::Request;

my $app = sub {
    my $env = shift;

    my $invalid_request = [
        '400',
        [ 'Content-Type' => 'application/json; charset=utf-8' ],
        [ to_json({
            jsonrpc => '2.0',
            error => {
                code => -32600,
                message => 'Invalid Request.'
            },
            id => undef
        }, {pretty => 1}) ]
    ];

    my ($method, $params, $id, $version, $jsonrpc);
    if ($env->{REQUEST_METHOD} eq 'GET') {
        my $req = Plack::Request->new($env);
        $method = $req->path_info;
        $method =~ s{^.*/}{};
        $params = $req->query_parameters->mixed;

    } elsif ($env->{REQUEST_METHOD} eq 'POST' &&
        $env->{HTTP_ACCEPT} eq 'application/json' &&
        $env->{CONTENT_TYPE} =~ m!^application/json!
    ) {
        my $json_input;
        $env->{'psgi.input'}->read($json_input, $env->{CONTENT_LENGTH});
        my $json_rpc_request = from_json($json_input);

        $method  = $json_rpc_request->{method};
        $params  = $json_rpc_request->{params};
        $id      = $json_rpc_request->{id};
        $version = $json_rpc_request->{version};
        $jsonrpc = $json_rpc_request->{jsonrpc};
    } else {
        return $invalid_request;
    }

    unless ($method =~ m/
        ^
        (?:
            ([a-zA-Z_][a-zA-Z0-9_]*) # namespace
        \.)?
        ([a-zA-Z_][a-zA-Z0-9_]*) # function name
        $
    /x && (!defined $params || ref($params) eq 'HASH')) {
        return $invalid_request;
    }
    my ($namespace, $function_name) = ($1, $2);

    my $dbh = DBI->connect("dbi:Pg:service=pg_proc_jsonrpc", '', '', {pg_enable_utf8 => 1}) or die "unable to connect to PostgreSQL";
    my $pg = DBIx::Pg::CallFunction->new($dbh);
    my $result = $pg->$function_name($params, $namespace);
    $dbh->disconnect;

    my $response = {
        result => $result,
        error  => undef
    };
    if (defined $id) {
        $response->{id} = $id;
    }
    if (defined $version && $version eq '1.1') {
        $response->{version} = $version;
    }
    if (defined $jsonrpc && $jsonrpc eq '2.0') {
        $response->{jsonrpc} = $jsonrpc;
        delete $response->{error};
    }

    return [
        '200',
        [ 'Content-Type' => 'application/json; charset=utf-8' ],
        [ to_json($response, {pretty => 1}) ]
    ];
};

__END__

=head1 NAME

pg_proc_jsonrpc.psgi - PostgreSQL Stored Procedures JSON-RPC Daemon

=head1 SYNOPSIS

How to setup using C<Apache2>, C<mod_perl> and L<Plack::Handler::Apache2>.
Instructions for a clean installation of Ubuntu 12.04 LTS.

Install necessary packages

  sudo apt-get install cpanminus build-essential postgresql-9.1 libplack-perl libdbd-pg-perl libjson-perl libmodule-install-perl libtest-exception-perl libapache2-mod-perl2 apache2-mpm-prefork

Create a database and database user for our shell user

  sudo -u postgres createuser --no-superuser --no-createrole --createdb $USER
  sudo -u postgres createdb --owner=$USER $USER

Try to connect

  psql -c "SELECT 'Hello world'"
    ?column?   
  -------------
   Hello world
  (1 row)

Create database user for apache

  sudo -u postgres createuser --no-superuser --no-createrole --no-createdb www-data

Download and build DBIx::Pg::CallFunction

  cpanm --sudo DBIx::Pg::CallFunction

Grant access to connect to our database

  psql -c "GRANT CONNECT ON DATABASE $USER TO \"www-data\""

Configure pg_service.conf

  # copy sample config
  sudo cp -n /usr/share/postgresql/9.1/pg_service.conf.sample /etc/postgresql-common/pg_service.conf

  echo "
  [pg_proc_jsonrpc]
  application_name=pg_proc_jsonrpc
  dbname=$USER
  " | sudo sh -c 'cat - >> /etc/postgresql-common/pg_service.conf'


Configure Apache

  # Add the lines below between <VirtualHost *:80> and </VirtualHost>
  # to your sites-enabled file, or to the default file if this
  # is a new installation.

  # /etc/apache2/sites-enabled/000-default
  <Location /postgres>
    SetHandler perl-script
    PerlResponseHandler Plack::Handler::Apache2
    PerlSetVar psgi_app /usr/local/bin/pg_proc_jsonrpc.psgi
  </Location>
  <Perl>
    use Plack::Handler::Apache2;
    Plack::Handler::Apache2->preload("/usr/local/bin/pg_proc_jsonrpc.psgi");
  </Perl>

Restart Apache

  sudo service apache2 restart

Done!

You can now access PostgreSQL Stored Procedures, e.g.
L<http://127.0.0.1/postgres/now> using any JSON-RPC client,
such as a web browser, some Perl program, or
any application capable of talking HTTP and JSON-RPC.

Let's try it with an example!

Connect to our database using psql and copy/paste the SQL commands
to create a simple schema with some Stored Procedures.

Note the C<SECURITY DEFINER> below. It means the functions will
be executed by the same rights as our C<$USER>, with full access
to our database C<$USER>. The C<www-data> user is only granted
C<EXECUTE> access to the functions, and cannot touch the tables
using C<SELECT>, C<UPDATE>, C<INSERT> or C<DELETE> SQL commands.
You can think of C<SECURITY DEFINER> as a sudo for SQL.

  psql
  
  -- Some tables:
  
  CREATE TABLE users (
  userid serial not null,
  username text not null,
  datestamp timestamptz not null default now(),
  PRIMARY KEY (userid),
  UNIQUE(username)
  );
  
  CREATE TABLE usercomments (
  usercommentid serial not null,
  userid integer not null,
  comment text not null,
  datestamp timestamptz not null default now(),
  PRIMARY KEY (usercommentid),
  FOREIGN KEY (userid) REFERENCES Users(userid)
  );
  
  -- By default, all users including www-data, will be able to execute any functions.
  -- Revoke all access on functions from public, which allows us to explicitly grant
  -- access only to those functions we wish to expose publicly.
  
  ALTER DEFAULT PRIVILEGES REVOKE ALL ON FUNCTIONS FROM PUBLIC;
  
  -- Function to make a new comment
  
  CREATE OR REPLACE FUNCTION new_user_comment(_username text, _comment text) RETURNS BIGINT AS $$
  DECLARE
  _userid integer;
  _usercommentid integer;
  BEGIN
  SELECT userid INTO _userid FROM users WHERE username = _username;
  IF NOT FOUND THEN
      INSERT INTO users (username) VALUES (_username) RETURNING userid INTO STRICT _userid;
  END IF;
  INSERT INTO usercomments (userid, comment) VALUES (_userid, _comment) RETURNING usercommentid INTO STRICT _usercommentid;
  RETURN _usercommentid;
  END;
  $$ LANGUAGE plpgsql SECURITY DEFINER;

  -- Function to get all comments by a user

  CREATE OR REPLACE FUNCTION get_user_comments(OUT usercommentid integer, OUT comment text, OUT datestamp timestamptz, _username text) RETURNS SETOF RECORD AS $$
  SELECT
      usercomments.usercommentid,
      usercomments.comment,
      usercomments.datestamp
  FROM usercomments JOIN users USING (userid) WHERE users.username = $1
  ORDER BY 1
  $$ LANGUAGE sql SECURITY DEFINER;

  -- Function to get all comments by all users

  CREATE OR REPLACE FUNCTION get_all_comments(OUT usercommentid integer, OUT username text, OUT comment text, OUT datestamp timestamptz) RETURNS SETOF RECORD AS $$
  SELECT
      usercomments.usercommentid,
      users.username,
      usercomments.comment,
      usercomments.datestamp
  FROM usercomments JOIN users USING (userid)
  ORDER BY 1
  $$ LANGUAGE sql SECURITY DEFINER;
  
  -- Grant EXECUTE on the functions to www-data
  
  GRANT EXECUTE ON FUNCTION new_user_comment(_username text, _comment text) TO "www-data";
  GRANT EXECUTE ON FUNCTION get_user_comments(OUT usercommentid integer, OUT comment text, OUT datestamp timestamptz, _username text) TO "www-data";
  GRANT EXECUTE ON FUNCTION get_all_comments(OUT usercommentid integer, OUT username text, OUT comment text, OUT datestamp timestamptz) TO "www-data";

The JSON-RPC service supports both GET and POST,
let's try GET as it is easiest to test using a web browser.
However, when developing for real ALWAYS use POST and
set Content-Type to application/json.


  L<http://127.0.0.1/postgres/new_user_comment?_username=joel&_comment=Accessing+PostgreSQL+from+a+browser+is+easy!>
  {
     "error" : null,
     "result" : "1"
  }
  
  L<http://127.0.0.1/postgres/new_user_comment?_username=lukas&_comment=I+must+agree!+Also+easy+from+JQuery!>
  {
     "error" : null,
     "result" : "2"
  }
  
  L<http://127.0.0.1/postgres/new_user_comment?_username=claes&_comment=Or+using+JSON::RPC::Simple>
  {
     "error" : null,
     "result" : "3"
  }
  
  L<http://127.0.0.1/postgres/get_all_comments>
  {
     "error" : null,
     "result" : [
        {
           "usercommentid" : 1,
           "comment" : "Accessing PostgreSQL from a browser is easy!",
           "datestamp" : "2012-06-03 01:20:25.653989+07",
           "username" : "joel"
        },
        {
           "usercommentid" : 2,
           "comment" : "I must agree! Also easy from JQuery!",
           "datestamp" : "2012-06-03 01:21:30.19081+07",
           "username" : "lukas"
        },
        {
           "usercommentid" : 3,
           "comment" : "Or using JSON::RPC::Simple",
           "datestamp" : "2012-06-03 01:22:09.149454+07",
           "username" : "claes"
        }
     ]
  }

=head1 DESCRIPTION

C<pg_proc_jsonrpc> is a JSON-RPC daemon to access PostgreSQL stored procedures.

The script implements the L<PSGI> standard and can be started using
the L<plackup> script, or by any webserver capable of handling PSGI files,
such as Apache using L<Plack::Handler::Apache2>.

As L<DBI> is not thread safe, you must not use threaded webservers,
such as C<apache2-mpm-worker>, use instead e.g. C<apache2-mpm-prefork>.

It only supports named parameters, JSON-RPC version 1.1 or 2.0.

L<DBIx::Pg::CallFunction> is used to map
method and params in the JSON-RPC call to the corresponding
PostgreSQL stored procedure.

=head1 SEE ALSO

L<plackup> L<Plack::Runner> L<PSGI|PSGI> L<DBIx::Pg::CallFunction>

=cut