#!perl

use strict;
use warnings;

use DBI;
use DBD::Pg;
use DBIx::Pg::CallFunction;
use JSON;

my $app = sub {
    my $env = shift;

    my $invalid_request = [
        '400',
        [ 'Content-Type' => 'application/json' ],
        [ to_json({
            jsonrpc => '2.0',
            error => {
                code => -32600,
                message => 'Invalid Request.'
            },
            id => undef
        }) ]
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
        (
            ([a-zA-Z_][a-zA-Z0-9_]*) # namespace
        \.)?
        ([a-zA-Z_][a-zA-Z0-9_]*) # function name
        $
    /x && (!defined $params || ref($params) eq 'HASH')) {
        return $invalid_request;
    }
    my ($namespace, $function_name) = ($1, $2);

    my $dbh = DBI->connect("dbi:Pg:service=pg_proc_jsonrpc", '', '') or die "unable to connect to PostgreSQL";
    my $pg = DBIx::Pg::CallFunction->new($dbh);
    my $result = $pg->call($function_name, $params, $namespace);
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
        [ 'Content-Type' => 'application/json' ],
        [ to_json($response) ]
    ];
};

__END__

=head1 NAME

pg_proc_jsonrpc.psgi - PostgreSQL Stored Procedures JSON-RPC Daemon

=head1 SYNOPSIS

How to setup using C<Apache2>, C<mod_perl> and L<Plack::Handler::Apache2>.
Instructions for a clean installation of Ubuntu 12.04 LTS.

Install necessary packages

  sudo apt-get install postgresql-9.1 libplack-perl libdbd-pg-perl libjson-perl libmodule-install-perl libtest-exception-perl libapache2-mod-perl2
  sudo cpan DBIx::Pg::CallFunction

Create database user for apache system user www-data

  sudo -u postgres createuser -D -R -S www-data

Create a database owned by the www-data user

  sudo -u postgres createdb -O www-data test

Setup connection parameters for pg_proc_jsonrpc
specifying the user is not necessary as it
will default to the system user running apache,
which is normally www-data.

  # copy sample config if you don't have it yet
  sudo cp /usr/share/postgresql/9.1/pg_service.conf.sample /etc/postgresql-common/pg_service.conf

  # /etc/postgresql-common/pg_service.conf:
  [pg_proc_jsonrpc]
  application_name=pg_proc_jsonrpc
  dbname=test

Configure Apache, add location for pg_proc_jsonrpcd

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

  service apache2 restart

You can now access PostgreSQL Stored Procedures at
http://127.0.0.1/postgres using any JSON-RPC client
or from a browser using Javascript.

=head1 DESCRIPTION

C<pg_proc_jsonrpc> is a JSON-RPC daemon to access PostgreSQL stored procedures.

The script implements the L<PSGI> standard and accepts the same parameters
as the L<plackup> script.

It only supports named parameters, JSON-RPC version 1.1 or 2.0.

L<DBIx::Pg::CallFunction> is used to map
method and params in the JSON-RPC call to the corresponding
PostgreSQL stored procedure.

=head1 SEE ALSO

L<Plack::Runner> L<PSGI|PSGI> L<DBIx::Pg::CallFunction>

=cut