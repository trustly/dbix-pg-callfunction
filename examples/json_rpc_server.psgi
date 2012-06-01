#!/usr/bin/perl
use strict;
use warnings;

use DBI;
use DBIx::Pg::CallFunction;
use JSON;
use Data::Dumper;

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

    unless ($env->{HTTP_ACCEPT}   eq 'application/json' &&
           $env->{REQUEST_METHOD} eq 'POST' &&
           $env->{CONTENT_TYPE}   =~ m!^application/json!
    ) {
        return $invalid_request;
    }

    my $json_input;
    $env->{'psgi.input'}->read($json_input, $env->{CONTENT_LENGTH});

    my $json_rpc_request = from_json($json_input);
    my $method  = $json_rpc_request->{method};
    my $params  = $json_rpc_request->{params};
    my $id      = $json_rpc_request->{id};
    my $version = $json_rpc_request->{version};
    my $jsonrpc = $json_rpc_request->{jsonrpc};

    unless ($method =~ m/^[a-zA-Z_][a-zA-Z0-9_]*/ && (!defined $params || ref($params) eq 'HASH')) {
        return $invalid_request;
    }

    my $dbh = DBI->connect("dbi:Pg:dbname=joel", 'joel', '');
    my $pg = DBIx::Pg::CallFunction->new($dbh);

    my $result = $pg->$method($params);

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