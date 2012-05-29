#!/usr/bin/perl
use strict;
use warnings;

use lib 'lib';
use DBIx::Pg::CallFunction;
use Data::Dumper;

my $dbh = DBI->connect("dbi:Pg:dbname=joel", 'joel', '');
my $pg = DBIx::Pg::CallFunction->new($dbh);

my $userid = $pg->get_userid_by_username({'username' => 'joel'});
my $hosts = $pg->get_user_hosts({userid => 123});
my $user_details = $pg->get_user_details({userid => 123});
my $user_friends = $pg->get_user_friends({userid => 123});

print Dumper $userid;
print Dumper $hosts;
print Dumper $user_details;
print Dumper $user_friends;

# $VAR1 = 123;
# $VAR1 = [
#           '127.0.0.1',
#           '192.168.0.1',
#           '10.0.0.1'
#         ];
# $VAR1 = {
#           'firstname' => 'Joel',
#           'lastname' => 'Jacobson',
#           'creationdate' => '2012-05-29 00:22:14.8537+07'
#         };
# $VAR1 = [
#           {
#             'firstname' => 'Claes',
#             'userid' => 234,
#             'lastname' => 'Jakobsson',
#             'creationdate' => '2012-05-29 00:22:14.854939+07'
#           },
#           {
#             'firstname' => 'Magnus',
#             'userid' => 345,
#             'lastname' => 'Hagander',
#             'creationdate' => '2012-05-29 00:22:14.854941+07'
#           },
#           {
#             'firstname' => 'Lukas',
#             'userid' => 456,
#             'lastname' => 'Gratte',
#             'creationdate' => '2012-05-29 00:22:14.854941+07'
#           }
#         ];
