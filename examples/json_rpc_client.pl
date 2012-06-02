use strict;
use warnings;

use JSON::RPC::Simple;
use Data::Dumper;

my $pg = JSON::RPC::Simple->connect("http://127.0.0.1:54321/API/", {
    timeout => 600,
});

my $random = $pg->random();
my $userid = $pg->get_userid_by_username({username => 'joel'});
my $hosts = $pg->get_user_hosts({userid => 123});
my $user_details = $pg->get_user_details({userid => 123});
my $user_friends = $pg->get_user_friends({userid => 123});

print Dumper $random;
print Dumper $userid;
print Dumper $hosts;
print Dumper $user_details;
print Dumper $user_friends;
