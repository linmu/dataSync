#!/usr/bin/perl

package configReader;

use strict;
use DBI;

#parse config file
sub config_parser {
    # if open file failed, exit
    my $config_file = shift @_;
    open CONFIG,"<$config_file" or die "Cannot open config file: $config_file ($!)";
    
    my %config_hash = ();
    while(<CONFIG>) {
        chomp;
        s/#.*//;
        s/^\s+//;
        s/\s+$//;
        next unless length;
        my($key,$value) = split(/\s*=\s*/,$_,2);
        $config_hash{$key} = $value;
    } 
    
    close CONFIG;
    return %config_hash;
}

## read config file
our %config = &config_parser("../conf/finance_autotest.conf");
## get input params from db:finance_data_task
our $finance_data_task_host = $config{"DB_FINANCE_DATA_TASK_HOST"};
our $finance_data_task_user = $config{"DB_FINANCE_DATA_TASK_USER"};
our $finance_data_task_pwd = $config{"DB_FINANCE_DATA_TASK_PWD"};
our $finance_data_task_db = $config{"FINANCE_DATA_TASK"};

our $dsn = "DBI:mysql:$finance_data_task_db:$finance_data_task_host";
our $user = $finance_data_task_user;
our $password = $finance_data_task_pwd;

1;
