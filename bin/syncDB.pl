#!/usr/bin/perl

use strict;
use DBI;
require "configReader.pl";
require "extractorChooser.pl";

my $extractor = &extractorChooser::choose_extractor;

# 1. sync DB
$extractor->syncData2Test();

# 2. update progress
my %userData = $extractor->getUserData();
my $owner = $userData{"user"};
my $process_id = $userData{"process_id"};

my $dbh = DBI->connect($configReader::dsn,$configReader::user,$configReader::password);
my $sth = $dbh->prepare("update finance_data_task set progress = 100 where id = $process_id and user = '$owner';");
$sth->execute();
$sth->finish();
$dbh->disconnect();
