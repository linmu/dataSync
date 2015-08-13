#!/bin/usr/perl

package extractorChooser;

use strict;
use DBI;
use ExtractorFactory;
require "configReader.pl";

sub choose_extractor {

    my $dbh = DBI->connect($configReader::dsn,$configReader::user,$configReader::password);
    my $sth = $dbh->prepare("select * from finance_data_task where progress = 0 order by `insert_time` desc limit 1;");
    $sth->execute();

    my $runtime_config = $sth->fetchrow_hashref();
    $sth->finish();
    $dbh->disconnect();
    
    defined $runtime_config or die "There is no waiting task!";  

    ## choose extractor
    (defined $runtime_config->{"test_db"} &&
    (defined $runtime_config->{"by_taskid"} || defined $runtime_config->{"by_deal_id"}))
    or die "Invalid input params!";

    if("1" == $runtime_config->{"test_db"}) {
        if(defined $runtime_config->{"by_dealid"}) {
            return ExtractorFactory->init("extractorFinanceByDealId",
                                          "from_host" => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_HOST"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_HOST"}
                                                         },
                                          "from_port" => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_PORT"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_PORT"}
                                                         },
                                          "from_user" => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_USER"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_USER"}
                                                         },
                                          "from_pwd"  => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_PWD"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_PWD"}
                                                         },
                                          "from_db"   => {
                                                          "autopay_old"=>$configReader::config{"AUTOPAY_OLD"},
                                                          "autopay_new"=>$configReader::config{"AUTOPAY_NEW"},
                                                          "detail"=>$configReader::config{"DETAIL"}
                                                         },
                                          "to_host"   => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_TEST_HOST"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_TEST_DDBS_HOST"}
                                                         },
                                          "to_port"   => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_TEST_PORT"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_TEST_DDBS_PORT"}
                                                         },
                                          "to_user"   => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_TEST_USER"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_TEST_DDBS_USER"}
                                                         },
                                          "to_pwd"    => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_TEST_PWD"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_TEST_DDBS_PWD"}
                                                         },
                                          "to_db"     => {
                                                          "autopay_old"=>$configReader::config{"AUTOPAY_TEST_OLD_FUNC"},
                                                          "autopay_new"=>$configReader::config{"AUTOPAY_TEST_NEW_FUNC"},
                                                          "detail"=>$configReader::config{"DETAIL_TEST_DDBS"}
                                                         },
                                          "group_size"=>  $configReader::config{"GROUP_SIZE"},
                                          "auto_pay_table" => $runtime_config->{"auto_pay_table"},
                                          "finance_table" => $runtime_config->{"finance_table"},
                                          "ddbs_table" => $runtime_config->{"ddbs_table"},  
                                          "id_list"   =>  $runtime_config->{"by_dealid"},
                                          "is_delete" =>  $runtime_config->{"to_delete"},
                                          "user"     =>  $runtime_config->{"user"},
                                          "process_id"=>  $runtime_config->{"id"}
                                         );
        } else {
            return ExtractorFactory->init("extractorFinanceByTaskId",
                                          "from_host" => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_HOST"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_HOST"}
                                                         },
                                          "from_port" => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_PORT"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_PORT"}
                                                         },
                                          "from_user" => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_USER"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_USER"}
                                                         },
                                          "from_pwd"  => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_PWD"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_PWD"}
                                                         },
                                          "from_db"   => {
                                                          "autopay_old"=>$configReader::config{"AUTOPAY_OLD"},
                                                          "autopay_new"=>$configReader::config{"AUTOPAY_NEW"},
                                                          "detail"=>$configReader::config{"DETAIL"}
                                                         },
                                          "to_host"   => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_TEST_HOST"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_TEST_DDBS_HOST"}
                                                         },
                                          "to_port"   => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_TEST_PORT"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_TEST_DDBS_PORT"}
                                                         },
                                          "to_user"   => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_TEST_USER"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_TEST_DDBS_USER"}
                                                         },
                                          "to_pwd"    => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_TEST_PWD"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_TEST_DDBS_PWD"}
                                                         },
                                          "to_db"     => {
                                                          "autopay_old"=>$configReader::config{"AUTOPAY_TEST_OLD_FUNC"},
                                                          "autopay_new"=>$configReader::config{"AUTOPAY_TEST_NEW_FUNC"},
                                                          "detail"=>$configReader::config{"DETAIL_TEST_DDBS"}
                                                         },
                                          "group_size"=>  $configReader::config{"GROUP_SIZE"},
                                          "auto_pay_table" => $runtime_config->{"auto_pay_table"},
                                          "finance_table" => $runtime_config->{"finance_table"},
                                          "ddbs_table" => $runtime_config->{"ddbs_table"},  
                                          "id_list"   => $runtime_config->{"by_taskid"},
                                          "is_delete" => $runtime_config->{"to_delete"},
                                          "user"     =>  $runtime_config->{"user"},
                                          "process_id"=>  $runtime_config->{"id"}
                                         );
        }
    } 
    elsif ("2" == $runtime_config->{"test_db"}) {
        if(defined $runtime_config->{"by_dealid"}) {
            return ExtractorFactory->init("extractorFinanceByDealId",
                                          "from_host" => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_HOST"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_HOST"}
                                                         },
                                          "from_port" => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_PORT"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_PORT"}
                                                         },
                                          "from_user" => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_USER"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_USER"}
                                                         },
                                          "from_pwd"  => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_PWD"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_PWD"}
                                                         },
                                          "from_db"   => {
                                                          "autopay_old"=>$configReader::config{"AUTOPAY_OLD"},
                                                          "autopay_new"=>$configReader::config{"AUTOPAY_NEW"},
                                                          "detail"=>$configReader::config{"DETAIL"}
                                                         },
                                          "to_host"   => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_TEST_HOST"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_TEST_HOST"}
                                                         },
                                          "to_port"   => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_TEST_PORT"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_TEST_PORT"}
                                                         },
                                          "to_user"   => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_TEST_USER"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_TEST_USER"}
                                                         },
                                          "to_pwd"    => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_TEST_PWD"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_TEST_PWD"}
                                                         },
                                          "to_db"     => {
                                                          "autopay_old"=>$configReader::config{"AUTOPAY_TEST_OLD_AUTO"},
                                                          "autopay_new"=>$configReader::config{"AUTOPAY_TEST_NEW_AUTO"},
                                                          "detail"=>$configReader::config{"DETAIL_TEST"}
                                                         },
                                          "group_size"=>  $configReader::config{"GROUP_SIZE"},
                                          "auto_pay_table" => $runtime_config->{"auto_pay_table"},
                                          "finance_table" => $runtime_config->{"finance_table"},
                                          "ddbs_table" => $runtime_config->{"ddbs_table"},  
                                          "id_list"   => $runtime_config->{"by_dealid"},
                                          "is_delete" => $runtime_config->{"to_delete"},
                                          "user"     =>  $runtime_config->{"user"},
                                          "process_id"=>  $runtime_config->{"id"}
                                         );
            
        } else {
            return ExtractorFactory->init("extractorFinanceByTaskId",
                                          "from_host" => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_HOST"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_HOST"}
                                                         },
                                          "from_port" => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_PORT"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_PORT"}
                                                         },
                                          "from_user" => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_USER"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_USER"}
                                                         },
                                          "from_pwd"  => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_PWD"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_PWD"}
                                                         },
                                          "from_db"   => {
                                                          "autopay_old"=>$configReader::config{"AUTOPAY_OLD"},
                                                          "autopay_new"=>$configReader::config{"AUTOPAY_NEW"},
                                                          "detail"=>$configReader::config{"DETAIL"}
                                                         },
                                          "to_host"   => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_TEST_HOST"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_TEST_HOST"}
                                                         },
                                          "to_port"   => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_TEST_PORT"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_TEST_PORT"}
                                                         },
                                          "to_user"   => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_TEST_USER"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_TEST_USER"}
                                                         },
                                          "to_pwd"    => {
                                                          "autopay"=>$configReader::config{"DB_AUTOPAY_TEST_PWD"},
                                                          "detail"=>$configReader::config{"DB_DETAIL_TEST_PWD"}
                                                         },
                                          "to_db"     => {
                                                          "autopay_old"=>$configReader::config{"AUTOPAY_TEST_OLD_AUTO"},
                                                          "autopay_new"=>$configReader::config{"AUTOPAY_TEST_NEW_AUTO"},
                                                          "detail"=>$configReader::config{"DETAIL_TEST"}
                                                         },
                                          "group_size"=>  $configReader::config{"GROUP_SIZE"},
                                          "auto_pay_table" => $runtime_config->{"auto_pay_table"},
                                          "finance_table" => $runtime_config->{"finance_table"},
                                          "ddbs_table" => $runtime_config->{"ddbs_table"},  
                                          "id_list"   => $runtime_config->{"by_taskid"},
                                          "is_delete" => $runtime_config->{"to_delete"},
                                          "user"     =>  $runtime_config->{"user"},
                                          "process_id"=>  $runtime_config->{"id"}
                                         );
        }
    }
    else {
      die "Invalid input params!"
    }
}

1;
