#!/bin/usr/perl

use strict;

package Extractor::extractorFinanceByDealId;
use parent ("Extractor::extractorBase");

sub new {
    my $invocant = shift;
    my $class = ref($invocant) || $invocant;
    my $self = {@_};
    bless($self,$class);
    return $self;
}

sub getUserData{
    my ($self) = @_;
    
    my %userData = ("user"=>$self->{"user"},"process_id"=>$self->{"process_id"});
    return %userData;
}

sub syncData2Test {
    my ($self) = @_;
    
    # write deal_ids to a file
    my @deal_ids_list = split(/,/,$self->{"id_list"});
    my %count;
    ## squeeze an array
    @deal_ids_list = grep {++$count{$_} < 2} @deal_ids_list;
    open OUTPUT_FILE,">>../data/deal_id" or die "Cannot open file."; 
    foreach (@deal_ids_list) {
        print OUTPUT_FILE $_."\n";
    }
    close OUTPUT_FILE;
    
    
    
    # 1. get online data 2 files
    $self->getRecord2FileById($self->{"from_host"}->{"autopay"},
                              $self->{"from_port"}->{"autopay"},
                              $self->{"from_user"}->{"autopay"},
                              $self->{"from_pwd"}->{"autopay"},
                              $self->{"from_db"}->{"autopay_old"},
                              "select distinct queue_id from contract_relation where product_id in (\%s)",
                              "../data/deal_id",
                              "../data/queue_id",
                              $self->{"group_size"}
                             );
    my @auto_pay_table = split(/,/,$self->{"auto_pay_table"});
    my $id_field;
    foreach my $table (@auto_pay_table) {
        if($table =~ m/contract_queue/) {
            $id_field = "id";
        } else {
            $id_field = "queue_id";
        }
        if($table =~ m/settlement_rule_ext/) {
            die "data/rule_id file is lost" if (! -e "../data/rule_id");
            $self->getRecord2FileById($self->{"from_host"}->{"autopay"},
                                      $self->{"from_port"}->{"autopay"},
                                      $self->{"from_user"}->{"autopay"},
                                      $self->{"from_pwd"}->{"autopay"},
                                      $self->{"from_db"}->{"autopay_old"},
                                      "set names utf8;select * from $table where rule_id in (\%s)",
                                      "../data/rule_id",
                                      "../data/$table",
                                      $self->{"group_size"}
                                     );
            next;
        } elsif ($table =~ m/payment_task_rule/ || $table =~ m/payment_task_belong/) {
            die "data/task_id file is lost" if (! -e "../data/task_id");
            $self->getRecord2FileById($self->{"from_host"}->{"autopay"},
                                      $self->{"from_port"}->{"autopay"},
                                      $self->{"from_user"}->{"autopay"},
                                      $self->{"from_pwd"}->{"autopay"},
                                      $self->{"from_db"}->{"autopay_old"},
                                      "set names utf8;select * from $table where task_id in (\%s)",
                                      "../data/task_id",
                                      "../data/$table",
                                      $self->{"group_size"}
                                     );
            next;
        }
        if($table =~ m/(payment_task|nonautomatic_payment_task)/) {
            $self->getRecord2FileById($self->{"from_host"}->{"autopay"},
                                      $self->{"from_port"}->{"autopay"},
                                      $self->{"from_user"}->{"autopay"},
                                      $self->{"from_pwd"}->{"autopay"},
                                      $self->{"from_db"}->{"autopay_old"},
                                      "select task_id from $table where $id_field in (\%s)",
                                      "../data/queue_id",
                                      "../data/task_id",
                                      $self->{"group_size"}
                                     );
        }
        elsif($table =~ m/settlement_rule/){
            $self->getRecord2FileById($self->{"from_host"}->{"autopay"},
                                      $self->{"from_port"}->{"autopay"},
                                      $self->{"from_user"}->{"autopay"},
                                      $self->{"from_pwd"}->{"autopay"},
                                      $self->{"from_db"}->{"autopay_old"},
                                      "select rule_id from $table where $id_field in (\%s)",
                                      "../data/queue_id",
                                      "../data/rule_id",
                                      $self->{"group_size"}
                                     );
        }
        $self->getRecord2FileById($self->{"from_host"}->{"autopay"},
                                  $self->{"from_port"}->{"autopay"},
                                  $self->{"from_user"}->{"autopay"},
                                  $self->{"from_pwd"}->{"autopay"},
                                  $self->{"from_db"}->{"autopay_old"},
                                  "set names utf8;select * from $table where $id_field in (\%s)",
                                  "../data/queue_id",
                                  "../data/$table",
                                  $self->{"group_size"}
                                 );
    }
    my @finance_table = split(/,/,$self->{"finance_table"});
    foreach my $table (@finance_table) {
        $self->getRecord2FileById($self->{"from_host"}->{"autopay"},
                                  $self->{"from_port"}->{"autopay"},
                                  $self->{"from_user"}->{"autopay"},
                                  $self->{"from_pwd"}->{"autopay"},
                                  $self->{"from_db"}->{"autopay_new"},
                                  "set names utf8;select * from $table where deal_id in (\%s)",
                                  "../data/deal_id",
                                  "../data/$table",
                                  $self->{"group_size"}
                                 );
    }
    my @ddbs_table = split(/,/,$self->{"ddbs_table"});
    foreach my $table (@ddbs_table) {
        if($table =~ m/detail_info/) {
            $id_field = "deal_id";
        }
        elsif($table =~ m/settlement_detail/) {
            $id_field = "queue_id";
        }
        $self->getRecord2FileById($self->{"from_host"}->{"detail"},
                                  $self->{"from_port"}->{"detail"},
                                  $self->{"from_user"}->{"detail"},
                                  $self->{"from_pwd"}->{"detail"},
                                  $self->{"from_db"}->{"detail"},
                                  "set names utf8;select * from $table where $id_field in (\%s)",
                                  "../data/$id_field",
                                  "../data/$table",
                                  $self->{"group_size"}
                                 );
    }
 
    # 2. clear test environment
    if("1" == $self->{"is_delete"}) { ## truncate tables!!
        my @auto_pay_table = split(/,/,$self->{"auto_pay_table"});
        foreach (@auto_pay_table) {
            $self->clearTestTables($self->{"to_host"}->{"autopay"},
                                   $self->{"to_port"}->{"autopay"},
                                   $self->{"to_user"}->{"autopay"},
                                   $self->{"to_pwd"}->{"autopay"},
                                   $self->{"to_db"}->{"autopay_old"},
                                   $_
                                  );
        }
        my @finance_table = split(/,/,$self->{"finance_table"});
        foreach (@finance_table) {
            $self->clearTestTables($self->{"to_host"}->{"autopay"},
                                   $self->{"to_port"}->{"autopay"},
                                   $self->{"to_user"}->{"autopay"},
                                   $self->{"to_pwd"}->{"autopay"},
                                   $self->{"to_db"}->{"autopay_new"},
                                   $_
                                  );
        }
        my @ddbs_table = split(/,/,$self->{"ddbs_table"});
        foreach (@ddbs_table) {
            $self->clearTestTables($self->{"to_host"}->{"detail"},
                                   $self->{"to_port"}->{"detail"},
                                   $self->{"to_user"}->{"detail"},
                                   $self->{"to_pwd"}->{"detail"},
                                   $self->{"to_db"}->{"detail"},
                                   $_
                                  );
        }
    } 
    elsif("0" == $self->{"is_delete"}) { ## delete tables by ids
        my @auto_pay_table = split(/,/,$self->{"auto_pay_table"});
        my $id_filed;
        foreach my $table (@auto_pay_table) {
            if($table =~ m/contract_queue/) {
                $id_field = "id";
            } else {
                $id_field = "queue_id";
            }
            if($table =~ m/settlement_rule_ext/) {
                die "data/rule_id file is lost" if(! -e "../data/rule_id");
                $self->deleteTestTablesById($self->{"to_host"}->{"autopay"},
                                          $self->{"to_port"}->{"autopay"},
                                          $self->{"to_user"}->{"autopay"},
                                          $self->{"to_pwd"}->{"autopay"},
                                          $self->{"to_db"}->{"autopay_old"},
                                          "delete from $table where rule_id in (\%s)",
                                          "../data/rule_id",
                                          $self->{"group_size"}
                                         );
                next;
            } elsif ($table =~ m/payment_task_rule/ || $table =~ m/payment_task_belong/) {
                die "data/task_id file is lost" if(! -e "../data/task_id");
                $self->deleteTestTablesById($self->{"to_host"}->{"autopay"},
                                          $self->{"to_port"}->{"autopay"},
                                          $self->{"to_user"}->{"autopay"},
                                          $self->{"to_pwd"}->{"autopay"},
                                          $self->{"to_db"}->{"autopay_old"},
                                          "delete from $table where task_id in (\%s)",
                                          "../data/task_id",
                                          $self->{"group_size"}
                                         );
                next;
            }
            $self->deleteTestTablesById($self->{"to_host"}->{"autopay"},
                                      $self->{"to_port"}->{"autopay"},
                                      $self->{"to_user"}->{"autopay"},
                                      $self->{"to_pwd"}->{"autopay"},
                                      $self->{"to_db"}->{"autopay_old"},
                                      "delete from $table where $id_field in (\%s)",
                                      "../data/queue_id",
                                      $self->{"group_size"}
                                     );
        }
        my @finance_table = split(/,/,$self->{"finance_table"});
        foreach my $table (@finance_table) {
            $self->deleteTestTablesById($self->{"to_host"}->{"autopay"},
                                      $self->{"to_port"}->{"autopay"},
                                      $self->{"to_user"}->{"autopay"},
                                      $self->{"to_pwd"}->{"autopay"},
                                      $self->{"to_db"}->{"autopay_new"},
                                      "delete from $table where deal_id in (\%s)",
                                      "../data/deal_id",
                                      $self->{"group_size"}
                                     );
        }
        my @ddbs_table = split(/,/,$self->{"ddbs_table"});
        foreach my $table (@ddbs_table) {
            if($table =~ m/detail_info/) {
                $id_field = "deal_id";
            }
            elsif($table =~ m/settlement_detail/) {
                $id_field = "queue_id";
            }
            $self->deleteTestTablesById($self->{"to_host"}->{"detail"},
                                      $self->{"to_port"}->{"detail"},
                                      $self->{"to_user"}->{"detail"},
                                      $self->{"to_pwd"}->{"detail"},
                                      $self->{"to_db"}->{"detail"},
                                      "delete from $table where $id_field in (\%s)",
                                      "../data/$id_field",
                                      $self->{"group_size"}
                                     );
        }
    }
    
    # 3. import data to test environment
    my @auto_pay_table = split(/,/,$self->{"auto_pay_table"});
    foreach my $table (@auto_pay_table) { 
        my $columns = $self->getTableFieldName($self->{"from_host"}->{"autopay"},
                                               $self->{"from_port"}->{"autopay"},
                                               $self->{"from_user"}->{"autopay"},
                                               $self->{"from_pwd"}->{"autopay"},
                                               $self->{"from_db"}->{"autopay_old"},
                                               $table
                                              );
        $self->import2DestDB($self->{"to_host"}->{"autopay"},
                             $self->{"to_port"}->{"autopay"},
                             $self->{"to_user"}->{"autopay"},
                             $self->{"to_pwd"}->{"autopay"},
                             $self->{"to_db"}->{"autopay_old"},
                             $table,
                             $columns,
                             "../data/$table"
                            );

    }
    my @finance_table = split(/,/,$self->{"finance_table"});
    foreach my $table (@finance_table) {
        my $columns = $self->getTableFieldName($self->{"from_host"}->{"autopay"},
                                               $self->{"from_port"}->{"autopay"},
                                               $self->{"from_user"}->{"autopay"},
                                               $self->{"from_pwd"}->{"autopay"},
                                               $self->{"from_db"}->{"autopay_new"},
                                               $table
                                              );
        $self->import2DestDB($self->{"to_host"}->{"autopay"},
                             $self->{"to_port"}->{"autopay"},
                             $self->{"to_user"}->{"autopay"},
                             $self->{"to_pwd"}->{"autopay"},
                             $self->{"to_db"}->{"autopay_new"},
                             $table,
                             $columns,
                             "../data/$table"
                            );

    }
    my @ddbs_table = split(/,/,$self->{"ddbs_table"});
    foreach my $table (@ddbs_table) {
        my $columns = $self->getTableFieldName($self->{"from_host"}->{"detail"},
                                               $self->{"from_port"}->{"detail"},
                                               $self->{"from_user"}->{"detail"},
                                               $self->{"from_pwd"}->{"detail"},
                                               $self->{"from_db"}->{"detail"},
                                               $table
                                              );
        $self->import2DestDBddbs($self->{"to_host"}->{"detail"},
                             $self->{"to_port"}->{"detail"},
                             $self->{"to_user"}->{"detail"},
                             $self->{"to_pwd"}->{"detail"},
                             $self->{"to_db"}->{"detail"},
                             $table,
                             $columns,
                             "../data/$table"
                            );

    }
}

1;
