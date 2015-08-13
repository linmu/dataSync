#!/bin/usr/perl

use strict;

package Extractor::extractorBase;

sub new {
    my $invocant = shift;
    my $class = ref($invocant) || $invocant;
    my $self = {};
    bless($self,$class);
    return $self;
}

sub loopExecMySql {
    my ($self,$from_host,$from_port,$from_user,$from_pwd,$from_db,$sql_cmd,$output_file,$append_flag) = @_;

    my @counter = (1..3);
    foreach (@counter) {
        system("./lib.sh","execMySql2File",$from_host,$from_port,$from_user,$from_pwd,$from_db,$sql_cmd,$output_file,$append_flag);
        last if(0 == $?);
        if($_ < scalar @counter) {
            sleep(5);
        } else {
            die "Cannot connect to mysql DB!";
        }
    }
}

sub getRecord2FileById {
    my ($self,$from_host,$from_port,$from_user,$from_pwd,$from_db,$sql_cmd,$input_file,$output_file,$group_size) = @_;
    
    die "group size is zero!" if(0 == $group_size);
    my $line_count;
    my $id_str;
    my $sql;
    open INPUT_FILE,"<$input_file";
    while(<INPUT_FILE>) {
        chomp;
        if(0 != $line_count && 0 == $line_count % $group_size) {
            $id_str =~ s/,$//;
            $sql = sprintf($sql_cmd,$id_str);
            $self->loopExecMySql($from_host,$from_port,$from_user,$from_pwd,$from_db,$sql,$output_file,"appendTrue");
            $line_count = 0;
            $id_str = "";
        } 
        $id_str .= ($_.",");
        $line_count++;
    }
    if(defined $id_str) {
        $id_str =~ s/,$//;
        $sql = sprintf($sql_cmd,$id_str);
        $self->loopExecMySql($from_host,$from_port,$from_user,$from_pwd,$from_db,$sql,$output_file,"appendTrue");
    }
}

sub clearTestTables {
    my ($self,$to_host,$to_port,$to_user,$to_pwd,$to_db,$to_tb) = @_;
    
    system("./lib.sh","clearTable",$to_host,$to_port,$to_user,$to_pwd,$to_db,$to_tb);
    if(0 != $?) {
        die "clear table $to_db".".$to_tb"."failed!";
    }
}

sub deleteTestTablesById {
    my ($self,$to_host,$to_port,$to_user,$to_pwd,$to_db,$sql_cmd,$input_file,$group_size) = @_;
    
    die "group size is zero!" if(0 == $group_size);
    my $line_count;
    my $id_str;
    my $sql;
    open INPUT_FILE,"<$input_file";
    while(<INPUT_FILE>) {
        chomp;
        if(0 != $line_count && 0 == $line_count % $group_size) {
            $id_str =~ s/,$//;
            $sql = sprintf($sql_cmd,$id_str);
            system("./lib.sh","execDeleteOrInsertMySql",$to_host,$to_port,$to_user,$to_pwd,$to_db,$sql);
            die "delete cmd $sql failed" if(0 != $?);
            $line_count = 0;
            $id_str = "";
        }
        $id_str .= ($_.",");
        $line_count++;
    }
    if(defined $id_str){
        $id_str =~ s/,$//;
        $sql = sprintf($sql_cmd,$id_str);
        system("./lib.sh","execDeleteOrInsertMySql",$to_host,$to_port,$to_user,$to_pwd,$to_db,$sql);
        die "delete cmd $sql falied" if(0 != $?);
    }
}

sub getTableFieldName {
    my ($self,$from_host,$from_port,$from_user,$from_pwd,$from_db,$from_tb) = @_;
    
    system("./lib.sh","execMySql2File",$from_host,$from_port,$from_user,$from_pwd,$from_db,"desc $from_tb","../data/$from_tb.desc","appendFalse");
    if(0 != $?) {
        die "Get fields name from table $from_tb failed!"
    } 
    
    open INPUT_FILE,"<../data/$from_tb.desc" or die "Cannot open file data/$from_tb.desc";
    my $ret_str;
    while(<INPUT_FILE>) {
        chomp;
        $ret_str .= ((split)[0].",");
    }
    close INPUT_FILE;
    $ret_str =~ s/,$//;
    $ret_str;
}

sub import2DestDB {
    my ($self,$to_host,$to_port,$to_user,$to_pwd,$to_db,$to_tb,$columns,$data_file) = @_;
    
    system("./lib.sh","import2DestDB",$data_file,$to_tb,"utf8","\\t",$columns,$to_host,$to_port,$to_user,$to_pwd,$to_db,"../logs/finance_autotest_sql.log");     
    if(0 != $?) {
        die "Cannot load data into table: $to_db.$to_tb";
    }
}

sub import2DestDBddbs {
    my ($self,$to_host,$to_port,$to_user,$to_pwd,$to_db,$to_tb,$columns,$data_file) = @_;
    
    my $line_count;
    my $sql;
    open INPUT_FILE,"<$data_file" or die "Cannot open file $data_file";
    open OUTPUT_FILE,">>$data_file.ddbs" or die "Cannot open file $data_file.ddbs";
    while(<INPUT_FILE>) {
        chomp;
        s/\t/","/g;
        s/^/"/g;
        s/$/"/g;
        $sql = "replace into ".$to_tb." (".$columns.") "."values"." (".$_.");\n";
        print OUTPUT_FILE $sql;
    }
    close INPUT_FILE;
    close OUTPUT_FILE;

    if(-e "$data_file.ddbs") {
        system("cat $data_file.ddbs | mysql -h$to_host -P$to_port -u$to_user -p$to_pwd $to_db");
        die "insert data failed" if(0 != $?);
    }
}

1;
