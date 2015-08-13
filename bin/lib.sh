#!/bin/bash

############################
##! @Author:Mu Lin
##! @Date:2014-02-01
##! @TODO:public functions
############################
FUNC_SUCC=0
FUNC_ERROR=1
LOOP_TEST_COUNT=3
function getTime()
{
    echo "$(date '+%Y-%m-%d %H:%M:%S')"
}

#input: msg or msg outputfile
function printMsg()
{
    if [[ $# -eq 1 ]]
    then
        echo "$1"
    elif [[ $# -eq 2 ]]
    then
        echo "$1" >> "$2"
    fi
}

function loginfo()
{
    echo "`getTime` [`caller 0 | awk -F' ' '{print $1,$2}'`] $1" >> "../logs/finance_autotest_sql.log"
}

function failExit()
{
    loginfo "Error: $1, exited, please check problem"
    exit $FUNC_ERROR
}

##! @TODO: extract record from DB to a global array variable
##! @IN: $1 => MYSQL_HOST
##! @IN: $2 => MYSQL_PORT
##! @IN: $3 => MYSQL_USERNAME
##! @IN: $4 => MYSQL_PWD
##! @IN: $5 => db name
##! @IN: $6 => sql
##! @OUT: $FUNC_SUCC => success; $FUNC_ERROR => failure
function execMySql2Array()
{
    if [[ $# -ne 6 ]]
    then
        loginfo "need params"
        failExit "execMySql2Array invalid params [$*]"
    fi

    local ret
    unset ret_array
    ret_array=(`mysql -h$1 -P$2 -u$3 -p$4 $5 -N -e "$6"`)

    ret=$?
    if [[ $ret -eq $FUNC_SUCC ]]
    then
        loginfo "apply sql $6 on db $5 successfully"
    else
        loginfo "apply sql $6 on db $5 failed"
    fi
    
    return $ret
}

##! @TODO: extract record from DB to a file
##! @IN: $1 => MYSQL_HOST
##! @IN: $2 => MYSQL_PORT
##! @IN: $3 => MYSQL_USERNAME
##! @IN: $4 => MYSQL_PWD
##! @IN: $5 => db name
##! @IN: $6 => sql
##! @IN: $7 => output file
##! @IN: $8 => if append
##! @OUT: $FUNC_SUCC => success; $FUNC_ERROR => failure
function execMySql2File()
{
    if [[ $# -ne 8 ]]
    then
        loginfo "need params"
        loginfo "params number is $#"
        failExit "execMySql2File invalid params [$*]"
    fi

    local ret
    if [[ "x$8" == "xappendTrue" ]]
    then
        mysql -h$1 -P$2 -u$3 -p$4 $5 -N -e "$6" >> $7
    elif [[ "x$8" == "xappendFalse" ]]
    then
        mysql -h$1 -P$2 -u$3 -p$4 $5 -N -e "$6" > $7
    else
        loginfo "do not support append flag $8, please check"
        failExit "wrong append flag $8"
    fi

    ret=$?
    if [[ $ret -eq $FUNC_SUCC ]]
    then
        loginfo "apply sql $6 on db $5 successfully"
    else
        loginfo "apply sql $6 on db $5 failed"
    fi

    return $ret
}

##! @TODO: batch get record from database to a file
##! @IN: $1 => inputfile
##! @IN: $2 => outputfile
##! @IN: $3 => groupsize
##! @IN: $4 => MYSQL_HOST
##! @IN: $5 => MYSQL_PORT
##! @IN: $6 => MYSQL_USERNAME
##! @IN: $7 => MYSQL_PWD
##! @IN: $8 => db name
##! @IN: $9 => sql
##! @IN: $10 => replace index
function getRecord2File()
{
    if [[ $# -ne 10 ]]
    then
        loginfo "need params"
        failExit "getRecord2File invalid params [$*]"
    fi
    
    local input_file=$1
    local output_file=$2
    local record_num=`wc -l ${input_file} | awk -F' ' '{print $1}'`
    local group_size=$3
    local sub_size=$(((${record_num}+${group_size}-1)/${group_size}))
    local begin=1
    local end=$((${begin}+${group_size}-1))
    local db_host=$4
    local db_port=$5
    local db_username=$6
    local db_pwd=$7
    local db_name=$8
    local sql=$9
    local index=${10}
    for((i=1;i<=${sub_size};i++))
    do
        sub_list=`sed -n "${begin},${end}p" $input_file | tr -t '\n' ',' | sed -e 's/,$//g'`
        sql=${sql//$index/${sub_list}}
        local counter=1
        for((counter=1;counter<=${LOOP_TEST_COUNT};counter++))
        do
            loginfo "start exec execMySql2File $counter times"
            execMySql2File "${db_host}" "${db_port}" "${db_username}" "${db_pwd}" "${db_name}" "${sql}" "${output_file}" "appendTrue"
            if [[ $? -eq $FUNC_SUCC ]]
            then
                break
            fi
            if [[ $counter -lt ${LOOP_TEST_COUNT} ]]
            then
                loginfo "loop invocation, sleep 2s"
                sleep 2
            else
                failExit "exec execMySql2File failed"
            fi
        done
        sleep 0.1
        begin=$(($begin+${group_size}))
        end=$(($end+${group_size}))
        index=${sub_list}
    done
}

##! @TODO: import data to database(load file)
##! @AUTHOR: Mu Lin
##! @IN: $1 => data file
##! @IN: $2 => table name
##! @IN: $3 => character set
##! @IN: $4 => field separator
##! @IN: $5 => table columns
##! @IN: $6 => MYSQL_HOST
##! @IN: $7 => MYSQL_PORT
##! @IN: $8 => MYSQL_USERNAME
##! @IN: $9 => MYSQL_PWD
##! @IN: $10 => MYSQL_DATABASE
##! @IN: $11 => log file
##! @OUT: $FUNC_SUCC => success; $FUNC_ERROR => failure
function import2DestDB()
{
    if [[ $# -ne 11 ]]
    then
        loginfo "need params"
        failExit "import2DestDB invalid params:[$*]"
    fi

    loginfo "start load data to DB, params length:$#, params:[$*]"
    loginfo "exec sql[$2]"
    local ret=`mysql -h$6 -P$7 -u$8 -p$9 ${10} << EOFMYSQL 2>&1 | tee -a ${11}
            load data LOCAL infile "$1" into table $2 CHARACTER SET $3 fields terminated by '$4' ($5)
EOFMYSQL`
    loginfo "load data ret=$ret"

    [[ "x" == "x$ret" ]] && loginfo "import file $1 success" && return $FUNC_SUCC || loginfo "import file $1 failed" && return $FUNC_ERROR
}

##! @TODO: truncate table
##! @AUTHOR: Mu Lin
##! @IN: $1 => MYSQL_HOST
##! @IN: $2 => MYSQL_PORT
##! @IN: $3 => MYSQL_USERNAME
##! @IN: $4 => MYSQL_PWD
##! @IN: $5 => MYSQL_DATABASE
##! @IN: $6 => table name
##! @OUT: $FUNC_SUCC => success; $FUNC_ERROR => failure
function clearTable()
{
   if [[ $# -ne 6 ]]
   then
       loginfo "need params"
       failExit "clearTable invalid params:[$*]"
   fi

   loginfo "clear table $5.$6"
   local ret=`mysql -h$1 -P$2 -u$3 -p$4 $5 -e "truncate table $6;"`

   [[ "x" == "x$ret" ]] && loginfo "clear table $5.$6 success" && return $FUNC_SUCC || loginfo "clear table $5.$6 failed" && return $FUNC_ERROR
}

##! @TODO: delete table by ids
##! @AUTHOR: Mu Lin
##! @IN: $1 => MYSQL_HOST
##! @IN: $2 => MYSQL_PORT
##! @IN: $3 => MYSQL_USERNAME
##! @IN: $4 => MYSQL_PWD
##! @IN: $5 => MYSQL_DATABASE
##! @IN: $6 => sql cmd
##! @OUT: $FUNC_SUCC => success; $FUNC_ERROR => failure
function deleteTableById() 
{
    if [[  $# -ne 6 ]]
    then
        loginfo "need params"
        failExit "deleteTableById invalid params:[$*]"
    fi

    loginfo "delete table by ids"
    local ret=`mysql -h$1 -P$2 -u$3 -p$4 $5 -e "$6"`
    
    [[ "x" == "x$ret" ]] && loginfo "delete table success" && return $FUNC_SUCC || loginfo "delete table failed" && return $FUNC_ERROR
}

##! @TODO: execute delete or insert sql
##! @AUTHOR: Mu Lin
##! @IN: $1 => MYSQL_HOST
##! @IN: $2 => MYSQL_PORT
##! @IN: $3 => MYSQL_USERNAME
##! @IN: $4 => MYSQL_PWD
##! @IN: $5 => MYSQL_DATABASE
##! @IN: $6 => sql
##! @OUT: $FUNC_SUCC => success; $FUNC_ERROR => failure
function execDeleteOrInsertMySql()
{
   if [[ $# -ne 6 ]]
   then
       loginfo "need params"
       failExit "execDeleteOrInsertMySql invalid params:[$*]"
   fi
   loginfo "apply SQL:$6 to $5"
   local ret=`mysql -h$1 -P$2 -u$3 -p$4 $5 -e "$6"`

   [[ "x" == "x$ret" ]] && loginfo "apply SQL: $6 to $5 success" && return $FUNC_SUCC || loginfo "apply SQL $6 to $5 failed" && return $FUNC_ERROR
}


#input: inputfile outputfile
function formatFile()
{
    if [[ $# -ne 2 ]]
    then
        loginfo "need params"
        failExit "formatFile invalid params [$*]"
    fi

    sed -e 's/ /\n/g' $1 > $2
    rm -rf $1   
}

function sendEmail()
{
    IFS=";"
    local from="nuomitongzhi@baidu.com"
    local to="linmu@baidu.com"
    local subject="$2"
    local body=`cat $1`
    local content_type="Content-type:text/plain;charset=gb2312"
    local mail_content="to:${to}\nfrom:${from}\nsubject:${subject}\n${content_type}\n${body}"
    echo  -e ${mail_content} | /usr/sbin/sendmail -t
}

case $1 in
    'execDeleteOrInsertMySql')
        execDeleteOrInsertMySql "$2" "$3" "$4" "$5" "$6" "$7"
     ;;
    'clearTable')
        clearTable "$2" "$3" "$4" "$5" "$6" "$7"
     ;;
    'import2DestDB')
        import2DestDB "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$9" "${10}" "${11}" "${12}"
     ;;
    'execMySql2File')
        execMySql2File "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$9"
     ;;
     *)
        exit $FUNC_ERROR
     ;;
esac
