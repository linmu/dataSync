#!/bin/bash

########################
##! @Author:Mu Lin
##! @Date:2015-02-01
##! @TODO:finance autotest
########################
PROGRAM=$(basename $0)
VERSION="1.0"
CURDATE=$(date "+%Y%m%d")

cd $(dirname $0)
BIN_DIR=$(pwd)
DEPLOY_DIR=${BIN_DIR%/*}

CONF_DIR=$(cd $DEPLOY_DIR/conf && pwd)
CONF_FILE_NAME=
ID_LIST_FILE=
TODAY=$CURDATE

IF_EXTRACT_ONLINE_DATA_RANDOMLY="false"
IF_EXTRACT_ONLINE_DATA_FROM_FILE="false"

function usage()
{
    echo "$PROGRAM usage: [-h] [-v] [-c 'config file name'] [-d 'YYYYMMDD'(default today)]"
}

function usage_and_exit()
{
    usage
    exit $1
}

function version()
{
    echo "$PROGRAM version $VERSION"
}

######### handle input parameters ##########
if [[ $# -lt 1 ]];then
    usage_and_exit 1
fi

while getopts :c:d:f:phv opt
do
    case $opt in
    c) CONF_FILE_NAME=$OPTARG
       ;;
    d) TODAY=$OPTARG
       ;;
    f) ID_LIST_FILE=$OPTARG
       IF_EXTRACT_ONLINE_DATA_FROM_FILE="true"
       ;;
    p) IF_EXTRACT_ONLINE_DATA_RANDOMLY="true"
       ;;
    v) version
       exit 0
       ;;
    h) usage_and_exit 0
       ;;
    ':') echo "$PROGRAM -$OPTARG requires an argument" >&2
       usage_and_exit 1
       ;;
    '?') echo "$PROGRAM: invalid option $OPTARG" >&2
       usage_and_exit 1
       ;;
    esac
done
shift $(($OPTIND-1))

###### load configures ######
source ${CONF_DIR}/${CONF_FILE_NAME}

LOG_DIR="${DEPLOY_DIR}/${LOG_PATH}"
LOG_FILE="${LOG_DIR}/${LOG_FILE_NAME}.${CURDATE}"
DATA_DIR="${DEPLOY_DIR}/${DATA_PATH}"
QUEUEID_DEALID_FILE="${DATA_DIR}/${QUEUEID_DEALID_FILE_NAME}_${TODAY}"
DEALID_FILE="${DATA_DIR}/${DEALID_FILE_NAME}_${TODAY}"
QUEUEID_FILE="${DATA_DIR}/${QUEUEID_FILE_NAME}_${TODAY}"
RULEID_FILE="${DATA_DIR}/${RULEID_FILE_NAME}_${TODAY}"
CONTRACT_QUEUE_FILE="${DATA_DIR}/${CONTRACT_QUEUE_FILE_NAME}_${TODAY}"
CONTRACT_RELATION_FILE="${DATA_DIR}/${CONTRACT_RELATION_FILE_NAME}_${TODAY}"
SETTLEMENT_RULE_FILE="${DATA_DIR}/${SETTLEMENT_RULE_FILE_NAME}_${TODAY}"
SETTLEMENT_RULE_EXT_FILE="${DATA_DIR}/${SETTLEMENT_RULE_EXT_FILE_NAME}_${TODAY}"
STAT_DAY_OPTION_FILE="${DATA_DIR}/${STAT_DAY_OPTION_FILE_NAME}_${TODAY}"
CERT_TIANKENG_STAT_FILE="${DATA_DIR}/${CERT_TIANKENG_STAT_FILE_NAME}_${TODAY}"
PAYMENT_RECORD_FILE="${DATA_DIR}/${PAYMENT_RECORD_FILE_NAME}_${TODAY}"
PAYMENT_TASK_FILE="${DATA_DIR}/${PAYMENT_TASK_FILE_NAME}_${TODAY}"
NON_PAYMENT_TASK_FILE="${DATA_DIR}/${NON_PAYMENT_TASK_FILE_NAME}_${TODAY}"
PAYMENT_TASK_BELONG_FILE="${DATA_DIR}/${PAYMENT_TASK_BELONG_FILE_NAME}_${TODAY}"
STAT_DEAL_FILE="${DATA_DIR}/${STAT_DEAL_FILE_NAME}_${TODAY}"
STAT_CERTIFICATE_FILE="${DATA_DIR}/${STAT_CERTIFICATE_FILE_NAME}_${TODAY}"
STAT_CERTIFICATE_USE_CANCEL_FILE="${DATA_DIR}/${STAT_CERTIFICATE_USE_CANCEL_FILE_NAME}_${TODAY}"
STAT_REFUND_RECORD_FILE="${DATA_DIR}/${STAT_REFUND_RECORD_FILE_NAME}_${TODAY}"
STAT_ORDER_FILE="${DATA_DIR}/${STAT_ORDER_FILE_NAME}_${TODAY}"
SETTLE_DATE_FILE="${DATA_DIR}/${SETTLE_DATE_FILE_NAME}_${TODAY}"


if [[ ! -d ${LOG_DIR} ]];then
    mkdir -p ${LOG_DIR}
fi

if [[ ! -d ${DATA_DIR} ]];then
    mkdir -p ${DATA_DIR}
fi

############################## delete exist files start ##############################
if [[ -f ${QUEUEID_DEALID_FILE} ]];then
    rm -rf ${QUEUEID_DEALID_FILE}
fi

if [[ -f ${QUEUEID_FILE} ]];then
    rm -rf ${QUEUEID_FILE}
fi

if [[ -f $DEALID_FILE ]];then
    rm -rf ${DEALID_FILE}
fi

if [[ -f ${RULEID_FILE} ]];then
    rm -rf ${RULEID_FILE}
fi

if [[ -f ${CONTRACT_QUEUE_FILE} ]];then
    rm -rf ${CONTRACT_QUEUE_FILE}
fi

if [[ -f ${CONTRACT_RELATION_FILE} ]];then
    rm -rf ${CONTRACT_RELATION_FILE}
fi

if [[ -f ${SETTLEMENT_RULE_FILE} ]];then
    rm -rf ${SETTLEMENT_RULE_FILE}
fi

if [[ -f ${SETTLEMENT_RULE_EXT_FILE} ]];then
    rm -rf ${SETTLEMENT_RULE_EXT_FILE}
fi

if [[ -f ${STAT_DAY_OPTION_FILE} ]];then
    rm -rf ${STAT_DAY_OPTION_FILE}
fi

if [[ -f ${CERT_TIANKENG_STAT_FILE} ]];then
    rm -rf ${CERT_TIANKENG_STAT_FILE}
fi

if [[ -f ${PAYMENT_RECORD_FILE} ]];then
    rm -rf ${PAYMENT_RECORD_FILE}
fi

if [[ -f ${PAYMENT_TASK_FILE} ]];then
    rm -rf ${PAYMENT_TASK_FILE}
fi

if [[ -f ${NON_PAYMENT_TASK_FILE} ]];then
    rm -rf ${NON_PAYMENT_TASK_FILE}
fi

if [[ -f ${PAYMENT_TASK_BELONG_FILE} ]];then
    rm -rf ${PAYMENT_TASK_BELONG_FILE}
fi

if [[ -f ${STAT_DEAL_FILE} ]];then
    rm -rf ${STAT_DEAL_FILE}
fi

if [[ -f ${STAT_CERTIFICATE_FILE} ]];then
    rm -rf ${STAT_CERTIFICATE_FILE}
fi

if [[ -f ${STAT_CERTIFICATE_USE_CANCEL_FILE} ]];then
    rm -rf ${STAT_CERTIFICATE_USE_CANCEL_FILE}
fi

if [[ -f ${STAT_REFUND_RECORD_FILE} ]];then
    rm -rf ${STAT_REFUND_RECORD_FILE}
fi

if [[ -f ${STAT_ORDER_FILE} ]];then
    rm -rf ${STAT_ORDER_FILE}
fi

if [[ -f ${SETTLE_DATE_FILE} ]];then
    rm -rf ${SETTLE_DATE_FILE}
fi
################## load public funnction #####################
source ${BIN_DIR}/lib.sh

##! @TODO: initialize tables
##! @AUTHOR: Mu Lin
function initialize()
{
    #1. update stat_*
    execUpdateorInsertMySql "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${AUTOPAY_TEST_NEW}" "${INI_STAT_DEAL_SQL}"
    execUpdateorInsertMySql "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${AUTOPAY_TEST_NEW}" "${INI_STAT_CERTIFICATE_SQL}"
    execUpdateorInsertMySql "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${AUTOPAY_TEST_NEW}" "${INI_STAT_CERTIFICATE_USE_CANCEL_SQL}"
    execUpdateorInsertMySql "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${AUTOPAY_TEST_NEW}" "${INI_STAT_REFUND_RECORD_SQL}"
    execUpdateorInsertMySql "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${AUTOPAY_TEST_NEW}" "${INI_STAT_ORDER_SQL}"

    #2. initialize settle_date
    execMySql2File "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" \
                   "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${AUTOPAY_TEST_OLD}" \
                   "${INI_SETTLE_DATE_SQL}" "${SETTLE_DATE_FILE}" "appendFalse"
    clearTable "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${FINANCE_AUTO}" "settle_date"
    local mysql_table="settle_date"
    local mysql_table_columns="deal_id,queue_id,settle_start_date,settle_current_date,settle_end_date,week_num"
    local mysql_read_columns_charset="utf8"
    local mysql_file_columns_sep="\\t"
    import2DestDB "${SETTLE_DATE_FILE}" \
                  "${mysql_table}" "${mysql_read_columns_charset}" "${mysql_file_columns_sep}" "${mysql_table_columns}" \
                  "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" \
                  "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${FINANCE_AUTO}" \
                  "${LOG_FILE}"

    #3. update contract_queue
    execUpdateorInsertMySql "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" \
                            "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${AUTOPAY_TEST_OLD}" \
                            "${INI_CONTRACT_QUEUE_SQL}"
   
    #4. clear payment_task,payment_task_rule,payment_taks_belong
    clearTable "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${AUTOPAY_TEST_OLD}" "payment_task"
    clearTable "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${AUTOPAY_TEST_OLD}" "payment_task_rule"
    clearTable "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${AUTOPAY_TEST_OLD}" "payment_task_belong"
    
    #5. update settlement_detail and detail_info
    execUpdateorInsertMySql "${DB_DETAIL_TEST_HOST}" "${DB_DETAIL_TEST_PORT}" "${DB_DETAIL_TEST_USER}" "${DB_DETAIL_TEST_PWD}" "${DETAIL}" "${INI_SETTLEMENT_DETAIL_SQL}"
    execUpdateorInsertMySql "${DB_DETAIL_TEST_HOST}" "${DB_DETAIL_TEST_PORT}" "${DB_DETAIL_TEST_USER}" "${DB_DETAIL_TEST_PWD}" "${DETAIL}" "${INI_DETAIL_INFO_SQL}"
}

##! @TODO: execute fss-* job (fss-detail or fss-task)
##! @AUTHOR: Mu Lin
##! @IN: $1 => module deploy path
##! @IN: $2 => script to be executed
##! @IN: $3 => script execute date
##! @OUT: $FUNC_SUCC => success;$FUNC_ERROR => failure
function execFssModule()
{
    if [[ $# -lt 2 ]];then
        loginfo "need params"
        failExit "execFssModule invalid params:[$*]"
    fi
    
    local module_name=${1##*/}
    loginfo "invoke module ${module_name}, deploy path: $1"
    if [[ $# -eq 2 ]];then 
        sh "$1"/bin/"$2"  > /dev/null 2>&1
    elif [[ $# -eq 3 ]];then
        sh "$1"/bin/"$2" "$3" > /dev/null 2>&1
    fi
    #local PID=$(pgrep -f "${1##*/}")
    wait $!
    if [[ $? -eq 0 ]];then
        return $FUNC_SUCC
    else
        return $FUNC_ERROR
    fi
}

##! @TODO: verify payment task
##! @AUTHOR: Mu Lin
##! @IN: $1 => current settle date YYYYmmdd
##! @OUT: $FUNC_SUCC => success;$FUNC_ERROR => failure
function verifyPaymentTask()
{
    if [[ $# -ne 1 ]];then
        loginfo "need params"
        failExit "verifyPaymentTask invalid params:[$*]"
    fi
    
    execMySql2Array "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" \
                    "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${FINANCE_AUTO}" \
                    "select queue_id from settle_date where settle_current_date=$1"
    local queue_ids=
    declare -a queue_ids=(${ret_array[@]})
    local queue_ids_size=${#queue_ids[@]}
    local queue_id=
    local settle_current_date=
    local settle_prev_date=
    local need_pay_money_expected=
    local need_pay_money_real=
    if [[ ${queue_ids_size} -ne 0 ]];then
        for((i=0;i<${queue_ids_size};i++))
        do
            queue_id=${queue_ids[$i]}
            execMySql2Array "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" \
                            "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${AUTOPAY_TEST_OLD}" \
                            "select need_pay_money from payment_task where queue_id=${queue_id} and task_timestamp=$1"
            if [[ ${#ret_array[@]} -eq 0 ]];then
                need_pay_money_real=0
            elif [[ ${#ret_array[@]} -eq 1 ]];then
                need_pay_money_real=${ret_array[0]}
            else
                loginfo "error: more than one payment_task selected!"
                failExit "more than one payment_task selected!"
            fi

            execMySql2Array "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" \
                            "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${FINANCE_AUTO}" \
                            "select settle_current_date,settle_prev_date from settle_date where queue_id=${queue_id}"
            settle_current_date=${ret_array[0]}
            settle_prev_date=${ret_array[1]}
            
            execMySql2Array "${DB_DETAIL_TEST_HOST}" "${DB_DETAIL_TEST_PORT}" \
                            "${DB_DETAIL_TEST_USER}" "${DB_DETAIL_TEST_PWD}" "${DETAIL}" \
                            "select ifnull(sum(is_income*settlement_money),0) from settlement_detail where queue_id=${queue_id} and detail_type in (2,3) and DATE_FORMAT(opt_time,'%Y%m%d')<=${settle_current_date} and DATE_FORMAT(opt_time,'%Y%m%d')>${settle_prev_date}"
            need_pay_money_expected=${ret_array[0]}
            
            printMsg "current settle date:${settle_date},queue_id:${queue_id},settlement_detail:${need_pay_money_expected},payment_task:${need_pay_money_real}"

            if [[ ${need_pay_money_expected} -ne ${need_pay_money_real} ]];then
                loginfo "need_pay_money error! queue_id:${queue_id},settlement_detail:${need_pay_money_expected},payment_task:${need_pay_money_real}"
                failExit "need_pay_money error!"
            else
                loginfo "verify need_pay_money success! queue_id:${queue_id},settlement_detail:${need_pay_money_expected},payment_task:${need_pay_money_real}"
            fi
        done
    fi
    return ${FUNC_SUCC}
}

##! @TODO: verify next_settle_date and update table settle_date
##! @AUTHOR: Mu Lin
##! @IN: $1 => current settle date YYYYmmdd
##! @OUT: $FUNC_SUCC => success;$FUNC_ERROR => failure
function verifyNextSettleDate()
{
    if [[ $# -ne 1 ]];then
        loginfo "need params"
        failExit "verifyNextSettleDate invalid params:[$*]"
    fi

    execMySql2Array "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" \
                    "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${FINANCE_AUTO}" \
                    "select queue_id from settle_date where settle_current_date=$1"
    local queue_ids=
    declare -a queue_ids=(${ret_array[@]})
    local queue_ids_size=${#queue_ids[@]}
    local queue_id=
    local contract_queue_status=
    local settle_current_date= 
    local settle_start_date=
    local settle_end_date=
    local week_num=
    local next_settle_date_expected=
    local next_settle_date_real=
    if [[ ${queue_ids_size} -ne 0 ]];then
        for((i=0;i<${queue_ids_size};i++))
        do
            queue_id=${queue_ids[$i]}
            execMySql2Array "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" \
                            "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${FINANCE_AUTO}" \
                            "select settle_current_date,settle_start_date,settle_end_date,week_num from settle_date where queue_id=${queue_id}"
            settle_current_date=${ret_array[0]}
            settle_start_date=${ret_array[1]}
            settle_end_date=${ret_array[2]}
            week_num=${ret_array[3]}

            execMySql2Array "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" \
                            "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${AUTOPAY_TEST_OLD}" \
                            "select next_settle_date,status from contract_queue where id=${queue_id}"
            next_settle_date_real=${ret_array[0]}
            contract_queue_status=${ret_array[1]}
            
            if [[ ${contract_queue_status} -eq 0 ]];then
                next_settle_date_expected=$(date -d "${settle_current_date} 1 days" +"%Y%m%d")
            elif [[ ${contract_queue_status} -eq 1 || ${contract_queue_status} -eq 2 ]];then
                next_settle_date_expected=$(date -d "${settle_current_date} $((${week_num}*7)) days" +"%Y%m%d")
            elif [[ ${contract_queue_status} -eq 3 ]];then
                continue
            fi
            
            if [[ ${next_settle_date_expected} -ge $(date -d "2 day ago ${settle_end_date}" +"%Y%m%d") ]];then
                next_settle_date_expected=${settle_end_date}
            fi

            printMsg "current settle date:${settle_date},queue_id:${queue_id},next settle date expected:${next_settle_date_expected},next settle date:${next_settle_date_real}"
            if [[ ${next_settle_date_expected} -ne ${next_settle_date_real} ]];then
                if [[ ${contract_queue_status} -eq 1 ]];then
                    local date_gap=$((($(date -d "${settle_current_date}" +%s)-$(date -d "${settle_start_date}" +%s))/(60*60*24)))
                    next_settle_date_expected=$(date -d "${settle_current_date} $((${week_num}*7-${date_gap}%(${week_num}*7))) days" +"%Y%m%d")
                fi 
                printMsg "current settle date:${settle_date},queue_id:${queue_id},next settle date expected:${next_settle_date_expected},next settle date:${next_settle_date_real}"
                if [[ ${next_settle_date_expected} -ne ${next_settle_date_real} ]];then
                    loginfo "next_settle_date error! queue_id:${queue_id},settle_date:${next_settle_date_expected},contract_queue:${next_settle_date_real}"
                    failExit "next_settle_date error!"
                else
                    loginfo "verify next_settle_date success! queue_id:${queue_id},settle_date:${next_settle_date_expected},contract_queue:${next_settle_date_real}"
                fi
            else
                loginfo "verify next_settle_date success! queue_id:${queue_id}"
            fi

            execUpdateorInsertMySql "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" \
                                    "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${FINANCE_AUTO}" \
                                    "update settle_date set settle_current_date=${next_settle_date_real},settle_prev_date=$1 where queue_id=${queue_id}"

            if [[ $? -eq $FUNC_SUCC ]];then
                loginfo "update settle_date success!; queue_id:${queue_id}"
            else 
                loginfo "update settle_date failed!; queue_id:${queue_id}"
                failExit "update settle_date failed!; queue_id:${queue_id}"
            fi
        done
    fi
    return $FUNC_SUCC
}

############### main ############
#if [[ "x${IF_EXTRACT_ONLINE_DATA_RANDOMLY}" == "xtrue" ]];then
#    printMsg "begin to extract online data randomly ..."
#    loginfo "begin to extract online data randomly ..."
#    extractOnlineData2Test
#elif [[ "x${IF_EXTRACT_ONLINE_DATA_FROM_FILE}" == "xtrue" ]];then
#    printMsg "begin to extract online data from file ${ID_LIST_FILE} ..."
#    loginfo "begin to extract online data from file ${ID_LIST_FILE} ..."
#    extractOnlineData2Test "${ID_LIST_FILE}"
#fi

printMsg "initialize test tables"
loginfo "initialize test tables"
initialize

#printMsg "generate detail info"
#loginfo "generate detail info"
#execFssModule "${FSS_DETAIL_JOB_DEPLOY_PATH}" "start.sh"
#if [[ $? -ne $FUNC_SUCC ]];then
#    printMsg "generate detail info failed"
#    failExit "generate detail info failed"
#else
#    printMsg "generate detail info success"
#fi

execMySql2Array "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" \
                "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${FINANCE_AUTO}" \
                "select min(settle_start_date),max(settle_end_date) from settle_date;"

first_settle_start_date=${ret_array[0]} 
last_settle_end_date=${ret_array[1]}
prev_settle_date=$(date -d "-1 day ${first_settle_start_date}" +"%Y%m%d")
settle_date=${first_settle_start_date}

execUpdateorInsertMySql "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" \
                        "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${AUTOPAY_TEST_OLD}" \
                        "update payment_control set prev_settle_date = ${prev_settle_date}"

while([[ ${settle_date} -le ${last_settle_end_date} ]])
do
    #1. generate payment task of ${settle_date}
    printMsg "generate payment task, date: ${settle_date}"
    loginfo "generate payment task, date: ${settle_date}"
    execFssModule "${FSS_TASK_JOB_DEPLOY_PATH}" "start_payment_task.sh" "${settle_date}"
    if [[ $? -ne $FUNC_SUCC ]];then
        loginfo "generate payment task failed, date: ${settle_date}"
        failExit "generate payemnt task failed, date: ${settle_date}"
    else
        loginfo "generate payment task success, date: ${settle_date}"
    fi
    
    #2. verify payment_task
#    printMsg "verify payment task, date: ${settle_date}"
#    loginfo "verify payment task, date: ${settle_date}"
#    verifyPaymentTask ${settle_date} 
#    if [[ $? -ne $FUNC_SUCC ]];then
#        loginfo "verify payment task failed, date: ${settle_date}"
#        failExit "verify payment task failed, date: ${settle_date}"
#    else
#        loginfo "verify payment task success, date: ${settle_date}"
#    fi
    
    #3. update next settle date of ${settle_date}
    printMsg "update next settle date, date: ${settle_date}"
    loginfo "update next settle date, date: ${settle_date}"
    execFssModule "${FSS_TASK_JOB_DEPLOY_PATH}" "start_settle_date.sh" "${settle_date}"
    if [[ $? -ne $FUNC_SUCC ]];then
        loginfo "update next settle date failed, date: ${settle_date}"
        failExit "update next settle date failed, date: ${settle_date}"
    else
        loginfo "update next settle date success, date: ${settle_date}"
    fi
    
#    #4. verify next_settle_date and update table settle_date
#    printMsg "verify next settle date, date: ${settle_date}"
#    loginfo "verify next settle date, date: ${settle_date}"
#    verifyNextSettleDate ${settle_date} 
#    if [[ $? -ne $FUNC_SUCC ]];then
#        loginfo "verify next settle date failed, date: ${settle_date}"
#        failExit "verify next settle date failed, date: ${settle_date}"
#    else
#        loginfo "verify next settle date success, date: ${settle_date}"
#    fi
    
    execUpdateorInsertMySql "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" \
                            "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${AUTOPAY_TEST_OLD}" \
                            "update payment_task set task_status = 4"
    
    execUpdateorInsertMySql "${DB_AUTOPAY_TEST_HOST}" "${DB_AUTOPAY_TEST_PORT}" \
                            "${DB_AUTOPAY_TEST_USER}" "${DB_AUTOPAY_TEST_PWD}" "${AUTOPAY_TEST_OLD}" \
                            "update payment_control set prev_settle_date = ${settle_date}"
    
    settle_date=$(date -d "${settle_date} 1 days" +"%Y%m%d")
done
#sendEmail ${DIFF_FILE} "Compare Details_${TODAY}"


exit $FUNC_SUCC
