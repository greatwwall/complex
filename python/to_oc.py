#!/usr/bin/python2.7
# -*- coding: UTF-8 -*-

import logging, datetime, time, subprocess, ssl, urllib2
import json, re, ast, sys, ConfigParser, os
from urllib2 import HTTPError


# send alert data to OC
def sendDataToOC(oc_token, oc_key, content, contentDict, config):
    # base data
    pattern01 = re.compile("[^A-Za-z0-9_-]")
    pattern02 = re.compile(r"(\w+)-\w+-\w+")
    pattern03 = re.compile(r"(\d+)-(\w+)")
    UNDERLINE = "_"
    SEVERITY_DICT = {"Not classified": "4", "Information": "4", "Warning": "3", "Average": "3", "High": "2","Disaster": "1"}

    SYSTEM_NAME = contentDict.get("SystemName")
    serviceNames = contentDict.get("ServiceName").replace(" ", "").replace(",", "-")
    HOST_IP = contentDict.get("HOST_IP")
    TRIGGER_ID = contentDict.get("TriggerID")
    category = contentDict.get("Category")
    itemName = contentDict["ItemName"][0:50]

    ServiceName = pattern01.sub(UNDERLINE,contentDict.get("ServiceName"))
    AlarmName = pattern01.sub(UNDERLINE,contentDict.get("AlarmName"))

    # The post field
    ServiceNameMatch = pattern02.match(ServiceName)
    AlarmNameMatch = pattern03.match(AlarmName)

    if ServiceNameMatch and AlarmNameMatch:
        alarmId = "{0}_{1}".format(ServiceNameMatch.group(1),AlarmNameMatch.group(1))
        alarmId = alarmId[:100]
        alarmName = AlarmNameMatch.group(2)[:100]

    else:
        alarmId = "{0}_{1}_{2}".format(SYSTEM_NAME, serviceNames.strip(), pattern01.sub("_", itemName))[0:118]
        alarmName = AlarmName[:100]

    severity = SEVERITY_DICT[contentDict["Severity"]]
    resourceId = pattern01.sub(UNDERLINE, "{0}_{1}".format(HOST_IP,TRIGGER_ID))[:128]
    resourceIdName = '{0}_{1}'.format(serviceNames, pattern01.sub(UNDERLINE, contentDict.get("HostName"))[:512])
    hostName = pattern01.sub(UNDERLINE, contentDict.get("HostName"))[0:512]
    eventId = contentDict.get("EventID")
    ItemValue = contentDict.get("ItemValue")
    additional = "ServiceName={ServiceName},TemplateName={TemplateName},HostName={HostName},ItemName={ItemName},ItemValue={0}".format(ItemValue,**contentDict)
    cause = contentDict["Cause"].replace("'", "")
    if cause == None or cause == "":
        cause = alarmName

    # get UTC time
    utc = datetime.datetime.strptime('{0} {1}'.format(contentDict["Date"], contentDict["Time"]), "%Y.%m.%d %H:%M:%S")
    timestruct = time.mktime(utc.timetuple())
    occurtime = str(datetime.datetime.utcfromtimestamp(timestruct))

    # prepare post data
    if category == "AddAlarm":  # add alarm
        data = ({"data": {"alarmid": alarmId, "alarmname": alarmName + "##" + alarmName,
                          "severity": severity, "resourceid": resourceId,
                          "resourceidname": resourceIdName, "moc": "vms", "sn": eventId, "category": "1",
                          "occurtime": occurtime,
                          "additional": additional + "##" + additional, "isclear": "1", "cause": cause + "##" + cause}})
    else:  # clear alarm
        data = ({"data": {"alarmid": alarmId, "alarmname": alarmName + "##" + alarmName,
                          "severity": severity, "resourceid": resourceId,
                          "resourceidname": hostName, "moc": "vms", "sn": eventId, "category": "2",
                          "occurtime": occurtime, "cleartime": occurtime,
                          "isclear": "1", "cleartype": "0"}})

    # number of retries
    tryLimit = config.getint("alert_to_oc","alert_attempt_count")
    timeout = config.getint("alert_to_oc", "alert_timeout")
    wccDir = config.get("alert_to_oc", "wcc_dir")

    # post data
    for i in range(tryLimit):
        logging.info("try {0} again.".format(i))
        token = decrypt(oc_token, wccDir)
        if len(token) != 0:
            response = doPost(data, token, contentDict, contentDict["OC_ALARM_PATH"], timeout)
            if response is None:
                logging.info("Alert to OC SUCCESS!")
                break
            elif str(response) == "401":
                logging.info("Token is expired,get token again.")
                getToken(contentDict, oc_token, oc_key, timeout, wccDir)
            else:
                continue
        else:
            getToken(contentDict, oc_token, oc_key, timeout, wccDir)
    else:
        logging.error("Alert to OC FAILED!")


# common http post method
def doPost(data, token, contentDict, url, timeout):
    oc_url = "https://{OC_IP}:{OC_PORT}/{OC_CONTEXT}/{OC_VERSION}/{0}".format(url, **contentDict)

    encoded_data = json.dumps(data)
    context = ssl._create_unverified_context()
    request = urllib2.Request(oc_url, encoded_data)
    request.add_header("Accept", "application/json;charset=UTF-8")
    request.add_header("content-type", "application/json;charset=UTF-8")
    if len(token) > 0:
        request.add_header("X-Auth-Token", token)

    try:
        result = urllib2.urlopen(request, context=context, timeout=timeout)
    except HTTPError as e:
        logging.error('We failed to reach a server.Reason: {0}'.format(e))
        return str(e.code)
    except Exception as e:
        logging.error('We failed to reach a server.Reason: {0}'.format(e))
        return e
    else:
        response = json.loads(result.read())
        result.close()
        if len(response) > 0:
            return response
        else:
            return None


# get authentication token from OC
def getToken(contentDict, oc_token, oc_key, timeout, wccDir):
    value = decrypt(oc_key,wccDir)
    data = ({"user_id": contentDict["OC_USER"], "value": value, "host_ip": contentDict["MONITOR_IP"]})
    responseData = doPost(data, "", contentDict, contentDict["OC_AUTH_PATH"], timeout)
    if responseData is not None and "data" in responseData:
        token = responseData["data"]
        if len(token) > 0:
            encrypt(oc_token, token, wccDir)
            logging.info("get token finished")
        else:
            logging.error("token from OC is none")
        return token


# decrypt content
def decrypt(bkname, wccDir):
    all_status = subprocess.Popen(
        "cd {1} && java -jar wcc-1.0.0.jar decrypt {0} 2>/dev/null | sed '/^$/!h;$!d;g'".format(bkname,wccDir),
        shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).communicate()
    if len(all_status[0]) > 0:
        return all_status[0]
    else:
        return ""


# encrypt content
def encrypt(bkname, content, wccDir):
    if not re.match("^[a-zA-Z0-9.-]+$", content):
        logging.error("Content: OC Tokens have special characters.")
        logging.error("Alert to OC FAILED!")
        raise SystemExit(4)

    subprocess.Popen(
        "cd {0} && java -jar wcc-1.0.0.jar encrypt {1} {2} 2>/dev/null | sed '/^$/!h;$!d;g'".format(wccDir,bkname,content),
        shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).communicate()


def main():
    try:
        config = ConfigParser.ConfigParser()
        config.read("/opt/OpsMonitor/alertscripts/alert_to_oc.conf")
        alertLogDir = config.get("alert_to_oc","alert_log_dir")
        alertLogName = config.get("alert_to_oc", "alert_log_name")

        LogPath = os.path.join(alertLogDir,alertLogName)
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename=LogPath,
                            filemode='a')
    except Exception:
        logging.error("Can't create log file.")

    # wcc decrypt key
    oc_key = "ocpwd"
    oc_token = "octoken"

    logging.info("######################################################")
    subject = sys.argv[2]
    content = sys.argv[3]

    try:
        pattern04 = re.compile(r"\x1b|\r|\n")
        contentDict = ast.literal_eval('{{{0}}}'.format(pattern04.sub(" ",content)))
        logging.info("subject={1}_{0}".format(subject,contentDict.get("HOST_IP")))
    except Exception:
        logging.error("Content:Possible Get Json Error,check Json data please! ")
        logging.error("Alert to OC FAILED!")

    try:
        sendDataToOC(oc_token, oc_key, content, contentDict, config)
    except Exception as e:
        logging.error(e)
    logging.info("######################################################")


# run main function
if __name__ == '__main__':
    main()
