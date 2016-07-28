#coding: utf-8
import smtplib
import time
from email.message import Message
from time import sleep
import email.utils
import base64
import urllib
import httplib
import datetime
import json

start_time = time.time()
today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
last_day = today - datetime.timedelta(days=2)


def send_mail_video(pv):
    smtpserver = 'smtp.sina.com.cn'
    username = 'yypushmonitor@sina.cn'
    password = 'uve2015'

    from_addr = 'yypushmonitor@sina.cn'
    to_addr = ['hanyan1@staff.weibo.com','yugang5@staff.weibo.com','delong1@staff.weibo.com','linhua@staff.weibo.com','zhenliang@staff.weibo.com','xinyu16@staff.weibo.com']
#    to_addr = ['hanyan1@staff.weibo.com','yugang5@staff.weibo.com']
    local_time = email.utils.formatdate(time.time(),True)

    message = Message()
    message['Subject'] = 'video alarmi , send time is  ' + local_time
    message['From'] = from_addr
    message['To'] = ",".join(to_addr)
    message.set_payload('video_streaming_recommandation request_pv is '+ str(pv) + ' , It is lower than 80% , time is ' + str(yesterday))
    msg = message.as_string()

    sm = smtplib.SMTP(smtpserver,port=25,timeout=20)
    #sm.set_debuglevel(1)
    sm.ehlo()
    sm.starttls()
    sm.ehlo()
    sm.login(username, password)

    sm.sendmail(from_addr, to_addr, msg)
    sleep(5)
    sm.quit()

video_data = '{"queryType": "groupBy","dataSource": "uve_stat_report","granularity": {"type": "period","period": "P1D","timeZone": "Asia/Shanghai"},"dimensions":[],"intervals": ["' + str(last_day)+ 'T16:00:00/' + str(yesterday) +'T16:00:00"],"filter": {"type": "selector","dimension": "service_name","value": "video_streaming_recommendation"},"aggregations": [{"type": "longSum","fieldName": "count","name": "impress_pv"},{"type": "hyperUnique","fieldName": "uv1","name": "impress_uv"},{"type": "longSum","fieldName": "feedsnum","name": "cardnum"}]}'

def link_druid_uve_process(data):
    requrl = "http://10.39.7.41:9082/druid/v2"
    headerdata = {"Host":"10.39.7.41","Content-type":"application/json"}
    conn = httplib.HTTPConnection("10.39.7.41",9082,False)
    conn.request("POST",requrl,data,headerdata)
    response = conn.getresponse()
    res = response.read()
    res = json.loads(res)
    return res


if __name__ == '__main__':
    res = link_druid_uve_process(video_data)

    impress_pv = res[0]['event']['impress_pv']
    if(impress_pv < 200000000):
        send_mail_video(impress_pv)
