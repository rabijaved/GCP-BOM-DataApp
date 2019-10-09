# Copyright 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import webapp2
import urllib2
from bs4 import BeautifulSoup
import time
import base64
import json
import logging
import os

from flask import current_app, Flask, render_template, request
from googleapiclient.discovery import build


app = Flask(__name__)

# Configure the following environment variables via app.yaml
APP_PUBSUB_TOPIC = os.environ['PUBSUB_TOPIC']
APP_GCLOUD_PROJECT = os.environ['GCLOUD_PROJECT']

site= "http://www.bom.gov.au/vic/observations/vicall.shtml"
hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
       'Accept-Encoding': 'none',
       'Accept-Language': 'en-US,en;q=0.8',
       'Connection': 'keep-alive'}


class MainPage(webapp2.RequestHandler):


    def getData(self):

        req = urllib2.Request(site, headers=hdr)
        try:
            page = urllib2.urlopen(req)
        except urllib2.HTTPError, e:
            print e.fp.read()

        content = page.read()
        return content


    def cleanData(self,content):
        dataSet = ""
        soup = BeautifulSoup(content, "html.parser") # Parse the HTML as a string
        table = soup.find_all('table')
        
        for mytable in table:
            table_rows = mytable.find_all('tr')
            for tr in table_rows:
                th = tr.find('th')
                thName = ""
                if len(str(th)) > 0:
                    strtIndex = str(th).find('shtml">')
                    endindex = str(th).find('</a></th>')
                    
                    if strtIndex > -1 and endindex > -1:
                        strtIndex = strtIndex +7
                        thName = "['"+ str(th)[strtIndex : endindex] + "]*"

                td = tr.find_all('td')
                row = [i.text for i in td]
                if len(str(row))>5:
                    finalOut = str(thName) + str(row).replace("u'","'")
                    finalOut = finalOut.replace("]*[","', ").replace("[","").replace("]","")
                    splitStr = finalOut.split(", ")
                        
                    if len(splitStr) == 19:
                        tempx = { "Location":splitStr[0].replace("'",""), 
                        "DateTime":splitStr[1].replace("'",""), 
                        "Temp":splitStr[2].replace("'",""),
                        "AppTemp":splitStr[3].replace("'",""),
                        "DewPoint":splitStr[4].replace("'",""),
                        "RelHum":splitStr[5].replace("'",""),
                        "DeltaT":splitStr[6].replace("'",""),
                        "WindDir":splitStr[7].replace("'",""),
                        "WindSpeed":splitStr[8].replace("'",""),
                        "WindGust":splitStr[9].replace("'",""),
                        "WindSpeedKts":splitStr[10].replace("'",""),
                        "WindGustKts":splitStr[11].replace("'",""),
                        "PressHPA":splitStr[12].replace("'",""),
                        "Rain":splitStr[13].replace("'",""),
                        "LowTempTime":splitStr[14].replace("'",""),
                        "HighTempTime":splitStr[15].replace("'",""),
                        "HWGDir":splitStr[16].replace("'",""),
                        "HWGTime":splitStr[17].replace("'",""),
                        "KTSTime":splitStr[18].replace("'","")}

                        app_json = json.dumps(tempx)

                        self.publish(app_json)
                        dataSet = dataSet + str(app_json)

        return dataSet
            

    def get(self):

        content = self.getData()
        cData = self.cleanData(content)
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.write(cData)
        


    def publish(self, data_lines):

        service = build('pubsub', 'v1')

        topic_path = 'projects/{0}/topics/{1}'.format(
            APP_GCLOUD_PROJECT,
            APP_PUBSUB_TOPIC
        )
        resp = service.projects().topics().publish(
            topic=topic_path, body={
            "messages": [{
                "data": base64.b64encode(data_lines)
            }]
            }).execute()


        return resp


app = webapp2.WSGIApplication([
    ('/bom', MainPage),
], debug=True)