import requests
import json
import html_to_json
from datetime import datetime
import re
from kafka import KafkaProducer
from json import dumps, loads

producer = KafkaProducer(bootstrap_servers=['<local host>:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))


###### UDF to scrap data from URL ##########
def get_url_data(url):
  r = requests.get(url)
  return r.text

def main(url):
  ######### pass url ##########
  data = get_url_data(url)
  data=html_to_json.convert(data)
  ######## current time #######
  CURRENT_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  ######## previous close rate #######
  PREVIOUS_CLOSE=data["html"][0]["body"][0]["c-wiz"][1]["div"][0]["div"][3]["div"][0]["main"][0]["div"][1]["div"][1]["div"][1]["div"][0]["_value"].replace(',','')
  PREVIOUS_CLOSE=''.join(re.findall("\d+\.\d+", PREVIOUS_CLOSE))
  ######## current rate #######
  CURRENT_VALUE=data["html"][0]['body'][0]['c-wiz'][1]['div'][0]['div'][3]['div'][0]['main'][0]['div'][1]['div'][0]['c-wiz'][0]['div'][0]['div'][0]['div'][0]['div'][0]['div'][0]['div'][0]['div'][0]['span'][0]['div'][0]['div'][0]['_value'].replace(',','')
  CURRENT_VALUE=''.join(re.findall("\d+\.\d+", CURRENT_VALUE))
  ######## stock name #######
  STOCK=data['html'][0]['head'][0]['meta'][10]['_attributes']['content'].split('Get the latest ')[1].split(') ')[0]+')'

  return ','.join([STOCK,CURRENT_VALUE,PREVIOUS_CLOSE,CURRENT_TIME])

while True:
  if __name__ == '__main__':
    url = 'https://www.google.com/finance/quote/SENSEX:INDEXBOM'
    data = main(url)
    producer.send('kafka-topic', data)
    producer.flush()
    print(data)