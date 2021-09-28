import requests, warnings, yaml

with open('secrets.yml') as f:
    secrets = yaml.safe_load(f)

URL = 'https://api.willowinc.com/v2/oauth2/token'
credential_json = {"clientId": secrets['clientId'],"clientSecret": secrets['clientSecret']}
headers={'Content-type':'application/json','Accept':'application/json'}
r = requests.post(url=URL, json = credential_json, headers=headers)
print(r)
print(r.content)

token = r.json()
access_token = token['accessToken']
URL = 'https://api.willowinc.com/v2/sites'
response = requests.get(url=URL,headers={'Content-Type':'application/json','Authorization': 'Bearer {}'.format(access_token)})
print(response)
print(response.content)

URL = 'https://api.willowinc.com/v2/sites/'+secrets['site']+'/points/8fe0028d-cc7d-49d4-891c-ed5d15e91810/trendlog?startDate=2021-08-30 00:00&endDate=2021-09-14 00:00&granularity=P0DT1H'

payload={}
headers = {'Content-Type':'application/json','Authorization': 'Bearer {}'.format(access_token)}
response = requests.get(url=URL, headers=headers, data=payload) 

print(response)
print(response.content)