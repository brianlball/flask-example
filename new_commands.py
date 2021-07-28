import requests, warnings

URL = 'http://10.40.7.78:5002/'
#URL = 'http://localhost:5002/'

#web brower functions
#http://10.40.7.78:5002/assets/62b1928c-7331-02a3-bc74-3dba18ca91a3
#http://10.40.7.78:5002/all_assets

#ASSETS

print("get all assets")     
r = requests.get(url=URL+'all_assets')
if r.status_code != 200:
    warnings.warn(f"Unexpected put error: {r.status_code}")
print("<Response [200]> means success")
print(r)
print(r.json())

print("get an asset")     
r = requests.get(url=URL+'assets/', params = {"id": "62b1928c-7331-02a3-bc74-3dba18ca91a3"})
if r.status_code != 200:
    warnings.warn(f"Unexpected put error: {r.status_code}")
print("<Response [200]> means success")
print(r)
print(r.json())

#POINTS
print("get all points")     
r = requests.get(url=URL+'points/', params = {"id": "0fcd55ad-251d-4111-bed9-de32c7addb52"})
if r.status_code != 200:
    warnings.warn(f"Unexpected put error: {r.status_code}")
print("<Response [200]> means success")
print(r)
print(r.json())

print("get all points for an ASSET")
r = requests.get(url=URL+'points', params = {"id": "62b1928c-7331-02a3-bc74-3dba18ca91a12"})
if r.status_code != 200:
    warnings.warn(f"Unexpected put error: {r.status_code}")
print("<Response [200]> means success")
print(r)
print(r.json())

print("get all Assets for a categoryId")
categoryId = "00300000-0000-0000-0000-000000010977"
r = requests.get(url=URL+'categories/'+categoryId+'/assets')
if r.status_code != 200:
    warnings.warn(f"Unexpected put error: {r.status_code}")
print("<Response [200]> means success")
print(r)
print(r.json())

print("get all categoryId")     
r = requests.get(url=URL+'categories')
if r.status_code != 200:
    warnings.warn(f"Unexpected put error: {r.status_code}")
print("<Response [200]> means success")
print(r)
print(r.json())

print("get all insights")     
r = requests.get(url=URL+'insights')
if r.status_code != 200:
    warnings.warn(f"Unexpected put error: {r.status_code}")
print("<Response [200]> means success")
print(r)
print(r.json())

print("create an insight") 
insight_json = '{"id": "8cf1743b-fcb7-44c8-9b6d-0038323ba9e4","sequenceNumber": "av-I-464","floorCode": "L010","equipmentId": "6be53768-811e-4db8-baee-471a01498df7","type": "fault","name": "258adba7","priority": 3,"status": "inProgress","state": "active","occurredDate": "2020-05-28T00:33:02.836Z","updatedDate": "2020-08-28T13:46:01.919Z","externalId": "20200428_d4b43edf","externalStatus": "active","externalMetadata": "string","customerId": "3fc260f3-3e91-470b-8285-15a11c799491","siteId": "1218614a-9822-43c5-94ca-1ecc29ab80b0","description": "Chilled Water Pump CWP-01.1 is running when Chiller CH-01 is off.","createdDate": "2020-05-28T01:25:02.881Z","detectedDate": "2020-05-28T01:02:02.836Z"}'
r = requests.post(url=URL+'insights', data = insight_json)
if r.status_code != 200:
    warnings.warn(f"Unexpected put error: {r.status_code}")
print("<Response [200]> means success")
print(r)
print(r.json())

print("update an insight") 
update_json = '{"name": "string","description": "string","priority": 0,"state": "active","occurredDate": "2019-08-24T14:15:22Z","detectedDate": "2019-08-24T14:15:22Z","externalId": "string","externalStatus": "string","externalMetadata": "string"}'
InsightId = "8cf1743b-fcb7-44c8-9b6d-0038323ba9e5"
r = requests.put(url=URL+'insights/'+InsightId, data = update_json)
if r.status_code != 200:
    warnings.warn(f"Unexpected put error: {r.status_code}")
print("<Response [200]> means success")
print(r)
print(r.json())

print("delete an insight")
InsightId = "8cf1743b-fcb7-44c8-9b6d-0038323ba9e5"
r = requests.delete(url=URL+'insights/'+InsightId, params = {"id": "8cf1743b-fcb7-44c8-9b6d-0038323ba9e5"})
if r.status_code != 204:
    warnings.warn(f"Unexpected put error: {r.status_code}")
print("<Response [204]> means success")
print(r)

print("create an insight") 
insight_json = '{"id": "8cf1743b-fcb7-44c8-9b6d-0038323ba9e5","sequenceNumber": "av-I-464","floorCode": "L010","equipmentId": "6be53768-811e-4db8-baee-471a01498df7","type": "fault","name": "258adba7","priority": 3,"status": "inProgress","state": "active","occurredDate": "2020-05-28T00:33:02.836Z","updatedDate": "2020-08-28T13:46:01.919Z","externalId": "20200428_d4b43edf","externalStatus": "active","externalMetadata": "string","customerId": "3fc260f3-3e91-470b-8285-15a11c799491","siteId": "1218614a-9822-43c5-94ca-1ecc29ab80b0","description": "Chilled Water Pump CWP-01.1 is running when Chiller CH-01 is off.","createdDate": "2020-05-28T01:25:02.881Z","detectedDate": "2020-05-28T01:02:02.836Z"}'
r = requests.post(url=URL+'insights', data = insight_json)
if r.status_code != 200:
    warnings.warn(f"Unexpected put error: {r.status_code}")
print("<Response [200]> means success")
print(r)
print(r.json())

print("update an insight state")
update_json = '{"state": "inactive"}'
InsightId = "8cf1743b-fcb7-44c8-9b6d-0038323ba9e5"
r = requests.put(url=URL+'insights/'+InsightId, data = update_json)
if r.status_code != 200:
    warnings.warn(f"Unexpected put error: {r.status_code}")
print("<Response [200]> means success")
print(r)
print(r.json())


print("create insight reduced")
insight_json = '{"floorCode": "xx","equipmentId": "d2a795e1-f645-4e24-bc0a-ab1965048fcc","type": "fault","name": "ChilledWaterValveLeaking","description": "Chilled Water Pump CWP-01.1 is running when Chiller CH-01 is off.","priority": 0,"state": "active","occurredDate": "2019-08-24T14:15:22Z","detectedDate": "2019-08-24T14:15:22Z","externalId": "string","externalStatus": "string","externalMetadata": "string"}'
r = requests.post(url=URL+'insights', data = insight_json)
if r.status_code != 200:
    warnings.warn(f"Unexpected put error: {r.status_code}")
print("<Response [200]> means success")
print(r)
print(r.json())