# import main Flask class and request object
import json
from flask import Flask, request, jsonify, Response
from flask_mongoengine import MongoEngine
from bson import json_util
import uuid

# create the Flask app
app = Flask(__name__)

app.config['MONGODB_SETTINGS'] = {
        'connect': False,
        'host': 'mongodb://127.0.0.1:27018/test_db?authSource=admin'
}

db = MongoEngine()
db.init_app(app)

class Tags(db.EmbeddedDocument):
    name = db.StringField()
    feature = db.StringField()

class CurrentValue(db.EmbeddedDocument):
    value = db.DynamicField()
    unit = db.StringField()

class AssetParameters(db.EmbeddedDocument):
    key = db.StringField()
    displayName = db.StringField()
    value = db.DynamicField()

class Assets(db.EmbeddedDocument):
    _id = db.StringField()
    id = db.StringField(db_field='id')
    name = db.StringField()
    categoryName = db.StringField()

class Points(db.EmbeddedDocument):
    _id = db.StringField()
    id = db.StringField(db_field='id')
    modelId = db.StringField()
    twinId = db.StringField()
    trendId = db.StringField()
    name = db.StringField()
    description = db.StringField()
    tags = db.EmbeddedDocumentListField(Tags)
    type = db.StringField()
    currentValue = db.EmbeddedDocumentField(CurrentValue)
    displayPriority = db.IntField()
    assets = db.EmbeddedDocumentListField(Assets)
    type = db.StringField()
    trenedInterval = db.IntField()
    isEnabled = db.BooleanField(default=False)
    isDetected = db.BooleanField(default=False)
    deviceId = db.StringField()
    categoryName = db.StringField()
    properties = db.DynamicField()
    metadata = db.DynamicField()

class Asset(db.DynamicDocument):
    _id = db.StringField()
    id = db.StringField(db_field='id')
    modelId = db.StringField()
    twinId = db.StringField()
    name = db.StringField()
    hasLiveData = db.BooleanField(default=False)
    categoryId = db.StringField()
    categoryName = db.StringField()
    floorId = db.StringField()
    identifier = db.StringField()
    forgeViewerModelId = db.StringField()
    tags = db.EmbeddedDocumentListField(Tags)
    points = db.EmbeddedDocumentListField(Points)
    parentId = db.StringField()
    assetParameters = db.EmbeddedDocumentListField(AssetParameters)

class Insight(db.DynamicDocument):
    id = db.StringField(primary_key=True)
    sequenceNumber = db.StringField()
    floorCode = db.StringField(default='av-I-466')
    equipmentId = db.StringField()
    type = db.StringField()
    name = db.StringField()
    priority = db.IntField()
    status = db.StringField()
    state = db.StringField()
    occurredDate = db.StringField()
    updatedDate = db.StringField()
    externalId = db.StringField()
    externalStatus = db.StringField()
    externalMetadata = db.StringField()
    customerId = db.StringField()
    siteId = db.StringField()
    description = db.StringField()
    createdDate = db.StringField()
    detectedDate = db.StringField()

#asset4 = Asset(_id = "62b1928c-7331-02a3-bc74-3dba18ca91a2",
#                modelId = "dtmi:com:willowinc:FanPoweredBox;1",
#                twinId = "WLW-NYC-575_5_AVE-FPB-17.02",
#                name = "Variable Air Volume Box VAV-01.1",
#                hasLiveData = True,
#                categoryId = "00300000-0000-0000-0000-000000010976",
#                categoryName = "Fan Powered Box",
#                floorId = "b7d170fd-48fd-4392-a4e9-1ad5880edc62",
#                identifier = "VAV-01.1",
#                forgeViewerModelId = "b7d170fd-48fd-4392-a4e9-1ad5880edc62")

#asset4.tags = [
#    Tags(name="equip"),
#    Tags(name="havc")
#]
#tag = Tags(name="equip")
#asset4.tags.append(tag)
#tag = Tags(name="hvac")
#asset4.tags.append(tag)
#asset4.save()

f = open('samplejson.json',)
data = json.load(f)
f.close()
            
for dt in data:
    #asset = Asset(_id = dt['id'])
    asset = Asset()
    asset._id = dt['id']
   #asset.modelId = dt['modelId']
    for key in dt:
        #print('FIRST: %s: %s'%(key,dt[key]))
        if key != "id":
            if key == "tags":
                for tag in dt[key]:
                    #print('TAG: %s: %s'%(tag,tag['name']))
                    t = Tags(name=tag['name'])
                    asset.tags.append(t)
                    #asset.save()
                    #print('asset: %s'%(asset.to_json()))
            elif key == "assetParameters":
                for item in dt[key]:
                    ap = AssetParameters()
                    #print('AP item: %s'%(item))
                    for key in item:
                        #print('AP key: %s: %s'%(key,item[key]))
                        setattr(ap, key, item[key])    
                    asset.assetParameters.append(ap)
                    #print('asset: %s'%(asset.to_json()))
                    #asset.save()
            elif key == "points":
                for item in dt[key]:
                    point = Points()
                    #print('POINT item: %s'%(item))
                    for key in item:
                        #print('POINT key: %s: %s'%(key,item[key]))
                        if key == "tags":
                            for tag in item[key]:
                                #print('TAG: %s: %s'%(tag,tag['name']))
                                t = Tags(name=tag['name'])
                                if 'feature' in tag.keys():
                                    setattr(t, 'feature', tag['feature'])
                                point.tags.append(t)
                                #print('point: %s'%(point.to_json()))
                        elif key == "currentValue":
                            #print("type(item[key]): %s"%(type(item[key])))
                            cv = CurrentValue()
                            setattr(cv, 'unit', item[key]['unit'])
                            setattr(cv, 'value', item[key]['value'])
                            point.currentValue = cv
                            #print('point: %s'%(point.to_json()))
                        elif key == "assets":
                            for assets in item[key]:
                                at = Assets()
                                for keys in assets:
                                    #print('AP key: %s: %s'%(key,item[key]))
                                    setattr(at, keys, assets[keys])    
                                point.assets.append(at)
                        elif key == "properties":
                            point.properties = {"property1": {"displayName": "phenomenon",
                                                              "value": "Water",
                                                              "kind": "string"}
                                               }
                        elif key == "metadata":
                            point.metadata = [{"key": "manufacturer",
                                               "value": "omega",
				                               "dataType": "string"
			                                 }]
                        else:                            
                            setattr(point, key, item[key])
                    #done loop over point keys
                    asset.points.append(point)
                    #print('asset: %s'%(asset.to_json()))
                    #asset.save()
            else:
                setattr(asset, key, dt[key])

    asset.save()
    
f = open('insights.json',)
data = json.load(f)
f.close()
            
for dt in data:
    insight = Insight()
    insight.id = dt['id']
    for key in dt:
        if key != "id":
            print('INSIGHT: %s: %s'%(key,dt[key]))
            setattr(insight, key, dt[key])
    insight.save()


# All Assets

#http://localhost:5002/all_assets
#
@app.route('/all_assets')
def get_all_assets():
    asset = Asset.objects()
    if not asset:
        return Response({'Not Found'}, mimetype="application/json", status=404)
    else:
        return Response(asset.to_json(), mimetype="application/json", status=200)

# Retrieve an Asset (send asset ID)    

#r = requests.get(url=URL+'assets/', params = {"id": "62b1928c-7331-02a3-bc74-3dba18ca91a3"})
#r.text
#
@app.route('/assets/', methods=['GET'])
def query_assets():
    id = request.args.get('id')
    asset = Asset.objects(_id=id).first()
    if not asset:
        return Response({'Not Found'}, mimetype="application/json", status=404)
    else:
        return Response(asset.to_json(), mimetype="application/json", status=200)

#http://localhost:5002/assets/62b1928c-7331-02a3-bc74-3dba18ca91a3
#
@app.route('/assets/<id>')
def get_one_assets(id: str):
    asset = Asset.objects.get_or_404(_id=id)
    return asset.to_json(), 200
     
# List Assets (send categoryId and get all assets with categoryId)

#categoryId = "00300000-0000-0000-0000-000000010977"
#r = requests.get(url=URL+'categories/'+categoryId+'/assets')
#r.text
#
@app.route('/categories/<id>/assets', methods=['GET'])
def query_categoryId(id: str):
    print(id)
    pipeline = [{'$match':
                  { '$and': [{"categoryId": id}] }
               }]
    asset = Asset.objects().aggregate(pipeline)
    if not asset:
        return Response({'Not Found'}, mimetype="application/json", status=404)
    else:
        value = list(asset)
        #print(value)
        for item in value:
            if item['categoryId'] == id:
                print(item)
                return Response(json.dumps(item), mimetype="application/json", status=200)
            else:
                return Response({'ASSET Not Found'}, mimetype="application/json", status=404)

# List Assets (send categoryId and get all assets with categoryId)
#
#http://localhost:5002/categories/00300000-0000-0000-0000-000000010977/assets
#
@app.route('/categories/<id>/assets')
def get_categoryId(id: str):
    pipeline = [{'$match':
                  { '$and': [{"categoryId": id}] }
               }]
    asset = Asset.objects().aggregate(pipeline)
    if not asset:
        return Response({'Not Found'}, mimetype="application/json", status=404)
    else:
        value = list(asset)
        #print(value)
        for item in value:
            if item['categoryId'] == id:
                print(item)
                return Response(json.dumps(item), mimetype="application/json", status=200)
            else:
                return Response({'ASSET Not Found'}, mimetype="application/json", status=404)

# List Asset categories (get all categoryIds)
#
#http://localhost:5002/categories/
#
@app.route('/categories')
def get_categories():
    asset = Asset.objects().distinct('categoryId')
    #print(asset)
    if not asset:
        return Response({'Not Found'}, mimetype="application/json", status=404)
    else:
        return Response(json.dumps(asset), mimetype="application/json", status=200)

#POINTS

# List Points (give asset Id and get points)     
#http://localhost:5002/points?id=62b1928c-7331-02a3-bc74-3dba18ca91a12
#r = requests.get(url=URL+'points', params = {"id": "62b1928c-7331-02a3-bc74-3dba18ca91a12"})
#print(r)
#print(r.json())
#
@app.route('/points', methods=['GET'])
def find_assets_points():
    id = request.args.get('id')
    asset = Asset.objects(_id=id).first()
    if not asset:
        return Response({'Not Found'}, mimetype="application/json", status=404)
    else:
        points = asset.points
        return_points = []
        print(len(points))
        for i in range(0, len(points)):
            print(i)
            print(points[i].to_json())
            return_points.append(points[i].to_json())
        return Response(json.dumps(return_points), mimetype="application/json", status=200)


# Retrieve a Point (give point id and get it)

#r = requests.get(url=URL+'points/', params = {"id": "0fcd55ad-251d-4111-bed9-de32c7addb52"})
#r.text
#
@app.route('/points/', methods=['GET'])
def query_point():
    id = request.args.get('id')
    pipeline = [{'$match':
                  { '$and': [{"points.id": id}] }
               }]
    asset = Asset.objects().aggregate(pipeline)
    if not asset:
        return Response({'Not Found'}, mimetype="application/json", status=404)
    else:
        value = list(asset)
        #print(value)
        for item in value:
            for point in item['points']:
                if point['id'] == id:
                    print(point)
                    return Response(json.dumps(point), mimetype="application/json", status=200)
                else:
                    return Response({'POINT Not Found'}, mimetype="application/json", status=404)

#http://localhost:5002/points/0fcd55ad-251d-4111-bed9-de32c7addb52
#
@app.route('/points/<id>')
def get_one_point(id: str):
    pipeline = [{'$match':
                  { '$and': [{"points.id": id}] }
               }]
    asset = Asset.objects().aggregate(pipeline)
    if not asset:
        return Response({'Not Found'}, mimetype="application/json", status=404)
    else:
        value = list(asset)
        #print(value)
        for item in value:
            for point in item['points']:
                if point['id'] == id:
                    print(point)
                    return Response(json.dumps(point), mimetype="application/json", status=200)
                else:
                    return Response({'POINT Not Found'}, mimetype="application/json", status=404)

# INSIGHTS        

# List Insights

#http://localhost:5002/insights
#r = requests.get(url=URL+'/sites/<id>/insights')
#
@app.route('/insights')
def get_all_insights():
    insights = Insight.objects()
    if not insights:
        return Response({'Not Found'}, mimetype="application/json", status=404)
    else:
        return Response(insights.to_json(), mimetype="application/json", status=200)

# Retrieve an Insights (send insight ID)    

#r = requests.get(url=URL+'insights', params = {"id": "8cf1743b-fcb7-44c8-9b6d-0038323ba9e5"})
#r.text
#
@app.route('/insights/', methods=['GET'])
def get_insights():
    id = request.args.get('id')
    insight = Insight.objects(_id=id).first()
    if not insight:
        return Response({'Not Found'}, mimetype="application/json", status=404)
    else:
        return Response(insight.to_json(), mimetype="application/json", status=200)

@app.route('/sites/siteId/insights/<id>')
def query_insights(id: str):
    insight = Insight.objects(_id=id).first()
    if not insight:
        return Response({'Not Found'}, mimetype="application/json", status=404)
    else:
        return Response(insight.to_json(), mimetype="application/json", status=200)

# Create an Insight

#insight_json = '{"id": "8cf1743b-fcb7-44c8-9b6d-0038323ba9e4","sequenceNumber": "av-I-464","floorCode": "L010","equipmentId": "6be53768-811e-4db8-baee-471a01498df7","type": "fault","name": "258adba7","priority": 3,"status": "inProgress","state": "active","occurredDate": "2020-05-28T00:33:02.836Z","updatedDate": "2020-08-28T13:46:01.919Z","externalId": "20200428_d4b43edf","externalStatus": "active","externalMetadata": "string","customerId": "3fc260f3-3e91-470b-8285-15a11c799491","siteId": "1218614a-9822-43c5-94ca-1ecc29ab80b0","description": "Chilled Water Pump CWP-01.1 is running when Chiller CH-01 is off.","createdDate": "2020-05-28T01:25:02.881Z","detectedDate": "2020-05-28T01:02:02.836Z"}'
#r = requests.post(url=URL+'insights', data = insight_json)
#
@app.route('/insights', methods=['POST'])
def create_insight():
    record = json.loads(request.data)
    print(record)
    if not record:
        return Response({'Not Found'}, mimetype="application/json", status=404)
    else:
        id = record.get('id')
        print('id: %s'%(id))
        if not id:
            new_uuid = str(uuid.uuid4())
            record['id'] = new_uuid
        sequenceNumber = record.get('sequenceNumber')
        print('sequenceNumber: %s'%(sequenceNumber))
        if not sequenceNumber:
            record['sequenceNumber'] = 'av-I-466'
        status = record.get('status')
        print('status: %s'%(status))
        if not status:
            record['status'] = 'default'
        updatedDate = record.get('updatedDate')
        print('updatedDate: %s'%(updatedDate))
        if not updatedDate:
            record['updatedDate'] = 'default'
        siteId = record.get('siteId')
        print('siteId: %s'%(siteId))
        if not siteId:
            record['siteId'] = '1218614a-9822-43c5-94ca-1ecc29ab80b0'
        print(record)
        insight = Insight(**record).save()
        return Response(insight.to_json(), mimetype="application/json", status=200)

# Update an insight
#update_json = '{"name": "string","description": "string","priority": 0,"state": "active","occurredDate": "2019-08-24T14:15:22Z","detectedDate": "2019-08-24T14:15:22Z","externalId": "string","externalStatus": "string","externalMetadata": "string"}'
#InsightId = "8cf1743b-fcb7-44c8-9b6d-0038323ba9e5"
#r = requests.put(url=URL+'insights/'+InsightId, data = update_json)
@app.route('/insights/<id>', methods=['PUT'])
def update_insight(id: str):
    record = json.loads(request.data)
    insight = Insight.objects(_id=id)
    if not insight:
        return Response({'Insight not found'}, mimetype="application/json", status=404)
    else:
        insight.update(**record)
        return Response(insight.to_json(), mimetype="application/json", status=200)

# Delete an insight

#
#r = requests.delete(url=URL+'insights/'+InsightId, params = {"_id": "8cf1743b-fcb7-44c8-9b6d-0038323ba9e5"})
#
@app.route('/insights/<id>', methods=['DELETE'])
def delete_insight(id: str):
    insight = Insight.objects(_id=id)
    if not insight:
        return Response({'Not Found'}, mimetype="application/json", status=404)
    else:
        insight.delete()
        return Response({'Insight Deleted'}, mimetype="application/json", status=204)

# Update an insight state

#update_json = '{"state": "inactive"}'
#InsightId = "8cf1743b-fcb7-44c8-9b6d-0038323ba9e5"
#r = requests.put(url=URL+'insights/'+InsightId, data = update_json)
@app.route('/insights/<id>/state', methods=['PUT'])
def update_insight_state(id: str):
    record = json.loads(request.data)
    insight = Insight.objects(_id=id)
    if not insight:
        return Response({'Insight not found'}, mimetype="application/json", status=404)
    else:
        insight.update(**record)
        return jsonify(insight.to_json())


if __name__ == '__main__':
    # run app in debug mode on port 5001
    app.run(host='0.0.0.0', port=5002, debug=True)
