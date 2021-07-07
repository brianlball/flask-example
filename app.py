# import main Flask class and request object
import json
from flask import Flask, request, jsonify
from flask_mongoengine import MongoEngine

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
    value = db.StringField()
    unit = db.StringField()
    
class AssetParameters(db.EmbeddedDocument):
    key = db.StringField()
    displayName = db.StringField()
    value = db.StringField()

class Assets(db.EmbeddedDocument):
    _id = db.StringField()
    id = db.StringField(db_field='id')
    name = db.StringField()
    categoryName = db.StringField()

class Properties(db.EmbeddedDocument):
    kind = db.StringField()
    displayName = db.StringField()
    value = db.StringField()

class Metadata(db.EmbeddedDocument):
    key = db.StringField()
    dataType = db.StringField()
    value = db.StringField()

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
    currentValue = db.EmbeddedDocumentListField(CurrentValue)
    displayPriority = db.IntField()
    assets = db.EmbeddedDocumentListField(Assets)
    type = db.StringField()
    trenedInterval = db.IntField()
    isEnabled = db.BooleanField(default=False)
    isDetected = db.BooleanField(default=False)
    deviceId = db.StringField()
    categoryName = db.StringField()
    properties = db.EmbeddedDocumentListField(Properties)
    metadata = db.EmbeddedDocumentListField(Metadata)
    
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

asset = Asset(_id = "ee349ca0-f8aa-4b83-9fc6-86d727399914",
            modelId = "Unitary HP",
            twinId = "6b25c3c7-39e4-4be7-84a9-17e80feecaf5")
asset.save()            
asset1 = Asset(_id = "ce731aa1-fb9c-40b9-8f6b-ec6c42c9eadb",
            modelId = "Fan",
            name = "ee349ca0-f8aa-4b83-9fc6-86d727399914")
asset1.save()    
asset2 = Asset(_id = "bf34cfc8-cfe2-44d0-bf0a-ce462e2dc1c2",
             modelId = "Return_Air_Temperature_Sensor",
             name = "ee349ca0-f8aa-4b83-9fc6-86d727399914")
asset2.save()
asset3 = Asset(_id = "af34cfc8-cfe2-44d0-bf0a-ce462e2dc1c1",
            modelId = "FanSpeed",
            name = "ce731aa1-fb9c-40b9-8f6b-ec6c42c9eadb")
asset3.save()

asset4 = Asset(_id = "62b1928c-7331-02a3-bc74-3dba18ca91a2",
                modelId = "dtmi:com:willowinc:FanPoweredBox;1",
                twinId = "WLW-NYC-575_5_AVE-FPB-17.02",
                name = "Variable Air Volume Box VAV-01.1",
                hasLiveData = True,
                categoryId = "00300000-0000-0000-0000-000000010976",
                categoryName = "Fan Powered Box",
                floorId = "b7d170fd-48fd-4392-a4e9-1ad5880edc62",
                identifier = "VAV-01.1",
                forgeViewerModelId = "b7d170fd-48fd-4392-a4e9-1ad5880edc62")
#asset4.tags = [
#    Tags(name="equip"),
#    Tags(name="havc")
#]
tag = Tags(name="equip")
asset4.tags.append(tag)
tag = Tags(name="hvac")
asset4.tags.append(tag)
asset4.save()

#
#r = requests.get('http://localhost:5002/', params = {"_id": "ee349ca0-f8aa-4b83-9fc6-86d727399914"})
#r.text
#
@app.route('/', methods=['GET'])
def query_records():
    _id = request.args.get('_id')
    asset = Asset.objects(_id=_id).first()
    if not asset:
        return jsonify({'error': 'data not found'})
    else:
        return jsonify(asset.to_json())

#
#http://localhost:5002/objects/ee349ca0-f8aa-4b83-9fc6-86d727399914
#
@app.route('/objects/<id>')
def get_one_object(id: str):
    asset = Asset.objects.get_or_404(_id=id)
    return asset.to_json(), 200

#
#http://localhost:5002/all_objects
#
@app.route('/all_objects')
def get_all_objects():
    asset = Asset.objects()
    return asset.to_json(), 200
    
#
#r = requests.put('http://localhost:5002/update', json = {"_id": "ee349ca0-f8aa-4b83-9fc6-86d727399914", "fan": "n"})
#THIS CHANGES 'fan' -> n'
#
@app.route('/update', methods=['PUT'])
def update_object():
    body = request.get_json()
    _id = body['_id']
    asset = Asset.objects(_id=_id)
    if not asset:
        return jsonify({'error': 'data were not found'})
    else:
        asset.update(**body)
        return jsonify(asset.to_json())
#
##HP_json = '{ "_id": "ee349ca0-f8aa-4b83-9fc6-86d727399914", "modelId": "Unitary HP", "fan": "m", "heatPump": "m", "twinId": "6b25c3c7-39e4-4be7-84a9-17e80feecaf5"}'
#r = requests.put('http://localhost:5002/', data = HP_json)
# THIS ADDS 'fan: m' THRU UPDATE
#
@app.route('/', methods=['PUT'])
def create_record():
    record = json.loads(request.data)
    _id = record['_id']
    asset = Asset.objects(_id=_id)
    if not asset:
        return jsonify({'error': 'data not found'})
    else:
        asset.update(**record)
        return jsonify(asset.to_json())
#
#HP_json = '{ "_id": "ee349ca0-f8aa-4b83-9fc6-86d727399914", "modelId": "Unitary HP", "equip": "m", "heatPump": "m", "twinId": "6b25c3c7-39e4-4be7-84a9-17e80feecaf5"}'
#r = requests.post('http://localhost:5002/', data = HP_json)
#
@app.route('/', methods=['POST'])
def update_record():
    record = json.loads(request.data)
    print(record)
    asset = Asset(**record).save()
    return jsonify(asset.to_json())
#
#r = requests.delete('http://localhost:5002/', params = {"_id": "ee349ca0-f8aa-4b83-9fc6-86d727399914"})
#
@app.route('/', methods=['DELETE'])
def delete_record():
    _id = request.args.get('_id')
    asset = Asset.objects(_id=_id)
    if not asset:
        return jsonify({'error': 'data not found'})
    else:
        asset.delete()
    return jsonify(asset.to_json())

if __name__ == '__main__':
    # run app in debug mode on port 5001
    app.run(host='0.0.0.0', port=5002, debug=True)
