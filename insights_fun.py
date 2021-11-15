#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 27 06:10:33 2021

@author: kwoldeki
"""
import pdb
from datetime import datetime, timedelta
import json

def insights(floor, equipmentId, name, prioritity, state, occdate, count):
    
    if name == 'ChilledWaterValveLeaking':
        description =' Chilled water valve leaking - this fault is triggered when the discharge air temperature is less than mixed air temperature atleast for 30 min while the cooling coil valve command is 0% open'
    elif name == 'SpaceOvercooling':
        description ='Space over cooling - this fault is triggered when the space temperature is lower than the space cooling setpoint by 5 deg F(adjustable) for atleast 30 min'
    elif name == 'SpaceOverheating':
        description ='Space over heating - this fault is triggered when the space temperature is greater than the space heating setpoint by 5 deg F(adjustable) for atleast 30 min'
    elif name == 'SFNIA':
        description ='Supply fan not in auto - this fault is triggered when the supply fan status is different from the supply fan run command. This could be resulted from manual ovrriding of the control command'
    elif name == 'RFNIA':
        description ='Retrun fan not in auto - this fault is triggered when the return fan status is different from the return fan run command. This could be resulted from manual ovrriding of the control command'
    elif name == 'ChilledWaterValveSClosed':
        description ='Chilled water valve stuck closed - this fault is trigged when no considerable temperatue diffence is observed between mixed and discharge air temperatures even if the cooling valve command shows a value > 0 %'
    elif name == 'AHUOvercooling':
        description ='AHU over cooling - this fault is triggered when the discharge air temperature is less than the discharge air setpoint for atleast 30 min. This could be resulted from cooling valve failure to close'    
    elif name == 'ChWVlvNIA':
        description ='Chilled water valve not in auto - this fault is triggered when no considerable cooling is observed (discharge air temp > discharge air temp setpoint + 5) while the chilled water valve is commaneded to open 100%. This could be a result of manual overriding of the cotrol signal'    
    elif name == 'HotWaterValveLeaking':
        description ='Hot water valve leaking - this fault is triggered when the discharge air temperature is greater than mixed air temperature for atleast 30 min while the heating coil valve command is 0% open'
    elif name == 'AHUOverheating':
        description ='AHU overheating - this fault is triggered when the discharge air temperature is greater than the discharge air setpoint for atleast 30 min. This could be resulted from heating valve failure to close'
    elif name == 'HWVlvNIA':
        description ='Hot water valve not in auto - this fault is triggered when no considerable heating is observed (discharge air temp + 5 < discharge air temp setpoint) while the heating coil valve is commaneded to open 100%. This could be a result of manual overriding of the cotrol signal'
    elif name == 'HighMinSFanSpeed':
        description ='High minimum fan speed - this fault is triggered when the duct static pressure is greater than the setpoint by 0.15 in wc for atleast 30 min. This could be a result of a higher minimum fan speed setpoint'
    elif name == 'SFanSpeedControlFault':
        description ='Supply fan speed control fault - this fault is triggered when the duct static pressure setpoint is greater than the static pressure by 0.15 in wc for atleast 30 min'
    elif name == 'HighMinRFanSpeed':
        description ='High minimum return fan speed'        
    elif name == 'RFanSpeedControlFault':
        description ='Return fan speed control fault'  
    elif name == 'SF_ShortCycling':
        description = 'Supply fan short cycling - this fault is triggered when the supply fan is truned on and off more than two times per hour'
    elif name == 'RF_ShortCycling':
        description = 'Return fan short cycling - this fault is triggered when the return fan is truned on and off more than two times per hour'
    elif name == 'SimultaneousHeatingandCooling':
        description = 'simultaneous heating and cooling - this fault is triggered when both cooling coil valve and heating coil valve are open (valve position > 0%) at the same time'
    elif name == 'OffHrsOperation':
        description ='Off hours AHU operation - this fault is triggered if the fan is running during an occuppeied periods'  
    elif "tempsensor" in name:
        description = "Out of range temperature reading - this fault is triggered when a temperature sensor is reading a value less that -20oF or greater that 120oF"
    elif "stalled" in name:
        description = "Sesnor reading is stalled - this fault is triggered when a sensor reading is not changing at least for an hour. This could be a result of communication failure"


    insight_json = {
      "floorCode": floor,
      "equipmentId": equipmentId,
      "type": "faultDetection",
      "name": name,
      "description": description,
      "priority": prioritity,
      "state": state,
      "occurredDate": occdate,
      "detectedDate": occdate,
      "occurrenceCount": count
    }
    return insight_json

def active_fault (df,timestamp):
    # this fucntion is to check if the fault has been Active for the last "timestamp" consequetive runs
    if len(df)>timestamp:
        df_last = df.tail(timestamp)
        tot = 0
        for i in df_last:
            if i == 'active':
                tot +=1
        if tot == timestamp:
            return True
        else:
            return False
    else:
        return False
    

def fault_update(fault_name_status, fault_name, ahu, status, secrets_site, willowdbins, requests, headers):
    #updating fault status
    
    if fault_name_status == "inactive": # Closing previously generated insights if the fault doesnt exist anymore

        for inss in willowdbins:  # check if insight exists and update its state if it exists
            if inss['equipmentId'] == ahu['id'] and inss['name'] == fault_name:
                URL = 'https://api.willowinc.com/v2/sites/'+secrets_site+'/insights/'+ inss['id']
                try:
                    response = requests.put(url=URL, headers=headers, json={"state": 'inactive', "Status": "closed"})
                except:
                    print('There was a connection error')
                    continue
    elif status == True:    
        ins_exist = 0
        for inss in willowdbins:  # check if insight exists and update it it does using put
            if inss['equipmentId'] == ahu['id'] and inss['name'] == fault_name :
                URL = 'https://api.willowinc.com/v2/sites/'+secrets_site+'/insights/'+ inss['id']
                try:
                    response = requests.put(url=URL, headers=headers, json={"state": 'active', "occurredDate": datetime.now().strftime("%m/%d/%Y, %H:%M:%S") })
                except:
                    print('There was a connection error')
                    continue
                ins_exist = 1
        if ins_exist == 0: # create a new insight if it has not been created before
            URL = 'https://api.willowinc.com/v2/sites/'+secrets_site+'/insights/'
            insight_json = insights(ahu["floorId"], ahu["id"], fault_name,1, 'active', datetime.now().strftime("%m/%d/%Y, %H:%M:%S"), 1)
            try:
                response = requests.post(url = URL, json=insight_json, headers=headers)
            except:
                print('There was a connection error')
                #continue
                 
            
        