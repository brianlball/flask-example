#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul  1 12:58:52 2021

@author: kwoldeki
"""
import json
import logging
import pdb
import time
import requests, warnings, yaml
import insights_fun as ins
from datetime import datetime, timedelta
import pandas as pd
from pandas.io.json import json_normalize


with open('secrets.yml') as f:
    secrets = yaml.safe_load(f)

URL = 'https://api.willowinc.com/v2/oauth2/token'
credential_json = {"clientId": secrets['clientId'],"clientSecret": secrets['clientSecret']}
headers={'Content-type':'application/json','Accept':'application/json'}
r = requests.post(url=URL, json = credential_json, headers=headers)
#print(r)
#print(r.content)

token = r.json()
access_token = token['accessToken']
URL = 'https://api.willowinc.com/v2/sites'
response = requests.get(url=URL,headers={'Content-Type':'application/json','Authorization': 'Bearer {}'.format(access_token)})
#print(response)
#print(response.content)


##############################################################

faults = pd.DataFrame() #Data Frame for storing Faults

j = 0
while True:
    
    
    ###########reading existing equipments form willow db ####################


    URL = 'https://api.willowinc.com/v2/sites/'+secrets['site']+'/equipments'
    payload={}
    headers = {'Content-Type':'application/json','Authorization': 'Bearer {}'.format(access_token)}
    try:
        response = requests.get(url=URL, headers=headers, data=payload)  
        willowdb = json.loads(response.content)
    except:
        print('There was a connection error')
        continue
    
    
    

#    start_date = (datetime.now() - timedelta(hours = 1)).strftime("%Y-%m-%d %H:%M")
#    end_date = datetime.now().strftime("%Y-%m-%d %H:%M")
#    URL = 'https://api.willowinc.com/v2/sites/'+secrets['site']+'/points/ d6fb1b33-e870-4ca3-b111-59dcd46dc539/trendlog?startDate=' + start_date + '&endDate=' + end_date + '&granularity=P0DT1H'
#    #URL = 'https://api.willowinc.com/v2/sites/'+secrets['site']+'/points/' + '8fe0028d-cc7d-49d4-891c-ed5d15e91810' + '/trendlog?startDate=' + start_date + '&endDate=' + end_date + '&granularity=P0DT1H'
#
#    response = requests.get(url=URL, headers=headers, data=payload)  
#    
#    id_hist = json_normalize(json.loads(response.text)["data"])
#    
#    pdb.set_trace()
    ######## reading existing insights ############################
    
    URL = 'https://api.willowinc.com/v2/sites/'+secrets['site']+'/insights'
    payload={}
    headers = {'Content-Type':'application/json','Authorization': 'Bearer {}'.format(access_token)}
    try:
        response = requests.get(url=URL, headers=headers, data=payload)  
        willowdbins = json.loads(response.content)
    except:
        print('There was a connection error')
        continue


    #######################################################################################################
    ################# Data Analysis and FDD Algorithms ##################################################
    #######################################################################################################

    
    ahus = [x for x in willowdb if x['categoryName'] == 'Air Handling Unit']
    for ahu in ahus:
        #print(ahu['id'])
        
        ####### sensor points ############
        #### temp sensors

        
        SAT = [x['currentValue']['value'] for x in ahu['points'] if (x['name'] == 'discharge air temp sensor' and len (x['currentValue']) > 0)]
        MAT = [x['currentValue']['value'] for x in ahu['points'] if (x['name'] == 'mixed air temp sensor' and len (x['currentValue']) > 0)]
        RAT = [x['currentValue']['value'] for x in ahu['points'] if (x['name'] == 'return air temp sensor' and len (x['currentValue']) > 0)]
        
        
        ZoneT = [x['currentValue']['value'] for x in ahu['points'] if (x['name'] ==  'zone air temp sensor' and len (x['currentValue']) > 0)]
        
    
        ChWST = [x['currentValue']['value'] for x in ahu['points'] if (x['name'] ==  'chilled water entering temp sensor' and len (x['currentValue']) > 0)]
        ChWRT = [x['currentValue']['value'] for x in ahu['points'] if (x['name'] ==  'chilled water leaving temp sensor' and len (x['currentValue']) > 0)]

    
        HWST = [x['currentValue']['value'] for x in ahu['points'] if (x['name'] ==  'hot water entering temp sensor' and len (x['currentValue']) > 0)]
        HWRT = [x['currentValue']['value'] for x in ahu['points'] if (x['name'] ==  'hot water leaving temp sensor' and len (x['currentValue']) > 0)]
        # status sensors
        SFSts = [x['currentValue']['value'] for x in ahu['points'] if (x['name'] ==  'discharge air fan run sensor' and len (x['currentValue']) > 0)]
        RFSts = [x['currentValue']['value'] for x in ahu['points'] if (x['name'] ==  'return air fan run sensor' and len (x['currentValue']) > 0)]
        
    
        # Pressure sensors
        SAPres = [x['currentValue']['value'] for x in ahu['points'] if (x['name'] == 'discharge air pressure sensor' and len (x['currentValue']) > 0)]
        SAPres_trendId = [x['trendId'] for x in ahu['points'] if (x['name'] ==  'discharge air pressure sensor' and len (x['id']) > 0)]

        RAPres = [x['currentValue']['value'] for x in ahu['points'] if (x['name'] == 'return air pressure sensor' and len (x['currentValue']) > 0)]
        RAPres_trendId = [x['trendId'] for x in ahu['points'] if (x['name'] ==  'return air pressure sensor' and len (x['id']) > 0)]

    
        ###### control outputs ##############
    
        ChWVlvs = [x['currentValue']['value'] for x in ahu['points'] if  ('cooling valve' in x['name'] and len (x['currentValue']) > 0)] # this is in case the AHU has multiple chilled water valve
        HWVlvs = [x['currentValue']['value'] for x in ahu['points'] if  ('heating valve' in x['name'] and len (x['currentValue']) > 0)] # this is in case the AHU has multiple hot water valve
    
        SFCmd = [x['currentValue']['value'] for x in ahu['points'] if (x['name'] ==  'discharge air fan start cmd' and len (x['currentValue']) > 0)]
        RFCmd = [x['currentValue']['value'] for x in ahu['points'] if (x['name'] ==  'return air fan start cmd' and len (x['currentValue']) > 0)]
        
        SFVFDCmd = [x['currentValue']['value'] for x in ahu['points'] if (x['name'] ==  'discharge air fan speed cmd' and len (x['currentValue']) > 0)]
        RFVFDCmd = [x['currentValue']['value'] for x in ahu['points'] if (x['name'] ==  'return air fan speed cmd' and len (x['currentValue']) > 0)]
        
        RDCmd =  [x['currentValue']['value'] for x in ahu['points'] if (x['name'] == 'return air damper cmd' and len (x['currentValue']) > 0)]
        OccCmd = [x['currentValue']['value'] for x in ahu['points'] if (x['name'] == 'occ cmd' and len (x['currentValue']) > 0)]
    
        ######## setpoints ##############
        SATSp = [x['currentValue']['value'] for x in ahu['points'] if ('discharge air temp sp' in x['name'] and len (x['currentValue']) > 0)]
        MATSp = [x['currentValue']['value'] for x in ahu['points'] if ('mixed air temp sp' in x['name'] and len (x['currentValue']) > 0)]

        ClgSp = [x['currentValue']['value'] for x in ahu['points'] if ('occ cooling temp sp' in x['name'] and len (x['currentValue']) > 0)]
        HtgSp = [x['currentValue']['value'] for x in ahu['points'] if ('occ heating temp sp' in x['name'] and len (x['currentValue']) > 0)]
                        
        SAPresSp = [x['currentValue']['value'] for x in ahu['points'] if ('discharge air pressure sp' in x['name'] and len (x['currentValue']) > 0)]
        RAPresSp = [x['currentValue']['value'] for x in ahu['points'] if ('return air pressure sp' in x['name'] and len (x['currentValue']) > 0)]
        
        
        # requesting historical data
        SFSts_trendId = [x['trendId'] for x in ahu['points'] if (x['name'] ==  'discharge air fan run sensor' and len (x['trendId']) > 0)]
        RFSts_trendId = [x['trendId'] for x in ahu['points'] if (x['name'] ==  'return air fan run sensor' and len (x['trendId']) > 0)]

       
        #############################################################
        ######  Rule Based Faults ###################################
        #############################################################       
        
        
        faults.at[j,'timeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M") 
        
            
        ########### temp sensor faults and stalling faults ##############################
        Temp_sensors =  [x for x in ahu['points'] if 'temp sensor' in x['name']] # filtering air temp sensor values
        for Temp_sensor in Temp_sensors:
            
            # Temperare out of range value reading faults
            temp_sensor_name = Temp_sensor['name']
            temp_sensor_name = temp_sensor_name.replace(' ','')
            temp_sensor_name = temp_sensor_name.replace('point','')
            fault_name = temp_sensor_name + ' fault'
            if len(Temp_sensor['currentValue']) > 0 and (Temp_sensor['currentValue']['value'] > 120 or Temp_sensor ['currentValue']['value'] < -20): 
                fault_name_status = "active"
            else:
                fault_name_status= "inactive"
                
            faults.at[j,fault_name + '_' + ahu['name']] = fault_name_status   
            status = ins.active_fault(faults[fault_name + '_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 
            
            #updating fault status
            ins.fault_update(fault_name_status, fault_name, ahu, status, secrets['site'], willowdbins, requests, headers)
            
            
            # temperaure sensor stalled fault
            tempsensor_trendId = [x['trendId'] for x in ahu['points'] if (x['name'] ==  Temp_sensor['name'] and len (x['trendId']) > 0)]
            if len (tempsensor_trendId)> 0:
                temp_sensor_name = [x['name'] for x in ahu['points'] if x['trendId'] ==  tempsensor_trendId[0]][0]
                fault_name = temp_sensor_name + '_stalled fault'
                start_date = (datetime.now() - timedelta(hours = 1)).strftime("%Y-%m-%d %H:%M")
                end_date = datetime.now().strftime("%Y-%m-%d %H:%M")
                URL = 'https://api.willowinc.com/v2/sites/'+secrets['site']+'/points/' + tempsensor_trendId[0] + '/trendlog?startDate=' + start_date + '&endDate=' + end_date + '&granularity=P0DT1H'
    
                response = requests.get(url=URL, headers=headers, data=payload)  
                
                id_hist = json_normalize(json.loads(response.text)["data"])
                count = 1
                if len (id_hist) > 0:
                    for i in range(0,len(id_hist)):
                        if i>0:
                            if id_hist.value[i-1]==id_hist.value[i]:
                                count+=1
                        
                    if count == len((id_hist)): # fault will be active if the value doesnt change for the last one hour
                        SensorStalled_status = 'active'
                        status = True
                    else:
                        SensorStalled_status = 'inactive'
                        status = False
                else:
                    SensorStalled_status = 'inactive'
                    status = False
                    
                faults.at[j,fault_name + '_' + ahu['name']] = SensorStalled_status    
       
                # updating fault status
                ins.fault_update(SensorStalled_status, fault_name, ahu, status, secrets['site'], willowdbins, requests, headers)
        
        
        
        #############  pressure Sensors Stalling Faults #############
        
        sensor_ids = [RAPres_trendId,SAPres_trendId] # sensors that are expected to change frequently
        for ids in sensor_ids:
            
            if len (ids)> 0:
                temp_sensor_name = [x['name'] for x in ahu['points'] if x['trendId'] ==  ids[0]][0]
                fault_name = temp_sensor_name + '_stalled fault'
                start_date = (datetime.now() - timedelta(hours = 1)).strftime("%Y-%m-%d %H:%M")
                end_date = datetime.now().strftime("%Y-%m-%d %H:%M")
                URL = 'https://api.willowinc.com/v2/sites/'+secrets['site']+'/points/' + ids[0] + '/trendlog?startDate=' + start_date + '&endDate=' + end_date + '&granularity=P0DT1H'
                #URL = 'https://api.willowinc.com/v2/sites/'+secrets['site']+'/points/' + '8fe0028d-cc7d-49d4-891c-ed5d15e91810' + '/trendlog?startDate=' + start_date + '&endDate=' + end_date + '&granularity=P0DT1H'
    
                response = requests.get(url=URL, headers=headers, data=payload)  
                
                id_hist = json_normalize(json.loads(response.text)["data"])
                count = 1
                if len (id_hist) > 0:
                    for i in range(0,len(id_hist)):
                        if i>0:
                            if id_hist.value[i-1]==id_hist.value[i]:
                                count+=1
                        
                    if count == len((id_hist)): # fault will be active if the value doesnt change for the last one hour
                        SensorStalled_status = 'active'
                        status = True
                    else:
                        SensorStalled_status = 'inactive'
                        status = False
                else:
                    SensorStalled_status = 'inactive'
                    status = False
                    
                faults.at[j,fault_name + '_' + ahu['name']] = SensorStalled_status    
       
                # updating fault status
                ins.fault_update(SensorStalled_status, fault_name, ahu, status, secrets['site'], willowdbins, requests, headers)
                
                

        ################ Short cycling Faults ###########################
        if len (SFSts_trendId)> 0:
            start_date = (datetime.now() - timedelta(hours = 1)).strftime("%Y-%m-%d %H:%M")
            end_date = datetime.now().strftime("%Y-%m-%d %H:%M")
            URL = 'https://api.willowinc.com/v2/sites/'+secrets['site']+'/points/' + SFSts_trendId[0] + '/trendlog?startDate=' + start_date + '&endDate=' + end_date + '&granularity=P0DT1H'
            #URL = 'https://api.willowinc.com/v2/sites/'+secrets['site']+'/points/' + '8fe0028d-cc7d-49d4-891c-ed5d15e91810' + '/trendlog?startDate=' + start_date + '&endDate=' + end_date + '&granularity=P0DT1H'

            response = requests.get(url=URL, headers=headers, data=payload)  
            
            SFSts_hist = json_normalize(json.loads(response.text)["data"])
            if len(SFSts_hist) > 0:
                count = 0
                for i in range(0,len(SFSts_hist)):
                    if i>0:
                        if SFSts_hist.value[i-1]!=SFSts_hist.value[i]:
                            count+=1
                    
                if count > 2: # fault will be active if the fan cycle on and off more than 2 times in an hour
                    SF_ShortCycling = 'active'
                    status = True
                else:
                    SF_ShortCycling = 'inactive'
                    status = False
            else:  
                SF_ShortCycling = 'inactive'
                status = False
                
            faults.at[j,'SF_ShortCycling_' + ahu['name']] = SF_ShortCycling   
   
            # updating fault status
            ins.fault_update(SF_ShortCycling, 'SF_ShortCycling', ahu, False, secrets['site'], willowdbins, requests, headers)


        if len (RFSts_trendId)> 0:
            start_date = (datetime.now() - timedelta(hours = 1)).strftime("%Y-%m-%d %H:%M")
            end_date = datetime.now().strftime("%Y-%m-%d %H:%M")
            URL = 'https://api.willowinc.com/v2/sites/'+secrets['site']+'/points/' + RFSts_trendId[0] + '/trendlog?startDate=' + start_date + '&endDate=' + end_date + '&granularity=P0DT1H'
            #URL = 'https://api.willowinc.com/v2/sites/'+secrets['site']+'/points/' + '8fe0028d-cc7d-49d4-891c-ed5d15e91810' + '/trendlog?startDate=' + start_date + '&endDate=' + end_date + '&granularity=P0DT1H'

            response = requests.get(url=URL, headers=headers, data=payload)  
            
            RFSts_hist = json_normalize(json.loads(response.text)["data"])
            if len(RFSts_hist) > 0:
                count = 0
                for i in range(0,len(RFSts_hist)):
                    if i>0:
                        if RFSts_hist.value[i-1]!=RFSts_hist.value[i]:
                            count+=1
                if count > 2: # fault will be active if the fan cycle on and off more than 2 times in an hour
                    RF_ShortCycling = 'active'
                    status = True
                else:
                    RF_ShortCycling = 'inactive'
                    status = False
            else:
                RF_ShortCycling = 'inactive'
                status = False
            faults.at[j,'RF_ShortCycling_' + ahu['name']] = RF_ShortCycling   
            
            # updating fault status
            ins.fault_update(RF_ShortCycling, 'RF_ShortCycling', ahu, status, secrets['site'], willowdbins, requests, headers)




        ####### Space Overcooling Fault  ##################
        if len(SFSts) > 0 and len(ClgSp) > 0 and len(ZoneT) > 0:
            if SFSts[0] == True and (ClgSp[0] - ZoneT[0]) > 5:
                SpaceOvercooling = "active"
            else:
                SpaceOvercooling = "inactive"
            faults.at[j,'SpaceOvercooling_' + ahu['name']] = SpaceOvercooling   
            status = ins.active_fault(faults['SpaceOvercooling_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 

            #updating fault status
            ins.fault_update(SpaceOvercooling, "SpaceOvercooling", ahu, status, secrets['site'], willowdbins, requests, headers)

    
    
        ######## Space Overheating Fault  #################
        if len(SFSts) > 0 and len(HtgSp) > 0 and len(ZoneT) > 0:
            if SFSts[0] == True and (ZoneT[0] - HtgSp[0]) > 5:
                SpaceOverheating = "active"
            else:
                SpaceOverheating = "inactive"
            faults.at[j,'SpaceOverheating_'+ ahu['name']] = SpaceOverheating 
            status = ins.active_fault (faults['SpaceOverheating_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 

            #updating fault status
            ins.fault_update(SpaceOverheating, "SpaceOverheating", ahu, status, secrets['site'], willowdbins, requests, headers)

   
    
        ######## Supply fan not it auto  ###################
        if len(SFSts) > 0 and len(SFCmd) > 0:
            if SFSts[0] == True and SFCmd[0]  == False:
                SFNIA = "active"
            else:
                SFNIA = "inactive"
            faults.at[j,'SFNIA_'+ ahu['name']] = SFNIA 
            status = ins.active_fault (faults['SFNIA_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 

            #updating fault status
            ins.fault_update(SFNIA, "SFNIA", ahu, status, secrets['site'], willowdbins, requests, headers)

    
        ######## Return fan not it auto -4- ###################
        if len(RFSts) > 0 and len(RFCmd) > 0:
            if RFSts[0] == True and RFCmd[0]  == False:
                RFNIA = "active"
            else:
                RFNIA = "inactive"
            faults.at[j,'RFNIA_'+ ahu['name']] = RFNIA
            status = ins.active_fault (faults['RFNIA_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 

            #updating fault status
            ins.fault_update(RFNIA, "RFNIA", ahu, status, secrets['site'], willowdbins, requests, headers)

   
        ######## Chilled water valve related faults ###############
        if len(ChWVlvs) > 0: # checking if there is atleast on cooling coil valve for the AHU
            chwValveOpening = 0
            for chwVlv in ChWVlvs:
                chwValveOpening += chwVlv # a value = zero confirms that all chilled water valves are closed
    
            ######## chilled water valve leaking fault  #################
            # chilled water valve leaking fault detection using mixed and discharge air temp
            if len(SFSts) > 0 and len(MAT) > 0 and len(SAT) > 0:
                if SFSts[0] == True and chwValveOpening == 0 and (MAT[0] - SAT[0]) > 5:
                    ChilledWaterValveLeaking = "active"
                else:
                    ChilledWaterValveLeaking = "inactive"
  
                faults.at[j,'ChilledWaterValveLeaking_'+ ahu['name']] = ChilledWaterValveLeaking    
                status = ins.active_fault (faults['ChilledWaterValveLeaking_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 

                #updating fault status
                ins.fault_update(ChilledWaterValveLeaking, "ChilledWaterValveLeaking", ahu, status, secrets['site'], willowdbins, requests, headers)

    
            # chilled water valve leaking fault detection using return damper opening, retrun temp and discharge air temp
            elif len(SFSts) > 0 and len(RDCmd) > 0 and len(RAT) > 0 and len(SAT) > 0:
                if SFSts[0] == True and RDCmd[0] == 100 and chwValveOpening == 0 and (RAT[0] - SAT[0]) > 5:
                    ChilledWaterValveLeaking = "active"
                else:
                    ChilledWaterValveLeaking = "inactive"
                    
                faults.at[j,'ChilledWaterValveLeaking_'+ ahu['name']] = ChilledWaterValveLeaking  
                status = ins.active_fault (faults['ChilledWaterValveLeaking_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 

                #updating fault status
                ins.fault_update(ChilledWaterValveLeaking, "ChilledWaterValveLeaking", ahu, status, secrets['site'], willowdbins, requests, headers)


            else:
                logging.debug("the required points don't exist for applying chilled water valve leaking fault")
    
            ######### chilled water valve stuck closed fault  ######################
            # chilled water valve stuck closed fault detection using mixed and discharge air temp
            if len(SFSts) > 0 and len(MAT) > 0 and len(SAT) > 0:
                if SFSts[0] == True and chwValveOpening >30 and (MAT[0] - SAT[0]) < 5:
                    ChilledWaterValveSClosed = "active"
                else:
                    ChilledWaterValveSClosed = "inactive"
                    startTime_chwvscl = datetime.now()
                faults.at[j,'ChilledWaterValveSClosed_'+ ahu['name']] = ChilledWaterValveSClosed  
                status = ins.active_fault (faults['ChilledWaterValveSClosed_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 


                #updating fault status
                ins.fault_update(ChilledWaterValveSClosed, "ChilledWaterValveSClosed", ahu, status, secrets['site'], willowdbins, requests, headers)

                   
                   
            # chilled water valve leaking fault detection using return damper opening, retrun temp and discharge air temp
            elif len(SFSts) > 0 and len(RDCmd) > 0 and len(RAT) > 0 and len(SAT) > 0:
                if SFSts[0] == True and RDCmd[0] == 100 and chwValveOpening >30 and (RAT[0] - SAT[0]) < 5:
                    ChilledWaterValveSClosed = "active"
                else:
                    ChilledWaterValveSClosed = "inactive"
                    startTime_chwvscl = datetime.now()
                    
                faults.at[j,'ChilledWaterValveSClosed_'+ ahu['name']] = ChilledWaterValveSClosed 
                status = ins.active_fault (faults['ChilledWaterValveSClosed_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 


                #updating fault status
                ins.fault_update(ChilledWaterValveSClosed, "ChilledWaterValveSClosed", ahu, status, secrets['site'], willowdbins, requests, headers)
            else:
                logging.debug("the required points dont exist for applying chilled water valve stuck closed fault")
    
            ########## AHU Overcooling  #######################
            if len(SFSts) > 0 and len(SAT) > 0 and len(SATSp) > 0:
                
                if SFSts[0] == True  and chwValveOpening >10 and (SATSp[0] - SAT[0]) > 5:
                    AHUOvercooling = "active"
                else:
                    AHUOvercooling = "inactive"
                    startTime_ahuoc  = datetime.now()
                    
                faults.at[j,'AHUOvercooling_' + ahu['name']] = AHUOvercooling 
                status = ins.active_fault (faults['AHUOvercooling_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 
               
                
                #updating fault status
                ins.fault_update(AHUOvercooling, "AHUOvercooling", ahu, status, secrets['site'], willowdbins, requests, headers)




            ########## Chilled Water Valve Not in Auto #######################
            if len(SFSts) > 0 and len(SAT) > 0 and len(SATSp) > 0:
                if SFSts[0] == True  and chwValveOpening == 100 and (SAT[0] - SATSp[0]) > 5:
                    ChWVlvNIA = "active"
                else:
                    ChWVlvNIA = "inactive"
                    
                faults.at[j,'ChWVlvNIA_'+ ahu['name']] = ChWVlvNIA 
                status = ins.active_fault (faults['ChWVlvNIA_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 
                
                #updating fault status
                ins.fault_update(ChWVlvNIA, "ChWVlvNIA", ahu, status, secrets['site'], willowdbins, requests, headers)

    
        ######## Hot water valve related faults ###############
        if len(HWVlvs) > 0: # checking if there is atleast one heating coil valve for the AHU
            hwValveOpening = 0
            for hwVlv in HWVlvs:
                hwValveOpening += hwVlv # a value = zero confirms that all hot water valves are closed
    
            ######## hot water valve leaking fault #################
            # hot water valve leaking fault detection using mixed and discharge air temp
            if len(SFSts) > 0 and len(MAT) > 0 and len(SAT) > 0:
                if SFSts[0] == True and hwValveOpening == 0 and (SAT[0] - MAT[0]) > 5:
                    HotWaterValveLeaking = "active"
                else:
                    HotWaterValveLeaking = "inactive"
                    
                faults.at[j,'HotWaterValveLeaking_' + ahu['name']] = HotWaterValveLeaking 
                status = ins.active_fault (faults['HotWaterValveLeaking_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 
                
                #updating fault status
                ins.fault_update(HotWaterValveLeaking, "HotWaterValveLeaking", ahu, status, secrets['site'], willowdbins, requests, headers)


            # hot water valve leaking fault detection using return damper opening, retrun temp and discharge air temp
            elif len(SFSts) > 0 and len(RDCmd) > 0 and len(RAT) > 0 and len(SAT) > 0:
                if SFSts[0] == True and RDCmd[0] == 100 and hwValveOpening == 0 and (SAT[0] - RAT[0]) > 5:
                    HotWaterValveLeaking = "active"
                else:
                    HotWaterValveLeaking = "inactive"
                    
                faults.at[j,'HotWaterValveLeaking_'+ ahu['name']] = HotWaterValveLeaking
                status = ins.active_fault (faults['HotWaterValveLeaking_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 
                
                #updating fault status
                ins.fault_update(HotWaterValveLeaking, "HotWaterValveLeaking", ahu, status, secrets['site'], willowdbins, requests, headers)

            else:
                logging.debug("the required points don't exist for applying hot water valve leaking fault")
    
            ######### hot water valve stuck closed fault ######################
            # hot water valve stuck closed fault detection using mixed and discharge air temp
            if len(SFSts) > 0 and len(MAT) > 0 and len(SAT) > 0:
                if SFSts[0] == True and hwValveOpening >30 and (SAT[0] - MAT[0]) < 5:
                    HotWaterValveSClosed = "active"
                else:
                    HotWaterValveSClosed = "inactive"
                    
                faults.at[j,'HotWaterValveSClosed_' + ahu['name']] = HotWaterValveSClosed
                status = ins.active_fault (faults['HotWaterValveSClosed_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 
                
                #updating fault status
                ins.fault_update(HotWaterValveSClosed, "HotWaterValveSClosed", ahu, status, secrets['site'], willowdbins, requests, headers)

                    
            # hot water valve leaking fault detection using return damper opening, retrun temp and discharge air temp
            elif len(SFSts) > 0 and len(RDCmd) > 0 and len(RAT) > 0 and len(SAT) > 0:
                if SFSts[0] == True and RDCmd == 100 and hwValveOpening >30 and (SAT[0] - RAT[0]) < 5:
                    HotWaterValveSClosed = "active"
                else:
                    HotWaterValveSClosed = "inactive"
                    
                faults.at[j,'HotWaterValveSClosed_'+ ahu['name']] = HotWaterValveSClosed
                status = ins.active_fault (faults['HotWaterValveSClosed_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 
                
                #updating fault status
                ins.fault_update(HotWaterValveSClosed, "HotWaterValveSClosed", ahu, status, secrets['site'], willowdbins, requests, headers)
                
                   
            else:
                logging.debug("the required points dont exist for applying hot water valve stuck closed fault")
            
            
            
            ##### simultaneous coooling and heating ##################
            
            if len(HWVlvs) > 0 and len(ChWVlvs) > 0:
                chwValveOpening = 0
                for chwVlv in ChWVlvs:
                    chwValveOpening += chwVlv # a value = zero confirms that all chilled water valves are closed. This check is if in case multiple cooling coils exist
                hwValveOpening = 0
                for hwVlv in HWVlvs:
                    hwValveOpening += hwVlv # a value = zero confirms that all chilled water valves are closed. This check is if in case multiple cooling coils exist
                
                if chwValveOpening > 0 and hwValveOpening > 0:
                    SimultaneousHeatingandCooling = "active"
                else:
                    SimultaneousHeatingandCooling = "inactive"
                
                faults.at[j,'SimultaneousHeatingandCooling_'+ ahu['name']] = SimultaneousHeatingandCooling
                status = ins.active_fault (faults['SimultaneousHeatingandCooling_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 
                
                #updating fault status
                ins.fault_update(SimultaneousHeatingandCooling, "SimultaneousHeatingandCooling", ahu, status, secrets['site'], willowdbins, requests, headers)

                    
                
            ########## AHU Oveheating #######################
            if len(SFSts) > 0 and len(SAT) > 0 and len(SATSp) > 0 :
                if SFSts[0] == True and hwValveOpening >10 and (SAT[0] - SATSp[0]) > 5:
                    AHUOverheating = "active"
                else:
                    AHUOverheating = "inactive"
                    
                faults.at[j,'AHUOverheating_'+ ahu['name']] = AHUOverheating
                status = ins.active_fault (faults['AHUOverheating_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 
                
                #updating fault status
                ins.fault_update(AHUOverheating, "AHUOverheating", ahu, status, secrets['site'], willowdbins, requests, headers)

                   
            ########## Hot Water Valve Not in Auto  #######################
            if len(SFSts) > 0 and len(SAT) > 0 and len(SATSp) > 0:
                if SFSts[0] == True  and hwValveOpening ==100 and (SATSp[0] - SAT[0]) > 5:
                    HWVlvNIA = "active"
                else:
                    HWVlvNIA = "inactive"
                    
                faults.at[j,'HWVlvNIA_'+ ahu['name']] = HWVlvNIA
                status = ins.active_fault (faults['HWVlvNIA_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 
                
                #updating fault status
                ins.fault_update(HWVlvNIA, "HWVlvNIA", ahu, status, secrets['site'], willowdbins, requests, headers)


    
        ########## High Min Supply Fan Speed fault  #####################
        if len(SFSts) > 0 and len(SAPresSp) > 0 and len(SAPres) > 0 and len(SFVFDCmd) > 0:
            if SFSts[0] == True and (SAPres[0] - SAPresSp[0]) > 0.15: # please note that press is in "in Wc"
                HighMinSFanSpeed = "active"
            else:
                HighMinSFanSpeed = "inactive"
                
            faults.at[j,'HighMinSFanSpeed_'+ ahu['name']] = HighMinSFanSpeed
            status = ins.active_fault (faults['HighMinSFanSpeed_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 
            
            #updating fault status
            ins.fault_update(HighMinSFanSpeed, "HighMinSFanSpeed", ahu, status, secrets['site'], willowdbins, requests, headers)

                    
    
        ########## Fan Speed Control fault #####################
        if len(SFSts) > 0 and len(SAPresSp) > 0 and len(SAPres) > 0 and len(SFVFDCmd) > 0:
            if SFSts[0] == True and (SAPresSp[0] - SAPres[0]) > 0.15: # please note that press is in "in Wc"
                SFanSpeedControlFault = "active"
            else:
                SFanSpeedControlFault = "inactive"
                
            faults.at[j,'SFanSpeedControlFault_'+ ahu['name']] = SFanSpeedControlFault 
            status = ins.active_fault (faults['SFanSpeedControlFault_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 
            
            #updating fault status
            ins.fault_update(SFanSpeedControlFault, "SFanSpeedControlFault", ahu, status, secrets['site'], willowdbins, requests, headers)

                
        ########## High Min Return Fan Speed fault  #####################
        if len(RFSts) > 0 and len(RAPresSp) > 0 and len(RAPres) > 0 and len(RFVFDCmd) > 0:
            if RFSts[0] == True and (RAPres[0] - RAPresSp[0]) > 0.15: # please note that press is in "in Wc"
                HighMinRFanSpeed = "active"
            else:
                HighMinRFanSpeed = "inactive"

                
            faults.at[j,'HighMinRFanSpeed_'+ ahu['name']] = HighMinRFanSpeed
            status = ins.active_fault (faults['HighMinRFanSpeed_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 
            
            #updating fault status
            ins.fault_update(HighMinRFanSpeed, "HighMinRFanSpeed", ahu, status, secrets['site'], willowdbins, requests, headers)


        ########## High Min Return Fan Speed fault  #####################
        if len(RFSts) > 0 and len(RAPresSp) > 0 and len(RAPres) > 0 and len(RFVFDCmd) > 0:
            if RFSts[0] == True and (RAPresSp[0] - RAPres[0]) > 0.15: # please note that press is in "in Wc"
                RFanSpeedControlFault = "active"
            else:
                RFanSpeedControlFault = "inactive"

            faults.at[j,'RFanSpeedControlFault_' + ahu['name']] = RFanSpeedControlFault
            status = ins.active_fault (faults['RFanSpeedControlFault_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 
            
            #updating fault status
            ins.fault_update(RFanSpeedControlFault, "RFanSpeedControlFault", ahu, status, secrets['site'], willowdbins, requests, headers)


                
        ##################               Off Hours Operation          #######################
        if len(SFSts) > 0 and len(OccCmd) > 0:
            if OccCmd == 0 and SFSts > 1:
                OffHrsOperation = "active"
            else:
                OffHrsOperation = "inactive"
            
            faults.at[j,'OffHrsOperation_' + ahu['name']] = OffHrsOperation
            status = ins.active_fault (faults['OffHrsOperation_' + ahu['name']],4) # check if the fault has been active for x (4- adjustable)number of runs 
            
            #updating fault status
            ins.fault_update(OffHrsOperation, "OffHrsOperation", ahu, status, secrets['site'], willowdbins, requests, headers)

        
    if len(faults)>10:
        faults = faults.iloc[1:,:] # droping the first row of the 'Faults' data frame when its length is greater than 10 (adjustable)
    print(j)
    j +=1 
    #time.sleep(5*30)   # sleeping for 5 min before the next scan   
