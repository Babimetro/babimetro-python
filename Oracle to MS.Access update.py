#==========================================
#Imports
#===============================================
import pandas as pd
import numpy as np
import datetime
from pandas import DataFrame, Series
from pandas import ExcelWriter
from pandas import DataFrame, Series
import csv
import sys,time
import math
import cx_Oracle
import pyodbc
#===============================================
#loop function
#============================================
def my_range(start, end, step):
    while start <= end:
        yield start
        start += step

#===============================================
#A function for progress bar
#===============================================
def progressBar(count,total,suffix=''):
    barlenght=60
    filledlenght=int(round(barlenght*count/float(total)))
    percent=round(100.0*count/float(total),1)
    bar='O'*filledlenght+'-'*(barlenght-filledlenght)
    sys.stdout.write('[%s] %s%s...%s\r' %(bar,percent,'%',suffix))
    sys.stdout.flush()
#===============================================	
#Connection to Atoll	
#===============================================	
dsn_tns = cx_Oracle.makedsn('10.132.59.100','1521','ATOLLDB')
conn = cx_Oracle.connect(user='FACTS_GSM', password='atoll', dsn=dsn_tns) 
  
CF_LTE_BAND=3500 
print('Hi there')
print('Whats up, Atoll jan please give me TDD informtion')

Site_query ="""SELECT D.NAME,D.LONGITUDE,D.LATITUDE,D.ALTITUDE,D.COMMENT_,D.CF_CITY_ROAD_NAME,D.CF_CLUSTER_REFERENCE,D.CF_2G_STATUS,D.CF_RELOCATION_SITE,D.CF_NAME,D.CF_PROVINCE_NAME,D.CF_RADIO_SITE_TYPE,D.CF_NETWORK_REGION,D.CF_GIS_SITE_TYPE,D.CF_BTS_TECH_CURRENT,D.CF_COLLOCATION_BY,D.CF_ADDRESS1,D.CF_ADDRESS2,D.CLUTTER_CLASS_NAME,D.CLUTTER_CLASS_HEIGHT,D.ALTITUDE_DTM,D.CF_CONFIG_CURRENT_TDD,D.CF_CONFIG_FINAL_TDD_2016,D.CF_TOWER_NUMBERS,D.CF_2ND_SUPPORT_HEIGHT,D.CF_2ND_SUPPORT_TYPE,D.CF_TDD_BTS_TYPE,D.CF_TDD_CLUSTER,D.CF_TDD_PHASE,D.CF_TDD_EQUIPMENT_VENDOR,D.CF_TDD_TAC,D.CF_TDD_MME,D.CF_TDD_SGW,D.CF_TDD3500_STATUS,D.CF_CONFIG_FINAL_TDD_2017,D.CF_WIFI_STATUS,D.CF_PLANNED_ACCESS_POINTS,D.CF_ONAIR_ACCESS_POINTS,D.CF_WIFI_CONFIGURATION,D.CF_WIFI_SERVICE_TYPE,D.CF_DOWNSITE_COMMENT,D.CF_CONFIG_FINAL_TDD_2018,D.CLUTTER_HEIGHT_DHM,D.CF_CONFIG_FINAL_TDD_2019 FROM ATOLL_MRAT.sites D where D.CF_TDD3500_STATUS <> '---'"""


Transmitter_query ="""SELECT  B.cell_id as Transmitter, B.PHY_CELL_ID as PCI, A.site_name as Site_ID, D.Longitude, D.Latitude,D.cf_city_road_name as city, D.CF_PROVINCE_NAME as province, D.CF_GIS_Site_Type as GIS_Type, D.CF_TDD_CLUSTER as TDD_cluster_reference, A.CF_MUN_REGION as Municipality_Region,D.CF_NETWORK_REGION as Region, D.CF_TDD_EQUIPMENT_VENDOR as Vendor, D.CF_PRIORITY as Priority,
   D.Cf_Address1 as Address1, D.Cf_Address2 as Address2, D.Cf_Name as Site_Name, D.Cf_Radio_Site_Type as Site_Type, D.Cf_Site_Install_Type as Site_Install_Type, D.Cf_BTS_Tech_Plan2018 as Tech_plan_Final,
   A.CF_LTE_Band as Frequency_Band, A.CF_Status as Status, A.ANTENNA_NAME, A.HEIGHT, A.Azimut as Azimuth,
   C.electrical_tilt, A.TILT as Mechanical_tilt, c.beamwidth as Horizental_Beamwidth, c.cf_vbeamwidth as Vertical_Beamwidth,
   A.Feeder_Name as feeder FROM ATOLL_MRAT.ltransmitters A, ATOLL_MRAT.lcells B, ATOLL_MRAT.antennas C, ATOLL_MRAT.sites D where   cell_id like 'D%' and A.TX_ID=B.tx_id and A.antenna_name=C.name and A.Site_Name=D.NAME"""
#===============================================	
#Site and Transmitter Query from Atoll	
#=============================================== 
tdd_site = pd.read_sql(Site_query, conn)
tdd_transmitter=pd.read_sql(Transmitter_query, conn)
print("Site Number"+str(len(tdd_site)))
print("Transmitter Number"+str(len(tdd_transmitter)))

print('Thank you, Query Done')
tdd_site.to_csv(r'C:\Applications\temp\Site.csv')
tdd_transmitter.to_csv(r'C:\Applications\temp\Transmitter.csv')
print('\n')
print(' Site and transmitter from atoll Wrote on HD\n')
conn.close()

#===============================================	
#File Load from Atoll	
#===============================================



site = pd.read_csv(r'C:\Applications\temp\Site.csv',low_memory=False)
Tx = pd.read_csv(r'C:\Applications\temp\Transmitter.csv',low_memory=False)
 
print('File loaded.')



site['NAME'].replace(regex=True,inplace=True,to_replace='X',value=r'')

Tx["SITE_ID"].replace(regex=True,inplace=True,to_replace='X',value=r'')


Tx['ELECTRICAL_TILT']=Tx['ELECTRICAL_TILT'].astype('float64', errors='ignore')
Tx['MECHANICAL_TILT']=Tx['MECHANICAL_TILT'].astype('float64', errors='ignore')
Tx['PCI']=Tx['PCI'].astype('float64', errors='ignore')
Tx['LATITUDE']=Tx['LATITUDE'].astype('float64', errors='ignore')
Tx['LONGITUDE']=Tx['LONGITUDE'].astype('float64', errors='ignore')
Tx['AZIMUTH']=Tx['AZIMUTH'].astype('float64', errors='ignore')
Tx['HEIGHT']=Tx['HEIGHT'].astype('float64', errors='ignore')

#==========================
#Site Add From Atoll to FIBOT
#========================
print(datetime.datetime.now())
suc=0
fail=0
connection = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=R:\FIBOT_DB.accdb;PWD=babimetro;')
le=len(site)
print('lent:'+str(le))
sql="""insert into [site list](SiteName,Technology,Status,2G_Status,Planned_Config,Current_Config,Latitude,Longitude,New_Region
,Province,City,Cluster,[City Type],phase,[TDD Equipment vendor],[Collocation Comment]) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
for x in my_range(1, le-1, 1):
    progressBar(round((x*100/le),1),100) 
    try :
        crsr = connection.execute(sql,(site['NAME'][x],'TDLTE',site['CF_TDD3500_STATUS'][x],site['CF_2G_STATUS'][x],site['CF_CONFIG_FINAL_TDD_2019'][x],site['CF_CONFIG_CURRENT_TDD'][x],site['LATITUDE'][x],site['LONGITUDE'][x],site['CF_NETWORK_REGION'][x],site['CF_PROVINCE_NAME'][x],site['CF_CITY_ROAD_NAME'][x],site['CF_TDD_CLUSTER'][x],site['CF_GIS_SITE_TYPE'][x],site['CF_TDD_PHASE'][x],site['CF_TDD_EQUIPMENT_VENDOR'][x],site['CF_COLLOCATION_BY'][x]))
        crsr.commit()
    except:
        pass
print('Success:'+str(suc))  
print('Fail:'+str(fail))
print(datetime.datetime.now())
print("Sites Added")
connection.close()

#==========================
#TX Add From Atoll to FIBOT
#========================
connection = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=R:\FIBOT_DB.accdb;PWD=babimetro;')

print(datetime.datetime.now())
suc=0
fail=0
le=len(Tx)
print('lent:'+str(le))
sql="""insert into [Transmitter](Transmitter) values (?)"""
for x in my_range(1, le-1, 1):
    progressBar(round((x*100/le),1),100) 
    try :
        crsr = connection.execute(sql,(Tx['TRANSMITTER'][x]))
        crsr.commit()
    except:
        pass
print('Success:'+str(suc))  
print('\nFail:'+str(fail))
print(datetime.datetime.now())
print("Transmitters added")
connection.close()
#==========================
#FIBOT Site Update From Atoll
#========================
connection = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=R:\FIBOT_DB.accdb;PWD=babimetro;')

print(datetime.datetime.now())
suc=0
fail=0
cursor = connection.cursor()

le=len(site)
print('lent:'+str(le))
                                                                        
for x in my_range(1, le-1, 1):
    sql = """UPDATE [site list] SET Technology ='TDLTE',Status=?,2G_Status=?,Planned_Config=?,Current_Config=?,Latitude=?,Longitude=?,New_Region=?,Province=?,City=?,Cluster=?,[City Type]=?,phase=?,[TDD Equipment vendor]=?,[Collocation Comment]=? WHERE SiteName=?"""                    
                     
    progressBar(round((x*100/le),1),100) 
    #try :
    cursor.execute(sql,(site['CF_TDD3500_STATUS'][x],site['CF_2G_STATUS'][x],site['CF_CONFIG_FINAL_TDD_2019'][x],site['CF_CONFIG_CURRENT_TDD'][x],site['LATITUDE'][x],site['LONGITUDE'][x],site['CF_NETWORK_REGION'][x],site['CF_PROVINCE_NAME'][x],site['CF_CITY_ROAD_NAME'][x],site['CF_TDD_CLUSTER'][x],site['CF_GIS_SITE_TYPE'][x],site['CF_TDD_PHASE'][x],site['CF_TDD_EQUIPMENT_VENDOR'][x],site['CF_COLLOCATION_BY'][x],site['NAME'][x]))
 
    connection.commit()
    # except:
    #  pass
print('Success:'+str(suc))  
print('Fail:'+str(fail))
print(datetime.datetime.now())
print("Sites Updated")
connection.close()

#==========================
# FIBOT TX Update From Atoll
#========================
print(datetime.datetime.now())
connection = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=R:\FIBOT_DB.accdb;PWD=babimetro;')
suc=0
fail=0
cursor = connection.cursor()

le=len(Tx)
#le=20
print('lent:'+str(le))
                                                                        
for x in my_range(1, le-1, 1):
    sql = """UPDATE Transmitter SET SiteName=?,Status=?,[Height (m)]=?,Azimuth=?,[M-Tilt]=?,[E-Tilt]=?,Latitude=?,Longitude=?,[New_Region]=?,Province=?,City=?,Cluster=?,[TDD Equipment vendor]=? WHERE Transmitter=?"""                    
    #																			
    #TDD Cell Name,On Air Date,TDD Site Code,TDD Cell ID,eNB ID,New_eNB ID,ECI,New_ECI, RSI	PRACH(H)/grpAssignPUSCH(N)	Frequency BW	Frequecy	EARFCN	SSP	first Service Vendor	2nd service vendor	Last Modified Modified By	Provincial_Code_uu	Technology_ID	Provincial_Code_prefix	Carrier_ID	Sector_ID_Digit	Site_ID_Letter	Site_ID_Letter_Code          
    progressBar(round((x*100/le),1),100) 
    #try :
    cursor.execute(sql,(Tx['SITE_ID'][x],Tx['STATUS'][x],Tx['HEIGHT'][x],Tx['AZIMUTH'][x],Tx['MECHANICAL_TILT'][x],Tx['ELECTRICAL_TILT'][x],Tx['LATITUDE'][x],Tx['LONGITUDE'][x],Tx['REGION'][x],Tx['PROVINCE'][x],Tx['CITY'][x],Tx['TDD_CLUSTER_REFERENCE'][x],Tx['VENDOR'][x],Tx['TRANSMITTER'][x]))

   #							GIS_TYPE		MUNICIPALITY_REGION			PRIORITY	ADDRESS1	ADDRESS2	SITE_NAME	SITE_TYPE	SITE_INSTALL_TYPE	TECH_PLAN_FINAL	FREQUENCY_BAND		ANTENNA_NAME					HORIZENTAL_BEAMWIDTH	VERTICAL_BEAMWIDTH	FEEDER

    connection.commit()
    # except:
    #  pass
print('Success:'+str(suc))  
print('Fail:'+str(fail))
print(datetime.datetime.now())
print("Transmitters Updated")
connection.close()

