import requests


import pyodbc
#import gmplot
import json
import googlemaps

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]

#def insert_GIS(NTWK_NODE_NM,NODE_APPL_INSTC_NBR,LATD,LNGT):
 #query = "INSERT INTO sandbox..GLE_NWK_NETWORK_NODE(NTWK_NODE_NM,NODE_APPL_INSTC_NBR,LATD,LNGT) VALUES(?,?,?,?)"
 #args = (NTWK_NODE_NM,NODE_APPL_INSTC_NBR,LATD,LNGT)
 #curs5.execute(query, args)

key1 = "AIzaSyA0nTCG-J9UFA2KURWgAvjs6p96AW1oP6s"
client = googlemaps.Client(key1)

conn4 = pyodbc.connect("DRIVER={NetezzaSQL};SERVER=npsdwh.con-way.com;PORT=5480;DATABASE=PRD_WHSE;UID=YALAM.NISHANTH;PWD=nishanth519;")
curs4 = conn4.cursor()
conn5 = pyodbc.connect("DRIVER={NetezzaSQL};SERVER=npsdwh.con-way.com;PORT=5480;DATABASE=SANDBOX;UID=YALAM.NISHANTH;PWD=nishanth519;")
curs5 = conn5.cursor()
curs4.execute("SELECT DISTINCT NODE_APPL, ADDR, CUSTNM from ( SELECT NODE_APPL, ADDR, CUSTNM, ROW_NUM from (SELECT NODE_APPL, ADDR, CUSTNM, ROW_NUMBER() over ( order by NODE_APPL) as ROW_NUM from (WITH TRIP AS ( SELECT A.TRP_INSTC_NBR, A.TRP_TYP , B.TRP_NODE_ROL_TYP,C.NODE_APPL_INSTC_NBR as NODE_APPL, B.TRP_NODE_SEQ_NBR ,STR_ADDR||' '||CITY_NM||' '||ST_CNTRY_SBDIVN_CD||' '||PSTL_CD as ADDR, NTWK_NODE_NM as CUSTNM, B.NODE_APPL_INSTC_NBR , B.NODE_TYP , B.NODE_SUGG_EXEC_SEQ_NBR , B.NODE_ACTL_EXEC_SEQ_NBR , G.ORIGIN_POINT_LAT_COOR as LAT , G.ORIGIN_POINT_LNG_COOR as LOG , G.DEST_POINT_LAT_COOR, G.DEST_POINT_LNG_COOR, B.ACTL_ARIV_DTTM_PDX, B.ACTL_DISP_DTTM_PDX FROM UFO_TRIP A join prd_whse..ufo_TRIP_NODE B ON A.TRP_INSTC_NBR = B.TRP_INSTC_NBR left join NWK_NETWORK_NODE C ON B.NODE_APPL_INSTC_NBR = C.NODE_APPL_INSTC_NBR left join GIS_TRIP_STOP_ESTIMATION_RDS G ON A.TRP_INSTC_NBR = G.TRIP_INST_ID AND B.TRP_TYP = G.TRIP_TYPE AND B.TRP_NODE_SEQ_NBR = G.TRIP_NODE_SEQ_NBR WHERE A.TRP_INSTC_NBR IN (4243966015222,4243978598972) AND A.TRP_STAT_CD='COMP' AND A.TRP_TYP='CITY' AND B.NODE_ACTL_EXEC_SEQ_NBR is not null ) select 1 as TRIP_ORDER,* from TRIP where TRP_NODE_ROL_TYP='ORIG' UNION select 2 as TRIP_ORDER,* from TRIP where TRP_NODE_ROL_TYP='STOP' union select 3 as TRIP_ORDER,* from TRIP where TRP_NODE_ROL_TYP='DEST' order by TRP_INSTC_NBR,TRIP_ORDER,NODE_ACTL_EXEC_SEQ_NBR ,NODE_SUGG_EXEC_SEQ_NBR)D )X ) D;")
rows4 = curs4.fetchall()

ADDR = []
i = 0
print(rows4)
for row4 in rows4:
    url = 'https://maps.googleapis.com/maps/api/place/textsearch/json'
    addr = row4.ADDR
    params = {'sensor': 'true', 'query': addr, 'key': key1}
    r = requests.get(url, params=params,verify = False)
    results = r.json()['results']
    try:
      location = results[0]['geometry']['location']
      type = results[0]['types']
      types = str(type).strip('[ ]')
      ADDR.append(row4.CUSTNM)
      ADDR.append(row4.NODE_APPL)
      ADDR.append(location['lat'])
      ADDR.append(location['lng'])
      ADDR.append(types)
      print(ADDR)
     # ADDR.append(results[0]['geometry']['location_type'])
    except Exception:
     print(r.json())
     pass

row_chunks = chunks(ADDR,5)
print(ADDR)
for rows in row_chunks:
 NTWK_NODE_NM = rows[0]
 NODE_APPL_INSTC_NBR = rows[1]
 LATD = rows[2]
 LNGT = rows[3]
 TYPE = rows[4]
 #insert_GIS(NTWK_NODE_NM,NODE_APPL_INSTC_NBR,LATD,LNGT)
 curs5.execute("INSERT INTO sandbox..GLE_NWK_NETWORK_NODE_3(NTWK_NODE_NM,NODE_APPL_INSTC_NBR,LATD,LNGT,TYPE) select ?,?,?,?,?", [NTWK_NODE_NM,NODE_APPL_INSTC_NBR,LATD,LNGT,TYPE])
curs5.commit()