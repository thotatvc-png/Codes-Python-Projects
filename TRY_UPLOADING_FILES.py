import jaydebeapi
import smtplib
import os
import subprocess
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import datetime
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload 
from googleapiclient.errors import HttpError
from datetime import datetime
import urllib
import io
import httplib2
import numpy as np
import pandas as pd
import numpy as np
import matplotlib as plt
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import matplotlib.pyplot as plt
from datetime import datetime as dt
import time
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import matplotlib.pyplot as plt
from datetime import datetime as dt
from datetime import datetime, timedelta
import numpy as np
import numpy as np
import pandas as pd
import numpy as np
import matplotlib as plt
from googleapiclient.http import MediaFileUpload
import pytz

output = subprocess.check_output(['bash', '-c', 'echo $CLASSPATH:/usr/local/fdp-infra-hive/lib/*'])
os.environ['CLASSPATH'] = '/usr/local/fdp-infra-hive/lib/*'
url = ("jdbc:hive2://fkp-fdp-galaxy-sun-zkjn-0001.c.fkp-fdp-galaxy.internal:2181,fkp-fdp-galaxy-sun-zkjn-0002.c.fkp-fdp-galaxy.internal:2181,fkp-fdp-galaxy-sun-zkjn-0003.c.fkp-fdp-galaxy.internal:2181/default;transportMode=http;httpPath=cliservice;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=fkp-fdp-galaxy-hive3-hs2-agni;hive.client.read.socket.timeout=300?hive.metastore.client.socket.timeout=180;hive.server.tcp.keepalive=true;hive.server.read.socket.timeout=2000")

conn = jaydebeapi.connect("org.apache.hive.jdbc.HiveDriver", url,{'user': "thotat.vc", 'password': "kiV1thai!@#%"})

cursor = conn.cursor()
cursor.execute("ADD JAR /usr/share/fk-bigfoot-hivejsonserde/json-serde-1.3-SNAPSHOT-jar-with-dependencies.jar")
cursor.execute("ADD JAR /usr/share/fk-bigfoot-dimlookup/dimlookup-hive-udf-1.0-SNAPSHOT-jar-with-dependencies.jar")
cursor.execute("ADD JAR gs://fkpdp-mhosy-2nig-1a5d-systemlibs/libraries/hive/jars/hive-udfs-1.0-SNAPSHOT.jar")
cursor.execute("CREATE TEMPORARY FUNCTION lookup as 'com.flipkart.bigfoot.dimlookup.udf.HiveLookupUDF'")
cursor.execute("CREATE TEMPORARY FUNCTION arrToString as 'com.flipkart.fdp.CustomCaster'")
cursor.execute("CREATE TEMPORARY FUNCTION lookupkey as 'com.flipkart.bigfoot.dimlookup.udf.HiveKeyGeneratorUDF'")
cursor.execute("CREATE TEMPORARY FUNCTION lookup_date as 'com.flipkart.bigfoot.dimlookup.udf.DateLookupUDF'")
cursor.execute("CREATE TEMPORARY FUNCTION lookup_time as 'com.flipkart.bigfoot.dimlookup.udf.TimeLookupUDF'")
cursor.execute("CREATE TEMPORARY FUNCTION aggregate_filter as 'com.flipkart.bigfoot.dimlookup.udf.AggregateConditionUDF'")
cursor.execute("CREATE TEMPORARY FUNCTION aggregate as 'com.flipkart.bigfoot.dimlookup.udf.AggregateUDF'")
cursor.execute("CREATE TEMPORARY FUNCTION ISOToSqlTs as 'com.flipkart.bigfoot.dimlookup.udf.DateISOToSqlUDF'")
cursor.execute("CREATE TEMPORARY FUNCTION business_date_diff as 'com.flipkart.bigfoot.dimlookup.udf.BusinessDayDiffUDF'")
cursor.execute("set mapreduce.input.fileinputformat.input.dir.recursive=true")
sql1='''
select
a.listing_id as listing_id,
b.listing_status as listing_status,
c.display_name as display_name,
b.analytic_category as analytic_category,
CASE
  WHEN d.analytic_super_category in ('ShopsyLifeStyle') and lower(d.analytic_category) IN ('na', 'shopsykidclothing','shopsymensclothingcasualtopwear','shopsymensclothingessentialsandethnic','shopsymensclothingjeans','shopsymensclothingsmartandformals','shopsywomenethniccontemporary','shopsywomenethniccore','shopsywomenwesterncore') THEN 'ShopsyLifeStyle Apparel'
  WHEN d.analytic_super_category in ('ShopsyLifeStyle') and lower(d.analytic_category) not IN ('na', 'shopsykidclothing','shopsymensclothingcasualtopwear','shopsymensclothingessentialsandethnic','shopsymensclothingjeans','shopsymensclothingsmartandformals','shopsywomenethniccontemporary','shopsywomenethniccore','shopsywomenwesterncore') THEN 'ShopsyLifeStyle Non-Apparel'
  WHEN d.analytic_super_category in ('ShopsyBGM') AND LOWER(d.analytic_category) IN ('shopsygrooming','shopsymakeupfragrances') THEN 'ShopsyBGM BPC'
  WHEN d.analytic_super_category in ('ShopsyBGM') AND LOWER(d.analytic_category) NOT IN ('shopsygrooming','shopsymakeupfragrances') THEN 'ShopsyBGM GM'
  ELSE d.analytic_super_category END AS analytic_super_category_l2
from
fdp_uploads.ds_fkint_2gud_partner_vamsi_optin_shopsy_1_0 a
left join
bigfoot_external_neo.sp_product__listing_hive_dim b
on a.listing_id = b.listing_id
left join
bigfoot_external_neo.sp_seller__seller_hive_dim c
on b.seller_id=c.seller_id
left join
bigfoot_external_neo.sp_product__product_categorization_hive_dim d
on b.product_id = d.product_id  
'''
data = pd.read_sql_query(sql1, con=conn)
df_listing=pd.DataFrame(data)
print(df_listing)

df_listing.to_csv(f'/home/thotat.vc/analytics_scripts/listing_raw_new_consolidated_1.csv') 
# df_listing=pd.read_csv(f'/home/thotat.vc/analytics_scripts/listing_raw_new_consolidated_1.csv') # Run this when code fails so that you pickup the last query output
# df_listing=df_listing.rename(columns={"b.analytic_category":"analytic_category"})
print(df_listing)

# # Load the CSV and prepare offer_ids list
# df = pd.read_csv(r'/home/thotat.vc/Downloads/Consolidated_GSM_All_Offers.csv', encoding='cp1252')
# offer_ids = df['Offer IDs'].dropna().unique().tolist()
# offer_ids_cleaned = [f"'{str(oid)}'" for oid in offer_ids]
# offer_ids_clause = ', '.join(offer_ids_cleaned)
# existing_fpo_ids = set([str(oid).strip() for oid in offer_ids])

from datetime import datetime as dt2
today=dt2.now().strftime('%d_%b_%Y')
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe


scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('/home/thotat.vc/analytics_scripts/creds.json', scope)
client = gspread.authorize(creds)

wks = client.open("FPO and Cofunding Offer IDs - Mar'26").sheet1
ws = client.open("FPO and Cofunding Offer IDs - Mar'26").worksheet("Final Offers")

data_new_fpo = ws.get_all_records()
df_new_fpo = pd.DataFrame(data_new_fpo)
print(df_new_fpo)
df_new_fpo.to_csv('/home/thotat.vc/analytics_scripts/FPO_COFUNDED_Offers.csv')

df_new_fpo = pd.read_csv(r'/home/thotat.vc/analytics_scripts/FPO_COFUNDED_Offers.csv', encoding='cp1252')
offer_ids_new_fpo = df_new_fpo['Offer ID'].dropna().unique().tolist()
offer_ids_new_fpo_cleaned = [f"'{str(oid)}'" for oid in offer_ids_new_fpo]
offer_ids_new_fpo_clause = ', '.join(offer_ids_new_fpo_cleaned)
print(offer_ids_new_fpo_clause)

# Inject offer_ids_clause directly into the SQL string
sql2 = f"""SELECT DISTINCT a.offer_id AS offer_id, 
                b.product_id AS product_id, 
                a.listing_id AS listing_id, 
                b.seller_id AS seller_id, 
                c.cms_vertical AS cms_vertical, 
                b.listing_status AS listing_status
FROM (
    SELECT offer_id, listing_id
    FROM (
        SELECT data.listingid AS listing_id,
               data.offerid AS offer_id,
               MAX(CASE WHEN data.action = 'Mapped' THEN eventtime END) AS opt_in_time,
               MAX(CASE WHEN data.action = 'Unmapped' THEN eventtime END) AS opt_out_time
        FROM bigfoot_journal.dart_fkint_cp_santa_offerlistingmappingchange_5
        WHERE day >= 20250731
          AND data.offerid IN ({offer_ids_new_fpo_clause})
        GROUP BY data.listingid, data.offerid
    ) a
    WHERE opt_out_time IS NULL OR opt_in_time > opt_out_time
) a
JOIN bigfoot_external_neo.sp_product__listing_hive_dim b 
  ON a.listing_id = b.listing_id
JOIN bigfoot_external_neo.sp_product__product_categorization_hive_dim c
  ON b.product_id = c.product_id
  """
data2 = pd.read_sql_query(sql2, con=conn)
df_optin=pd.DataFrame(data2)
df_optin.to_csv(f'/home/thotat.vc/analytics_scripts/optin_raw_new_consolidated_1.csv', index=False)
import pandas as pd
import numpy as np
# df_optin = pd.read_csv(f'/home/thotat.vc/analytics_scripts/optin_raw_new_consolidated_1.csv') 
print(df_optin)

if df_optin.empty:
    print("Empty set error – df_optin has no data.")
else:
    # Process Base File
    file_path_1 = '/home/thotat.vc/analytics_scripts/FPO_Cofunding_Optin_Base_file_Final.csv'
    df_1 = pd.read_csv(file_path_1, encoding='utf-8')
    df_1['owner'] = df_1['owner'].replace(['', 'null'], np.nan).fillna('UM')
    df_1 = df_1.drop_duplicates()

    df_1_1 = df_1.copy()
    df_1_1['listing_id'] = df_1_1['listing_id'].astype(str).str.strip()
    df_1_1['seller_id'] = df_1_1['seller_id'].astype(str).str.strip()
    df_1_1['cms_vertical'] = df_1_1['cms_vertical'].astype(str).str.strip()
    
    #=======================================================================
    #---Excluding lid's for sanskriti------------------------------------
    #=======================================================================
    excluded_lids=['LSTXMCGCT92YGP8GJZJJ9DCPQ','LSTXMCGDRHFB7HWGRUCN4NRVC',
                   'LSTXMCGDRGFQSBHA5HWYTMBPH','LSTXMCHFUE8HZ7RG8G8DKNHRW',
                   'LSTXMCGEEXZ2HUTUCRGTXALXM','LSTXMCGDRGBY7KW6VFJVY1S6G',
                   'LSTXMCGDMXKCQZQPETBV0VDHW','LSTXMCGDRD9BXBAAAFCCUZKBP',
                   'LSTXMCGHK35NQAB5HSQGADDTT','LSTXMCGDRGFHFDTP5ATKUUL96',
                   'LSTXMCGDMZYVCC8J58ZGWPK8V','LSTXMCHFW9FBGBZBBBJJQRNQL',
                   'LSTXMCH23C9HCGXZS6DOAAQHM','LSTXMCGDNFCD3UMKCDFX88M0W',
                   'LSTXMCGDTMGNGSHWS76IVYJBC','LSTXMCGCRZP7YU7B24KR3X1HH',
                   'LSTXMCGDRKJVH9SJF2RROB5X3','LSTXMCGEEWCG69GSJDS58HFPU',
                   'LSTXMCH9JYGUPGVF5JBIAWAFV','LSTXMCGZZH2Y2EHRHQUHGSU3F',
                   'LSTXMCHYY54AHGRBGSZ3R2ICW','LSTXMCH887X5QRRZPW7SVF5EC',
                   'LSTXMCGZ7UTXSCCZTRV0PSDEI','LSTXMCGZH59SHZGRY4PUH0YCJ',
                   'LSTXMCGZ7TTFDFEHXGYQ8NW1I','LSTXMCH882YZEQYKFZZHH2BOQ',
                   'LSTXMCH29XCDREKS4ZMWAVGOA','LSTXMCGZNGVERC2ZNP4ZHWY5X',
                   'LSTXMCG87FFXSNZZWVGNDDDVU','LSTXMCHF4WCZ65S5EYRUSFMLX',
                   'LSTXMCH2GQKSKMUS2HNKCAYBN','LSTXMCGZ7VE7SWH9YZ3JHJQYP',
                   'LSTXMCHFCEFJNTXBGDSHTKRIM','LSTXMCH9GWCKRUGSUG22VWA7F',
                   'LSTXMCH9GZYHH4QUPZHTECWPW','LSTXMCGZME3FPYKJGDM6M2WIO',
                   'LSTXMCHFH9PGCZZYDWYR8JDGX','LSTXMCH7X29SGP78MBUY5U1EQ',
                   'LSTXMCH9GZFBYHRSPYGCQOGST','LSTXMCHFK2THC4KKZEFAFAXQD',
                   'LSTXMCH25HBAYMYTJSQYGJIA1','LSTXMCGPQSVFBVBPQRHFFSNSV',
                   'LSTXMCH883FHZFSQAT6IFZTAU','LSTXMCH9GUVHQJRZFAWBLBSG5',
                   'LSTXMCGZW733KA4HWGHANETMB','LSTXMCH226QTSFPZXTKVZRKKO']
    df_1_1=df_1_1[~df_1_1['listing_id'].isin(excluded_lids)]
    #=======================================================================
    #---Excluding lid's for sanskriti-----END ----HERE----------------------
    #=======================================================================


    #=================OFFER_ID'S==Mapping=========Here=========================================
    # 1. Group df_optin by 'listing_id' and combine 'offer_id's into a list
    offer_mapping = df_optin.groupby('listing_id')['offer_id'].apply(list)

    # 2. Map those lists to df_1_1 based on 'listing_id'
    df_1_1['offer_ids'] = df_1_1['listing_id'].map(offer_mapping)
    
    #=======================================================
    #--Added blank name to Neeket Shrivastava
    #=======================================================
    df_1_1['Lead'] = np.where(
    (df_1_1['KAM'].str.strip().str.lower() == 'pratik') & (df_1_1['Lead'].isna() | (df_1_1['Lead'].str.strip() == '')),
    'Neeket Shrivastava',
    df_1_1['Lead'])
    #=======================================================
    #============ENDS--HERE---------------------------------
    #=======================================================


    # Active Listings Filter
    removal_ids_1 = set(df_listing[df_listing['listing_status'] == 'ACTIVE']['listing_id'].astype(str).str.strip())
    
    # Merge Category
    df_listing_subset = df_listing[['listing_id', 'analytic_super_category_l2']].drop_duplicates()
    df_1_1 = df_1_1.drop(columns=['analytic_super_category_l2'], errors='ignore')
    df_1_1 = df_1_1.merge(df_listing_subset, on=['listing_id'], how='left')
    
    df_1_1['listing_status_current'] = df_1_1['listing_id'].apply(lambda x: 'ACTIVE' if x in removal_ids_1 else 'INACTIVE')
    df_1_1 = df_1_1[(df_1_1['listing_status_current'] == 'ACTIVE') & (df_1_1['analytic_category'] != 'ShopsyMobileProtection')]

    # --- FULL OFFER ID SETS ---
    fpo_ids = {
      'nb:mp:02632ce120','nb:mp:024684ae20','nb:mp:029a8c6b26','nb:mp:020c3b1526',
      'nb:mp:02ed4aef26','nb:mp:02e9b4f826','nb:mp:027b5e5226','nb:mp:02c2b88826',
      'nb:mp:02c8f70426','nb:mp:0701582615','nb:mp:07a3aac822','nb:mp:0782fc4919',
      'nb:mp:0785ab0319','nb:mp:076cbb3315','nb:mp:07bf325715','nb:mp:07dad43515',
      'nb:mp:07286ce718','nb:mp:07d8926a19','nb:mp:077d6b9519','nb:mp:0769cdd015',
      'nb:mp:07e1108119','nb:mp:07cdcc4819','nb:mp:075b85bd19','nb:mp:07739cb219',
      'nb:mp:077864ec18','nb:mp:0752295619','nb:mp:07cc717a15','nb:mp:07b9399419',
      'nb:mp:078bbc6418','nb:mp:0017a2ee30','nb:mp:0045ba3031','nb:mp:004e529e31',
      'nb:mp:004fdc5530','nb:mp:006d2fd430','nb:mp:0094afe714','nb:mp:009d480231',
      'nb:mp:00a3c91730','nb:mp:00e01f9b16','nb:mp:00e2aa6530','nb:mp:00e9f7b414',
      'nb:mp:00ff2b7931','nb:mp:0103c4f203','nb:mp:01136ad603','nb:mp:0123467303',
      'nb:mp:012b537303','nb:mp:0142a05b08','nb:mp:0156d6ec03','nb:mp:0159f47e03',
      'nb:mp:0164ba7319','nb:mp:0179a3e708','nb:mp:017de39519','nb:mp:0180093f25',
      'nb:mp:01847deb03','nb:mp:019924ff08','nb:mp:019e9f5e08','nb:mp:01a28b2e19',
      'nb:mp:01a610f608','nb:mp:01ad7da708','nb:mp:01b7be1808','nb:mp:01c6556d08',
      'nb:mp:01cea70408','nb:mp:01d9fd8508','nb:mp:0701582615','nb:mp:07286ce718',
      'nb:mp:0752295619','nb:mp:075b85bd19','nb:mp:0769cdd015','nb:mp:076cbb3315',
      'nb:mp:07739cb219','nb:mp:077864ec18','nb:mp:077d6b9519','nb:mp:0782fc4919',
      'nb:mp:0785ab0319','nb:mp:078bbc6418','nb:mp:07a3aac822','nb:mp:07b9399419',
      'nb:mp:07bf325715','nb:mp:07cc717a15','nb:mp:07cdcc4819','nb:mp:07d8926a19',
      'nb:mp:07dad43515','nb:mp:07e1108119','nb:mp:080117c217','nb:mp:081b417f07',
      'nb:mp:092fa29416','nb:mp:094a2bd716','nb:mp:0966e17516','nb:mp:09a67e6a16',
      'nb:mp:09b0d0e116','nb:mp:09b3e16116','nb:mp:09c0eab416','nb:mp:100f9bf713',
      'nb:mp:102032b313','nb:mp:104c70e813','nb:mp:1057f6bf13','nb:mp:10588f4413',
      'nb:mp:1063166213','nb:mp:107539c805','nb:mp:107f878213','nb:mp:10880aab13',
      'nb:mp:109c239d13','nb:mp:109cc8ed13','nb:mp:10aa795004','nb:mp:10aae3a813',
      'nb:mp:10b3bb9e13','nb:mp:10c3763c13','nb:mp:10d5ab6f13','nb:mp:10defb1d13',
      'nb:mp:10e0aa0713','nb:mp:10effc4e13','nb:mp:1167d39f30','nb:mp:024684ae20',
      'nb:mp:02243f2720','nb:mp:02151a2c20','nb:mp:02632ce120','nb:mp:02d6c15c26',
      'nb:mp:02f1145926','nb:mp:02f3aab726','nb:mp:02ecb7aa26','nb:mp:022a1d6f26',
      'nb:mp:02cbdd4f30','nb:mp:02ffe8ba30','nb:mp:02338a8430'

    }

    cofunded_ids = {

        'nb:mp:11eb190620','nb:mp:1177721420','nb:mp:11b08f4820','nb:mp:116bb71220',
        'nb:mp:11eba3da20','nb:mp:1103338820','nb:mp:11d0595520','nb:mp:1165573220',
        'nb:mp:114b52f320','nb:mp:0240ede927','nb:mp:02caff7b27','nb:mp:02a9a9a527',
        'nb:mp:02a70f9d27','nb:mp:025a2ac927','nb:mp:02298cc527','nb:mp:0251927627',
        'nb:mp:0271f56127','nb:mp:02e91acc27','nb:mp:029e4f9827','nb:mp:02fc80bc27',
        'nb:mp:00ecd7f730','nb:mp:102108e927','nb:mp:10b7a6ab27','nb:mp:10fad17027',
        'nb:mp:00b1d15930','nb:mp:110804d709','nb:mp:111aac2809','nb:mp:1128f42708',
        'nb:mp:113ce3de09','nb:mp:11668a1a08','nb:mp:1186753308','nb:mp:11aa5b2408',
        'nb:mp:11b16e9309','nb:mp:11f21a6b08','nb:mp:003c13a330','nb:mp:0068a60c30',
        'nb:mp:1024399124','nb:mp:1060b4e124','nb:mp:108cc54b24','nb:mp:10e2e37124',
        'nb:mp:10e6d97324','nb:mp:10eaa02724','nb:mp:10ef8c4f24','nb:mp:110139f019',
        'nb:mp:1103338820','nb:mp:1103831f19','nb:mp:1107f63b19','nb:mp:111aa15219',
        'nb:mp:11250b9219','nb:mp:11308e7d19','nb:mp:114b52f320','nb:mp:115183c419',
        'nb:mp:1151dcc119','nb:mp:11698fd619','nb:mp:117337c919','nb:mp:1177721420',
        'nb:mp:117d762c19','nb:mp:119a465619','nb:mp:11b08f4820','nb:mp:11c3376a19',
        'nb:mp:11d3045419','nb:mp:11e4ef7519','nb:mp:11ea8fb019','nb:mp:11eb190620',
        'nb:mp:11eba3da20','nb:mp:00bc834530','nb:mp:00d70e6d30','nb:mp:10535e1b24',
        'nb:mp:10df1f4324','nb:mp:1112958f02','nb:mp:11ccc39319','nb:mp:11f367cc19',
        'nb:mp:119a465619','nb:mp:11c3376a19','nb:mp:11d3045419','nb:mp:11e4ef7519',
        'nb:mp:11ea8fb019','nb:mp:00bc834530','nb:mp:00d70e6d30','nb:mp:10535e1b24',
        'nb:mp:10df1f4324','nb:mp:1112958f02','nb:mp:11ccc39319','nb:mp:11f367cc19'

    }

    # --- OPT-IN LOGIC (LISTING & SELLER LEVEL) ---
    df_optin['listing_id'] = df_optin['listing_id'].astype(str).str.strip()
    df_optin['seller_id'] = df_optin['seller_id'].astype(str).str.strip()
    df_optin['cms_vertical'] = df_optin['cms_vertical'].astype(str).str.strip()

    fpo_optin_listings = set(df_optin[df_optin['offer_id'].isin(fpo_ids)]['listing_id'])
    cofund_optin_listings = set(df_optin[df_optin['offer_id'].isin(cofunded_ids)]['listing_id'])
    overall_optin_listings = set(df_optin['listing_id'])

    # Apply Opt-in Flags
    df_1_1['Opted-in listing_id Overall'] = df_1_1['listing_id'].apply(lambda x: 'Opted-in' if x in overall_optin_listings else 'Not opted-in')
    df_1_1['Opted-in listing_id New FPO'] = df_1_1['listing_id'].apply(lambda x: 'Opted-in' if x in fpo_optin_listings else 'Not opted-in')
    df_1_1['Opted-in listing_id New Cofunded'] = df_1_1['listing_id'].apply(lambda x: 'Opted-in' if x in cofund_optin_listings else 'Not opted-in')

    # Eligibility (Set to Eligible for calculation purposes)
    df_1_1['New FPO'] = 'Eligible'
    df_1_1['New Cofunded'] = 'Eligible'

    df_1_edited = df_1_1.copy()
    # df_kam_cut = df_1_edited[df_1_edited['owner'] == 'KAM'].copy()
    # --- AGGREGATION LOGIC (WITH CONTRIBUTIONS) ---
    summary_owner = df_1_edited.groupby('owner').agg(
        orders_overall=('orders', 'sum'),
        opted_in_orders_overall=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'Opted-in listing_id Overall'] == 'Opted-in'].sum()),
        eligible_orders_cofund=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'New Cofunded'] != 'Not Eligible'].sum()),
        opted_in_orders_cofund=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'Opted-in listing_id New Cofunded'] == 'Opted-in'].sum()),
        eligible_orders_fpo=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'New FPO'] != 'Not Eligible'].sum()),
        opted_in_orders_fpo=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'Opted-in listing_id New FPO'] == 'Opted-in'].sum())
    ).reset_index()

    # --- OLD CODE (Causes AttributeError) ---
# def get_pct_str(numerator, denominator):
#     if denominator == 0: return "0.0%"
#     return f"{(numerator / denominator * 100).round(1)}%"

# --- FIXED CODE ---
    # 1. Calculation Helper
def get_pct_str(numerator, denominator):
    if denominator == 0: 
        return "0.0%"
    percentage = round((numerator / denominator * 100), 1)
    return f"{percentage}%"

# 2. Aggregation Logic
# =============================================================================
# 1. OWNER LEVEL SUMMARY
# =============================================================================
summary_owner = df_1_edited.groupby('owner').agg(
    orders_overall=('orders', 'sum'),
    opted_in_orders_overall=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'Opted-in listing_id Overall'] == 'Opted-in'].sum()),
    eligible_orders_cofund=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'New Cofunded'] != 'Not Eligible'].sum()),
    opted_in_orders_cofund=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'Opted-in listing_id New Cofunded'] == 'Opted-in'].sum()),
    eligible_orders_fpo=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'New FPO'] != 'Not Eligible'].sum()),
    opted_in_orders_fpo=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'Opted-in listing_id New FPO'] == 'Opted-in'].sum())
).reset_index()

summary_owner['Overall Optin %'] = summary_owner.apply(lambda r: get_pct_str(r['opted_in_orders_overall'], r['orders_overall']), axis=1)
summary_owner['Cofund Optin %'] = summary_owner.apply(lambda r: get_pct_str(r['opted_in_orders_cofund'], r['eligible_orders_cofund']), axis=1)
summary_owner['FPO Optin %'] = summary_owner.apply(lambda r: get_pct_str(r['opted_in_orders_fpo'], r['eligible_orders_fpo']), axis=1)
summary_owner['Cofund Contrib %'] = summary_owner.apply(lambda r: get_pct_str(r['opted_in_orders_cofund'], r['orders_overall']), axis=1)
summary_owner['FPO Contrib %'] = summary_owner.apply(lambda r: get_pct_str(r['opted_in_orders_fpo'], r['orders_overall']), axis=1)

total_nums = summary_owner.select_dtypes(include=[np.number]).sum()
tr_data = {'owner': 'TOTAL', **total_nums}
tr_data['Overall Optin %'] = get_pct_str(tr_data['opted_in_orders_overall'], tr_data['orders_overall'])
tr_data['Cofund Optin %'] = get_pct_str(tr_data['opted_in_orders_cofund'], tr_data['eligible_orders_cofund'])
tr_data['FPO Optin %'] = get_pct_str(tr_data['opted_in_orders_fpo'], tr_data['eligible_orders_fpo'])
tr_data['Cofund Contrib %'] = get_pct_str(tr_data['opted_in_orders_cofund'], tr_data['orders_overall'])
tr_data['FPO Contrib %'] = get_pct_str(tr_data['opted_in_orders_fpo'], tr_data['orders_overall'])

final_summary_owner = pd.concat([pd.DataFrame([tr_data]), summary_owner], ignore_index=True)

html_table_owner = '''
<table border="1" style="width:100%; border-collapse: collapse; font-family: Arial, sans-serif;">
    <thead>
        <tr style="text-align: center;">
            <th style="padding: 10px; background-color: darkred; color: white;">Owner</th>
            <th style="padding: 10px; background-color: #2c3e50; color: white;">Total DRR</th> 
            <th style="padding: 10px; background-color: #2c3e50; color: white;">Total Opted-in DRR</th>
            <th style="padding: 10px; background-color: #2c3e50; color: white;">Total Opted-in %</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Eligible DRR</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Opted-in DRR</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Opt-in %</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Contrib %</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Eligible DRR</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Opted-in DRR</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Opt-in %</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Contrib %</th>
        </tr>
    </thead>
    <tbody>
'''
for index, row in final_summary_owner.iterrows():
    row_style = 'background-color: #e3f2fd; font-weight: bold;' if row['owner'] == 'TOTAL' else ''
    html_table_owner += f'''
        <tr style="{row_style} text-align: center;">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['owner']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['orders_overall']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['opted_in_orders_overall']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['Overall Optin %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['eligible_orders_cofund']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['opted_in_orders_cofund']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['Cofund Optin %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['Cofund Contrib %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['eligible_orders_fpo']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['opted_in_orders_fpo']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['FPO Optin %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['FPO Contrib %']}</td>
        </tr>
    '''
html_table_owner += '</tbody></table>'

# =============================================================================
# 2. KAM LEVEL SUMMARY (FILTERED)
# =============================================================================
df_kam_cut = df_1_edited[df_1_edited['owner'] == 'KAM'].copy()

summary_KAM = df_kam_cut.groupby('KAM').agg(
    orders_overall=('orders', 'sum'),
    opted_in_orders_overall=('orders', lambda x: df_kam_cut.loc[x.index, 'orders'][df_kam_cut.loc[x.index, 'Opted-in listing_id Overall'] == 'Opted-in'].sum()),
    eligible_orders_cofund=('orders', lambda x: df_kam_cut.loc[x.index, 'orders'][df_kam_cut.loc[x.index, 'New Cofunded'] != 'Not Eligible'].sum()),
    opted_in_orders_cofund=('orders', lambda x: df_kam_cut.loc[x.index, 'orders'][df_kam_cut.loc[x.index, 'Opted-in listing_id New Cofunded'] == 'Opted-in'].sum()),
    eligible_orders_fpo=('orders', lambda x: df_kam_cut.loc[x.index, 'orders'][df_kam_cut.loc[x.index, 'New FPO'] != 'Not Eligible'].sum()),
    opted_in_orders_fpo=('orders', lambda x: df_kam_cut.loc[x.index, 'orders'][df_kam_cut.loc[x.index, 'Opted-in listing_id New FPO'] == 'Opted-in'].sum())
).reset_index()

summary_KAM['Overall Optin %'] = summary_KAM.apply(lambda r: get_pct_str(r['opted_in_orders_overall'], r['orders_overall']), axis=1)
summary_KAM['Cofund Optin %'] = summary_KAM.apply(lambda r: get_pct_str(r['opted_in_orders_cofund'], r['eligible_orders_cofund']), axis=1)
summary_KAM['FPO Optin %'] = summary_KAM.apply(lambda r: get_pct_str(r['opted_in_orders_fpo'], r['eligible_orders_fpo']), axis=1)
summary_KAM['Cofund Contrib %'] = summary_KAM.apply(lambda r: get_pct_str(r['opted_in_orders_cofund'], r['orders_overall']), axis=1)
summary_KAM['FPO Contrib %'] = summary_KAM.apply(lambda r: get_pct_str(r['opted_in_orders_fpo'], r['orders_overall']), axis=1)

total_nums = summary_KAM.select_dtypes(include=[np.number]).sum()
tr_data_kam = {'KAM': 'TOTAL', **total_nums}
tr_data_kam['Overall Optin %'] = get_pct_str(tr_data_kam['opted_in_orders_overall'], tr_data_kam['orders_overall'])
tr_data_kam['Cofund Optin %'] = get_pct_str(tr_data_kam['opted_in_orders_cofund'], tr_data_kam['eligible_orders_cofund'])
tr_data_kam['FPO Optin %'] = get_pct_str(tr_data_kam['opted_in_orders_fpo'], tr_data_kam['eligible_orders_fpo'])
tr_data_kam['Cofund Contrib %'] = get_pct_str(tr_data_kam['opted_in_orders_cofund'], tr_data_kam['orders_overall'])
tr_data_kam['FPO Contrib %'] = get_pct_str(tr_data_kam['opted_in_orders_fpo'], tr_data_kam['orders_overall'])

final_summary_kam = pd.concat([pd.DataFrame([tr_data_kam]), summary_KAM], ignore_index=True)

html_table_KAM = '''
<table border="1" style="width:100%; border-collapse: collapse; font-family: Arial, sans-serif;">
    <thead>
        <tr style="text-align: center;">
            <th style="padding: 10px; background-color: darkred; color: white;">KAM</th>
            <th style="padding: 10px; background-color: #2c3e50; color: white;">Total DRR</th> 
            <th style="padding: 10px; background-color: #2c3e50; color: white;">Total Opted-in DRR</th>
            <th style="padding: 10px; background-color: #2c3e50; color: white;">Total Opted-in %</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Eligible DRR</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Opted-in DRR</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Opt-in %</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Contrib %</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Eligible DRR</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Opted-in DRR</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Opt-in %</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Contrib %</th>
        </tr>
    </thead>
    <tbody>
'''
for index, row in final_summary_kam.iterrows():
    row_style = 'background-color: #e3f2fd; font-weight: bold;' if row['KAM'] == 'TOTAL' else ''
    html_table_KAM += f'''
        <tr style="{row_style} text-align: center;">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['KAM']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['orders_overall']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['opted_in_orders_overall']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['Overall Optin %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['eligible_orders_cofund']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['opted_in_orders_cofund']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['Cofund Optin %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['Cofund Contrib %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['eligible_orders_fpo']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['opted_in_orders_fpo']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['FPO Optin %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['FPO Contrib %']}</td>
        </tr>
    '''
html_table_KAM += '</tbody></table>'

# =============================================================================
# 3. BU LEVEL SUMMARY (OVERALL)
# =============================================================================
summary_bu = df_1_edited.groupby('bu').agg(
    orders_overall=('orders', 'sum'),
    opted_in_orders_overall=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'Opted-in listing_id Overall'] == 'Opted-in'].sum()),
    eligible_orders_cofund=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'New Cofunded'] != 'Not Eligible'].sum()),
    opted_in_orders_cofund=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'Opted-in listing_id New Cofunded'] == 'Opted-in'].sum()),
    eligible_orders_fpo=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'New FPO'] != 'Not Eligible'].sum()),
    opted_in_orders_fpo=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'Opted-in listing_id New FPO'] == 'Opted-in'].sum())
).reset_index()

summary_bu['Overall Optin %'] = summary_bu.apply(lambda r: get_pct_str(r['opted_in_orders_overall'], r['orders_overall']), axis=1)
summary_bu['Cofund Optin %'] = summary_bu.apply(lambda r: get_pct_str(r['opted_in_orders_cofund'], r['eligible_orders_cofund']), axis=1)
summary_bu['FPO Optin %'] = summary_bu.apply(lambda r: get_pct_str(r['opted_in_orders_fpo'], r['eligible_orders_fpo']), axis=1)
summary_bu['Cofund Contrib %'] = summary_bu.apply(lambda r: get_pct_str(r['opted_in_orders_cofund'], r['orders_overall']), axis=1)
summary_bu['FPO Contrib %'] = summary_bu.apply(lambda r: get_pct_str(r['opted_in_orders_fpo'], r['orders_overall']), axis=1)

total_nums = summary_bu.select_dtypes(include=[np.number]).sum()
tr_data = {'bu': 'TOTAL', **total_nums}
tr_data['Overall Optin %'] = get_pct_str(tr_data['opted_in_orders_overall'], tr_data['orders_overall'])
tr_data['Cofund Optin %'] = get_pct_str(tr_data['opted_in_orders_cofund'], tr_data['eligible_orders_cofund'])
tr_data['FPO Optin %'] = get_pct_str(tr_data['opted_in_orders_fpo'], tr_data['eligible_orders_fpo'])
tr_data['Cofund Contrib %'] = get_pct_str(tr_data['opted_in_orders_cofund'], tr_data['orders_overall'])
tr_data['FPO Contrib %'] = get_pct_str(tr_data['opted_in_orders_fpo'], tr_data['orders_overall'])

final_summary_bu = pd.concat([pd.DataFrame([tr_data]), summary_bu], ignore_index=True)

html_table_bu = '''
<table border="1" style="width:100%; border-collapse: collapse; font-family: Arial, sans-serif;">
    <thead>
        <tr style="text-align: center;">
            <th style="padding: 10px; background-color: darkred; color: white;">BU</th>
            <th style="padding: 10px; background-color: #2c3e50; color: white;">Total DRR</th> 
            <th style="padding: 10px; background-color: #2c3e50; color: white;">Total Opted-in DRR</th>
            <th style="padding: 10px; background-color: #2c3e50; color: white;">Total Opted-in %</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Eligible DRR</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Opted-in DRR</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Opt-in %</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Contrib %</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Eligible DRR</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Opted-in DRR</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Opt-in %</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Contrib %</th>
        </tr>
    </thead>
    <tbody>
'''
for index, row in final_summary_bu.iterrows():
    row_style = 'background-color: #e3f2fd; font-weight: bold;' if row['bu'] == 'TOTAL' else ''
    html_table_bu += f'''
        <tr style="{row_style} text-align: center;">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['bu']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['orders_overall']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['opted_in_orders_overall']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['Overall Optin %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['eligible_orders_cofund']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['opted_in_orders_cofund']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['Cofund Optin %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['Cofund Contrib %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['eligible_orders_fpo']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['opted_in_orders_fpo']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['FPO Optin %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['FPO Contrib %']}</td>
        </tr>
    '''
html_table_bu += '</tbody></table>'

# =============================================================================
# 4. BU LEVEL SUMMARY (FILTERED FOR KAM)
# =============================================================================
summary_bu_KAM = df_kam_cut.groupby('bu').agg(
    orders_overall=('orders', 'sum'),
    opted_in_orders_overall=('orders', lambda x: df_kam_cut.loc[x.index, 'orders'][df_kam_cut.loc[x.index, 'Opted-in listing_id Overall'] == 'Opted-in'].sum()),
    eligible_orders_cofund=('orders', lambda x: df_kam_cut.loc[x.index, 'orders'][df_kam_cut.loc[x.index, 'New Cofunded'] != 'Not Eligible'].sum()),
    opted_in_orders_cofund=('orders', lambda x: df_kam_cut.loc[x.index, 'orders'][df_kam_cut.loc[x.index, 'Opted-in listing_id New Cofunded'] == 'Opted-in'].sum()),
    eligible_orders_fpo=('orders', lambda x: df_kam_cut.loc[x.index, 'orders'][df_kam_cut.loc[x.index, 'New FPO'] != 'Not Eligible'].sum()),
    opted_in_orders_fpo=('orders', lambda x: df_kam_cut.loc[x.index, 'orders'][df_kam_cut.loc[x.index, 'Opted-in listing_id New FPO'] == 'Opted-in'].sum())
).reset_index()

summary_bu_KAM['Overall Optin %'] = summary_bu_KAM.apply(lambda r: get_pct_str(r['opted_in_orders_overall'], r['orders_overall']), axis=1)
summary_bu_KAM['Cofund Optin %'] = summary_bu_KAM.apply(lambda r: get_pct_str(r['opted_in_orders_cofund'], r['eligible_orders_cofund']), axis=1)
summary_bu_KAM['FPO Optin %'] = summary_bu_KAM.apply(lambda r: get_pct_str(r['opted_in_orders_fpo'], r['eligible_orders_fpo']), axis=1)
summary_bu_KAM['Cofund Contrib %'] = summary_bu_KAM.apply(lambda r: get_pct_str(r['opted_in_orders_cofund'], r['orders_overall']), axis=1)
summary_bu_KAM['FPO Contrib %'] = summary_bu_KAM.apply(lambda r: get_pct_str(r['opted_in_orders_fpo'], r['orders_overall']), axis=1)

total_nums = summary_bu_KAM.select_dtypes(include=[np.number]).sum()
tr_data = {'bu': 'TOTAL', **total_nums}
tr_data['Overall Optin %'] = get_pct_str(tr_data['opted_in_orders_overall'], tr_data['orders_overall'])
tr_data['Cofund Optin %'] = get_pct_str(tr_data['opted_in_orders_cofund'], tr_data['eligible_orders_cofund'])
tr_data['FPO Optin %'] = get_pct_str(tr_data['opted_in_orders_fpo'], tr_data['eligible_orders_fpo'])
tr_data['Cofund Contrib %'] = get_pct_str(tr_data['opted_in_orders_cofund'], tr_data['orders_overall'])
tr_data['FPO Contrib %'] = get_pct_str(tr_data['opted_in_orders_fpo'], tr_data['orders_overall'])

final_summary_bu_kam = pd.concat([pd.DataFrame([tr_data]), summary_bu_KAM], ignore_index=True)

html_table_bu_kam = '''
<table border="1" style="width:100%; border-collapse: collapse; font-family: Arial, sans-serif;">
    <thead>
        <tr style="text-align: center;">
            <th style="padding: 10px; background-color: darkred; color: white;">BU (KAM Filtered)</th>
            <th style="padding: 10px; background-color: #2c3e50; color: white;">Total DRR</th> 
            <th style="padding: 10px; background-color: #2c3e50; color: white;">Total Opted-in DRR</th>
            <th style="padding: 10px; background-color: #2c3e50; color: white;">Total Opted-in %</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Eligible DRR</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Opted-in DRR</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Opt-in %</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Contrib %</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Eligible DRR</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Opted-in DRR</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Opt-in %</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Contrib %</th>
        </tr>
    </thead>
    <tbody>
'''
for index, row in final_summary_bu_kam.iterrows():
    row_style = 'background-color: #e3f2fd; font-weight: bold;' if row['bu'] == 'TOTAL' else ''
    html_table_bu_kam += f'''
        <tr style="{row_style} text-align: center;">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['bu']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['orders_overall']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['opted_in_orders_overall']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['Overall Optin %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['eligible_orders_cofund']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['opted_in_orders_cofund']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['Cofund Optin %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['Cofund Contrib %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['eligible_orders_fpo']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['opted_in_orders_fpo']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['FPO Optin %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['FPO Contrib %']}</td>
        </tr>
    '''
html_table_bu_kam += '</tbody></table>'

# =============================================================================
# 5. ANALYTIC CATEGORY LEVEL SUMMARY
# =============================================================================
summary_analytic_category = df_1_edited.groupby('analytic_category').agg(
    orders_overall=('orders', 'sum'),
    opted_in_orders_overall=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'Opted-in listing_id Overall'] == 'Opted-in'].sum()),
    eligible_orders_cofund=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'New Cofunded'] != 'Not Eligible'].sum()),
    opted_in_orders_cofund=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'Opted-in listing_id New Cofunded'] == 'Opted-in'].sum()),
    eligible_orders_fpo=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'New FPO'] != 'Not Eligible'].sum()),
    opted_in_orders_fpo=('orders', lambda x: df_1_edited.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'Opted-in listing_id New FPO'] == 'Opted-in'].sum())
).reset_index()

summary_analytic_category['Overall Optin %'] = summary_analytic_category.apply(lambda r: get_pct_str(r['opted_in_orders_overall'], r['orders_overall']), axis=1)
summary_analytic_category['Cofund Optin %'] = summary_analytic_category.apply(lambda r: get_pct_str(r['opted_in_orders_cofund'], r['eligible_orders_cofund']), axis=1)
summary_analytic_category['FPO Optin %'] = summary_analytic_category.apply(lambda r: get_pct_str(r['opted_in_orders_fpo'], r['eligible_orders_fpo']), axis=1)
summary_analytic_category['Cofund Contrib %'] = summary_analytic_category.apply(lambda r: get_pct_str(r['opted_in_orders_cofund'], r['orders_overall']), axis=1)
summary_analytic_category['FPO Contrib %'] = summary_analytic_category.apply(lambda r: get_pct_str(r['opted_in_orders_fpo'], r['orders_overall']), axis=1)

total_nums = summary_analytic_category.select_dtypes(include=[np.number]).sum()
tr_data = {'analytic_category': 'TOTAL', **total_nums}
tr_data['Overall Optin %'] = get_pct_str(tr_data['opted_in_orders_overall'], tr_data['orders_overall'])
tr_data['Cofund Optin %'] = get_pct_str(tr_data['opted_in_orders_cofund'], tr_data['eligible_orders_cofund'])
tr_data['FPO Optin %'] = get_pct_str(tr_data['opted_in_orders_fpo'], tr_data['eligible_orders_fpo'])
tr_data['Cofund Contrib %'] = get_pct_str(tr_data['opted_in_orders_cofund'], tr_data['orders_overall'])
tr_data['FPO Contrib %'] = get_pct_str(tr_data['opted_in_orders_fpo'], tr_data['orders_overall'])

final_summary_analytic = pd.concat([pd.DataFrame([tr_data]), summary_analytic_category], ignore_index=True)

html_table_analytic_category = '''
<table border="1" style="width:100%; border-collapse: collapse; font-family: Arial, sans-serif;">
    <thead>
        <tr style="text-align: center;">
            <th style="padding: 10px; background-color: darkred; color: white;">Analytic Category</th>
            <th style="padding: 10px; background-color: #2c3e50; color: white;">Total DRR</th> 
            <th style="padding: 10px; background-color: #2c3e50; color: white;">Total Opted-in DRR</th>
            <th style="padding: 10px; background-color: #2c3e50; color: white;">Total Opted-in %</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Eligible DRR</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Opted-in DRR</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Opt-in %</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Contrib %</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Eligible DRR</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Opted-in DRR</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Opt-in %</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Contrib %</th>
        </tr>
    </thead>
    <tbody>
'''
for index, row in final_summary_analytic.iterrows():
    row_style = 'background-color: #e3f2fd; font-weight: bold;' if row['analytic_category'] == 'TOTAL' else ''
    html_table_analytic_category += f'''
        <tr style="{row_style} text-align: center;">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['analytic_category']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['orders_overall']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['opted_in_orders_overall']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['Overall Optin %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['eligible_orders_cofund']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['opted_in_orders_cofund']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['Cofund Optin %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['Cofund Contrib %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['eligible_orders_fpo']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['opted_in_orders_fpo']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['FPO Optin %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['FPO Contrib %']}</td>
        </tr>
    '''
html_table_analytic_category += '</tbody></table>'

# =============================================================================
# 6. LEAD LEVEL SUMMARY
# =============================================================================
summary_Lead = df_kam_cut.groupby('Lead').agg(
    orders_overall=('orders', 'sum'),
    opted_in_orders_overall=('orders', lambda x: df_kam_cut.loc[x.index, 'orders'][df_kam_cut.loc[x.index, 'Opted-in listing_id Overall'] == 'Opted-in'].sum()),
    eligible_orders_cofund=('orders', lambda x: df_kam_cut.loc[x.index, 'orders'][df_kam_cut.loc[x.index, 'New Cofunded'] != 'Not Eligible'].sum()),
    opted_in_orders_cofund=('orders', lambda x: df_kam_cut.loc[x.index, 'orders'][df_kam_cut.loc[x.index, 'Opted-in listing_id New Cofunded'] == 'Opted-in'].sum()),
    eligible_orders_fpo=('orders', lambda x: df_kam_cut.loc[x.index, 'orders'][df_kam_cut.loc[x.index, 'New FPO'] != 'Not Eligible'].sum()),
    opted_in_orders_fpo=('orders', lambda x: df_kam_cut.loc[x.index, 'orders'][df_1_edited.loc[x.index, 'Opted-in listing_id New FPO'] == 'Opted-in'].sum())
).reset_index()

summary_Lead['Overall Optin %'] = summary_Lead.apply(lambda r: get_pct_str(r['opted_in_orders_overall'], r['orders_overall']), axis=1)
summary_Lead['Cofund Optin %'] = summary_Lead.apply(lambda r: get_pct_str(r['opted_in_orders_cofund'], r['eligible_orders_cofund']), axis=1)
summary_Lead['FPO Optin %'] = summary_Lead.apply(lambda r: get_pct_str(r['opted_in_orders_fpo'], r['eligible_orders_fpo']), axis=1)
summary_Lead['Cofund Contrib %'] = summary_Lead.apply(lambda r: get_pct_str(r['opted_in_orders_cofund'], r['orders_overall']), axis=1)
summary_Lead['FPO Contrib %'] = summary_Lead.apply(lambda r: get_pct_str(r['opted_in_orders_fpo'], r['orders_overall']), axis=1)

total_nums = summary_Lead.select_dtypes(include=[np.number]).sum()
tr_data = {'Lead': 'TOTAL', **total_nums}
tr_data['Overall Optin %'] = get_pct_str(tr_data['opted_in_orders_overall'], tr_data['orders_overall'])
tr_data['Cofund Optin %'] = get_pct_str(tr_data['opted_in_orders_cofund'], tr_data['eligible_orders_cofund'])
tr_data['FPO Optin %'] = get_pct_str(tr_data['opted_in_orders_fpo'], tr_data['eligible_orders_fpo'])
tr_data['Cofund Contrib %'] = get_pct_str(tr_data['opted_in_orders_cofund'], tr_data['orders_overall'])
tr_data['FPO Contrib %'] = get_pct_str(tr_data['opted_in_orders_fpo'], tr_data['orders_overall'])

final_summary_lead = pd.concat([pd.DataFrame([tr_data]), summary_Lead], ignore_index=True)

html_table_Lead = '''
<table border="1" style="width:100%; border-collapse: collapse; font-family: Arial, sans-serif;">
    <thead>
        <tr style="text-align: center;">
            <th style="padding: 10px; background-color: darkred; color: white;">Lead</th>
            <th style="padding: 10px; background-color: #2c3e50; color: white;">Total DRR</th> 
            <th style="padding: 10px; background-color: #2c3e50; color: white;">Total Opted-in DRR</th>
            <th style="padding: 10px; background-color: #2c3e50; color: white;">Total Opted-in %</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Eligible DRR</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Opted-in DRR</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Opt-in %</th>
            <th style="padding: 10px; background-color: #27ae60; color: white;">Cofunding Contrib %</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Eligible DRR</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Opted-in DRR</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Opt-in %</th>
            <th style="padding: 10px; background-color: #e67e22; color: white;">FPO Contrib %</th>
        </tr>
    </thead>
    <tbody>
'''
for index, row in final_summary_lead.iterrows():
    row_style = 'background-color: #e3f2fd; font-weight: bold;' if row['Lead'] == 'TOTAL' else ''
    html_table_Lead += f'''
        <tr style="{row_style} text-align: center;">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['Lead']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['orders_overall']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['opted_in_orders_overall']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['Overall Optin %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['eligible_orders_cofund']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['opted_in_orders_cofund']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['Cofund Optin %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['Cofund Contrib %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['eligible_orders_fpo']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['opted_in_orders_fpo']/42:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['FPO Optin %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px;">{row['FPO Contrib %']}</td>
        </tr>
    '''
html_table_Lead += '</tbody></table>'




import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from datetime import datetime as dt
import os

sender_email = 'thotat.vc@flipkart.com'
sender_password = 'kiV1thai!@#%'
    
link = "https://drive.google.com/drive/folders/1LNVWVoxHpYTH9Qlrz6whVi2fE5CvKIK-"
link_sheet = "https://docs.google.com/spreadsheets/d/1Jto0AhHEYGq5DxOdawZy96hSYIyCxLG1V7ro_CH4ld4/edit?usp=sharing"

# Original message metadata
original_from = 'thotat.vc@flipkart.com'
# original_to = ['shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com','shopsysell@flipkart.com', 'shopsypricing@flipkart.com']
original_to = ['h.kishandasmenon@flipkart.com','shopsy_buy@flipkart.com','shopsy_buy_cr@flipkart.com','shopsysell@flipkart.com','shopsypricing@flipkart.com','nakul.gulati@flipkart.com','sriram.m@flipkart.com']
# original_to = ['thotat.vc@flipkart.com']
# 'upasanaupadhak.vc@flipkart.com','thotat.vc@flipkart.com','vishnuac.vc@flipkart.com','shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com'
#'shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com']
#'shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com']
#'shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com'
original_cc = ['thotat.vc@flipkart.com','vishnuac.vc@flipkart.com']
#original_cc = ['thotat.vc@flipkart.com']
original_message_id = '<CADLy_NH-rFBymSZMsgdVFWH=+mvxozDdBR2+cNaDPDbb2R6GnA@mail.gmail.com>'

all_recipients = set([original_from] + original_to + original_cc)

from datetime import datetime
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

msg = MIMEMultipart()
msg['From'] = sender_email
msg['To'] = ', '.join(original_to)
msg['Cc'] = ', '.join(original_cc)
msg['Subject'] = f"	Streamlined Participation: FPO and Co-funding Together"
msg['In-Reply-To'] = original_message_id
msg['References'] = original_message_id

body = f"""
Hey Team,<br><br>
Please find the new consolidated opt-in status for the period of 42 days, i.e., February 1 to March 15, based on demand weight. This tracker includes contributions from FPO and co-funding at the overall level.<br><br>
Kindly note:<br><br>
Orders DRR should be considered as orders/42 for calculation purposes.<br><br>
Listing status is based on the current latest data. Only active listing IDs are being tracked.<br><br>
Please consider only the listing_status_current(column) while reviewing.<br><br>
If there are any doubts or clarifications needed regarding the calculations, please feel free to reach out.<br><br>
Please find the status as of {current_time} <br><br>
Please find the Owner Level Summary:<br><br>
{html_table_owner}<br><br>
Please find the BU Level Summary:<br><br>
{html_table_bu}<br><br>
Please find the BU (KAM) Level Summary:<br><br>
{html_table_bu_kam}<br><br>
Please find the Lead Level Summary:<br><br>
{html_table_Lead}
Please find the KAM Level Summary:<br><br>
{html_table_KAM}<br><br>
Please find the Analytic Catrgory Level Summary:<br><br>
{html_table_analytic_category}<br><br>
The attached File link contains the all the offer id's for your reference: {link_sheet}<br><br>
The attached drive link contains the raw and summary data for your reference: {link}<br><br>
In the drive file name mentioned as  (Consolidated_Report_cofund_fpo_)<br><br>
<b>Regards,<br>
Vamsi</b>
"""
msg.attach(MIMEText(body, 'html'))

#if os.path.exists(file_path):
    #with open(file_path, 'rb') as f:
        #file_attachment = MIMEApplication(f.read(), _subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        #file_attachment.add_header('Content-Disposition', 'attachment', filename=filename)
        #msg.attach(file_attachment)
#else:
    #print(f"File not found: {file_path}")


try:
    server = smtplib.SMTP('127.0.0.1', 25)
    server.set_debuglevel(1)
    server.sendmail(sender_email, list(all_recipients), msg.as_string())
    print("Reply-All email with HTML table and Excel attachment sent successfully!")
except smtplib.SMTPException as e:
    print(f"Error sending email: {e}")
finally:
    server.quit()


    # df_1_edited=df_1_edited.merge(df_optin_summary,on=['listing_id'],how='left')

    # df_1_edited['order_drr'] = pd.to_numeric(df_1_edited['orders'], errors='coerce') 
    # df_1_edited['order_drr'] = df_1_edited['order_drr'].round(2)

    from datetime import datetime as dt2
    today=dt2.now().strftime('%d_%b_%Y')

    df_1_edited.to_csv(f"/home/thotat.vc/Downloads/Consolidated_Report_cofund_fpo_{today}.csv", index=False)

    df_1_edited[df_1_edited['owner']=='KAM'].to_csv(f"/home/thotat.vc/Downloads/Consolidated_Report_cofund_fpo_{today}_KAM.csv")
    df_1_edited[df_1_edited['owner']=='RE'].to_csv(f"/home/thotat.vc/Downloads/Consolidated_Report_cofund_fpo_{today}_RE.csv")
    df_1_edited[df_1_edited['owner']=='UM'].to_csv(f"/home/thotat.vc/Downloads/Consolidated_Report_cofund_fpo_{today}_UM.csv")


    # with pd.ExcelWriter(f"/home/thotat.vc/Downloads/Consolidated_Report_{today}.xlsx") as writer:
    #     unique_listing_ids_df_summary_supercat_2.to_excel(writer, sheet_name='Summary Supercategory',index=False)
    #     df_1_edited.to_excel(writer, sheet_name='Raw',index=False)
    #     unique_listing_ids_df_summary_2.to_excel(writer, sheet_name='Summary',index=False)

    filename =f"Consolidated_Report_cofund_fpo_{today}.csv"
    file_path =f"/home/thotat.vc/Downloads/Consolidated_Report_cofund_fpo_{today}.csv" 
    file_path_kam=f"/home/thotat.vc/Downloads/Consolidated_Report_cofund_fpo_{today}_KAM.csv"
    file_path_re=f"/home/thotat.vc/Downloads/Consolidated_Report_cofund_fpo_{today}_RE.csv"
    file_path_um=f"/home/thotat.vc/Downloads/Consolidated_Report_cofund_fpo_{today}_UM.csv"

    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name('/home/thotat.vc/analytics_scripts/creds.json', scope)
    # client = gspread.authorize(creds)
    http = creds.authorize(httplib2.Http(timeout=500))
    drive_service = build('drive', 'v3', http=http)






    # # ---- Function to create a shareable link for a file ----
    # def create_shareable_link(drive_service, file_id):
    #     """Create a public shareable link for a Google Drive file."""
    #     permission = {
    #         'type': 'anyone',
    #         'role': 'reader',
    #     }
        
    #     drive_service.permissions().create(fileId=file_id, body=permission).execute()
        
    #     link = f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
    #     return link
    
    # link = "https://drive.google.com/drive/folders/1DSKaE5prHsmmUnjNyg4qSmZuAuXFRg78"
    #link2= "https://drive.google.com/drive/folders/1zmjaxwF6Ze3ePf92L8P1lPePO-noItBM"
    # link3= "https://docs.google.com/spreadsheets/d/1ltixD7xZeeFORC0wsA4X6fzlk5DueqH0TMQIt1UaX7A/edit?usp=sharing"
    
import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account

# ---- Setup Google Drive API ----
SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = '/home/thotat.vc/analytics_scripts/creds.json'  # Replace with your service account file path

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

drive_service = build('drive', 'v3', credentials=credentials)

# ---- Upload file to folder inside Shared Drive ----
import math
from googleapiclient.http import MediaFileUpload

def upload_file_to_shared_drive_folder(file_path, folder_id):
    """Upload a file to a folder inside a shared drive with progress tracking and retries."""
    file_metadata = {
        'name': os.path.basename(file_path),
        'parents': [folder_id]
    }
    media = MediaFileUpload(
        file_path,
        mimetype='application/octet-stream',
        resumable=True,
        chunksize=32*1024*1024
    )

    request = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id',
        supportsAllDrives=True
    )

    response = None
    while response is None:
        try:
            status, response = request.next_chunk()
            if status:
                percent = int(status.resumable_progress * 100 / status.total_size)
                print(f"Uploading {os.path.basename(file_path)}: {percent}% complete")
        except Exception as e:
            print(f"Chunk upload failed ({e}), retrying in 15s...")
            time.sleep(15)

    print(f"Upload finished: {os.path.basename(file_path)} (File ID: {response['id']})")
    return response['id']

# ---- Create a public shareable link ----
def create_shareable_link(file_id):
    """Create a public shareable link for a file in a shared drive."""
    permission = {
        'type': 'anyone',
        'role': 'reader',
    }

    drive_service.permissions().create(
        fileId=file_id,
        body=permission,
        supportsAllDrives=True
    ).execute()

    link = f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
    return link

# ---- Main function to upload and share ----
def upload_to_shared_drive_folder(drive_id, folder_id, file_path):
    """Upload file to a specific folder inside a shared drive and get shareable link."""
    try:
        file_id = upload_file_to_shared_drive_folder(file_path, folder_id)
        if file_id:
            link = create_shareable_link(file_id)
            print(f"Shareable link: {link}")
            return link
        else:
            print("File upload failed.")
            return None
    except HttpError as error:
        print(f"An error occurred: {error}")
        return None

# ---- Example usage ----
if __name__ == "__main__":

    from datetime import datetime as dt2
    today=dt2.now().strftime('%d_%b_%Y')

    drive_id = '0AJExn3WwBbdqUk9PVA'  # Shared Drive ID
    folder_id = '1LNVWVoxHpYTH9Qlrz6whVi2fE5CvKIK-'  # Replace with actual folder ID
    file_path = f"/home/thotat.vc/Downloads/Consolidated_Report_cofund_fpo_{today}.csv"
    file_path_kam=f"/home/thotat.vc/Downloads/Consolidated_Report_cofund_fpo_{today}_KAM.csv"
    file_path_re=f"/home/thotat.vc/Downloads/Consolidated_Report_cofund_fpo_{today}_RE.csv"
    file_path_um=f"/home/thotat.vc/Downloads/Consolidated_Report_cofund_fpo_{today}_UM.csv" 

# Replace with actual file path

    upload_to_shared_drive_folder(drive_id, folder_id,file_path) #file_path
    upload_to_shared_drive_folder(drive_id, folder_id,file_path_kam)
    upload_to_shared_drive_folder(drive_id, folder_id,file_path_re)
    upload_to_shared_drive_folder(drive_id, folder_id,file_path_um)
    link = "https://drive.google.com/drive/folders/1DSKaE5prHsmmUnjNyg4qSmZuAuXFRg78"
    #link2= "https://drive.google.com/drive/folders/1zmjaxwF6Ze3ePf92L8P1lPePO-noItBM"
    link3= "https://docs.google.com/spreadsheets/d/1ltixD7xZeeFORC0wsA4X6fzlk5DueqH0TMQIt1UaX7A/edit?usp=sharing"

import pandas as pd
import os
import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from datetime import datetime as dt
