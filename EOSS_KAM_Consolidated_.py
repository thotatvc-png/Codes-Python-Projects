# With DB

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

import pandas as pd

# Read Files - KAM / RE / UM

df_kam=pd.read_csv('June Optin Base File - KAM.csv')
df_kam.columns = df_kam.columns.str.strip()  # Only remove leading/trailing spaces


df_re=pd.read_csv('June Optin Base File - RE.csv')
df_re.columns = df_re.columns.str.strip() 


df_um=pd.read_csv('June Optin Base File - UM.csv')
df_um.columns = df_um.columns.str.strip() 

# Common Listing Fact
sql1='''
select
a.listing_id as listing_id,
b.listing_status as listing_status,
c.display_name as display_name
from
fdp_uploads.ds_fkint_2gud_partner_mka_listing_id_1_0 a
left join
bigfoot_external_neo.sp_product__listing_hive_dim b
on a.listing_id = b.listing_id
left join
bigfoot_external_neo.sp_seller__seller_hive_dim c
on b.seller_id=c.seller_id
'''

data = pd.read_sql_query(sql1, con=conn)
df_listing=pd.DataFrame(data)
print(df_listing)

# KAM Offer Codes
sql2='''
select distinct a.offer_id as offer_id, b.product_id as product_id,a.listing_id as listing_id, b.seller_id as seller_id, c.cms_vertical as cms_vertical, b.listing_status as listing_status
from 
(
  select offer_id, listing_id
  from 
  (
    SELECT
    data.listingid AS listing_id,
    data.offerid AS offer_id,
    MAX(case when data.action = 'Mapped' then eventtime end) as opt_in_time,
    MAX(case when data.action = 'Unmapped' then eventtime end) as opt_out_time
    FROM bigfoot_journal.dart_fkint_cp_santa_offerlistingmappingchange_5
    WHERE day > lookup_date(date_sub(CURRENT_DATE, 60))
    and data.offerid in ('nb:mp:04bf2bb928', 'nb:mp:04fcdfd226','nb:mp:049ef01226','nb:mp:04478e5226','nb:mp:0417e74226','nb:mp:0400864026','nb:mp:0409078d26','nb:mp:04e1a29d26','nb:mp:04424bc926','nb:mp:04ea77f226','nb:mp:0486ddeb26','nb:mp:049b7aeb26','nb:mp:04477e1f26','nb:mp:04d62e0826','nb:mp:04e379cf26','nb:mp:046e5d3326', 'nb:mp:04402fec28', 'nb:mp:04bd644328', 'nb:mp:046c0ac727','nb:mp:04a622c827','nb:mp:04b7aab827','nb:mp:043697da27','nb:mp:04b5fb3727','nb:mp:04bd3fe127','nb:mp:0401818327','nb:mp:04b0ca2527','nb:mp:0481bd4027','nb:mp:049378ca27','nb:mp:042e51b427','nb:mp:04da95b327','nb:mp:0485438e27','nb:mp:0409039027', 'nb:mp:0412dda827','nb:mp:04397bd727','nb:mp:04767cca27','nb:mp:04f2283527','nb:mp:04d0db1a27','nb:mp:04ed8f2127','nb:mp:047950c027','nb:mp:0477e8d027','nb:mp:04438f7b27','nb:mp:04baefb927','nb:mp:04b8abea27') 
    GROUP BY
    data.listingid,
    data.offerid
  )a 
  where opt_out_time is null  or opt_in_time > opt_out_time  
)a 
join bigfoot_external_neo.sp_product__listing_hive_dim b 
on a.listing_id = b.listing_id
JOIN bigfoot_external_neo.sp_product__product_categorization_hive_dim c
 ON b.product_id =  c.product_id
'''
data2 = pd.read_sql_query(sql2, con=conn)
df_optin_kam=pd.DataFrame(data2)
print(df_optin_kam)


# RE Offer Codes
sql3='''
select distinct a.offer_id as offer_id, b.product_id as product_id,a.listing_id as listing_id, b.seller_id as seller_id, c.cms_vertical as cms_vertical, b.listing_status as listing_status
from 
(
  select offer_id, listing_id
  from 
  (
    SELECT
    data.listingid AS listing_id,
    data.offerid AS offer_id,
    MAX(case when data.action = 'Mapped' then eventtime end) as opt_in_time,
    MAX(case when data.action = 'Unmapped' then eventtime end) as opt_out_time
    FROM bigfoot_journal.dart_fkint_cp_santa_offerlistingmappingchange_5
    WHERE day > lookup_date(date_sub(CURRENT_DATE, 60))
    and data.offerid in ('nb:mp:03bf618f29','nb:mp:034df3b229','nb:mp:039f1fe429','nb:mp:02ce370f29','nb:mp:02f7c39629','nb:mp:026b47c929','nb:mp:0202049f29','nb:mp:0485ac5926','nb:mp:048615c926','nb:mp:041ad17f21','nb:mp:045680f621','nb:mp:04b6feff21','nb:mp:0452ec3321','nb:mp:043654e021','nb:mp:034c5e4914','nb:mp:03b9b47914','nb:mp:030c29f714','nb:mp:0399572419','nb:mp:0370462e19','nb:mp:03120f4319','nb:mp:03bc17de19','nb:mp:040d554321','nb:mp:0447168e22','nb:mp:045031eb22','nb:mp:0448232c26','nb:mp:0406d7ef26','nb:mp:04be4b0f26','nb:mp:045cf34a26','nb:mp:046e5d3326','nb:mp:04e379cf26','nb:mp:04d62e0826','nb:mp:049b7aeb26','nb:mp:04e1a29d26','nb:mp:0486ddeb26','nb:mp:04ea77f226','nb:mp:04477e1f26','nb:mp:04424bc926','nb:mp:0409078d26','nb:mp:0417e74226','nb:mp:04478e5226','nb:mp:0400864026','nb:mp:049ef01226','nb:mp:04fcdfd226','nb:mp:04759afa26','nb:mp:04ca80ec26','nb:mp:049a70b426','nb:mp:0462afcb26','nb:mp:0422aac826','nb:mp:0499149126','nb:mp:0451b9f726','nb:mp:04a873bd26','nb:mp:04da95b327','nb:mp:042e51b427','nb:mp:0409039027','nb:mp:04b7aab827','nb:mp:043697da27','nb:mp:04b0ca2527','nb:mp:049378ca27','nb:mp:0401818327','nb:mp:0481bd4027','nb:mp:04b5fb3727','nb:mp:04bd3fe127','nb:mp:04a622c827','nb:mp:046c0ac727','nb:mp:0485438e27','nb:mp:04a93f8d28') 
    GROUP BY
    data.listingid,
    data.offerid
  )a 
  where opt_out_time is null  or opt_in_time > opt_out_time  
)a 
join bigfoot_external_neo.sp_product__listing_hive_dim b 
on a.listing_id = b.listing_id
JOIN bigfoot_external_neo.sp_product__product_categorization_hive_dim c
 ON b.product_id =  c.product_id
'''
data3 = pd.read_sql_query(sql3, con=conn)
df_optin_re=pd.DataFrame(data3)
print(df_optin_re)


# UM Offer Codes
sql4='''
select distinct a.offer_id as offer_id, b.product_id as product_id,a.listing_id as listing_id, b.seller_id as seller_id, c.cms_vertical as cms_vertical, b.listing_status as listing_status
from 
(
  select offer_id, listing_id
  from 
  (
    SELECT
    data.listingid AS listing_id,
    data.offerid AS offer_id,
    MAX(case when data.action = 'Mapped' then eventtime end) as opt_in_time,
    MAX(case when data.action = 'Unmapped' then eventtime end) as opt_out_time
    FROM bigfoot_journal.dart_fkint_cp_santa_offerlistingmappingchange_5
    WHERE day > lookup_date(date_sub(CURRENT_DATE, 60))
    and data.offerid in ('nb:mp:0202049f29', 'nb:mp:026b47c929','nb:mp:02f7c39629','nb:mp:02ce370f29','nb:mp:03bf618f29','nb:mp:034df3b229','nb:mp:039f1fe429','nb:mp:02ce370f29','nb:mp:02f7c39629','nb:mp:026b47c929','nb:mp:0202049f29','nb:mp:0485ac5926','nb:mp:048615c926','nb:mp:041ad17f21','nb:mp:045680f621','nb:mp:04b6feff21','nb:mp:0452ec3321','nb:mp:043654e021','nb:mp:034c5e4914','nb:mp:03b9b47914','nb:mp:030c29f714','nb:mp:0399572419','nb:mp:0370462e19','nb:mp:03120f4319','nb:mp:03bc17de19','nb:mp:040d554321','nb:mp:0447168e22','nb:mp:045031eb22','nb:mp:0448232c26','nb:mp:0406d7ef26','nb:mp:04be4b0f26','nb:mp:045cf34a26','nb:mp:046e5d3326','nb:mp:04e379cf26','nb:mp:04d62e0826','nb:mp:049b7aeb26','nb:mp:04e1a29d26','nb:mp:0486ddeb26','nb:mp:04ea77f226','nb:mp:04477e1f26','nb:mp:04424bc926','nb:mp:0409078d26','nb:mp:0417e74226','nb:mp:04478e5226','nb:mp:0400864026','nb:mp:049ef01226','nb:mp:04fcdfd226','nb:mp:04759afa26','nb:mp:04ca80ec26','nb:mp:049a70b426','nb:mp:0462afcb26','nb:mp:0422aac826','nb:mp:0499149126','nb:mp:0451b9f726','nb:mp:04a873bd26','nb:mp:04da95b327','nb:mp:042e51b427','nb:mp:0409039027','nb:mp:04b7aab827','nb:mp:043697da27','nb:mp:04b0ca2527','nb:mp:049378ca27','nb:mp:0401818327','nb:mp:0481bd4027','nb:mp:04b5fb3727','nb:mp:04bd3fe127','nb:mp:04a622c827','nb:mp:046c0ac727','nb:mp:0485438e27','nb:mp:04a93f8d28','nb:mp:04bf2bb928', 'nb:mp:04fcdfd226','nb:mp:049ef01226','nb:mp:04478e5226','nb:mp:0417e74226','nb:mp:0400864026','nb:mp:0409078d26','nb:mp:04e1a29d26','nb:mp:04424bc926','nb:mp:04ea77f226','nb:mp:0486ddeb26','nb:mp:049b7aeb26','nb:mp:04477e1f26','nb:mp:04d62e0826','nb:mp:04e379cf26','nb:mp:046e5d3326', 'nb:mp:04402fec28', 'nb:mp:04bd644328', 'nb:mp:046c0ac727','nb:mp:04a622c827','nb:mp:04b7aab827','nb:mp:043697da27','nb:mp:04b5fb3727','nb:mp:04bd3fe127','nb:mp:0401818327','nb:mp:04b0ca2527','nb:mp:0481bd4027','nb:mp:049378ca27','nb:mp:042e51b427','nb:mp:04da95b327','nb:mp:0485438e27','nb:mp:0409039027', 'nb:mp:0412dda827','nb:mp:04397bd727','nb:mp:04767cca27','nb:mp:04f2283527','nb:mp:04d0db1a27','nb:mp:04ed8f2127','nb:mp:047950c027','nb:mp:0477e8d027','nb:mp:04438f7b27','nb:mp:04baefb927','nb:mp:04b8abea27')
    GROUP BY
    data.listingid,
    data.offerid
  )a 
  where opt_out_time is null  or opt_in_time > opt_out_time  
)a 
join bigfoot_external_neo.sp_product__listing_hive_dim b 
on a.listing_id = b.listing_id
JOIN bigfoot_external_neo.sp_product__product_categorization_hive_dim c
 ON b.product_id =  c.product_id
'''
data4 = pd.read_sql_query(sql4, con=conn)
df_optin_um=pd.DataFrame(data4)
print(df_optin_um)


# Read Input File - Offers
import pandas as pd

file_path_1 = '/home/thotat.vc/analytics_scripts/EOSS_JUNE_NON_APPAREL+ADDITIONAL_INPUTS_OFFER_SHEET.csv'
df_1 = pd.read_csv(file_path_1, encoding='cp1252')
df_1 = df_1.drop_duplicates()

file_path_2 = '/home/thotat.vc/analytics_scripts/EOSS_JUNE_JIRA_OPT-IN-OPT-OUT.csv'
df_2 = pd.read_csv(file_path_2, encoding='cp1252')
df_2 = df_1.drop_duplicates()

file_path_3 = '/home/thotat.vc/analytics_scripts/JUNE_EOSS_BGM_HELF_WW_MENS_APPAREL_OFFER_SHEET.csv'
df_3 = pd.read_csv(file_path_3, encoding='cp1252')
df_3 = df_1.drop_duplicates()

file_path_4 = '/home/thotat.vc/analytics_scripts/EOSS_JUNE_rest_of_Apparels_offer_sheet.csv'
df_4 = pd.read_csv(file_path_4, encoding='cp1252')
df_4 = df_1.drop_duplicates()

file_path_5 = '/home/thotat.vc/analytics_scripts/JUNE_EOSS_Clearance_Offers_sheet.csv'
df_5 = pd.read_csv(file_path_5, encoding='cp1252')
df_5 = df_1.drop_duplicates()

all_listing_ids_1 = pd.concat([
    df_1[df_1['Offer ID'] != 'Manual Pricing']['LID'],
    df_2[df_2['Offer ID'] != 'Manual Pricing']['LID'],
    df_3[df_3['Offer ID'] != 'Manual Pricing']['LID'],
    df_4[df_4['Offer ID'] != 'Manual Pricing']['LID'],
    df_5[df_5['Offer ID'] != 'Manual Pricing']['LID']
], ignore_index=True).dropna().drop_duplicates()

all_listing_ids_df = pd.DataFrame(all_listing_ids_1, columns=['LID'])
input_lids_1 = set(all_listing_ids_df['LID']) #here changed listing_id to LID

# Equate to today for uniqueness of files
from datetime import datetime as dt2
today=dt2.now().strftime('%d_%b_%Y')

# Temp assignment
df_kam['LID'] = df_kam['LID'].astype(str).str.strip()
df_kam['Seller Id'] = df_kam['Seller Id'].astype(str).str.strip()
df_kam['Correct cms_vertical']=df_kam['Correct cms_vertical'].astype(str).str.strip()


df_re['LID'] = df_re['LID'].astype(str).str.strip()
df_re['Seller Id'] = df_re['Seller Id'].astype(str).str.strip()
df_re['Correct cms_vertical']=df_re['Correct cms_vertical'].astype(str).str.strip()

df_um['LID'] = df_um['LID'].astype(str).str.strip()
df_um['Seller Id'] = df_um['Seller Id'].astype(str).str.strip()
df_um['Correct cms_vertical']=df_um['Correct cms_vertical'].astype(str).str.strip()


# Read from listing_status and remove inactive LIDs from the base by comparing it with current listing_status

removal_df_1 = df_listing.drop_duplicates()
removal_df_1 = removal_df_1[removal_df_1['listing_status'] == 'ACTIVE']
removal_df_1['listing_id'] = removal_df_1['listing_id'].astype(str).str.strip()
removal_ids_1 = set(removal_df_1['listing_id'])

df_kam['LID'] = df_kam['LID'].astype(str).str.strip()
df_kam['listing_status'] = df_kam['LID'].apply(lambda x: 'ACTIVE' if x in removal_ids_1 else 'INACTIVE')
df_kam = df_kam[df_kam['listing_status'] == 'ACTIVE']
df_kam['Input Flag'] = df_kam['LID'].apply(lambda x: 'Input' if x in input_lids_1 else 'Non-Input')
df_kam = df_kam.drop_duplicates()


df_re['LID'] = df_re['LID'].astype(str).str.strip()
df_re['listing_status'] = df_re['LID'].apply(lambda x: 'ACTIVE' if x in removal_ids_1 else 'INACTIVE')
df_re = df_re[df_re['listing_status'] == 'ACTIVE']
df_re['Input Flag'] = df_re['LID'].apply(lambda x: 'Input' if x in input_lids_1 else 'Non-Input')
df_re = df_re.drop_duplicates()

df_um['LID'] = df_um['LID'].astype(str).str.strip()
df_um['listing_status'] = df_um['LID'].apply(lambda x: 'ACTIVE' if x in removal_ids_1 else 'INACTIVE')
df_um = df_um[df_um['listing_status'] == 'ACTIVE']
df_um['Input Flag'] = df_um['LID'].apply(lambda x: 'Input' if x in input_lids_1 else 'Non-Input')
df_um = df_um.drop_duplicates()


# Return the optin report - KAM

df_optin_kam=df_optin_kam
df_optin_kam=df_optin_kam.drop_duplicates()
df_optin_kam

# Cross check if unique listing_id from the base file is present in the optin report.
optin_combinations_kam = set(zip(df_optin_kam['listing_id']))
df_kam=df_kam
df_kam['Opted-in listing_id'] = df_kam.apply(
    lambda row: 'Opted-in' if (row['LID']) in optin_combinations_kam else 'Not opted-in', 
    axis=1
)
df_kam=df_kam.drop_duplicates()
df_kam

# Make unique combinations of seller_id and cms_vertical from the base file and cross check in the optin report
optin_combinations_kam = set(zip(df_optin_kam['seller_id'], df_optin_kam['cms_vertical']))
df_kam['Opted-in seller_id'] = df_kam.apply(
    lambda row: 'Opted-in' if (row['Seller Id'], row['Correct cms_vertical']) in optin_combinations_kam else 'Not opted-in', 
    axis=1
)
df_kam=df_kam.drop_duplicates()
df_kam

# Map orders for DW - this is a one time run only.
# df_orders=pd.read_csv('/home/thotat.vc/analytics_scripts/EOSS_ORDERS.csv')
# df_orders['orders']=df_orders['orders'].fillna(0)
# df_orders=df_orders.drop_duplicates()
# df_orders

# # Map unique listing with optin status to the orders for getting seller_id, orders
# cols=['listing_id', 'orders']
# df_orders_1=df_orders[cols]
# df_1_raw=pd.merge(df_1_raw, df_orders_1, left_on='LID', right_on='listing_id', how='left')
# df_1_raw['orders']=df_1_raw['orders'].fillna(0)
# df_1_raw=df_1_raw.drop_duplicates()
# df_1_raw

# df_kam=df_kam[df_kam['Offer ID']!='Manual Pricing']
# df_kam

# Make summary at KAM level from result - which contains LID, SID, CMS vert, Orders, Optin of both seller and LID and add the necessary aggregations.
unique_listing_ids_df_summary_kam=df_kam.groupby('Input Flag').agg(
    listing_id=('LID', 'nunique'),
    opted_in_listing_id=('LID', lambda x: (x[df_kam.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).nunique()),
    seller_id=('Seller Id','nunique'),
    opted_in_seller_id=('Seller Id', lambda x: (x[df_kam.loc[x.index, 'Opted-in seller_id'] == 'Opted-in']).nunique()),
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[df_kam.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
).reset_index()


unique_listing_ids_df_summary_kam['Owner'] = 'KAM'


# Return the optin report - RE

df_optin_re=df_optin_re
df_optin_re=df_optin_re.drop_duplicates()
df_optin_re

# Cross check if unique listing_id from the base file is present in the optin report.
optin_combinations_re = set(zip(df_optin_re['listing_id']))
df_re=df_re
df_re['Opted-in listing_id'] = df_re.apply(
    lambda row: 'Opted-in' if (row['LID']) in optin_combinations_re else 'Not opted-in', 
    axis=1
)
df_re=df_re.drop_duplicates()
df_re

# Make unique combinations of seller_id and cms_vertical from the base file and cross check in the optin report
optin_combinations_re = set(zip(df_optin_re['seller_id'], df_optin_re['cms_vertical']))
df_re['Opted-in seller_id'] = df_re.apply(
    lambda row: 'Opted-in' if (row['Seller Id'], row['Correct cms_vertical']) in optin_combinations_re else 'Not opted-in', 
    axis=1
)
df_re=df_re.drop_duplicates()
df_re

# Make summary at RE level from result - which contains LID, SID, CMS vert, Orders, Optin of both seller and LID and add the necessary aggregations.
unique_listing_ids_df_summary_re=df_re.groupby('Input Flag').agg(
    listing_id=('LID', 'nunique'),
    opted_in_listing_id=('LID', lambda x: (x[df_re.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).nunique()),
    seller_id=('Seller Id','nunique'),
    opted_in_seller_id=('Seller Id', lambda x: (x[df_re.loc[x.index, 'Opted-in seller_id'] == 'Opted-in']).nunique()),
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[df_re.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
).reset_index()

unique_listing_ids_df_summary_re['Owner'] = 'RE'
df_re.columns = df_re.columns.str.strip()



# Return Optin Report - UM

df_optin_um=df_optin_um
df_optin_um=df_optin_um.drop_duplicates()
df_optin_um

# Cross check if unique listing_id from the base file is present in the optin report.
optin_combinations_um = set(zip(df_optin_um['listing_id']))
df_um=df_um
df_um['Opted-in listing_id'] = df_um.apply(
    lambda row: 'Opted-in' if (row['LID']) in optin_combinations_re else 'Not opted-in', 
    axis=1
)
df_um=df_um.drop_duplicates()
df_um

# Make unique combinations of seller_id and cms_vertical from the base file and cross check in the optin report
optin_combinations_um = set(zip(df_optin_um['seller_id'], df_optin_um['cms_vertical']))
df_um['Opted-in seller_id'] = df_um.apply(
    lambda row: 'Opted-in' if (row['Seller Id'], row['Correct cms_vertical']) in optin_combinations_re else 'Not opted-in', 
    axis=1
)
df_um=df_um.drop_duplicates()
df_um

# Make summary at RE level from result - which contains LID, SID, CMS vert, Orders, Optin of both seller and LID and add the necessary aggregations.
unique_listing_ids_df_summary_um=df_um.groupby('Input Flag').agg(
    listing_id=('LID', 'nunique'),
    opted_in_listing_id=('LID', lambda x: (x[df_um.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).nunique()),
    seller_id=('Seller Id','nunique'),
    opted_in_seller_id=('Seller Id', lambda x: (x[df_um.loc[x.index, 'Opted-in seller_id'] == 'Opted-in']).nunique()),
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[df_um.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
).reset_index()

unique_listing_ids_df_summary_um['Owner'] = 'UM'

# Combine all summaries and generate %

combined_df = pd.concat([
    unique_listing_ids_df_summary_um,
    unique_listing_ids_df_summary_re,
    unique_listing_ids_df_summary_kam
], ignore_index=True)

total_row = combined_df.drop(columns=['Owner']).sum(numeric_only=True)
total_row['Owner'] = 'Total'

combined_df_with_total = pd.concat([
    combined_df,
    pd.DataFrame([total_row])
], ignore_index=True)


import numpy as np
combined_df_with_total['Opt In LID %']=np.where(combined_df_with_total['listing_id']==0, 0, combined_df_with_total['opted_in_listing_id']/combined_df_with_total['listing_id']).round(2)
combined_df_with_total['Opt In SID %']=np.where(combined_df_with_total['seller_id']==0, 0, combined_df_with_total['opted_in_seller_id']/combined_df_with_total['seller_id']).round(2)
combined_df_with_total['DW %']=np.where(combined_df_with_total['orders']==0, 0, combined_df_with_total['opted_in_orders']/combined_df_with_total['orders']).round(2)
combined_df_with_total['Opt In LID %'] = (combined_df_with_total['Opt In LID %'] * 100).round(2).astype(str) + '%'
combined_df_with_total['Opt In SID %'] = (combined_df_with_total['Opt In SID %'] * 100).round(2).astype(str) + '%'
combined_df_with_total['DW %'] = (combined_df_with_total['DW %'] * 100).round(2).astype(str) + '%'
combined_df_with_total

# Build the HTML table with grid lines
html_table = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">'
html_table += '<thead><tr><th style="border: 1px solid #ddd; padding: 8px;">KAM</th><th style="border: 1px solid #ddd; padding: 8px;">listing_id</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_listing_id</th><th style="border: 1px solid #ddd; padding: 8px;">seller_id</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_seller_id</th><th style="border: 1px solid #ddd; padding: 8px;">orders</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders</th><th style="border: 1px solid #ddd; padding: 8px;">Opt In LID %</th><th style="border: 1px solid #ddd; padding: 8px;">Opt In SID %</th><th style="border: 1px solid #ddd; padding: 8px;">DW %</th></tr></thead>'
html_table += '<tbody>'

# Loop through the dataframe to add rows with conditional formatting
for index, row in combined_df_with_total.iterrows():
    html_table += f'''
        <tr>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['Owner']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['listing_id']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_listing_id']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['seller_id']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_seller_id']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['Opt In LID %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['Opt In SID %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW %']}</td>                                                                               
        </tr>
    '''

html_table += '</tbody></table>'

# Add custom CSS for the table (optional styling like borders)
css_style = """
<style>
    .left-aligned-table {
        width: 100%;
        border-collapse: collapse;
        border: 1px solid #ddd;
    }
    .left-aligned-table th, .left-aligned-table td {
        padding: 8px;
        text-align: left;
        border: 1px solid #ddd;
    }
    .left-aligned-table th {
        background-color: #f2f2f2;
    }
    .left-aligned-table td {
        background-color: #ffffff;
    }
</style>
"""

# Combine the CSS and HTML table
html_table = css_style + html_table

## KAM File

with pd.ExcelWriter(f"/home/thotat.vc/Downloads/EOSS_KAM_Consolidated_{today}.xlsx") as writer:
    df_kam.to_excel(writer, sheet_name='Raw',index=False)
    combined_df_with_total.to_excel(writer, sheet_name='Summary',index=False)

filename_kam = f"EOSS_KAM_Consolidated_{today}.xlsx"
file_path_kam = f"/home/thotat.vc/Downloads/EOSS_KAM_Consolidated_{today}.xlsx"   

## RE File

with pd.ExcelWriter(f"/home/thotat.vc/Downloads/EOSS_RE_Consolidated_{today}.xlsx") as writer:
    df_re.to_excel(writer, sheet_name='Raw',index=False)
    combined_df_with_total.to_excel(writer, sheet_name='Summary',index=False)

filename_re = f"EOSS_RE_Consolidated_{today}.xlsx"
file_path_re = f"/home/thotat.vc/Downloads/EOSS_RE_Consolidated_{today}.xlsx"  

## UM File

with pd.ExcelWriter(f"/home/thotat.vc/Downloads/EOSS_UM_Consolidated_{today}.xlsx") as writer:
    df_um.to_excel(writer, sheet_name='Raw',index=False)
    combined_df_with_total.to_excel(writer, sheet_name='Summary',index=False)

filename_um = f"EOSS_UM_Consolidated_{today}.xlsx"
file_path_um = f"/home/thotat.vc/Downloads/EOSS_UM_Consolidated_{today}.xlsx"   

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

creds = ServiceAccountCredentials.from_json_keyfile_name('/home/thotat.vc/creds.json', scope)
# client = gspread.authorize(creds)
http = creds.authorize(httplib2.Http(timeout=600))
drive_service = build('drive', 'v3', http=http)

import os
import urllib.parse
from datetime import datetime, timedelta
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

# ---- Function to upload file to a specific Google Drive folder ----
def upload_file_to_folder(file_path, folder_id):
    """Upload a file to a Google Drive folder."""
    file_metadata = {
        'name': os.path.basename(file_path),
        'parents': [folder_id]
    }
    media = MediaFileUpload(file_path, mimetype='application/octet-stream')
    
    file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()
    
    print(f"File uploaded to folder ID: {folder_id} with file ID: {file['id']}.")
    return file['id']

# ---- Function to find a shared folder by exact name and upload file to it ----
def navigate_to_folder_and_upload_file(folder_name, file_path):
    """Find a shared folder by name and upload a file to it."""
    try:
        # Get shared folders
        results = drive_service.files().list(
            q="sharedWithMe and mimeType='application/vnd.google-apps.folder'",
            fields="files(id, name, mimeType)"
        ).execute()
        
        items = results.get('files', [])
        target_folder = None
        
        for item in items:
            if item.get('mimeType') == 'application/vnd.google-apps.folder' and item.get('name') == folder_name:
                target_folder = item
                print(f"Found folder: {target_folder['name']} (ID: {target_folder['id']})")
                break
        
        if not target_folder:
            print(f"No folder found with the name '{folder_name}'.")
            return None
        
        # Upload the file directly to the found folder
        file_id = upload_file_to_folder(file_path, target_folder['id'])
        if file_id:
            print(f"File uploaded successfully with ID: {file_id}")
            return file_id
        else:
            print("File upload failed.")
            return None

    except HttpError as error:
        print(f"An error occurred: {error}")
        return None

# ---- Function to create a shareable link for a file ----
# def create_shareable_link(drive_service, file_id):
#     """Create a public shareable link for a Google Drive file."""
#     permission = {
#         'type': 'anyone',
#         'role': 'reader',
#     }
    
#     drive_service.permissions().create(fileId=file_id, body=permission).execute()
    
#     link = f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
#     return link

file_id = navigate_to_folder_and_upload_file('EOSS Consolidated Optins', file_path_kam)
# link = create_shareable_link(drive_service, file_id)
file_id = navigate_to_folder_and_upload_file('EOSS Consolidated Optins', file_path_re)
file_id = navigate_to_folder_and_upload_file('EOSS Consolidated Optins', file_path_um)

link="https://drive.google.com/drive/folders/1loNG1kPjAj_XLbCQQNFip315b_Sjduxl?usp=drive_link"

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from datetime import datetime as dt
import os

sender_email = 'thotat.vc@flipkart.com'
sender_password = 'kiV1thai!@#%'

# Original message metadata
original_from = 'thotat.vc@flipkart.com'
original_to = ['vishnuac.vc@flipkart.com','upasanaupadhak.vc@flipkart.com']
#'shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com'
original_cc = ['thotat.vc@flipkart.com']
original_message_id = '<CAPjHi7x-datJhaqcxTay2n_NjKdMCKhFr6_EF=dAQiVjucaHSw@mail.gmail.com>'

all_recipients = set([original_from] + original_to + original_cc)
msg = MIMEMultipart()
msg['From'] = sender_email
msg['To'] = ', '.join(original_to)
msg['Cc'] = ', '.join(original_cc)
msg['Subject'] = f"[IMP] EOSS Sale Optin Tracking - Consolidated - KAM-RE-UM"
msg['In-Reply-To'] = original_message_id
msg['References'] = original_message_id

body = f"""
Hi Team,<br><br>
Please find below the optin status for EOSS:<br><br>
{html_table}<br><br>
The below links contain the drives where the files are uploaded:{link}<br><br>
<b>Regards,<br>
Kishan Menon</b>
"""
msg.attach(MIMEText(body, 'html'))

# if os.path.exists(file_path):
#     with open(file_path, 'rb') as f:
#         file_attachment = MIMEApplication(f.read(), _subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet')
#         file_attachment.add_header('Content-Disposition', 'attachment', filename=filename)
#         msg.attach(file_attachment)
# else:
#     print(f"File not found: {file_path}")


try:
    server = smtplib.SMTP('127.0.0.1', 25)
    server.set_debuglevel(1)
    server.sendmail(sender_email, list(all_recipients), msg.as_string())
    print("Reply-All email with HTML table and Excel attachment sent successfully!")
except smtplib.SMTPException as e:
    print(f"Error sending email: {e}")
finally:
    server.quit()