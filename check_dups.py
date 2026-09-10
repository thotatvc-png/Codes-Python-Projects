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

# output = subprocess.check_output(['bash', '-c', 'echo $CLASSPATH:/usr/local/fdp-infra-hive/lib/*'])
# os.environ['CLASSPATH'] = '/usr/local/fdp-infra-hive/lib/*'
# url = ("jdbc:hive2://fkp-fdp-galaxy-sun-zkjn-0001.c.fkp-fdp-galaxy.internal:2181,fkp-fdp-galaxy-sun-zkjn-0002.c.fkp-fdp-galaxy.internal:2181,fkp-fdp-galaxy-sun-zkjn-0003.c.fkp-fdp-galaxy.internal:2181/default;transportMode=http;httpPath=cliservice;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=fkp-fdp-galaxy-hive3-hs2-agni;hive.client.read.socket.timeout=300?hive.metastore.client.socket.timeout=180;hive.server.tcp.keepalive=true;hive.server.read.socket.timeout=2000")

# conn = jaydebeapi.connect("org.apache.hive.jdbc.HiveDriver", url,{'user': "thotat.vc", 'password': "jljmydqrxxmtdtvz"})

# cursor = conn.cursor()
# cursor.execute("ADD JAR /usr/share/fk-bigfoot-hivejsonserde/json-serde-1.3-SNAPSHOT-jar-with-dependencies.jar")
# cursor.execute("ADD JAR /usr/share/fk-bigfoot-dimlookup/dimlookup-hive-udf-1.0-SNAPSHOT-jar-with-dependencies.jar")
# cursor.execute("ADD JAR gs://fkpdp-mhosy-2nig-1a5d-systemlibs/libraries/hive/jars/hive-udfs-1.0-SNAPSHOT.jar")
# cursor.execute("CREATE TEMPORARY FUNCTION lookup as 'com.flipkart.bigfoot.dimlookup.udf.HiveLookupUDF'")
# cursor.execute("CREATE TEMPORARY FUNCTION arrToString as 'com.flipkart.fdp.CustomCaster'")
# cursor.execute("CREATE TEMPORARY FUNCTION lookupkey as 'com.flipkart.bigfoot.dimlookup.udf.HiveKeyGeneratorUDF'")
# cursor.execute("CREATE TEMPORARY FUNCTION lookup_date as 'com.flipkart.bigfoot.dimlookup.udf.DateLookupUDF'")
# cursor.execute("CREATE TEMPORARY FUNCTION lookup_time as 'com.flipkart.bigfoot.dimlookup.udf.TimeLookupUDF'")
# cursor.execute("CREATE TEMPORARY FUNCTION aggregate_filter as 'com.flipkart.bigfoot.dimlookup.udf.AggregateConditionUDF'")
# cursor.execute("CREATE TEMPORARY FUNCTION aggregate as 'com.flipkart.bigfoot.dimlookup.udf.AggregateUDF'")
# cursor.execute("CREATE TEMPORARY FUNCTION ISOToSqlTs as 'com.flipkart.bigfoot.dimlookup.udf.DateISOToSqlUDF'")
# cursor.execute("CREATE TEMPORARY FUNCTION business_date_diff as 'com.flipkart.bigfoot.dimlookup.udf.BusinessDayDiffUDF'")
# cursor.execute("set mapreduce.input.fileinputformat.input.dir.recursive=true")
# sql1='''
# select
# a.listing_id as listing_id,
# b.listing_status as listing_status,
# c.display_name as display_name,
# b.analytic_category as analytic_category,
# CASE
#   WHEN d.analytic_super_category in ('ShopsyLifeStyle') and lower(d.analytic_category) IN ('na', 'shopsykidclothing','shopsymensclothingcasualtopwear','shopsymensclothingessentialsandethnic','shopsymensclothingjeans','shopsymensclothingsmartandformals','shopsywomenethniccontemporary','shopsywomenethniccore','shopsywomenwesterncore') THEN 'ShopsyLifeStyle Apparel'
#   WHEN d.analytic_super_category in ('ShopsyLifeStyle') and lower(d.analytic_category) not IN ('na', 'shopsykidclothing','shopsymensclothingcasualtopwear','shopsymensclothingessentialsandethnic','shopsymensclothingjeans','shopsymensclothingsmartandformals','shopsywomenethniccontemporary','shopsywomenethniccore','shopsywomenwesterncore') THEN 'ShopsyLifeStyle Non-Apparel'
#   WHEN d.analytic_super_category in ('ShopsyBGM') AND LOWER(d.analytic_category) IN ('shopsygrooming','shopsymakeupfragrances') THEN 'ShopsyBGM BPC'
#   WHEN d.analytic_super_category in ('ShopsyBGM') AND LOWER(d.analytic_category) NOT IN ('shopsygrooming','shopsymakeupfragrances') THEN 'ShopsyBGM GM'
#   ELSE d.analytic_super_category END AS analytic_super_category_l2
# from
#  fdp_uploads.ds_fkint_2gud_partner_vamsi_adhoc_1_0 a
# left join
# bigfoot_external_neo.sp_product__listing_hive_dim b
# on a.listing_id = b.listing_id
# left join
# bigfoot_external_neo.sp_seller__seller_hive_dim c
# on b.seller_id=c.seller_id
# left join
# bigfoot_external_neo.sp_product__product_categorization_hive_dim d
# on b.product_id = d.product_id  
# '''
# data = pd.read_sql_query(sql1, con=conn)
# df_listing=pd.DataFrame(data)
# print(df_listing)

from datetime import datetime as dt2
today=dt2.now().strftime('%d_%b_%Y')
# df_listing.to_csv(f'/home/thotat.vc/analytics_scripts/listing_raw_consolidated.csv') 
df_listing=pd.read_csv(f'/home/thotat.vc/analytics_scripts/listing_raw_consolidated.csv', encoding='cp1252') # Run this when code fails so that you pickup the last query output
print(df_listing)
# df_listing=df_listing.rename(columns={"b.analytic_category":"analytic_category"})


import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe


scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('/home/thotat.vc/analytics_scripts/creds.json', scope)
client = gspread.authorize(creds)

wks = client.open("Offer Extension Sheet").sheet1
ws = client.open("Offer Extension Sheet").worksheet("Offers to be extended till 11th Jan 2026")

data = ws.get_all_records()
df = pd.DataFrame(data)
print(df)

df.to_csv('/home/thotat.vc/analytics_scripts/consolidated_fpo_all_offers.csv')

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe


scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('/home/thotat.vc/analytics_scripts/creds.json', scope)
client = gspread.authorize(creds)

wks = client.open("Offer Extension Sheet").sheet1
ws = client.open("Offer Extension Sheet").worksheet("New FPOs Feb2026 Onwards")

data_new_fpo = ws.get_all_records()
df_new_fpo = pd.DataFrame(data_new_fpo)
print(df_new_fpo)

df_new_fpo.to_csv('/home/thotat.vc/analytics_scripts/new_fpo_all_offers.csv')



# # Read the CSV
import pandas as pd

# # # Load the CSV and prepare offer_ids list
df = pd.read_csv(r'/home/thotat.vc/analytics_scripts/consolidated_fpo_all_offers.csv', encoding='cp1252')
offer_ids = df['Offer IDs'].dropna().unique().tolist()
offer_ids_cleaned = [f"'{str(oid)}'" for oid in offer_ids]
offer_ids_clause = ', '.join(offer_ids_cleaned)

df_new_fpo = pd.read_csv(r'/home/thotat.vc/analytics_scripts/new_fpo_all_offers.csv', encoding='cp1252')
offer_ids_new_fpo = df_new_fpo['Offer IDs'].dropna().unique().tolist()
offer_ids_new_fpo_cleaned = [f"'{str(oid)}'" for oid in offer_ids_new_fpo]
offer_ids_new_fpo_clause = ', '.join(offer_ids_new_fpo_cleaned)

# # Inject offer_ids_clause directly into the SQL string
# sql2 = f"""
# SELECT DISTINCT a.offer_id AS offer_id, 
#                 b.product_id AS product_id, 
#                 a.listing_id AS listing_id, 
#                 b.seller_id AS seller_id, 
#                 c.cms_vertical AS cms_vertical, 
#                 b.listing_status AS listing_status
# FROM (
#     SELECT offer_id, listing_id
#     FROM (
#         SELECT data.listingid AS listing_id,
#                data.offerid AS offer_id,
#                MAX(CASE WHEN data.action = 'Mapped' THEN eventtime END) AS opt_in_time,
#                MAX(CASE WHEN data.action = 'Unmapped' THEN eventtime END) AS opt_out_time
#         FROM bigfoot_journal.dart_fkint_cp_santa_offerlistingmappingchange_5
#         WHERE day >= 20250731
#           AND data.offerid IN ({offer_ids_new_fpo_clause})
#         GROUP BY data.listingid, data.offerid
#     ) a
#     WHERE opt_out_time IS NULL OR opt_in_time > opt_out_time
# ) a
# JOIN bigfoot_external_neo.sp_product__listing_hive_dim b 
#   ON a.listing_id = b.listing_id
# JOIN bigfoot_external_neo.sp_product__product_categorization_hive_dim c
#   ON b.product_id = c.product_id
# """
# data2 = pd.read_sql_query(sql2, con=conn)
# df_optin=pd.DataFrame(data2)
# print(df_optin)

# df_optin.to_csv(f'/home/thotat.vc/analytics_scripts/optin_raw_consolidated.csv')
df_optin=pd.read_csv(f'/home/thotat.vc/analytics_scripts/optin_raw_consolidated.csv') # Run this when code fails so that you pickup the last query output
print(df_optin)

df_optin = df_optin[df_optin['offer_id'].isin(offer_ids_new_fpo)]


if df_optin.empty:
    print("Empty set error – df_optin has no data beyond headers.")

else:
    # Read Input File - Which is L3M Transacting

    # PROCESS BASE
    
    
    import pandas as pd
    file_path_1 = '/home/thotat.vc/analytics_scripts/FEB_2026_Consolidated_Base_File_Newly_updated.csv'
    df_1 = pd.read_csv(file_path_1, encoding='cp1252')
    df_1['owner'].replace(['', 'null'], pd.NA, inplace=True)
    df_1['owner'] = df_1['owner'].fillna('UM')
    df_1 = df_1.drop_duplicates()
    print(df_1.columns)