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

conn = jaydebeapi.connect("org.apache.hive.jdbc.HiveDriver", url,{'user': "thotat.vc", 'password': "jljmydqrxxmtdtvz"})

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
 fdp_uploads.ds_fkint_2gud_partner_vamsi_adhoc_1_0 a
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

from datetime import datetime as dt2
today=dt2.now().strftime('%d_%b_%Y')
df_listing.to_csv(f'/home/thotat.vc/analytics_scripts/listing_raw_consolidated.csv') 
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
sql2 = f"""
SELECT DISTINCT a.offer_id AS offer_id, 
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
print(df_optin)

df_optin.to_csv(f'/home/thotat.vc/analytics_scripts/optin_raw_consolidated.csv')
df_optin=pd.read_csv(f'/home/thotat.vc/analytics_scripts/optin_raw_consolidated.csv') # Run this when code fails so that you pickup the last query output
print(df_optin)


if df_optin.empty:
    print("Empty set error – df_optin has no data beyond headers.")

else:
    # Read Input File - Which is L3M Transacting

    # PROCESS BASE

    import pandas as pd
    file_path_1 = '/home/thotat.vc/analytics_scripts/FEB_2026_Consolidated_Base_File_Updated.csv'
    df_1 = pd.read_csv(file_path_1, encoding='cp1252')
    df_1['owner'].replace(['', 'null'], pd.NA, inplace=True)
    df_1['owner'] = df_1['owner'].fillna('UM')
    df_1 = df_1.drop_duplicates()
    print(df_1.columns)

    # Equate to today for uniqueness of files
    from datetime import datetime as dt2
    today=dt2.now().strftime('%d_%b_%Y')

    # Temp assignment
    df_1_1=df_1
    #df_1_1['LID'] = df_1_1['LID'].astype(str).str.strip()
    df_1_1['listing_id'] = df_1_1['listing_id'].astype(str).str.strip()
    df_1_1['seller_id'] = df_1_1['seller_id'].astype(str).str.strip()
    #df_1_1['Offer ID'] = df_1_1['Offer ID'].astype(str).str.strip()
    df_1_1['cms_vertical'] = df_1_1['cms_vertical'].astype(str).str.strip()
    # df_1['KAM']='' # Comment if KAM is blank
    df_1_1['KAM'] = df_1_1['KAM'].str.upper()
    
    print(df_1_1['owner'].unique())


    # Read from listing_status and remove inactive LIDs from the base by comparing it with current listing_status

    removal_df_1 = df_listing.drop_duplicates()
    removal_df_1 = removal_df_1[removal_df_1['listing_status'] == 'ACTIVE']
    removal_df_1['listing_id'] = removal_df_1['listing_id'].astype(str).str.strip()
    removal_ids_1 = set(removal_df_1['listing_id'])


    df_1_1['listing_status_current'] = df_1_1['listing_id'].apply(lambda x: 'ACTIVE' if x in removal_ids_1 else 'INACTIVE')
    # Ensure 'listing_id' is in the list of selected columns
    
    # cols=['listing_id','analytic_super_category_l2','analytic_category']
    # df_listing_1=df_listing[cols]

    # df_1_1=df_1_1.merge(df_listing_1, left_on='listing_id', right_on='listing_id',how='left')
    # df_1_1['analytic_super_category_l2'] = df_1_1['analytic_super_category_l2'].astype(str).str.strip()
    # df_1_1['analytic_category'] = df_1_1['analytic_category'].astype(str).str.strip()
    df_1_1 = df_1_1[df_1_1['listing_status_current'] == 'ACTIVE']
    df_1_1 = df_1_1[df_1_1['analytic_category']!='ShopsyMobileProtection']

    ## Read Input - which is current offer universe

    df_universe=pd.read_csv('/home/thotat.vc/analytics_scripts/Input_KAM_Mapping.csv',encoding='cp1252')
    df_1_1['listing_id']=df_1_1['listing_id'].astype(str).str.strip()
    df_universe['LID'] = df_universe['LID'].astype(str).str.strip()
    input_lids = set(df_universe['LID'])

    df_1_1['Set'] = df_1_1['listing_id'].apply(lambda x: 'INPUT SET' if x in input_lids else 'NON-INPUT SET')
    
    df_final_mapping=pd.read_csv('/home/thotat.vc/analytics_scripts/Input_KAM_Mapping.csv',encoding='cp1252')
    df_1_1['listing_id']=df_1_1['listing_id'].astype(str).str.strip()
    df_final_mapping['listing_id']=df_final_mapping['listing_id'].astype(str).str.strip()
    df_final_mapping=df_final_mapping.rename(columns = {'KAM':'Input KAM', 'LID':'listing_id'})
    df_1_1=df_1_1.rename(columns={'KAM':'Primary KAM'})
    df_1_1=df_1_1.merge(df_final_mapping, on=['listing_id'],how='left')
    df_1_1['KAM']=df_1_1['Input KAM'].fillna(df_1_1['Primary KAM'])
    
    df_lead_mapping=pd.read_csv('/home/thotat.vc/analytics_scripts/KAM_Lead_Mapping.csv',encoding='cp1252') # Should have Lead and KAM columns
    df_1_1=df_1_1.merge(df_lead_mapping, on=['KAM'], how='left')
    
    df_optin=df_optin
    df_optin=df_optin.drop_duplicates()
    df_optin

    df_optin['listing_id'] = df_optin['listing_id'].astype(str).str.strip()
    df_optin['offer_id'] = df_optin['offer_id'].astype(str).str.strip()
    df_optin['seller_id'] = df_optin['seller_id'].astype(str).str.strip()
    df_optin_summary = df_optin.groupby('listing_id').agg(
    opted_in_offer_id=('offer_id', list)).reset_index()

    df_optin_new_fpo = df_optin[df_optin["offer_id"].isin(offer_ids_new_fpo_clause)].copy()


    # Cross check if unique listing_id from the base file is present in the optin report.
    optin_combinations = set(df_optin['listing_id'])
    df_1_raw=df_1_1
    df_1_raw['Opted-in listing_id'] = df_1_raw.apply(
        lambda row: 'Opted-in' if (row['listing_id']) in optin_combinations else 'Not opted-in', 
        axis=1
        #,row['Offer ID']
    )
    df_1_raw=df_1_raw.drop_duplicates()
    df_1_raw

    # Make unique combinations of seller_id and cms_vertical from the base file and cross check in the optin report
    optin_combinations = set(zip(df_optin['seller_id'], df_optin['cms_vertical']))
    df_1_raw['Opted-in seller_id'] = df_1_raw.apply(
        lambda row: 'Opted-in' if (row['seller_id'], row['cms_vertical']) in optin_combinations else 'Not opted-in', 
        axis=1
    )
    df_1_raw=df_1_raw.drop_duplicates()
    df_1_raw

    # Map orders for DW - this is a one time run only.
    # df_orders=pd.read_csv('/home/thotat.vc/analytics_scripts/non_apparel_sss_orders.csv')
    # df_orders['orders']=(df_orders['orders'].fillna(0))/60
    # df_orders=df_orders.drop_duplicates()
    # df_orders

    # Map unique listing with optin status to the orders for getting seller_id, orders
    # cols=['listing_id', 'orders']
    # df_orders_1=df_orders[cols]
    # df_1_raw=pd.merge(df_1_raw, df_orders_1, left_on='LID', right_on='listing_id', how='left')
    # df_1_raw['orders']=df_1_raw['orders'].fillna(0)
    df_1_raw=df_1_raw.drop_duplicates()
    df_1_raw

    # df_1_raw['order_drr']=(df_1_raw['orders'].fillna(0))
    df_1_raw['order_drr']=df_1_raw['order_drr'].round(2).astype(int)

    # df_1_edited=df_1_raw[df_1_raw['Offer ID']!='Manual Pricing']
    # df_1_edited = df_1_raw[df_1_raw['Offer ID'] != 'Manual Pricing']
    df_1_edited = df_1_raw
    df_1_edited

    # Make summary at KAM level from result - which contains LID, SID, CMS vert, Orders, Optin of both seller and LID and add the necessary aggregations.
    # unique_listing_ids_df_summary=df_1_edited.groupby('KAM').agg(
    #     listing_id=('LID', 'nunique'),
    #     opted_in_listing_id=('LID', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).nunique()),
    #     seller_id=('Seller Id','nunique'),
    #     opted_in_seller_id=('Seller Id', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in seller_id'] == 'Opted-in']).nunique()),
    #     orders=('orders', 'sum'),
    #     opted_in_orders=('orders', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    # ).reset_index()

    # Input Summary
    df_1_edited_kam=df_1_edited[df_1_edited['owner']=='KAM']

    # OWNER ---

    # Input SET Summary
    input_df_owner = df_1_edited[df_1_edited['Set'] == 'INPUT SET']
    summary_input_owner = input_df_owner.groupby('owner').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[input_df_owner.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_input_owner.rename(columns={'owner': 'KAM'}, inplace=True)
    summary_input_owner.columns = [col if col == 'owner' else col + '_input' for col in summary_input_owner.columns]

    # Non-Input SET Summary
    non_input_df_owner = df_1_edited[df_1_edited['Set'] == 'NON-INPUT SET']
    summary_non_input_owner = non_input_df_owner.groupby('owner').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[non_input_df_owner.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_non_input_owner.rename(columns={'owner': 'KAM'}, inplace=True)
    summary_non_input_owner.columns = [col if col == 'owner' else col + '_non_input' for col in summary_non_input_owner.columns]

    # Overall Summary
    summary_overall_owner = df_1_edited.groupby('owner').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_overall_owner.rename(columns={'owner': 'KAM'}, inplace=True)
    summary_overall_owner.columns = [col if col == 'owner' else col + '_overall' for col in summary_overall_owner.columns]

    # Final merge
    summary_final_owner = summary_input_owner.merge(summary_non_input_owner, on='owner', how='outer')
    summary_final_owner = summary_final_owner.merge(summary_overall_owner, on='owner', how='outer')

    unique_listing_ids_df_summary_owner = summary_final_owner


    import numpy as np
    # unique_listing_ids_df_summary_owner['Opt In LID %']=np.where(unique_listing_ids_df_summary_owner['listing_id']==0, 0, unique_listing_ids_df_summary_owner['opted_in_listing_id']/unique_listing_ids_df_summary_owner['listing_id']).round(2)
    # unique_listing_ids_df_summary_owner['Opt In SID %']=np.where(unique_listing_ids_df_summary_owner['seller_id']==0, 0, unique_listing_ids_df_summary_owner['opted_in_seller_id']/unique_listing_ids_df_summary_owner['seller_id']).round(2)
    unique_listing_ids_df_summary_owner['DW Input %']=np.where(unique_listing_ids_df_summary_owner['orders_input']==0, 0, unique_listing_ids_df_summary_owner['opted_in_orders_input']/unique_listing_ids_df_summary_owner['orders_input']).round(2)

    # unique_listing_ids_df_summary_owner['Opt In LID %'] = (unique_listing_ids_df_summary_owner['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_owner['Opt In SID %'] = (unique_listing_ids_df_summary_owner['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_owner['DW Input %'] = (unique_listing_ids_df_summary_owner['DW Input %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_owner
    
    

    unique_listing_ids_df_summary_owner['DW Non Input %']=np.where(unique_listing_ids_df_summary_owner['orders_non_input']==0, 0, unique_listing_ids_df_summary_owner['opted_in_orders_non_input']/unique_listing_ids_df_summary_owner['orders_non_input']).round(2)
    
    # unique_listing_ids_df_summary_owner['Opt In LID %'] = (unique_listing_ids_df_summary_owner['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_owner['Opt In SID %'] = (unique_listing_ids_df_summary_owner['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_owner['DW Non Input %'] = (unique_listing_ids_df_summary_owner['DW Non Input %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_owner

    
    unique_listing_ids_df_summary_owner['DW Overall %']=np.where(unique_listing_ids_df_summary_owner['orders_overall']==0, 0, unique_listing_ids_df_summary_owner['opted_in_orders_overall']/unique_listing_ids_df_summary_owner['orders_overall']).round(2)
    
    # unique_listing_ids_df_summary_owner['Opt In LID %'] = (unique_listing_ids_df_summary_owner['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_owner['Opt In SID %'] = (unique_listing_ids_df_summary_owner['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_owner['DW Overall %'] = (unique_listing_ids_df_summary_owner['DW Overall %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_owner
  

    total_row_owner = pd.Series({
        'owner': 'Total',
        # 'listing_id': df_1_edited['LID'].nunique(),
        # 'opted_in_listing_id': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['LID'].nunique(),
        # 'seller_id': df_1_edited['Seller Id'].nunique(),
        # 'opted_in_seller_id': df_1_edited[df_1_edited['Opted-in seller_id'] == 'Opted-in']['Seller Id'].nunique(),
        'orders_input': input_df_owner['orders'].sum(),
        'opted_in_orders_input': input_df_owner[input_df_owner['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_non_input': non_input_df_owner['orders'].sum(),
        'opted_in_orders_non_input': non_input_df_owner[non_input_df_owner['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_overall': df_1_edited['orders'].sum(),
        'opted_in_orders_overall': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_owner_df = pd.DataFrame([total_row_owner])

    total_row_owner_df['DW Input %'] = np.where(
        total_row_owner_df['orders_input'] == 0, 0, 
        total_row_owner_df['opted_in_orders_input'] / total_row_owner_df['orders_input']
    ).round(2)

    total_row_owner_df['DW Non Input %'] = np.where(
        total_row_owner_df['orders_non_input'] == 0, 0, 
        total_row_owner_df['opted_in_orders_non_input'] / total_row_owner_df['orders_non_input']
    ).round(2)

    total_row_owner_df['DW Overall %'] = np.where(
        total_row_owner_df['orders_overall'] == 0, 0, 
        total_row_owner_df['opted_in_orders_overall'] / total_row_owner_df['orders_overall']
    ).round(2)

    for col in ['DW Input %', 'DW Non Input %', 'DW Overall %']:
        total_row_owner_df[col] = (total_row_owner_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_owner_2 = pd.concat([total_row_owner_df, unique_listing_ids_df_summary_owner], ignore_index=True)
    unique_listing_ids_df_summary_owner_2

    unique_listing_ids_df_summary_owner_2=unique_listing_ids_df_summary_owner_2.fillna(0)

    # Build the HTML table with grid lines
    #html_table_owner = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd; background-color: yellow;">'
    # html_table_owner = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">'
    # html_table_owner += '<thead><tr><th style="border: 1px solid #ddd; padding: 8px;">owner</th><th style="border: 1px solid #ddd; padding: 8px;">orders_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">orders_non_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_non_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">orders_overall_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_overall_drr</th><th style="border: 1px solid #ddd; padding: 8px;">DW Input %</th><th style="border: 1px solid #ddd; padding: 8px;">DW Non Input %</th><th style="border: 1px solid #ddd; padding: 8px;">DW Overall %</th></tr></thead>'
    # html_table_owner += '<tbody>'
    html_table_owner = '''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">owner</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders DRR (Overall)</th> 
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted in Order DRR (Overall)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">DW Overall %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">Order DRR (Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">Opted in Order DRR (Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">DW Input %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">Order DRR (Non - Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">Opted in Order DRR (Non-Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">DW Non Input %</th>
        </tr>
    </thead>
    <tbody>
'''

    html_table_owner += '<tbody>'

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_owner_2.iterrows():
        if 'total' in str(row['owner']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
        html_table_owner += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['owner']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td> 
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Input %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_non_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_non_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Non Input %']}</td>                                                                           
            </tr>
        '''

    html_table_owner += '</tbody></table>'
    

    # Add custom CSS for the table (optional styling like borders)
    css_style_owner = """
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
    html_table_owner = css_style_owner + html_table_owner

    #----------Super_Cat
    # Input SET Summary
    input_df_supercat = df_1_edited[df_1_edited['Set'] == 'INPUT SET']
    summary_input_supercat = input_df_supercat.groupby('analytic_super_category_l2').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[input_df_supercat.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_input_supercat.rename(columns={'analytic_super_category_l2': 'KAM'}, inplace=True)
    summary_input_supercat.columns = [col if col == 'analytic_super_category_l2' else col + '_input' for col in summary_input_supercat.columns]

    # Non-Input SET Summary
    non_input_df_supercat = df_1_edited[df_1_edited['Set'] == 'NON-INPUT SET']
    summary_non_input_supercat = non_input_df_supercat.groupby('analytic_super_category_l2').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[non_input_df_supercat.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_non_input_supercat.rename(columns={'analytic_super_category_l2': 'KAM'}, inplace=True)
    summary_non_input_supercat.columns = [col if col == 'analytic_super_category_l2' else col + '_non_input' for col in summary_non_input_supercat.columns]

    # Overall Summary
    summary_overall_supercat = df_1_edited.groupby('analytic_super_category_l2').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_overall_supercat.rename(columns={'analytic_super_category_l2': 'KAM'}, inplace=True)
    summary_overall_supercat.columns = [col if col == 'analytic_super_category_l2' else col + '_overall' for col in summary_overall_supercat.columns]

    # Final merge
    summary_final_supercat = summary_input_supercat.merge(summary_non_input_supercat, on='analytic_super_category_l2', how='outer')
    summary_final_supercat = summary_final_supercat.merge(summary_overall_supercat, on='analytic_super_category_l2', how='outer')

    unique_listing_ids_df_summary_supercat = summary_final_supercat


    import numpy as np
    # unique_listing_ids_df_summary_supercat['Opt In LID %']=np.where(unique_listing_ids_df_summary_supercat['listing_id']==0, 0, unique_listing_ids_df_summary_supercat['opted_in_listing_id']/unique_listing_ids_df_summary_supercat['listing_id']).round(2)
    # unique_listing_ids_df_summary_supercat['Opt In SID %']=np.where(unique_listing_ids_df_summary_supercat['seller_id']==0, 0, unique_listing_ids_df_summary_supercat['opted_in_seller_id']/unique_listing_ids_df_summary_supercat['seller_id']).round(2)
    unique_listing_ids_df_summary_supercat['DW Input %']=np.where(unique_listing_ids_df_summary_supercat['orders_input']==0, 0, unique_listing_ids_df_summary_supercat['opted_in_orders_input']/unique_listing_ids_df_summary_supercat['orders_input']).round(2)

    # unique_listing_ids_df_summary_supercat['Opt In LID %'] = (unique_listing_ids_df_summary_supercat['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_supercat['Opt In SID %'] = (unique_listing_ids_df_summary_supercat['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_supercat['DW Input %'] = (unique_listing_ids_df_summary_supercat['DW Input %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_supercat
   
   

    unique_listing_ids_df_summary_supercat['DW Non Input %']=np.where(unique_listing_ids_df_summary_supercat['orders_non_input']==0, 0, unique_listing_ids_df_summary_supercat['opted_in_orders_non_input']/unique_listing_ids_df_summary_supercat['orders_non_input']).round(2)
   
    # unique_listing_ids_df_summary_supercat['Opt In LID %'] = (unique_listing_ids_df_summary_supercat['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_supercat['Opt In SID %'] = (unique_listing_ids_df_summary_supercat['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_supercat['DW Non Input %'] = (unique_listing_ids_df_summary_supercat['DW Non Input %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_supercat

   
    unique_listing_ids_df_summary_supercat['DW Overall %']=np.where(unique_listing_ids_df_summary_supercat['orders_overall']==0, 0, unique_listing_ids_df_summary_supercat['opted_in_orders_overall']/unique_listing_ids_df_summary_supercat['orders_overall']).round(2)
   
    # unique_listing_ids_df_summary_supercat['Opt In LID %'] = (unique_listing_ids_df_summary_supercat['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_supercat['Opt In SID %'] = (unique_listing_ids_df_summary_supercat['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_supercat['DW Overall %'] = (unique_listing_ids_df_summary_supercat['DW Overall %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_supercat
 

    total_row_supercat = pd.Series({
        'analytic_super_category_l2': 'Total',
        # 'listing_id': df_1_edited['LID'].nunique(),
        # 'opted_in_listing_id': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['LID'].nunique(),
        # 'seller_id': df_1_edited['Seller Id'].nunique(),
        # 'opted_in_seller_id': df_1_edited[df_1_edited['Opted-in seller_id'] == 'Opted-in']['Seller Id'].nunique(),
        'orders_input': input_df_supercat['orders'].sum(),
        'opted_in_orders_input': input_df_supercat[input_df_supercat['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
       
        'orders_non_input': non_input_df_supercat['orders'].sum(),
        'opted_in_orders_non_input': non_input_df_supercat[non_input_df_supercat['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
       
        'orders_overall': df_1_edited['orders'].sum(),
        'opted_in_orders_overall': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_supercat_df = pd.DataFrame([total_row_supercat])

    total_row_supercat_df['DW Input %'] = np.where(
        total_row_supercat_df['orders_input'] == 0, 0,
        total_row_supercat_df['opted_in_orders_input'] / total_row_supercat_df['orders_input']
    ).round(2)

    total_row_supercat_df['DW Non Input %'] = np.where(
        total_row_supercat_df['orders_non_input'] == 0, 0,
        total_row_supercat_df['opted_in_orders_non_input'] / total_row_supercat_df['orders_non_input']
    ).round(2)

    total_row_supercat_df['DW Overall %'] = np.where(
        total_row_supercat_df['orders_overall'] == 0, 0,
        total_row_supercat_df['opted_in_orders_overall'] / total_row_supercat_df['orders_overall']
    ).round(2)

    for col in ['DW Input %', 'DW Non Input %', 'DW Overall %']:
        total_row_supercat_df[col] = (total_row_supercat_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_supercat_2 = pd.concat([total_row_supercat_df, unique_listing_ids_df_summary_supercat], ignore_index=True)
    unique_listing_ids_df_summary_supercat_2

    unique_listing_ids_df_summary_supercat_2=unique_listing_ids_df_summary_supercat_2.fillna(0)

    # Build the HTML table with grid lines
    #html_table_supercat = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd; background-color: yellow;">'
    # html_table_supercat = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">'
    # html_table_supercat += '<thead><tr><th style="border: 1px solid #ddd; padding: 8px;">analytic_super_category_l2</th><th style="border: 1px solid #ddd; padding: 8px;">orders_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">orders_non_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_non_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">orders_overall_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_overall_drr</th><th style="border: 1px solid #ddd; padding: 8px;">DW Input %</th><th style="border: 1px solid #ddd; padding: 8px;">DW Non Input %</th><th style="border: 1px solid #ddd; padding: 8px;">DW Overall %</th></tr></thead>'
    # html_table_supercat += '<tbody>'
    html_table_supercat = '''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">analytic_super_category_l2</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders DRR (Overall)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted in Order DRR (Overall)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">DW Overall %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">Order DRR (Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">Opted in Order DRR (Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">DW Input %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">Order DRR (Non - Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">Opted in Order DRR (Non-Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">DW Non Input %</th>
        </tr>
    </thead>
    <tbody>
'''

    html_table_supercat += '<tbody>'

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_supercat_2.iterrows():
        if 'total' in str(row['analytic_super_category_l2']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
        html_table_supercat += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['analytic_super_category_l2']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Input %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_non_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_non_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Non Input %']}</td>                                                                        
            </tr>
        '''

    html_table_supercat += '</tbody></table>'
   

    # Add custom CSS for the table (optional styling like borders)
    css_style_supercat = """
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
    html_table_supercat = css_style_supercat + html_table_supercat
    
    #Category
    # Input SET Summary
    input_df_category = df_1_edited[df_1_edited['Set'] == 'INPUT SET']
    summary_input_category = input_df_category.groupby('analytic_category').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[input_df_category.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_input_category.rename(columns={'analytic_category': 'KAM'}, inplace=True)
    summary_input_category.columns = [col if col == 'analytic_category' else col + '_input' for col in summary_input_category.columns]

    # Non-Input SET Summary
    non_input_df_category = df_1_edited[df_1_edited['Set'] == 'NON-INPUT SET']
    summary_non_input_category = non_input_df_category.groupby('analytic_category').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[non_input_df_category.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_non_input_category.rename(columns={'analytic_category': 'KAM'}, inplace=True)
    summary_non_input_category.columns = [col if col == 'analytic_category' else col + '_non_input' for col in summary_non_input_category.columns]

    # Overall Summary
    summary_overall_category = df_1_edited.groupby('analytic_category').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_overall_category.rename(columns={'analytic_category': 'KAM'}, inplace=True)
    summary_overall_category.columns = [col if col == 'analytic_category' else col + '_overall' for col in summary_overall_category.columns]

    # Final merge
    summary_final_category = summary_input_category.merge(summary_non_input_category, on='analytic_category', how='outer')
    summary_final_category = summary_final_category.merge(summary_overall_category, on='analytic_category', how='outer')

    unique_listing_ids_df_summary_category = summary_final_category


    import numpy as np
    # unique_listing_ids_df_summary_category['Opt In LID %']=np.where(unique_listing_ids_df_summary_category['listing_id']==0, 0, unique_listing_ids_df_summary_category['opted_in_listing_id']/unique_listing_ids_df_summary_category['listing_id']).round(2)
    # unique_listing_ids_df_summary_category['Opt In SID %']=np.where(unique_listing_ids_df_summary_category['seller_id']==0, 0, unique_listing_ids_df_summary_category['opted_in_seller_id']/unique_listing_ids_df_summary_category['seller_id']).round(2)
    unique_listing_ids_df_summary_category['DW Input %']=np.where(unique_listing_ids_df_summary_category['orders_input']==0, 0, unique_listing_ids_df_summary_category['opted_in_orders_input']/unique_listing_ids_df_summary_category['orders_input']).round(2)

    # unique_listing_ids_df_summary_category['Opt In LID %'] = (unique_listing_ids_df_summary_category['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_category['Opt In SID %'] = (unique_listing_ids_df_summary_category['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_category['DW Input %'] = (unique_listing_ids_df_summary_category['DW Input %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_category
   
   

    unique_listing_ids_df_summary_category['DW Non Input %']=np.where(unique_listing_ids_df_summary_category['orders_non_input']==0, 0, unique_listing_ids_df_summary_category['opted_in_orders_non_input']/unique_listing_ids_df_summary_category['orders_non_input']).round(2)
   
    # unique_listing_ids_df_summary_category['Opt In LID %'] = (unique_listing_ids_df_summary_category['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_category['Opt In SID %'] = (unique_listing_ids_df_summary_category['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_category['DW Non Input %'] = (unique_listing_ids_df_summary_category['DW Non Input %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_category

   
    unique_listing_ids_df_summary_category['DW Overall %']=np.where(unique_listing_ids_df_summary_category['orders_overall']==0, 0, unique_listing_ids_df_summary_category['opted_in_orders_overall']/unique_listing_ids_df_summary_category['orders_overall']).round(2)
   
    # unique_listing_ids_df_summary_category['Opt In LID %'] = (unique_listing_ids_df_summary_category['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_category['Opt In SID %'] = (unique_listing_ids_df_summary_category['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_category['DW Overall %'] = (unique_listing_ids_df_summary_category['DW Overall %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_category
 

    total_row_category = pd.Series({
        'analytic_category': 'Total',
        # 'listing_id': df_1_edited['LID'].nunique(),
        # 'opted_in_listing_id': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['LID'].nunique(),
        # 'seller_id': df_1_edited['Seller Id'].nunique(),
        # 'opted_in_seller_id': df_1_edited[df_1_edited['Opted-in seller_id'] == 'Opted-in']['Seller Id'].nunique(),
        'orders_input': input_df_category['orders'].sum(),
        'opted_in_orders_input': input_df_category[input_df_category['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
       
        'orders_non_input': non_input_df_category['orders'].sum(),
        'opted_in_orders_non_input': non_input_df_category[non_input_df_category['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
       
        'orders_overall': df_1_edited['orders'].sum(),
        'opted_in_orders_overall': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_category_df = pd.DataFrame([total_row_category])

    total_row_category_df['DW Input %'] = np.where(
        total_row_category_df['orders_input'] == 0, 0,
        total_row_category_df['opted_in_orders_input'] / total_row_category_df['orders_input']
    ).round(2)

    total_row_category_df['DW Non Input %'] = np.where(
        total_row_category_df['orders_non_input'] == 0, 0,
        total_row_category_df['opted_in_orders_non_input'] / total_row_category_df['orders_non_input']
    ).round(2)

    total_row_category_df['DW Overall %'] = np.where(
        total_row_category_df['orders_overall'] == 0, 0,
        total_row_category_df['opted_in_orders_overall'] / total_row_category_df['orders_overall']
    ).round(2)

    for col in ['DW Input %', 'DW Non Input %', 'DW Overall %']:
        total_row_category_df[col] = (total_row_category_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_category_2 = pd.concat([total_row_category_df, unique_listing_ids_df_summary_category], ignore_index=True)
    unique_listing_ids_df_summary_category_2

    unique_listing_ids_df_summary_category_2=unique_listing_ids_df_summary_category_2.fillna(0)

    # Build the HTML table with grid lines
    #html_table_category = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd; background-color: yellow;">'
    # html_table_category = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">'
    # html_table_category += '<thead><tr><th style="border: 1px solid #ddd; padding: 8px;">analytic_category</th><th style="border: 1px solid #ddd; padding: 8px;">orders_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">orders_non_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_non_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">orders_overall_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_overall_drr</th><th style="border: 1px solid #ddd; padding: 8px;">DW Input %</th><th style="border: 1px solid #ddd; padding: 8px;">DW Non Input %</th><th style="border: 1px solid #ddd; padding: 8px;">DW Overall %</th></tr></thead>'
    # html_table_category += '<tbody>'
    html_table_category = '''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">analytic_category</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders DRR (Overall)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted in Order DRR (Overall)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">DW Overall %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">Order DRR (Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">Opted in Order DRR (Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">DW Input %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">Order DRR (Non - Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">Opted in Order DRR (Non-Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">DW Non Input %</th>
        </tr>
    </thead>
    <tbody>
'''

    html_table_category += '<tbody>'

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_category_2.iterrows():
        if 'total' in str(row['analytic_category']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
        html_table_category += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['analytic_category']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Input %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_non_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_non_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Non Input %']}</td>                                                                          
            </tr>
        '''

    html_table_category += '</tbody></table>'
   

    # Add custom CSS for the table (optional styling like borders)
    css_style_category = """
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
    html_table_category = css_style_category + html_table_category

    #KAM
    # Input SET Summary
    input_df_kam = df_1_edited_kam[df_1_edited_kam['Set'] == 'INPUT SET']
    summary_input_kam = input_df_kam.groupby('KAM').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[input_df_kam.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_input_kam.rename(columns={'KAM': 'KAM'}, inplace=True)
    summary_input_kam.columns = [col if col == 'KAM' else col + '_input' for col in summary_input_kam.columns]

    # Non-Input SET Summary
    non_input_df_kam = df_1_edited_kam[df_1_edited_kam['Set'] == 'NON-INPUT SET']
    summary_non_input_kam = non_input_df_kam.groupby('KAM').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[non_input_df_kam.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_non_input_kam.rename(columns={'KAM': 'KAM'}, inplace=True)
    summary_non_input_kam.columns = [col if col == 'KAM' else col + '_non_input' for col in summary_non_input_kam.columns]

    # Overall Summary
    summary_overall_kam = df_1_edited_kam.groupby('KAM').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[df_1_edited_kam.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_overall_kam.rename(columns={'KAM': 'KAM'}, inplace=True)
    summary_overall_kam.columns = [col if col == 'KAM' else col + '_overall' for col in summary_overall_kam.columns]

    # Final merge
    summary_final_kam = summary_input_kam.merge(summary_non_input_kam, on='KAM', how='outer')
    summary_final_kam = summary_final_kam.merge(summary_overall_kam, on='KAM', how='outer')

    unique_listing_ids_df_summary_kam = summary_final_kam


    import numpy as np
    # unique_listing_ids_df_summary_kam['Opt In LID %']=np.where(unique_listing_ids_df_summary_kam['listing_id']==0, 0, unique_listing_ids_df_summary_kam['opted_in_listing_id']/unique_listing_ids_df_summary_kam['listing_id']).round(2)
    # unique_listing_ids_df_summary_kam['Opt In SID %']=np.where(unique_listing_ids_df_summary_kam['seller_id']==0, 0, unique_listing_ids_df_summary_kam['opted_in_seller_id']/unique_listing_ids_df_summary_kam['seller_id']).round(2)
    unique_listing_ids_df_summary_kam['DW Input %']=np.where(unique_listing_ids_df_summary_kam['orders_input']==0, 0, unique_listing_ids_df_summary_kam['opted_in_orders_input']/unique_listing_ids_df_summary_kam['orders_input']).round(2)

    # unique_listing_ids_df_summary_kam['Opt In LID %'] = (unique_listing_ids_df_summary_kam['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_kam['Opt In SID %'] = (unique_listing_ids_df_summary_kam['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_kam['DW Input %'] = (unique_listing_ids_df_summary_kam['DW Input %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_kam
   
   

    unique_listing_ids_df_summary_kam['DW Non Input %']=np.where(unique_listing_ids_df_summary_kam['orders_non_input']==0, 0, unique_listing_ids_df_summary_kam['opted_in_orders_non_input']/unique_listing_ids_df_summary_kam['orders_non_input']).round(2)
   
    # unique_listing_ids_df_summary_kam['Opt In LID %'] = (unique_listing_ids_df_summary_kam['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_kam['Opt In SID %'] = (unique_listing_ids_df_summary_kam['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_kam['DW Non Input %'] = (unique_listing_ids_df_summary_kam['DW Non Input %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_kam

   
    unique_listing_ids_df_summary_kam['DW Overall %']=np.where(unique_listing_ids_df_summary_kam['orders_overall']==0, 0, unique_listing_ids_df_summary_kam['opted_in_orders_overall']/unique_listing_ids_df_summary_kam['orders_overall']).round(2)
   
    # unique_listing_ids_df_summary_kam['Opt In LID %'] = (unique_listing_ids_df_summary_kam['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_kam['Opt In SID %'] = (unique_listing_ids_df_summary_kam['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_kam['DW Overall %'] = (unique_listing_ids_df_summary_kam['DW Overall %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_kam
 

    total_row_kam = pd.Series({
        'KAM': 'Total',
        # 'listing_id': df_1_edited_kam['LID'].nunique(),
        # 'opted_in_listing_id': df_1_edited_kam[df_1_edited_kam['Opted-in listing_id'] == 'Opted-in']['LID'].nunique(),
        # 'seller_id': df_1_edited_kam['Seller Id'].nunique(),
        # 'opted_in_seller_id': df_1_edited_kam[df_1_edited_kam['Opted-in seller_id'] == 'Opted-in']['Seller Id'].nunique(),
        'orders_input': input_df_kam['orders'].sum(),
        'opted_in_orders_input': input_df_kam[input_df_kam['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
       
        'orders_non_input': non_input_df_kam['orders'].sum(),
        'opted_in_orders_non_input': non_input_df_kam[non_input_df_kam['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
       
        'orders_overall': df_1_edited_kam['orders'].sum(),
        'opted_in_orders_overall': df_1_edited_kam[df_1_edited_kam['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_kam_df = pd.DataFrame([total_row_kam])

    total_row_kam_df['DW Input %'] = np.where(
        total_row_kam_df['orders_input'] == 0, 0,
        total_row_kam_df['opted_in_orders_input'] / total_row_kam_df['orders_input']
    ).round(2)

    total_row_kam_df['DW Non Input %'] = np.where(
        total_row_kam_df['orders_non_input'] == 0, 0,
        total_row_kam_df['opted_in_orders_non_input'] / total_row_kam_df['orders_non_input']
    ).round(2)

    total_row_kam_df['DW Overall %'] = np.where(
        total_row_kam_df['orders_overall'] == 0, 0,
        total_row_kam_df['opted_in_orders_overall'] / total_row_kam_df['orders_overall']
    ).round(2)

    for col in ['DW Input %', 'DW Non Input %', 'DW Overall %']:
        total_row_kam_df[col] = (total_row_kam_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_kam_2 = pd.concat([total_row_kam_df, unique_listing_ids_df_summary_kam], ignore_index=True)
    unique_listing_ids_df_summary_kam_2

    unique_listing_ids_df_summary_kam_2=unique_listing_ids_df_summary_kam_2.fillna(0)

    # Build the HTML table with grid lines
    #html_table_kam = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd; background-color: yellow;">'
    # html_table_kam = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">'
    # html_table_kam += '<thead><tr><th style="border: 1px solid #ddd; padding: 8px;">KAM</th><th style="border: 1px solid #ddd; padding: 8px;">orders_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">orders_non_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_non_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">orders_overall_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_overall_drr</th><th style="border: 1px solid #ddd; padding: 8px;">DW Input %</th><th style="border: 1px solid #ddd; padding: 8px;">DW Non Input %</th><th style="border: 1px solid #ddd; padding: 8px;">DW Overall %</th></tr></thead>'
    # html_table_kam += '<tbody>'
    html_table_kam = '''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">KAM</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders DRR (Overall)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted in Order DRR (Overall)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">DW Overall %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">Order DRR (Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">Opted in Order DRR (Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">DW Input %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">Order DRR (Non - Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">Opted in Order DRR (Non-Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">DW Non Input %</th>
        </tr>
    </thead>
    <tbody>
'''

    html_table_kam += '<tbody>'

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_kam_2.iterrows():
        if 'total' in str(row['KAM']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
        html_table_kam += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['KAM']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Input %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_non_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_non_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Non Input %']}</td>                                                                          
            </tr>
        '''

    html_table_kam += '</tbody></table>'
   

    # Add custom CSS for the table (optional styling like borders)
    css_style_kam = """
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
    html_table_kam = css_style_kam + html_table_kam

    #analytic_super_cat + KAM cut
    # Input SET Summary
   
    input_df_supercat_kam = df_1_edited_kam[df_1_edited_kam['Set'] == 'INPUT SET']
    summary_input_supercat_kam = input_df_supercat_kam.groupby('analytic_super_category_l2').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[input_df_supercat_kam.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_input_supercat_kam.rename(columns={'analytic_super_category_l2': 'KAM'}, inplace=True)
    summary_input_supercat_kam.columns = [col if col == 'analytic_super_category_l2' else col + '_input' for col in summary_input_supercat_kam.columns]

    # Non-Input SET Summary
    non_input_df_supercat_kam = df_1_edited_kam[df_1_edited_kam['Set'] == 'NON-INPUT SET']
    summary_non_input_supercat_kam = non_input_df_supercat_kam.groupby('analytic_super_category_l2').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[non_input_df_supercat_kam.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_non_input_supercat_kam.rename(columns={'analytic_super_category_l2': 'KAM'}, inplace=True)
    summary_non_input_supercat_kam.columns = [col if col == 'analytic_super_category_l2' else col + '_non_input' for col in summary_non_input_supercat_kam.columns]

    # Overall Summary
    summary_overall_supercat_kam = df_1_edited_kam.groupby('analytic_super_category_l2').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[df_1_edited_kam.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_overall_supercat_kam.rename(columns={'analytic_super_category_l2': 'KAM'}, inplace=True)
    summary_overall_supercat_kam.columns = [col if col == 'analytic_super_category_l2' else col + '_overall' for col in summary_overall_supercat_kam.columns]

    # Final merge
    summary_final_supercat_kam = summary_input_supercat_kam.merge(summary_non_input_supercat_kam, on='analytic_super_category_l2', how='outer')
    summary_final_supercat_kam = summary_final_supercat_kam.merge(summary_overall_supercat_kam, on='analytic_super_category_l2', how='outer')

    unique_listing_ids_df_summary_supercat_kam = summary_final_supercat_kam


    import numpy as np
    # unique_listing_ids_df_summary_supercat_kam['Opt In LID %']=np.where(unique_listing_ids_df_summary_supercat_kam['listing_id']==0, 0, unique_listing_ids_df_summary_supercat_kam['opted_in_listing_id']/unique_listing_ids_df_summary_supercat_kam['listing_id']).round(2)
    # unique_listing_ids_df_summary_supercat_kam['Opt In SID %']=np.where(unique_listing_ids_df_summary_supercat_kam['seller_id']==0, 0, unique_listing_ids_df_summary_supercat_kam['opted_in_seller_id']/unique_listing_ids_df_summary_supercat_kam['seller_id']).round(2)
    unique_listing_ids_df_summary_supercat_kam['DW Input %']=np.where(unique_listing_ids_df_summary_supercat_kam['orders_input']==0, 0, unique_listing_ids_df_summary_supercat_kam['opted_in_orders_input']/unique_listing_ids_df_summary_supercat_kam['orders_input']).round(2)

    # unique_listing_ids_df_summary_supercat_kam['Opt In LID %'] = (unique_listing_ids_df_summary_supercat_kam['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_supercat_kam['Opt In SID %'] = (unique_listing_ids_df_summary_supercat_kam['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_supercat_kam['DW Input %'] = (unique_listing_ids_df_summary_supercat_kam['DW Input %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_supercat_kam
   
   

    unique_listing_ids_df_summary_supercat_kam['DW Non Input %']=np.where(unique_listing_ids_df_summary_supercat_kam['orders_non_input']==0, 0, unique_listing_ids_df_summary_supercat_kam['opted_in_orders_non_input']/unique_listing_ids_df_summary_supercat_kam['orders_non_input']).round(2)
   
    # unique_listing_ids_df_summary_supercat_kam['Opt In LID %'] = (unique_listing_ids_df_summary_supercat_kam['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_supercat_kam['Opt In SID %'] = (unique_listing_ids_df_summary_supercat_kam['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_supercat_kam['DW Non Input %'] = (unique_listing_ids_df_summary_supercat_kam['DW Non Input %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_supercat_kam

   
    unique_listing_ids_df_summary_supercat_kam['DW Overall %']=np.where(unique_listing_ids_df_summary_supercat_kam['orders_overall']==0, 0, unique_listing_ids_df_summary_supercat_kam['opted_in_orders_overall']/unique_listing_ids_df_summary_supercat_kam['orders_overall']).round(2)
   
    # unique_listing_ids_df_summary_supercat_kam['Opt In LID %'] = (unique_listing_ids_df_summary_supercat_kam['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_supercat_kam['Opt In SID %'] = (unique_listing_ids_df_summary_supercat_kam['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_supercat_kam['DW Overall %'] = (unique_listing_ids_df_summary_supercat_kam['DW Overall %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_supercat_kam
 

    total_row_supercat_kam = pd.Series({
        'analytic_super_category_l2': 'Total',
        # 'listing_id': df_1_edited_kam['LID'].nunique(),
        # 'opted_in_listing_id': df_1_edited_kam[df_1_edited_kam['Opted-in listing_id'] == 'Opted-in']['LID'].nunique(),
        # 'seller_id': df_1_edited_kam['Seller Id'].nunique(),
        # 'opted_in_seller_id': df_1_edited_kam[df_1_edited_kam['Opted-in seller_id'] == 'Opted-in']['Seller Id'].nunique(),
        'orders_input': input_df_supercat_kam['orders'].sum(),
        'opted_in_orders_input': input_df_supercat_kam[input_df_supercat_kam['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
       
        'orders_non_input': non_input_df_supercat_kam['orders'].sum(),
        'opted_in_orders_non_input': non_input_df_supercat_kam[non_input_df_supercat_kam['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
       
        'orders_overall': df_1_edited_kam['orders'].sum(),
        'opted_in_orders_overall': df_1_edited_kam[df_1_edited_kam['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_supercat_kam_df = pd.DataFrame([total_row_supercat_kam])

    total_row_supercat_kam_df['DW Input %'] = np.where(
        total_row_supercat_kam_df['orders_input'] == 0, 0,
        total_row_supercat_kam_df['opted_in_orders_input'] / total_row_supercat_kam_df['orders_input']
    ).round(2)

    total_row_supercat_kam_df['DW Non Input %'] = np.where(
        total_row_supercat_kam_df['orders_non_input'] == 0, 0,
        total_row_supercat_kam_df['opted_in_orders_non_input'] / total_row_supercat_kam_df['orders_non_input']
    ).round(2)

    total_row_supercat_kam_df['DW Overall %'] = np.where(
        total_row_supercat_kam_df['orders_overall'] == 0, 0,
        total_row_supercat_kam_df['opted_in_orders_overall'] / total_row_supercat_kam_df['orders_overall']
    ).round(2)

    for col in ['DW Input %', 'DW Non Input %', 'DW Overall %']:
        total_row_supercat_kam_df[col] = (total_row_supercat_kam_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_supercat_kam_2 = pd.concat([total_row_supercat_kam_df, unique_listing_ids_df_summary_supercat_kam], ignore_index=True)
    unique_listing_ids_df_summary_supercat_kam_2

    unique_listing_ids_df_summary_supercat_kam_2=unique_listing_ids_df_summary_supercat_kam_2.fillna(0)

    # Build the HTML table with grid lines
    #html_table_supercat_kam = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd; background-color: yellow;">'
    # html_table_supercat_kam = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">'
    # html_table_supercat_kam += '<thead><tr><th style="border: 1px solid #ddd; padding: 8px;">analytic_super_category_l2</th><th style="border: 1px solid #ddd; padding: 8px;">orders_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">orders_non_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_non_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">orders_overall_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_overall_drr</th><th style="border: 1px solid #ddd; padding: 8px;">DW Input %</th><th style="border: 1px solid #ddd; padding: 8px;">DW Non Input %</th><th style="border: 1px solid #ddd; padding: 8px;">DW Overall %</th></tr></thead>'
    # html_table_supercat_kam += '<tbody>'
    html_table_supercat_kam = '''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">analytic_super_category_l2</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders DRR (Overall)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted in Order DRR (Overall)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">DW Overall %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">Order DRR (Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">Opted in Order DRR (Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">DW Input %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">Order DRR (Non - Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">Opted in Order DRR (Non-Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">DW Non Input %</th>
        </tr>
    </thead>
    <tbody>
'''

    html_table_supercat_kam += '<tbody>'

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_supercat_kam_2.iterrows():
        if 'total' in str(row['analytic_super_category_l2']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
        html_table_supercat_kam += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['analytic_super_category_l2']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Input %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_non_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_non_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Non Input %']}</td>                                                                            
            </tr>
        '''

    html_table_supercat_kam += '</tbody></table>'
   

    # Add custom CSS for the table (optional styling like borders)
    css_style_supercat_kam = """
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
    html_table_supercat_kam = css_style_supercat_kam + html_table_supercat_kam

    #Lead + KAM cut

    # Input SET Summary
    input_df_lead = df_1_edited_kam[df_1_edited_kam['Set'] == 'INPUT SET']
    summary_input_lead = input_df_lead.groupby('Lead').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[input_df_lead.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_input_lead.rename(columns={'Lead': 'KAM'}, inplace=True)
    summary_input_lead.columns = [col if col == 'Lead' else col + '_input' for col in summary_input_lead.columns]

    # Non-Input SET Summary
    non_input_df_lead = df_1_edited_kam[df_1_edited_kam['Set'] == 'NON-INPUT SET']
    summary_non_input_lead = non_input_df_lead.groupby('Lead').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[non_input_df_lead.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_non_input_lead.rename(columns={'Lead': 'KAM'}, inplace=True)
    summary_non_input_lead.columns = [col if col == 'Lead' else col + '_non_input' for col in summary_non_input_lead.columns]

    # Overall Summary
    summary_overall_lead = df_1_edited_kam.groupby('Lead').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[df_1_edited_kam.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_overall_lead.rename(columns={'Lead': 'KAM'}, inplace=True)
    summary_overall_lead.columns = [col if col == 'Lead' else col + '_overall' for col in summary_overall_lead.columns]

    # Final merge
    summary_final_lead = summary_input_lead.merge(summary_non_input_lead, on='Lead', how='outer')
    summary_final_lead = summary_final_lead.merge(summary_overall_lead, on='Lead', how='outer')

    unique_listing_ids_df_summary_lead = summary_final_lead


    import numpy as np
    # unique_listing_ids_df_summary_lead['Opt In LID %']=np.where(unique_listing_ids_df_summary_lead['listing_id']==0, 0, unique_listing_ids_df_summary_lead['opted_in_listing_id']/unique_listing_ids_df_summary_lead['listing_id']).round(2)
    # unique_listing_ids_df_summary_lead['Opt In SID %']=np.where(unique_listing_ids_df_summary_lead['seller_id']==0, 0, unique_listing_ids_df_summary_lead['opted_in_seller_id']/unique_listing_ids_df_summary_lead['seller_id']).round(2)
    unique_listing_ids_df_summary_lead['DW Input %']=np.where(unique_listing_ids_df_summary_lead['orders_input']==0, 0, unique_listing_ids_df_summary_lead['opted_in_orders_input']/unique_listing_ids_df_summary_lead['orders_input']).round(2)

    # unique_listing_ids_df_summary_lead['Opt In LID %'] = (unique_listing_ids_df_summary_lead['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_lead['Opt In SID %'] = (unique_listing_ids_df_summary_lead['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_lead['DW Input %'] = (unique_listing_ids_df_summary_lead['DW Input %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_lead
   
   

    unique_listing_ids_df_summary_lead['DW Non Input %']=np.where(unique_listing_ids_df_summary_lead['orders_non_input']==0, 0, unique_listing_ids_df_summary_lead['opted_in_orders_non_input']/unique_listing_ids_df_summary_lead['orders_non_input']).round(2)
   
    # unique_listing_ids_df_summary_lead['Opt In LID %'] = (unique_listing_ids_df_summary_lead['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_lead['Opt In SID %'] = (unique_listing_ids_df_summary_lead['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_lead['DW Non Input %'] = (unique_listing_ids_df_summary_lead['DW Non Input %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_lead

   
    unique_listing_ids_df_summary_lead['DW Overall %']=np.where(unique_listing_ids_df_summary_lead['orders_overall']==0, 0, unique_listing_ids_df_summary_lead['opted_in_orders_overall']/unique_listing_ids_df_summary_lead['orders_overall']).round(2)
   
    # unique_listing_ids_df_summary_lead['Opt In LID %'] = (unique_listing_ids_df_summary_lead['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_lead['Opt In SID %'] = (unique_listing_ids_df_summary_lead['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_lead['DW Overall %'] = (unique_listing_ids_df_summary_lead['DW Overall %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_lead
 

    total_row_lead = pd.Series({
        'Lead': 'Total',
        # 'listing_id': df_1_edited_kam['LID'].nunique(),
        # 'opted_in_listing_id': df_1_edited_kam[df_1_edited_kam['Opted-in listing_id'] == 'Opted-in']['LID'].nunique(),
        # 'seller_id': df_1_edited_kam['Seller Id'].nunique(),
        # 'opted_in_seller_id': df_1_edited_kam[df_1_edited_kam['Opted-in seller_id'] == 'Opted-in']['Seller Id'].nunique(),
        'orders_input': input_df_lead['orders'].sum(),
        'opted_in_orders_input': input_df_lead[input_df_lead['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
       
        'orders_non_input': non_input_df_lead['orders'].sum(),
        'opted_in_orders_non_input': non_input_df_lead[non_input_df_lead['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
       
        'orders_overall': df_1_edited_kam['orders'].sum(),
        'opted_in_orders_overall': df_1_edited_kam[df_1_edited_kam['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_lead_df = pd.DataFrame([total_row_lead])

    total_row_lead_df['DW Input %'] = np.where(
        total_row_lead_df['orders_input'] == 0, 0,
        total_row_lead_df['opted_in_orders_input'] / total_row_lead_df['orders_input']
    ).round(2)

    total_row_lead_df['DW Non Input %'] = np.where(
        total_row_lead_df['orders_non_input'] == 0, 0,
        total_row_lead_df['opted_in_orders_non_input'] / total_row_lead_df['orders_non_input']
    ).round(2)

    total_row_lead_df['DW Overall %'] = np.where(
        total_row_lead_df['orders_overall'] == 0, 0,
        total_row_lead_df['opted_in_orders_overall'] / total_row_lead_df['orders_overall']
    ).round(2)

    for col in ['DW Input %', 'DW Non Input %', 'DW Overall %']:
        total_row_lead_df[col] = (total_row_lead_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_lead_2 = pd.concat([total_row_lead_df, unique_listing_ids_df_summary_lead], ignore_index=True)
    unique_listing_ids_df_summary_lead_2

    unique_listing_ids_df_summary_lead_2=unique_listing_ids_df_summary_lead_2.fillna(0)

    # Build the HTML table with grid lines
    #html_table_lead = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd; background-color: yellow;">'
    # html_table_lead = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">'
    # html_table_lead += '<thead><tr><th style="border: 1px solid #ddd; padding: 8px;">Lead</th><th style="border: 1px solid #ddd; padding: 8px;">orders_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">orders_non_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_non_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">orders_overall_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_overall_drr</th><th style="border: 1px solid #ddd; padding: 8px;">DW Input %</th><th style="border: 1px solid #ddd; padding: 8px;">DW Non Input %</th><th style="border: 1px solid #ddd; padding: 8px;">DW Overall %</th></tr></thead>'
    # html_table_lead += '<tbody>'
    html_table_lead = '''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">Lead</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders DRR (Overall)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted in Order DRR (Overall)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">DW Overall %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">Order DRR (Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">Opted in Order DRR (Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">DW Input %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">Order DRR (Non - Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">Opted in Order DRR (Non-Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">DW Non Input %</th>
        </tr>
    </thead>
    <tbody>
'''

    html_table_lead += '<tbody>'

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_lead_2.iterrows():
        if 'total' in str(row['Lead']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
        html_table_lead += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['Lead']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Input %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_non_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_non_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Non Input %']}</td>                                                                          
            </tr>
        '''

    html_table_lead += '</tbody></table>'
   

    # Add custom CSS for the table (optional styling like borders)
    css_style_lead = """
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
    html_table_lead = css_style_lead + html_table_lead

    #Category + KAM
    # Input SET Summary
    input_df_category_kam = df_1_edited_kam[df_1_edited_kam['Set'] == 'INPUT SET']
    summary_input_category_kam = input_df_category_kam.groupby('analytic_category').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[input_df_category_kam.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_input_category_kam.rename(columns={'analytic_category': 'KAM'}, inplace=True)
    summary_input_category_kam.columns = [col if col == 'analytic_category' else col + '_input' for col in summary_input_category_kam.columns]

    # Non-Input SET Summary
    non_input_df_category_kam = df_1_edited_kam[df_1_edited_kam['Set'] == 'NON-INPUT SET']
    summary_non_input_category_kam = non_input_df_category_kam.groupby('analytic_category').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[non_input_df_category_kam.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_non_input_category_kam.rename(columns={'analytic_category': 'KAM'}, inplace=True)
    summary_non_input_category_kam.columns = [col if col == 'analytic_category' else col + '_non_input' for col in summary_non_input_category_kam.columns]

    # Overall Summary
    summary_overall_category_kam = df_1_edited_kam.groupby('analytic_category').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[df_1_edited_kam.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_overall_category_kam.rename(columns={'analytic_category': 'KAM'}, inplace=True)
    summary_overall_category_kam.columns = [col if col == 'analytic_category' else col + '_overall' for col in summary_overall_category_kam.columns]

    # Final merge
    summary_final_category_kam = summary_input_category_kam.merge(summary_non_input_category_kam, on='analytic_category', how='outer')
    summary_final_category_kam = summary_final_category_kam.merge(summary_overall_category_kam, on='analytic_category', how='outer')

    unique_listing_ids_df_summary_category_kam = summary_final_category_kam


    import numpy as np
    # unique_listing_ids_df_summary_category_kam['Opt In LID %']=np.where(unique_listing_ids_df_summary_category_kam['listing_id']==0, 0, unique_listing_ids_df_summary_category_kam['opted_in_listing_id']/unique_listing_ids_df_summary_category_kam['listing_id']).round(2)
    # unique_listing_ids_df_summary_category_kam['Opt In SID %']=np.where(unique_listing_ids_df_summary_category_kam['seller_id']==0, 0, unique_listing_ids_df_summary_category_kam['opted_in_seller_id']/unique_listing_ids_df_summary_category_kam['seller_id']).round(2)
    unique_listing_ids_df_summary_category_kam['DW Input %']=np.where(unique_listing_ids_df_summary_category_kam['orders_input']==0, 0, unique_listing_ids_df_summary_category_kam['opted_in_orders_input']/unique_listing_ids_df_summary_category_kam['orders_input']).round(2)

    # unique_listing_ids_df_summary_category_kam['Opt In LID %'] = (unique_listing_ids_df_summary_category_kam['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_category_kam['Opt In SID %'] = (unique_listing_ids_df_summary_category_kam['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_category_kam['DW Input %'] = (unique_listing_ids_df_summary_category_kam['DW Input %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_category_kam
   
   

    unique_listing_ids_df_summary_category_kam['DW Non Input %']=np.where(unique_listing_ids_df_summary_category_kam['orders_non_input']==0, 0, unique_listing_ids_df_summary_category_kam['opted_in_orders_non_input']/unique_listing_ids_df_summary_category_kam['orders_non_input']).round(2)
   
    # unique_listing_ids_df_summary_category_kam['Opt In LID %'] = (unique_listing_ids_df_summary_category_kam['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_category_kam['Opt In SID %'] = (unique_listing_ids_df_summary_category_kam['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_category_kam['DW Non Input %'] = (unique_listing_ids_df_summary_category_kam['DW Non Input %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_category_kam

   
    unique_listing_ids_df_summary_category_kam['DW Overall %']=np.where(unique_listing_ids_df_summary_category_kam['orders_overall']==0, 0, unique_listing_ids_df_summary_category_kam['opted_in_orders_overall']/unique_listing_ids_df_summary_category_kam['orders_overall']).round(2)
   
    # unique_listing_ids_df_summary_category_kam['Opt In LID %'] = (unique_listing_ids_df_summary_category_kam['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_category_kam['Opt In SID %'] = (unique_listing_ids_df_summary_category_kam['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_category_kam['DW Overall %'] = (unique_listing_ids_df_summary_category_kam['DW Overall %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_category_kam
 

    total_row_category_kam = pd.Series({
        'analytic_category': 'Total',
        # 'listing_id': df_1_edited_kam['LID'].nunique(),
        # 'opted_in_listing_id': df_1_edited_kam[df_1_edited_kam['Opted-in listing_id'] == 'Opted-in']['LID'].nunique(),
        # 'seller_id': df_1_edited_kam['Seller Id'].nunique(),
        # 'opted_in_seller_id': df_1_edited_kam[df_1_edited_kam['Opted-in seller_id'] == 'Opted-in']['Seller Id'].nunique(),
        'orders_input': input_df_category_kam['orders'].sum(),
        'opted_in_orders_input': input_df_category_kam[input_df_category_kam['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
       
        'orders_non_input': non_input_df_category_kam['orders'].sum(),
        'opted_in_orders_non_input': non_input_df_category_kam[non_input_df_category_kam['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
       
        'orders_overall': df_1_edited_kam['orders'].sum(),
        'opted_in_orders_overall': df_1_edited_kam[df_1_edited_kam['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_category_kam_df = pd.DataFrame([total_row_category_kam])

    total_row_category_kam_df['DW Input %'] = np.where(
        total_row_category_kam_df['orders_input'] == 0, 0,
        total_row_category_kam_df['opted_in_orders_input'] / total_row_category_kam_df['orders_input']
    ).round(2)

    total_row_category_kam_df['DW Non Input %'] = np.where(
        total_row_category_kam_df['orders_non_input'] == 0, 0,
        total_row_category_kam_df['opted_in_orders_non_input'] / total_row_category_kam_df['orders_non_input']
    ).round(2)

    total_row_category_kam_df['DW Overall %'] = np.where(
        total_row_category_kam_df['orders_overall'] == 0, 0,
        total_row_category_kam_df['opted_in_orders_overall'] / total_row_category_kam_df['orders_overall']
    ).round(2)

    for col in ['DW Input %', 'DW Non Input %', 'DW Overall %']:
        total_row_category_kam_df[col] = (total_row_category_kam_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_category_kam_2 = pd.concat([total_row_category_kam_df, unique_listing_ids_df_summary_category_kam], ignore_index=True)
    unique_listing_ids_df_summary_category_kam_2

    unique_listing_ids_df_summary_category_kam_2=unique_listing_ids_df_summary_category_kam_2.fillna(0)

    # Build the HTML table with grid lines
    #html_table_category_kam = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd; background-color: yellow;">'
    # html_table_category_kam = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">'
    # html_table_category_kam += '<thead><tr><th style="border: 1px solid #ddd; padding: 8px;">analytic_category</th><th style="border: 1px solid #ddd; padding: 8px;">orders_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">orders_non_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_non_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">orders_overall_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_overall_drr</th><th style="border: 1px solid #ddd; padding: 8px;">DW Input %</th><th style="border: 1px solid #ddd; padding: 8px;">DW Non Input %</th><th style="border: 1px solid #ddd; padding: 8px;">DW Overall %</th></tr></thead>'
    # html_table_category_kam += '<tbody>'
    html_table_category_kam = '''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">analytic_category</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders DRR (Overall)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted in Order DRR (Overall)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">DW Overall %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">Order DRR (Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">Opted in Order DRR (Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgreen; color: white;">DW Input %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">Order DRR (Non - Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">Opted in Order DRR (Non-Input Set)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkgoldenrod; color: white;">DW Non Input %</th>
        </tr>
    </thead>
    <tbody>
'''

    html_table_category_kam += '<tbody>'

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_category_kam_2.iterrows():
        if 'total' in str(row['analytic_category']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
        html_table_category_kam += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['analytic_category']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Input %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_non_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_non_input']:.2f}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Non Input %']}</td>                                                                          
            </tr>
        '''

    html_table_category_kam += '</tbody></table>'
   

    # Add custom CSS for the table (optional styling like borders)
    css_style_category_kam = """
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
    html_table_category_kam = css_style_category_kam + html_table_category_kam

    df_1_edited=df_1_edited.merge(df_optin_summary,on=['listing_id'],how='left')

    df_1_edited['order_drr'] = pd.to_numeric(df_1_edited['orders'], errors='coerce') 
    df_1_edited['order_drr'] = df_1_edited['order_drr'].round(2)

    df_1_edited.to_csv(f"/home/thotat.vc/Downloads/Consolidated_Report_{today}.csv", index=False)

    df_1_edited[df_1_edited['owner']=='KAM'].to_csv(f"/home/thotat.vc/Downloads/Consolidated_Report_{today}_KAM.csv")
    df_1_edited[df_1_edited['owner']=='RE'].to_csv(f"/home/thotat.vc/Downloads/Consolidated_Report_{today}_RE.csv")
    df_1_edited[df_1_edited['owner']=='UM'].to_csv(f"/home/thotat.vc/Downloads/Consolidated_Report_{today}_UM.csv")


    # with pd.ExcelWriter(f"/home/thotat.vc/Downloads/Consolidated_Report_{today}.xlsx") as writer:
    #     unique_listing_ids_df_summary_supercat_2.to_excel(writer, sheet_name='Summary Supercategory',index=False)
    #     df_1_edited.to_excel(writer, sheet_name='Raw',index=False)
    #     unique_listing_ids_df_summary_2.to_excel(writer, sheet_name='Summary',index=False)

    filename =f"Consolidated_Report_{today}.csv"
    file_path =f"/home/thotat.vc/Downloads/Consolidated_Report_{today}.csv" 
    file_path_kam=f"/home/thotat.vc/Downloads/Consolidated_Report_{today}_KAM.csv"
    file_path_re=f"/home/thotat.vc/Downloads/Consolidated_Report_{today}_RE.csv"
    file_path_um=f"/home/thotat.vc/Downloads/Consolidated_Report_{today}_UM.csv"

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
    
    link = "https://drive.google.com/drive/folders/1T0v-tvsuz95kVW7oDNWht44Cq_XvVLNs"
    #link2= "https://drive.google.com/drive/folders/1zmjaxwF6Ze3ePf92L8P1lPePO-noItBM"
    link3= "https://docs.google.com/spreadsheets/d/1ltixD7xZeeFORC0wsA4X6fzlk5DueqH0TMQIt1UaX7A/edit?usp=sharing"
    
    # Process today's file
    from datetime import datetime as dt2
    today = dt2.now()
    formatted_date = today.strftime("%d_%b_%Y")
    formatted_date_2 = today.strftime("%d-%b-%Y")

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
    # original_to = ['shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com','shopsysell@flipkart.com', 'shopsypricing@flipkart.com']
    original_to = ['thotat.vc@flipkart.com', 'vishnuac.vc@flipkart.com', 'thotat.vc@flipkart.com']
    #original_to = ['thotat.vc@flipkart.com','vishnuac.vc@flipkart.com','shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com','nakul.gulati@flipkart.com','abhishek.patwari@flipkart.com','s.abhishek@flipkart.com','avijit.mohapatra@flipkart.com','bharambe.pradnya@flipkart.com']
    # 'upasanaupadhak.vc@flipkart.com','thotat.vc@flipkart.com','vishnuac.vc@flipkart.com','shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com'
    #'shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com']
    #'shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com']
    #'shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com'
    original_cc = ['thotat.vc@flipkart.com', 'vishnuac.vc@flipkart.com', 'thotat.vc@flipkart.com']
    original_message_id ='<CACZpucyXhj=WTsYgJuSzFYrgbJn5BgCU_+Rm8dpLKPN7SdvXTA@mail.gmail.com>'

    all_recipients = set([original_from] + original_to + original_cc)

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(original_to)
    msg['Cc'] = ', '.join(original_cc)
    msg['Subject'] = f"	New Consolidated Optin Thread - FeB 2026"
    msg['In-Reply-To'] = original_message_id
    msg['References'] = original_message_id

    body = f"""
    Hi Team, <br><br>
    We are now tracking consolidated optins on the new base file for FEB 2026 for New FPOs separately. The order DRR is calculated from Dec 1st + 50 Days.<br><br>
    Please find the status as of {formatted_date_2} - <br><br>
    Please find the Owner Level Summary:<br><br>
    {html_table_owner}<br><br>
    Please find the Supercategory Summary:<br><br>
    {html_table_supercat}<br><br>
     Please find the Supercategory Summary at KAM level:<br><br>
    {html_table_supercat_kam}<br><br>
    Please find the Summary at Lead Level:<br><br>
    {html_table_lead}<br><br>
    Please find the Summary at KAM Level:<br><br>
    {html_table_kam}<br><br>
    Please find the Category Summary:<br><br>
    {html_table_category}<br><br>
    Please find the Category Summary at KAM Level:<br><br>
    {html_table_category_kam}<br><br>
    The attached drive link contains the raw and summary data for your reference: {link}<br><br>
    The attached sheet link contains all the offer codes which is considered for this optin: {link3}<br><br>
    <b>Regards,<br>
    Kishan Menon</b>
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

    drive_id = '0AJExn3WwBbdqUk9PVA'  # Shared Drive ID
    folder_id = '1T0v-tvsuz95kVW7oDNWht44Cq_XvVLNs'  # Replace with actual folder ID
    file_path = f"/home/thotat.vc/Downloads/Consolidated_Report_{today}.csv"
    file_path_kam=f"/home/thotat.vc/Downloads/Consolidated_Report_{today}_KAM.csv"
    file_path_re=f"/home/thotat.vc/Downloads/Consolidated_Report_{today}_RE.csv"
    file_path_um=f"/home/thotat.vc/Downloads/Consolidated_Report_{today}_UM.csv" 

# Replace with actual file path

    upload_to_shared_drive_folder(drive_id, folder_id,file_path) #file_path
    upload_to_shared_drive_folder(drive_id, folder_id,file_path_kam)
    upload_to_shared_drive_folder(drive_id, folder_id,file_path_re)
    upload_to_shared_drive_folder(drive_id, folder_id,file_path_um)
    link = "https://drive.google.com/drive/folders/1T0v-tvsuz95kVW7oDNWht44Cq_XvVLNs"
    #link2= "https://drive.google.com/drive/folders/1zmjaxwF6Ze3ePf92L8P1lPePO-noItBM"
    link3= "https://docs.google.com/spreadsheets/d/1ltixD7xZeeFORC0wsA4X6fzlk5DueqH0TMQIt1UaX7A/edit?usp=sharing"

import pandas as pd
import os
import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from datetime import datetime as dt

# try:
#     # Load base file
#     df_base_optin = pd.read_csv('/home/thotat.vc/Downloads/Nov_2025_Base_DoD_Optin_File.csv')

#     # Process today's file
#     today = dt.now()
#     formatted_date = today.strftime("%d_%b_%Y")
#     formatted_date_2 = today.strftime("%d-%b-%Y")

#     df_today_optin = pd.read_csv(f'/home/thotat.vc/Downloads/Consolidated_Report_{formatted_date}.csv')
#     new_col = f'LID Opt-in {formatted_date_2}'
#     df_today_optin = df_today_optin.rename(columns={'Opted-in listing_id': new_col})

#     cols = ['listing_id', new_col]
#     df_today_optin_1 = df_today_optin[cols]

#     # Strip spaces
#     df_base_optin['listing_id'] = df_base_optin['listing_id'].astype(str).str.strip()
#     df_today_optin_1['listing_id'] = df_today_optin_1['listing_id'].astype(str).str.strip()

#     # --- SAFE MERGE ---
#     df_base_optin_optout_1 = df_base_optin.merge(df_today_optin_1, on='listing_id', how='left')

#     # Deduplicate: if column already exists, drop the old one and keep the latest
#     if new_col in df_base_optin_optout_1.columns:
#         # Fill missing values with 'INACTIVE'
#         df_base_optin_optout_1[new_col] = df_base_optin_optout_1[new_col].fillna('INACTIVE')

#     # Drop unnamed columns (from CSV index issues)
#     df_base_optin_optout_1 = df_base_optin_optout_1.loc[:, ~df_base_optin_optout_1.columns.str.contains('^Unnamed')]

#     # Save merged file
#     base_path = '/home/thotat.vc/Downloads/Nov_2025_Base_DoD_Optin_File.csv'
#     df_base_optin_optout_1.to_csv(base_path, index=False)

#     # Split by owner
#     df_base_optin_optout_1[df_base_optin_optout_1['owner'] == 'KAM'].to_csv(base_path.replace('.csv', '_KAM.csv'), index=False)
#     df_base_optin_optout_1[df_base_optin_optout_1['owner'] == 'RE'].to_csv(base_path.replace('.csv', '_RE.csv'), index=False)
#     df_base_optin_optout_1[df_base_optin_optout_1['owner'] == 'UM'].to_csv(base_path.replace('.csv', '_UM.csv'), index=False)

#     # Upload to shared drive
#     folder_id2 = '1zmjaxwF6Ze3ePf92L8P1lPePO-noItBM'
#     upload_to_shared_drive_folder(drive_id, folder_id2, base_path)
#     upload_to_shared_drive_folder(drive_id, folder_id2, base_path.replace('.csv', '_KAM.csv'))
#     upload_to_shared_drive_folder(drive_id, folder_id2, base_path.replace('.csv', '_RE.csv'))
#     upload_to_shared_drive_folder(drive_id, folder_id2, base_path.replace('.csv', '_UM.csv'))

# except FileNotFoundError as e:
#     print(f"❌ File not found: {e}")
# except pd.errors.EmptyDataError:
#     print("❌ One of the CSV files is empty.")
# except KeyError as e:
#     print(f"❌ Missing expected column: {e}")
# except Exception as e:
#     print(f"⚠️ Unexpected error: {e}")