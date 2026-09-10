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
fdp_uploads.ds_fkint_2gud_partner_vamsi_shopsy_1_optin_1_0 a
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

df_listing.to_csv(f'/home/thotat.vc/analytics_scripts/FOS_SOS_Cofunded_listing_raw_fifteen_percent.csv')
# df_listing=pd.read_csv(r'/home/thotat.vc/analytics_scripts/FOS_SOS_Cofunded_listing_raw.csv') 
print(df_listing)

# df_listing=pd.read_csv(r'/home/thotat.vc/analytics_scripts/listing_raw_new_consolidated_1.csv')

from datetime import datetime as dt2
today=dt2.now().strftime('%d_%b_%Y')
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe


# scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
# creds = ServiceAccountCredentials.from_json_keyfile_name('/home/thotat.vc/analytics_scripts/creds.json', scope)
# client = gspread.authorize(creds)

# wks = client.open("May Co-Funding Offers").sheet1
# ws = client.open("May Co-Funding Offers").worksheet("Offer IDs")

# data_new_fpo = ws.get_all_records()
# df_new_fpo = pd.DataFrame(data_new_fpo)
# print(df_new_fpo)
# df_new_fpo.to_csv('/home/thotat.vc/analytics_scripts/New_April_cofunded_offers.csv')

# df_new_fpo = pd.read_csv(r'/home/thotat.vc/analytics_scripts/New_April_cofunded_offers.csv', encoding='cp1252')
# offer_ids_new_fpo = df_new_fpo['Offer ID'].dropna().unique().tolist()
# offer_ids_new_fpo_cleaned = [f"'{str(oid)}'" for oid in offer_ids_new_fpo]
# offer_ids_new_fpo_clause = ', '.join(offer_ids_new_fpo_cleaned)
# print(offer_ids_new_fpo_clause)


# #============Optin=========OFFER===CODE===================================
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
          AND data.offerid IN ('nb:mp:03a9c53120','nb:mp:03d2a7ea20','nb:mp:03a3b07220','nb:mp:0367af1c20','nb:mp:03a9881220','nb:mp:03c20b8a20','nb:mp:0389ae4523')
        GROUP BY data.listingid, data.offerid
    ) a
    WHERE opt_out_time IS NULL OR opt_in_time > opt_out_time
) a
JOIN bigfoot_external_neo.sp_product__listing_hive_dim b 
  ON a.listing_id = b.listing_id
JOIN bigfoot_external_neo.sp_product__product_categorization_hive_dim c
  ON b.product_id = c.product_id
JOIN fdp_uploads.ds_fkint_2gud_partner_vamsi_shopsy_1_optin_1_0 d
  ON a.listing_id = d.listing_id
  """
data2 = pd.read_sql_query(sql2, con=conn)
df_optin=pd.DataFrame(data2)
# df_optin=pd.read_csv(r'/home/thotat.vc/analytics_scripts/FOS_SOS_Cofunded_optin_raw.csv')

# fifteen_percent_offers = [
# 'nb:mp:0390e92520','nb:mp:03410ac020','nb:mp:032c154720','nb:mp:03a67a0d20','nb:mp:03e971ae20','nb:mp:03fdb74a20'
# ]

# df_optin=df_optin[df_optin['offer_id'].isin(fifteen_percent_offers)]
df_optin.to_csv(f'/home/thotat.vc/analytics_scripts/FOS_SOS_Cofunded_optin_raw_Fifteen_Percent.csv', index=False)

print(df_optin)
# df_optin=pd.read_csv(r'/home/thotat.vc/analytics_scripts/optin_raw_new_consolidated_1.csv')

#==============BASEFILE====CODE===STARTS==HERE=================================================

if df_optin.empty:
    print("Empty set error – df_optin has no data.")
else:
    # =============================================================================
    # 4. PROCESS BASE FILE & CLEANING
    # =============================================================================
    file_path_1 = r'/home/thotat.vc/analytics_scripts/New_Final_Base_file_for_new_optin.csv'
    df_1 = pd.read_csv(file_path_1, encoding='utf-8')
    df_1['owner'] = df_1['owner'].replace(['', 'null'], np.nan).fillna('UM')
    df_1 = df_1.drop_duplicates()

    df_1_1 = df_1.copy()
    df_1_1['listing_id'] = df_1_1['listing_id'].astype(str).str.strip()
    df_1_1['seller_id'] = df_1_1['seller_id'].astype(str).str.strip()
    df_1_1['cms_vertical'] = df_1_1['cms_vertical'].astype(str).str.strip()
    
# Exclude venu LIDs
#=======================================================================
#---Excluding lid's for venu------------------------------------
#=======================================================================
    excluded_lids=[
        'LSTXHOH6DT55TZU3WMJ5POALG','LSTXHOH2HVFWYUYNDMGYL8XHB',
        'LSTXHOHM8NZPPGVZMUKX0CFLF','LSTXHOHM8NZFBJQVPSCQJQOYE',
        'LSTXHOHM8NZZFG7PNZZOBGAE2','LSTXHOH9BCTTY5GGXQZZ9AZYK',
        'LSTXHOHABUYZNTA42MSZXJAUN','LSTXHOHB2DR6FZZV8MZOYMR7K',
        'LSTXHOHHFFBCGGU8BUNJLH87H'
    ]
    df_1_1=df_1_1[~df_1_1['listing_id'].isin(excluded_lids)]

    excluded_sids=['6c15ac9e653642b3','90b61c58a3e14b08','0c4bb9b6039a470e','4fbd098e6a8e4080']
    df_1_1=df_1_1[~df_1_1['seller_id'].isin(excluded_sids)]
    #=======================================================================
    #---Excluding lid's for sanskriti-----END ----HERE----------------------
    #=======================================================================


    # Active Listings Filter & Category Merge
    removal_ids_1 = set(df_listing[df_listing['listing_status'] == 'ACTIVE']['listing_id'].astype(str).str.strip())
    
    df_listing_subset = df_listing[['listing_id', 'analytic_super_category_l2']].drop_duplicates()
    df_1_1 = df_1_1.drop(columns=['analytic_super_category_l2'], errors='ignore')
    df_1_1 = df_1_1.merge(df_listing_subset, on=['listing_id'], how='left')
    
    df_1_1['listing_status_current'] = df_1_1['listing_id'].apply(lambda x: 'ACTIVE' if x in removal_ids_1 else 'INACTIVE')
    df_1_1 = df_1_1[df_1_1['analytic_category'] != 'ShopsyMobileProtection']
    
    # =============================================================================
    # 5. OFFER ID SETS
    # =============================================================================
    cofunded_ids = {
        'nb:mp:03fe219017','nb:mp:039828fb17','nb:mp:03cb7c1917','nb:mp:037f765117','nb:mp:0389611c17',
          'nb:mp:0382306117','nb:mp:033082cd17','nb:mp:0322416317','nb:mp:03065f6217','nb:mp:030fa81517','nb:mp:03525a7f18',
          'nb:mp:03a115da18','nb:mp:0377c78018','nb:mp:0310e4dc18','nb:mp:03e30d3c18','nb:mp:037fff6118','nb:mp:034e68bc18',
          'nb:mp:037e35b618','nb:mp:03a7f1fe18','nb:mp:03c6778518','nb:mp:0333e5a818','nb:mp:030e2f7618','nb:mp:03c3a5f618',
          'nb:mp:03563ec118','nb:mp:032c77a918','nb:mp:03a3a7ae18','nb:mp:03e7dbb418','nb:mp:036fb42e18','nb:mp:03fbda7c18',
          'nb:mp:03bfceef18','nb:mp:03d0079318','nb:mp:03b9964b18','nb:mp:03901fd618','nb:mp:0386beea18','nb:mp:03564a4818',
          'nb:mp:038c2dce18','nb:mp:03bd70f518','nb:mp:0395d80d18','nb:mp:03e06eb818','nb:mp:03b59cb618'
    }

    # =============================================================================
    # 6. OPT-IN LOGIC & MERGING
    # =============================================================================
    df_optin['listing_id'] = df_optin['listing_id'].astype(str).str.strip()
    df_optin['seller_id'] = df_optin['seller_id'].astype(str).str.strip()
    df_optin['cms_vertical'] = df_optin['cms_vertical'].astype(str).str.strip()

    overall_optin_listings = set(df_optin['listing_id'])

    # Apply Opt-in Flags
    df_1_1['Opted-in listing_id'] = df_1_1['listing_id'].apply(lambda x: 'Opted-in' if x in overall_optin_listings else 'Not opted-in')
    # df_1_1['Opted-in listing_id New FPO'] = df_1_1['listing_id'].apply(lambda x: 'Opted-in' if x in fpo_optin_listings else 'Not opted-in')
    # df_1_1['Opted-in listing_id New Cofunded'] = df_1_1['listing_id'].apply(lambda x: 'Opted-in' if x in cofund_optin_listings else 'Not opted-in')

    # Eligibility (Set to Eligible for calculation purposes)
    # df_1_1['New FPO'] = 'Eligible'
    # df_1_1['New Cofunded'] = 'Eligible'

#=================OFFER_ID'S==Mapping=========Here=========================================
    # 1. Group df_optin by 'listing_id' and combine 'offer_id's into a list
    offer_mapping = df_optin.groupby('listing_id')['offer_id'].apply(list)

    # 2. Map those lists to df_1_1 based on 'listing_id'
    df_1_1['offer_ids'] = df_1_1['listing_id'].map(offer_mapping)

    df_1_edited = df_1_1.copy()

#===============SUMMARY==PART======OWNER=CUT=======STARTS=HERE========================
if df_optin.empty:
    print("Empty set error – df_optin has no data.")
else:

# OWNER ---

    # Input SET Summary
    input_df_owner = df_1_edited[df_1_edited['SOS / FOS Flag'] == 'SOS']
    summary_input_owner = input_df_owner.groupby('owner').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[input_df_owner.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_input_owner.rename(columns={'owner': 'KAM'}, inplace=True)
    summary_input_owner.columns = [col if col == 'owner' else col + '_SOS' for col in summary_input_owner.columns]

    # Non-Input SET Summary
    non_input_df_owner = df_1_edited[df_1_edited['SOS / FOS Flag'] == 'FOS']
    summary_non_input_owner = non_input_df_owner.groupby('owner').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[non_input_df_owner.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_non_input_owner.rename(columns={'owner': 'KAM'}, inplace=True)
    summary_non_input_owner.columns = [col if col == 'owner' else col + '_FOS' for col in summary_non_input_owner.columns]

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
    unique_listing_ids_df_summary_owner['DW SOS %']=np.where(unique_listing_ids_df_summary_owner['orders_SOS']==0, 0, unique_listing_ids_df_summary_owner['opted_in_orders_SOS']/unique_listing_ids_df_summary_owner['orders_SOS']).round(2)

    # unique_listing_ids_df_summary_owner['Opt In LID %'] = (unique_listing_ids_df_summary_owner['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_owner['Opt In SID %'] = (unique_listing_ids_df_summary_owner['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_owner['DW SOS %'] = (unique_listing_ids_df_summary_owner['DW SOS %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_owner
    
    

    unique_listing_ids_df_summary_owner['DW FOS %']=np.where(unique_listing_ids_df_summary_owner['orders_FOS']==0, 0, unique_listing_ids_df_summary_owner['opted_in_orders_FOS']/unique_listing_ids_df_summary_owner['orders_FOS']).round(2)
    
    # unique_listing_ids_df_summary_owner['Opt In LID %'] = (unique_listing_ids_df_summary_owner['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_owner['Opt In SID %'] = (unique_listing_ids_df_summary_owner['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_owner['DW FOS %'] = (unique_listing_ids_df_summary_owner['DW FOS %'] * 100).round(2).astype(str) + '%'
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
        'orders_SOS': input_df_owner['orders'].sum(),
        'opted_in_orders_SOS': input_df_owner[input_df_owner['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_FOS': non_input_df_owner['orders'].sum(),
        'opted_in_orders_FOS': non_input_df_owner[non_input_df_owner['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_overall': df_1_edited['orders'].sum(),
        'opted_in_orders_overall': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_owner_df = pd.DataFrame([total_row_owner])

    total_row_owner_df['DW SOS %'] = np.where(
        total_row_owner_df['orders_SOS'] == 0, 0, 
        total_row_owner_df['opted_in_orders_SOS'] / total_row_owner_df['orders_SOS']
    ).round(2)

    total_row_owner_df['DW FOS %'] = np.where(
        total_row_owner_df['orders_FOS'] == 0, 0, 
        total_row_owner_df['opted_in_orders_FOS'] / total_row_owner_df['orders_FOS']
    ).round(2)

    total_row_owner_df['DW Overall %'] = np.where(
        total_row_owner_df['orders_overall'] == 0, 0, 
        total_row_owner_df['opted_in_orders_overall'] / total_row_owner_df['orders_overall']
    ).round(2)

    for col in ['DW SOS %', 'DW FOS %', 'DW Overall %']:
        total_row_owner_df[col] = (total_row_owner_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_owner_2 = pd.concat([total_row_owner_df, unique_listing_ids_df_summary_owner], ignore_index=True)
    unique_listing_ids_df_summary_owner_2

    unique_listing_ids_df_summary_owner_2=unique_listing_ids_df_summary_owner_2.fillna(0)
    
    html_table_owner = f'''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">Owner</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders Overall</th> 
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS) %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS) %</th>
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
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW SOS %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW FOS %']}</td>
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

#===================================HERE==SELL-BU-CUT-STARTS=====================================
if df_optin.empty:
    print("Empty set error – df_optin has no data.")
else:

# sell_bu ---

    # Input SET Summary
    input_df_sell_bu = df_1_edited[df_1_edited['SOS / FOS Flag'] == 'SOS']
    summary_input_sell_bu = input_df_sell_bu.groupby('sell_bu').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[input_df_sell_bu.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_input_sell_bu.rename(columns={'sell_bu': 'KAM'}, inplace=True)
    summary_input_sell_bu.columns = [col if col == 'sell_bu' else col + '_SOS' for col in summary_input_sell_bu.columns]

    # Non-Input SET Summary
    non_input_df_sell_bu = df_1_edited[df_1_edited['SOS / FOS Flag'] == 'FOS']
    summary_non_input_sell_bu = non_input_df_sell_bu.groupby('sell_bu').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[non_input_df_sell_bu.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_non_input_sell_bu.rename(columns={'sell_bu': 'KAM'}, inplace=True)
    summary_non_input_sell_bu.columns = [col if col == 'sell_bu' else col + '_FOS' for col in summary_non_input_sell_bu.columns]

    # Overall Summary
    summary_overall_sell_bu = df_1_edited.groupby('sell_bu').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_overall_sell_bu.rename(columns={'sell_bu': 'KAM'}, inplace=True)
    summary_overall_sell_bu.columns = [col if col == 'sell_bu' else col + '_overall' for col in summary_overall_sell_bu.columns]

    # Final merge
    summary_final_sell_bu = summary_input_sell_bu.merge(summary_non_input_sell_bu, on='sell_bu', how='outer')
    summary_final_sell_bu = summary_final_sell_bu.merge(summary_overall_sell_bu, on='sell_bu', how='outer')

    unique_listing_ids_df_summary_sell_bu = summary_final_sell_bu

    import numpy as np
    # unique_listing_ids_df_summary_sell_bu['Opt In LID %']=np.where(unique_listing_ids_df_summary_sell_bu['listing_id']==0, 0, unique_listing_ids_df_summary_sell_bu['opted_in_listing_id']/unique_listing_ids_df_summary_sell_bu['listing_id']).round(2)
    # unique_listing_ids_df_summary_sell_bu['Opt In SID %']=np.where(unique_listing_ids_df_summary_sell_bu['seller_id']==0, 0, unique_listing_ids_df_summary_sell_bu['opted_in_seller_id']/unique_listing_ids_df_summary_sell_bu['seller_id']).round(2)
    unique_listing_ids_df_summary_sell_bu['DW SOS %']=np.where(unique_listing_ids_df_summary_sell_bu['orders_SOS']==0, 0, unique_listing_ids_df_summary_sell_bu['opted_in_orders_SOS']/unique_listing_ids_df_summary_sell_bu['orders_SOS']).round(2)

    # unique_listing_ids_df_summary_sell_bu['Opt In LID %'] = (unique_listing_ids_df_summary_sell_bu['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_sell_bu['Opt In SID %'] = (unique_listing_ids_df_summary_sell_bu['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_sell_bu['DW SOS %'] = (unique_listing_ids_df_summary_sell_bu['DW SOS %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_sell_bu
    
    

    unique_listing_ids_df_summary_sell_bu['DW FOS %']=np.where(unique_listing_ids_df_summary_sell_bu['orders_FOS']==0, 0, unique_listing_ids_df_summary_sell_bu['opted_in_orders_FOS']/unique_listing_ids_df_summary_sell_bu['orders_FOS']).round(2)
    
    # unique_listing_ids_df_summary_sell_bu['Opt In LID %'] = (unique_listing_ids_df_summary_sell_bu['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_sell_bu['Opt In SID %'] = (unique_listing_ids_df_summary_sell_bu['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_sell_bu['DW FOS %'] = (unique_listing_ids_df_summary_sell_bu['DW FOS %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_sell_bu

    
    unique_listing_ids_df_summary_sell_bu['DW Overall %']=np.where(unique_listing_ids_df_summary_sell_bu['orders_overall']==0, 0, unique_listing_ids_df_summary_sell_bu['opted_in_orders_overall']/unique_listing_ids_df_summary_sell_bu['orders_overall']).round(2)
    
    # unique_listing_ids_df_summary_sell_bu['Opt In LID %'] = (unique_listing_ids_df_summary_sell_bu['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_sell_bu['Opt In SID %'] = (unique_listing_ids_df_summary_sell_bu['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_sell_bu['DW Overall %'] = (unique_listing_ids_df_summary_sell_bu['DW Overall %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_sell_bu
  

    total_row_sell_bu = pd.Series({
        'sell_bu': 'Total',
        # 'listing_id': df_1_edited['LID'].nunique(),
        # 'opted_in_listing_id': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['LID'].nunique(),
        # 'seller_id': df_1_edited['Seller Id'].nunique(),
        # 'opted_in_seller_id': df_1_edited[df_1_edited['Opted-in seller_id'] == 'Opted-in']['Seller Id'].nunique(),
        'orders_SOS': input_df_sell_bu['orders'].sum(),
        'opted_in_orders_SOS': input_df_sell_bu[input_df_sell_bu['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_FOS': non_input_df_sell_bu['orders'].sum(),
        'opted_in_orders_FOS': non_input_df_sell_bu[non_input_df_sell_bu['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_overall': df_1_edited['orders'].sum(),
        'opted_in_orders_overall': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_sell_bu_df = pd.DataFrame([total_row_sell_bu])

    total_row_sell_bu_df['DW SOS %'] = np.where(
        total_row_sell_bu_df['orders_SOS'] == 0, 0, 
        total_row_sell_bu_df['opted_in_orders_SOS'] / total_row_sell_bu_df['orders_SOS']
    ).round(2)

    total_row_sell_bu_df['DW FOS %'] = np.where(
        total_row_sell_bu_df['orders_FOS'] == 0, 0, 
        total_row_sell_bu_df['opted_in_orders_FOS'] / total_row_sell_bu_df['orders_FOS']
    ).round(2)

    total_row_sell_bu_df['DW Overall %'] = np.where(
        total_row_sell_bu_df['orders_overall'] == 0, 0, 
        total_row_sell_bu_df['opted_in_orders_overall'] / total_row_sell_bu_df['orders_overall']
    ).round(2)

    for col in ['DW SOS %', 'DW FOS %', 'DW Overall %']:
        total_row_sell_bu_df[col] = (total_row_sell_bu_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_sell_bu_2 = pd.concat([total_row_sell_bu_df, unique_listing_ids_df_summary_sell_bu], ignore_index=True)
    unique_listing_ids_df_summary_sell_bu_2

    unique_listing_ids_df_summary_sell_bu_2=unique_listing_ids_df_summary_sell_bu_2.fillna(0)
    
    html_table_sell_bu = f'''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">sell_bu</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders Overall</th> 
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS) %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS) %</th>
        </tr>
    </thead>
    <tbody>
'''

    html_table_sell_bu += '<tbody>'

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_sell_bu_2.iterrows():
        if 'total' in str(row['sell_bu']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
        html_table_sell_bu += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['sell_bu']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW SOS %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW FOS %']}</td>
            </tr>
        '''

    html_table_sell_bu += '</tbody></table>'
    

    # Add custom CSS for the table (optional styling like borders)
    css_style_sell_bu = """
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
    html_table_sell_bu = css_style_sell_bu + html_table_sell_bu

#=============================SELL=BU==KAM==CUT======================================================================
import numpy as np
import pandas as pd

if df_optin.empty:
    print("Empty set error – df_optin has no data.")
else:

# sell_bu ---

    # CORRECTED KAM FILTER: Handles case sensitivity, spaces, and nulls
    kam_df = df_1_edited[df_1_edited['owner'].astype(str).str.strip().str.upper() == 'KAM'].copy()
    
    # Note: If your data contains values like "KAM 1" or "KAM North", use this line instead:
    # kam_df = df_1_edited[df_1_edited['owner'].astype(str).str.contains('KAM', case=False, na=False)].copy()

    # Input SET Summary
    input_df_sell_bu_kam_cut = kam_df[kam_df['SOS / FOS Flag'].astype(str).str.strip().str.upper() == 'SOS']
    summary_input_sell_bu_kam_cut = input_df_sell_bu_kam_cut.groupby('sell_bu').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[input_df_sell_bu_kam_cut.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_input_sell_bu_kam_cut.columns = [col if col == 'sell_bu' else col + '_SOS' for col in summary_input_sell_bu_kam_cut.columns]

    # Non-Input SET Summary
    non_input_df_sell_bu_kam_cut = kam_df[kam_df['SOS / FOS Flag'].astype(str).str.strip().str.upper() == 'FOS']
    summary_non_input_sell_bu_kam_cut = non_input_df_sell_bu_kam_cut.groupby('sell_bu').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[non_input_df_sell_bu_kam_cut.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_non_input_sell_bu_kam_cut.columns = [col if col == 'sell_bu' else col + '_FOS' for col in summary_non_input_sell_bu_kam_cut.columns]

    # Overall Summary
    summary_overall_sell_bu_kam_cut = kam_df.groupby('sell_bu').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[kam_df.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_overall_sell_bu_kam_cut.columns = [col if col == 'sell_bu' else col + '_overall' for col in summary_overall_sell_bu_kam_cut.columns]

    # Final merge
    summary_final_sell_bu_kam_cut = summary_input_sell_bu_kam_cut.merge(summary_non_input_sell_bu_kam_cut, on='sell_bu', how='outer')
    summary_final_sell_bu_kam_cut = summary_final_sell_bu_kam_cut.merge(summary_overall_sell_bu_kam_cut, on='sell_bu', how='outer')

    unique_listing_ids_df_summary_sell_bu_kam_cut = summary_final_sell_bu_kam_cut

    # Percentages
    unique_listing_ids_df_summary_sell_bu_kam_cut['DW SOS %'] = np.where(unique_listing_ids_df_summary_sell_bu_kam_cut['orders_SOS']==0, 0, unique_listing_ids_df_summary_sell_bu_kam_cut['opted_in_orders_SOS']/unique_listing_ids_df_summary_sell_bu_kam_cut['orders_SOS']).round(2)
    unique_listing_ids_df_summary_sell_bu_kam_cut['DW SOS %'] = (unique_listing_ids_df_summary_sell_bu_kam_cut['DW SOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_sell_bu_kam_cut['DW FOS %'] = np.where(unique_listing_ids_df_summary_sell_bu_kam_cut['orders_FOS']==0, 0, unique_listing_ids_df_summary_sell_bu_kam_cut['opted_in_orders_FOS']/unique_listing_ids_df_summary_sell_bu_kam_cut['orders_FOS']).round(2)
    unique_listing_ids_df_summary_sell_bu_kam_cut['DW FOS %'] = (unique_listing_ids_df_summary_sell_bu_kam_cut['DW FOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_sell_bu_kam_cut['DW Overall %'] = np.where(unique_listing_ids_df_summary_sell_bu_kam_cut['orders_overall']==0, 0, unique_listing_ids_df_summary_sell_bu_kam_cut['opted_in_orders_overall']/unique_listing_ids_df_summary_sell_bu_kam_cut['orders_overall']).round(2)
    unique_listing_ids_df_summary_sell_bu_kam_cut['DW Overall %'] = (unique_listing_ids_df_summary_sell_bu_kam_cut['DW Overall %'] * 100).round(2).astype(str) + '%'

    # Total Row Calculation
    total_row_sell_bu_kam_cut = pd.Series({
        'sell_bu': 'Total',
        'orders_SOS': input_df_sell_bu_kam_cut['orders'].sum(),
        'opted_in_orders_SOS': input_df_sell_bu_kam_cut[input_df_sell_bu_kam_cut['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_FOS': non_input_df_sell_bu_kam_cut['orders'].sum(),
        'opted_in_orders_FOS': non_input_df_sell_bu_kam_cut[non_input_df_sell_bu_kam_cut['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_overall': kam_df['orders'].sum(), # Fixed to kam_df to match the KAM cut correctly
        'opted_in_orders_overall': kam_df[kam_df['Opted-in listing_id'] == 'Opted-in']['orders'].sum() # Fixed to kam_df
    })

    total_row_sell_bu_kam_cut_df = pd.DataFrame([total_row_sell_bu_kam_cut])

    total_row_sell_bu_kam_cut_df['DW SOS %'] = np.where(
        total_row_sell_bu_kam_cut_df['orders_SOS'] == 0, 0, 
        total_row_sell_bu_kam_cut_df['opted_in_orders_SOS'] / total_row_sell_bu_kam_cut_df['orders_SOS']
    ).round(2)

    total_row_sell_bu_kam_cut_df['DW FOS %'] = np.where(
        total_row_sell_bu_kam_cut_df['orders_FOS'] == 0, 0, 
        total_row_sell_bu_kam_cut_df['opted_in_orders_FOS'] / total_row_sell_bu_kam_cut_df['orders_FOS']
    ).round(2)

    total_row_sell_bu_kam_cut_df['DW Overall %'] = np.where(
        total_row_sell_bu_kam_cut_df['orders_overall'] == 0, 0, 
        total_row_sell_bu_kam_cut_df['opted_in_orders_overall'] / total_row_sell_bu_kam_cut_df['orders_overall']
    ).round(2)

    for col in ['DW SOS %', 'DW FOS %', 'DW Overall %']:
        total_row_sell_bu_kam_cut_df[col] = (total_row_sell_bu_kam_cut_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_sell_bu_kam_cut_2 = pd.concat([total_row_sell_bu_kam_cut_df, unique_listing_ids_df_summary_sell_bu_kam_cut], ignore_index=True)
    unique_listing_ids_df_summary_sell_bu_kam_cut_2 = unique_listing_ids_df_summary_sell_bu_kam_cut_2.fillna(0)
    
    # HTML Table Generation
    html_table_sell_bu_kam_cut = '''
    <table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
        <thead>
            <tr>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">sell_bu(KAM)</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders Overall</th> 
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall %</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (SOS)</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS)</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS) %</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (FOS)</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS)</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS) %</th>
            </tr>
        </thead>
        <tbody>
    '''

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_sell_bu_kam_cut_2.iterrows():
        if 'total' in str(row['sell_bu']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
            
        html_table_sell_bu_kam_cut += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['sell_bu']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW SOS %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW FOS %']}</td>
            </tr>
        '''

    html_table_sell_bu_kam_cut += '</tbody></table>'
    
    # Add custom CSS for the table
    css_style_sell_bu_kam_cut = """
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
    html_table_sell_bu_kam_cut = css_style_sell_bu_kam_cut + html_table_sell_bu_kam_cut

#================Over-ALL-Analytic_category--Cut==================================================
if df_optin.empty:
    print("Empty set error – df_optin has no data.")
else:

# analytic_category ---

    # Input SET Summary
    input_df_analytic_category_cut = df_1_edited[df_1_edited['SOS / FOS Flag'] == 'SOS']
    summary_input_analytic_category_cut = input_df_analytic_category_cut.groupby('analytic_category').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[input_df_analytic_category_cut.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_input_analytic_category_cut.rename(columns={'analytic_category': 'KAM'}, inplace=True)
    summary_input_analytic_category_cut.columns = [col if col == 'analytic_category' else col + '_SOS' for col in summary_input_analytic_category_cut.columns]

    # Non-Input SET Summary
    non_input_df_analytic_category_cut = df_1_edited[df_1_edited['SOS / FOS Flag'] == 'FOS']
    summary_non_input_analytic_category_cut = non_input_df_analytic_category_cut.groupby('analytic_category').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[non_input_df_analytic_category_cut.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_non_input_analytic_category_cut.rename(columns={'analytic_category': 'KAM'}, inplace=True)
    summary_non_input_analytic_category_cut.columns = [col if col == 'analytic_category' else col + '_FOS' for col in summary_non_input_analytic_category_cut.columns]

    # Overall Summary
    summary_overall_analytic_category_cut = df_1_edited.groupby('analytic_category').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_overall_analytic_category_cut.rename(columns={'analytic_category': 'KAM'}, inplace=True)
    summary_overall_analytic_category_cut.columns = [col if col == 'analytic_category' else col + '_overall' for col in summary_overall_analytic_category_cut.columns]

    # Final merge
    summary_final_analytic_category_cut = summary_input_analytic_category_cut.merge(summary_non_input_analytic_category_cut, on='analytic_category', how='outer')
    summary_final_analytic_category_cut = summary_final_analytic_category_cut.merge(summary_overall_analytic_category_cut, on='analytic_category', how='outer')

    unique_listing_ids_df_summary_analytic_category_cut = summary_final_analytic_category_cut

    import numpy as np
    # unique_listing_ids_df_summary_analytic_category_cut['Opt In LID %']=np.where(unique_listing_ids_df_summary_analytic_category_cut['listing_id']==0, 0, unique_listing_ids_df_summary_analytic_category_cut['opted_in_listing_id']/unique_listing_ids_df_summary_analytic_category_cut['listing_id']).round(2)
    # unique_listing_ids_df_summary_analytic_category_cut['Opt In SID %']=np.where(unique_listing_ids_df_summary_analytic_category_cut['seller_id']==0, 0, unique_listing_ids_df_summary_analytic_category_cut['opted_in_seller_id']/unique_listing_ids_df_summary_analytic_category_cut['seller_id']).round(2)
    unique_listing_ids_df_summary_analytic_category_cut['DW SOS %']=np.where(unique_listing_ids_df_summary_analytic_category_cut['orders_SOS']==0, 0, unique_listing_ids_df_summary_analytic_category_cut['opted_in_orders_SOS']/unique_listing_ids_df_summary_analytic_category_cut['orders_SOS']).round(2)

    # unique_listing_ids_df_summary_analytic_category_cut['Opt In LID %'] = (unique_listing_ids_df_summary_analytic_category_cut['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_analytic_category_cut['Opt In SID %'] = (unique_listing_ids_df_summary_analytic_category_cut['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_analytic_category_cut['DW SOS %'] = (unique_listing_ids_df_summary_analytic_category_cut['DW SOS %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_analytic_category_cut
    
    

    unique_listing_ids_df_summary_analytic_category_cut['DW FOS %']=np.where(unique_listing_ids_df_summary_analytic_category_cut['orders_FOS']==0, 0, unique_listing_ids_df_summary_analytic_category_cut['opted_in_orders_FOS']/unique_listing_ids_df_summary_analytic_category_cut['orders_FOS']).round(2)
    
    # unique_listing_ids_df_summary_analytic_category_cut['Opt In LID %'] = (unique_listing_ids_df_summary_analytic_category_cut['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_analytic_category_cut['Opt In SID %'] = (unique_listing_ids_df_summary_analytic_category_cut['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_analytic_category_cut['DW FOS %'] = (unique_listing_ids_df_summary_analytic_category_cut['DW FOS %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_analytic_category_cut

    
    unique_listing_ids_df_summary_analytic_category_cut['DW Overall %']=np.where(unique_listing_ids_df_summary_analytic_category_cut['orders_overall']==0, 0, unique_listing_ids_df_summary_analytic_category_cut['opted_in_orders_overall']/unique_listing_ids_df_summary_analytic_category_cut['orders_overall']).round(2)
    
    # unique_listing_ids_df_summary_analytic_category_cut['Opt In LID %'] = (unique_listing_ids_df_summary_analytic_category_cut['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_analytic_category_cut['Opt In SID %'] = (unique_listing_ids_df_summary_analytic_category_cut['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_analytic_category_cut['DW Overall %'] = (unique_listing_ids_df_summary_analytic_category_cut['DW Overall %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_analytic_category_cut
  

    total_row_analytic_category_cut = pd.Series({
        'analytic_category': 'Total',
        # 'listing_id': df_1_edited['LID'].nunique(),
        # 'opted_in_listing_id': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['LID'].nunique(),
        # 'seller_id': df_1_edited['Seller Id'].nunique(),
        # 'opted_in_seller_id': df_1_edited[df_1_edited['Opted-in seller_id'] == 'Opted-in']['Seller Id'].nunique(),
        'orders_SOS': input_df_analytic_category_cut['orders'].sum(),
        'opted_in_orders_SOS': input_df_analytic_category_cut[input_df_analytic_category_cut['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_FOS': non_input_df_analytic_category_cut['orders'].sum(),
        'opted_in_orders_FOS': non_input_df_analytic_category_cut[non_input_df_analytic_category_cut['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_overall': df_1_edited['orders'].sum(),
        'opted_in_orders_overall': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_analytic_category_cut_df = pd.DataFrame([total_row_analytic_category_cut])

    total_row_analytic_category_cut_df['DW SOS %'] = np.where(
        total_row_analytic_category_cut_df['orders_SOS'] == 0, 0, 
        total_row_analytic_category_cut_df['opted_in_orders_SOS'] / total_row_analytic_category_cut_df['orders_SOS']
    ).round(2)

    total_row_analytic_category_cut_df['DW FOS %'] = np.where(
        total_row_analytic_category_cut_df['orders_FOS'] == 0, 0, 
        total_row_analytic_category_cut_df['opted_in_orders_FOS'] / total_row_analytic_category_cut_df['orders_FOS']
    ).round(2)

    total_row_analytic_category_cut_df['DW Overall %'] = np.where(
        total_row_analytic_category_cut_df['orders_overall'] == 0, 0, 
        total_row_analytic_category_cut_df['opted_in_orders_overall'] / total_row_analytic_category_cut_df['orders_overall']
    ).round(2)

    for col in ['DW SOS %', 'DW FOS %', 'DW Overall %']:
        total_row_analytic_category_cut_df[col] = (total_row_analytic_category_cut_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_analytic_category_cut_2 = pd.concat([total_row_analytic_category_cut_df, unique_listing_ids_df_summary_analytic_category_cut], ignore_index=True)
    unique_listing_ids_df_summary_analytic_category_cut_2

    unique_listing_ids_df_summary_analytic_category_cut_2=unique_listing_ids_df_summary_analytic_category_cut_2.fillna(0)
    
    html_table_analytic_category_cut = f'''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">analytic_category</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders Overall</th> 
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS) %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS) %</th>
        </tr>
    </thead>
    <tbody>
'''

    html_table_analytic_category_cut += '<tbody>'

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_analytic_category_cut_2.iterrows():
        if 'total' in str(row['analytic_category']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
        html_table_analytic_category_cut += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['analytic_category']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW SOS %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW FOS %']}</td>
            </tr>
        '''

    html_table_analytic_category_cut += '</tbody></table>'
    

    # Add custom CSS for the table (optional styling like borders)
    css_style_analytic_category_cut = """
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
    html_table_analytic_category_cut = css_style_analytic_category_cut + html_table_analytic_category_cut

#==========Analytic_category=======================KAM====CUT===========================================
import numpy as np
import pandas as pd

if df_optin.empty:
    print("Empty set error – df_optin has no data.")
else:

# analytic_category ---

    # CORRECTED KAM FILTER: Handles case sensitivity, spaces, and nulls
    kam_df = df_1_edited[df_1_edited['owner'].astype(str).str.strip().str.upper() == 'KAM'].copy()
    
    # Note: If your data contains values like "KAM 1" or "KAM North", use this line instead:
    # kam_df = df_1_edited[df_1_edited['owner'].astype(str).str.contains('KAM', case=False, na=False)].copy()

    # Input SET Summary
    input_df_analytic_category_kam_cut = kam_df[kam_df['SOS / FOS Flag'].astype(str).str.strip().str.upper() == 'SOS']
    summary_input_analytic_category_kam_cut = input_df_analytic_category_kam_cut.groupby('analytic_category').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[input_df_analytic_category_kam_cut.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_input_analytic_category_kam_cut.columns = [col if col == 'analytic_category' else col + '_SOS' for col in summary_input_analytic_category_kam_cut.columns]

    # Non-Input SET Summary
    non_input_df_analytic_category_kam_cut = kam_df[kam_df['SOS / FOS Flag'].astype(str).str.strip().str.upper() == 'FOS']
    summary_non_input_analytic_category_kam_cut = non_input_df_analytic_category_kam_cut.groupby('analytic_category').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[non_input_df_analytic_category_kam_cut.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_non_input_analytic_category_kam_cut.columns = [col if col == 'analytic_category' else col + '_FOS' for col in summary_non_input_analytic_category_kam_cut.columns]

    # Overall Summary
    summary_overall_analytic_category_kam_cut = kam_df.groupby('analytic_category').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[kam_df.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_overall_analytic_category_kam_cut.columns = [col if col == 'analytic_category' else col + '_overall' for col in summary_overall_analytic_category_kam_cut.columns]

    # Final merge
    summary_final_analytic_category_kam_cut = summary_input_analytic_category_kam_cut.merge(summary_non_input_analytic_category_kam_cut, on='analytic_category', how='outer')
    summary_final_analytic_category_kam_cut = summary_final_analytic_category_kam_cut.merge(summary_overall_analytic_category_kam_cut, on='analytic_category', how='outer')

    unique_listing_ids_df_summary_analytic_category_kam_cut = summary_final_analytic_category_kam_cut

    # Percentages
    unique_listing_ids_df_summary_analytic_category_kam_cut['DW SOS %'] = np.where(unique_listing_ids_df_summary_analytic_category_kam_cut['orders_SOS']==0, 0, unique_listing_ids_df_summary_analytic_category_kam_cut['opted_in_orders_SOS']/unique_listing_ids_df_summary_analytic_category_kam_cut['orders_SOS']).round(2)
    unique_listing_ids_df_summary_analytic_category_kam_cut['DW SOS %'] = (unique_listing_ids_df_summary_analytic_category_kam_cut['DW SOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_analytic_category_kam_cut['DW FOS %'] = np.where(unique_listing_ids_df_summary_analytic_category_kam_cut['orders_FOS']==0, 0, unique_listing_ids_df_summary_analytic_category_kam_cut['opted_in_orders_FOS']/unique_listing_ids_df_summary_analytic_category_kam_cut['orders_FOS']).round(2)
    unique_listing_ids_df_summary_analytic_category_kam_cut['DW FOS %'] = (unique_listing_ids_df_summary_analytic_category_kam_cut['DW FOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_analytic_category_kam_cut['DW Overall %'] = np.where(unique_listing_ids_df_summary_analytic_category_kam_cut['orders_overall']==0, 0, unique_listing_ids_df_summary_analytic_category_kam_cut['opted_in_orders_overall']/unique_listing_ids_df_summary_analytic_category_kam_cut['orders_overall']).round(2)
    unique_listing_ids_df_summary_analytic_category_kam_cut['DW Overall %'] = (unique_listing_ids_df_summary_analytic_category_kam_cut['DW Overall %'] * 100).round(2).astype(str) + '%'

    # Total Row Calculation
    total_row_analytic_category_kam_cut = pd.Series({
        'analytic_category': 'Total',
        'orders_SOS': input_df_analytic_category_kam_cut['orders'].sum(),
        'opted_in_orders_SOS': input_df_analytic_category_kam_cut[input_df_analytic_category_kam_cut['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_FOS': non_input_df_analytic_category_kam_cut['orders'].sum(),
        'opted_in_orders_FOS': non_input_df_analytic_category_kam_cut[non_input_df_analytic_category_kam_cut['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_overall': kam_df['orders'].sum(), # Fixed to kam_df to match the KAM cut correctly
        'opted_in_orders_overall': kam_df[kam_df['Opted-in listing_id'] == 'Opted-in']['orders'].sum() # Fixed to kam_df
    })

    total_row_analytic_category_kam_cut_df = pd.DataFrame([total_row_analytic_category_kam_cut])

    total_row_analytic_category_kam_cut_df['DW SOS %'] = np.where(
        total_row_analytic_category_kam_cut_df['orders_SOS'] == 0, 0, 
        total_row_analytic_category_kam_cut_df['opted_in_orders_SOS'] / total_row_analytic_category_kam_cut_df['orders_SOS']
    ).round(2)

    total_row_analytic_category_kam_cut_df['DW FOS %'] = np.where(
        total_row_analytic_category_kam_cut_df['orders_FOS'] == 0, 0, 
        total_row_analytic_category_kam_cut_df['opted_in_orders_FOS'] / total_row_analytic_category_kam_cut_df['orders_FOS']
    ).round(2)

    total_row_analytic_category_kam_cut_df['DW Overall %'] = np.where(
        total_row_analytic_category_kam_cut_df['orders_overall'] == 0, 0, 
        total_row_analytic_category_kam_cut_df['opted_in_orders_overall'] / total_row_analytic_category_kam_cut_df['orders_overall']
    ).round(2)

    for col in ['DW SOS %', 'DW FOS %', 'DW Overall %']:
        total_row_analytic_category_kam_cut_df[col] = (total_row_analytic_category_kam_cut_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_analytic_category_kam_cut_2 = pd.concat([total_row_analytic_category_kam_cut_df, unique_listing_ids_df_summary_analytic_category_kam_cut], ignore_index=True)
    unique_listing_ids_df_summary_analytic_category_kam_cut_2 = unique_listing_ids_df_summary_analytic_category_kam_cut_2.fillna(0)
    
    # HTML Table Generation
    html_table_analytic_category_kam_cut = '''
    <table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
        <thead>
            <tr>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">analytic_category(KAM)</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders Overall</th> 
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall %</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (SOS)</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS)</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS) %</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (FOS)</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS)</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS) %</th>
            </tr>
        </thead>
        <tbody>
    '''

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_analytic_category_kam_cut_2.iterrows():
        if 'total' in str(row['analytic_category']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
            
        html_table_analytic_category_kam_cut += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['analytic_category']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW SOS %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW FOS %']}</td>
            </tr>
        '''

    html_table_analytic_category_kam_cut += '</tbody></table>'
    
    # Add custom CSS for the table
    css_style_analytic_category_kam_cut = """
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
    html_table_analytic_category_kam_cut = css_style_analytic_category_kam_cut + html_table_analytic_category_kam_cut

#===============================Owner--Cut--------Top-70%--flag--cut=====================================================
# Filter rows where top_70_flag == 1
#======================================TOp--70%-----CUT==========================================
#=====================================owner======================================================
if df_optin.empty:
    print("Empty set error – df_optin has no data.")
else:

# OWNER ---

    # Input SET Summary
    filtered_df = df_1_edited[df_1_edited['top_70_flag'] == 1].copy()
    
    input_df_owner_top_70 = filtered_df[filtered_df['SOS / FOS Flag'] == 'SOS']
    summary_input_owner_top_70 = input_df_owner_top_70.groupby('owner').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[input_df_owner_top_70.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_input_owner_top_70.columns = [col if col == 'owner' else col + '_SOS' for col in summary_input_owner_top_70.columns]

    # Non-Input SET Summary
    non_input_df_owner_top_70 = filtered_df[filtered_df['SOS / FOS Flag'] == 'FOS']
    summary_non_input_owner_top_70 = non_input_df_owner_top_70.groupby('owner').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[non_input_df_owner_top_70.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_non_input_owner_top_70.columns = [col if col == 'owner' else col + '_FOS' for col in summary_non_input_owner_top_70.columns]

    # Overall Summary
    summary_overall_owner_top_70 = filtered_df.groupby('owner').agg(
        orders=('orders', 'sum'),
        # FIXED: Replaced df_1_edited with filtered_df
        opted_in_orders=('orders', lambda x: (x[filtered_df.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_overall_owner_top_70.columns = [col if col == 'owner' else col + '_overall' for col in summary_overall_owner_top_70.columns]

    # Final merge
    summary_final_owner_top_70 = summary_input_owner_top_70.merge(summary_non_input_owner_top_70, on='owner', how='outer')
    summary_final_owner_top_70 = summary_final_owner_top_70.merge(summary_overall_owner_top_70, on='owner', how='outer')

    unique_listing_ids_df_summary_owner_top_70 = summary_final_owner_top_70

    import numpy as np
    
    unique_listing_ids_df_summary_owner_top_70['DW SOS %'] = np.where(unique_listing_ids_df_summary_owner_top_70['orders_SOS']==0, 0, unique_listing_ids_df_summary_owner_top_70['opted_in_orders_SOS']/unique_listing_ids_df_summary_owner_top_70['orders_SOS']).round(2)
    unique_listing_ids_df_summary_owner_top_70['DW SOS %'] = (unique_listing_ids_df_summary_owner_top_70['DW SOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_owner_top_70['DW FOS %'] = np.where(unique_listing_ids_df_summary_owner_top_70['orders_FOS']==0, 0, unique_listing_ids_df_summary_owner_top_70['opted_in_orders_FOS']/unique_listing_ids_df_summary_owner_top_70['orders_FOS']).round(2)
    unique_listing_ids_df_summary_owner_top_70['DW FOS %'] = (unique_listing_ids_df_summary_owner_top_70['DW FOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_owner_top_70['DW Overall %'] = np.where(unique_listing_ids_df_summary_owner_top_70['orders_overall']==0, 0, unique_listing_ids_df_summary_owner_top_70['opted_in_orders_overall']/unique_listing_ids_df_summary_owner_top_70['orders_overall']).round(2)
    unique_listing_ids_df_summary_owner_top_70['DW Overall %'] = (unique_listing_ids_df_summary_owner_top_70['DW Overall %'] * 100).round(2).astype(str) + '%'

    total_row_owner_top_70 = pd.Series({
        'owner': 'Total',
        'orders_SOS': input_df_owner_top_70['orders'].sum(),
        'opted_in_orders_SOS': input_df_owner_top_70[input_df_owner_top_70['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_FOS': non_input_df_owner_top_70['orders'].sum(),
        'opted_in_orders_FOS': non_input_df_owner_top_70[non_input_df_owner_top_70['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        # FIXED: Replaced df_1_edited with filtered_df
        'orders_overall': filtered_df['orders'].sum(),
        'opted_in_orders_overall': filtered_df[filtered_df['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_owner_top_70_df = pd.DataFrame([total_row_owner_top_70])

    total_row_owner_top_70_df['DW SOS %'] = np.where(
        total_row_owner_top_70_df['orders_SOS'] == 0, 0, 
        total_row_owner_top_70_df['opted_in_orders_SOS'] / total_row_owner_top_70_df['orders_SOS']
    ).round(2)

    total_row_owner_top_70_df['DW FOS %'] = np.where(
        total_row_owner_top_70_df['orders_FOS'] == 0, 0, 
        total_row_owner_top_70_df['opted_in_orders_FOS'] / total_row_owner_top_70_df['orders_FOS']
    ).round(2)

    total_row_owner_top_70_df['DW Overall %'] = np.where(
        total_row_owner_top_70_df['orders_overall'] == 0, 0, 
        total_row_owner_top_70_df['opted_in_orders_overall'] / total_row_owner_top_70_df['orders_overall']
    ).round(2)

    for col in ['DW SOS %', 'DW FOS %', 'DW Overall %']:
        total_row_owner_top_70_df[col] = (total_row_owner_top_70_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_owner_top_70_2 = pd.concat([total_row_owner_top_70_df, unique_listing_ids_df_summary_owner_top_70], ignore_index=True)
    unique_listing_ids_df_summary_owner_top_70_2 = unique_listing_ids_df_summary_owner_top_70_2.fillna(0)
    
    html_table_owner_top_70 = f'''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">Owner</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders Overall</th> 
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Order (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Order (SOS) %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Order (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Order (FOS) %</th>
        </tr>
    </thead>
    <tbody>
'''

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_owner_top_70_2.iterrows():
        if 'total' in str(row['owner']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
        html_table_owner_top_70 += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['owner']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW SOS %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW FOS %']}</td>
            </tr>
        '''

    html_table_owner_top_70 += '</tbody></table>'
    
    # Add custom CSS for the table (optional styling like borders)
    css_style_owner_top_70 = """
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
    html_table_owner_top_70 = css_style_owner_top_70 + html_table_owner_top_70

#=================----KAM----------------LEVEl========================================================
import numpy as np
import pandas as pd

if df_optin.empty:
    print("Empty set error – df_optin has no data.")
else:

# KAM ---

    # CORRECTED KAM FILTER: Handles case sensitivity, spaces, and nulls
    kam_df = df_1_edited[df_1_edited['owner'].astype(str).str.strip().str.upper() == 'KAM'].copy()
    
    # Note: If your data contains values like "KAM 1" or "KAM North", use this line instead:
    # kam_df = df_1_edited[df_1_edited['owner'].astype(str).str.contains('KAM', case=False, na=False)].copy()

    # Input SET Summary
    input_df_KAM_ = kam_df[kam_df['SOS / FOS Flag'].astype(str).str.strip().str.upper() == 'SOS']
    summary_input_KAM_ = input_df_KAM_.groupby('KAM').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[input_df_KAM_.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_input_KAM_.columns = [col if col == 'KAM' else col + '_SOS' for col in summary_input_KAM_.columns]

    # Non-Input SET Summary
    non_input_df_KAM_ = kam_df[kam_df['SOS / FOS Flag'].astype(str).str.strip().str.upper() == 'FOS']
    summary_non_input_KAM_ = non_input_df_KAM_.groupby('KAM').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[non_input_df_KAM_.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_non_input_KAM_.columns = [col if col == 'KAM' else col + '_FOS' for col in summary_non_input_KAM_.columns]

    # Overall Summary
    summary_overall_KAM_ = kam_df.groupby('KAM').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[kam_df.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_overall_KAM_.columns = [col if col == 'KAM' else col + '_overall' for col in summary_overall_KAM_.columns]

    # Final merge
    summary_final_KAM_ = summary_input_KAM_.merge(summary_non_input_KAM_, on='KAM', how='outer')
    summary_final_KAM_ = summary_final_KAM_.merge(summary_overall_KAM_, on='KAM', how='outer')

    unique_listing_ids_df_summary_KAM_ = summary_final_KAM_

    # Percentages
    unique_listing_ids_df_summary_KAM_['DW SOS %'] = np.where(unique_listing_ids_df_summary_KAM_['orders_SOS']==0, 0, unique_listing_ids_df_summary_KAM_['opted_in_orders_SOS']/unique_listing_ids_df_summary_KAM_['orders_SOS']).round(2)
    unique_listing_ids_df_summary_KAM_['DW SOS %'] = (unique_listing_ids_df_summary_KAM_['DW SOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_KAM_['DW FOS %'] = np.where(unique_listing_ids_df_summary_KAM_['orders_FOS']==0, 0, unique_listing_ids_df_summary_KAM_['opted_in_orders_FOS']/unique_listing_ids_df_summary_KAM_['orders_FOS']).round(2)
    unique_listing_ids_df_summary_KAM_['DW FOS %'] = (unique_listing_ids_df_summary_KAM_['DW FOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_KAM_['DW Overall %'] = np.where(unique_listing_ids_df_summary_KAM_['orders_overall']==0, 0, unique_listing_ids_df_summary_KAM_['opted_in_orders_overall']/unique_listing_ids_df_summary_KAM_['orders_overall']).round(2)
    unique_listing_ids_df_summary_KAM_['DW Overall %'] = (unique_listing_ids_df_summary_KAM_['DW Overall %'] * 100).round(2).astype(str) + '%'

    # Total Row Calculation
    total_row_KAM_ = pd.Series({
        'KAM': 'Total',
        'orders_SOS': input_df_KAM_['orders'].sum(),
        'opted_in_orders_SOS': input_df_KAM_[input_df_KAM_['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_FOS': non_input_df_KAM_['orders'].sum(),
        'opted_in_orders_FOS': non_input_df_KAM_[non_input_df_KAM_['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_overall': kam_df['orders'].sum(), # Fixed to kam_df to match the KAM cut correctly
        'opted_in_orders_overall': kam_df[kam_df['Opted-in listing_id'] == 'Opted-in']['orders'].sum() # Fixed to kam_df
    })

    total_row_KAM__df = pd.DataFrame([total_row_KAM_])

    total_row_KAM__df['DW SOS %'] = np.where(
        total_row_KAM__df['orders_SOS'] == 0, 0, 
        total_row_KAM__df['opted_in_orders_SOS'] / total_row_KAM__df['orders_SOS']
    ).round(2)

    total_row_KAM__df['DW FOS %'] = np.where(
        total_row_KAM__df['orders_FOS'] == 0, 0, 
        total_row_KAM__df['opted_in_orders_FOS'] / total_row_KAM__df['orders_FOS']
    ).round(2)

    total_row_KAM__df['DW Overall %'] = np.where(
        total_row_KAM__df['orders_overall'] == 0, 0, 
        total_row_KAM__df['opted_in_orders_overall'] / total_row_KAM__df['orders_overall']
    ).round(2)

    for col in ['DW SOS %', 'DW FOS %', 'DW Overall %']:
        total_row_KAM__df[col] = (total_row_KAM__df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_KAM__2 = pd.concat([total_row_KAM__df, unique_listing_ids_df_summary_KAM_], ignore_index=True)
    unique_listing_ids_df_summary_KAM__2 = unique_listing_ids_df_summary_KAM__2.fillna(0)
    
    # HTML Table Generation
    html_table_KAM_ = '''
    <table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
        <thead>
            <tr>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">KAM</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders Overall</th> 
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall %</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (SOS)</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS)</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS) %</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (FOS)</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS)</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS) %</th>
            </tr>
        </thead>
        <tbody>
    '''

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_KAM__2.iterrows():
        if 'total' in str(row['KAM']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
            
        html_table_KAM_ += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['KAM']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW SOS %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW FOS %']}</td>
            </tr>
        '''

    html_table_KAM_ += '</tbody></table>'
    
    # Add custom CSS for the table
    css_style_KAM_ = """
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
    html_table_KAM_ = css_style_KAM_ + html_table_KAM_

#=====================SELL-BU--Top-70%--CUT====================================================
# Filter rows where top_70_flag == 1
#======================================TOp--70%-----CUT==========================================
#=====================================sell_bu======================================================
if df_optin.empty:
    print("Empty set error – df_optin has no data.")
else:

# sell_bu ---

    # Input SET Summary
    filtered_df = df_1_edited[df_1_edited['top_70_flag'] == 1].copy()
    
    input_df_sell_bu_top_70 = filtered_df[filtered_df['SOS / FOS Flag'] == 'SOS']
    summary_input_sell_bu_top_70 = input_df_sell_bu_top_70.groupby('sell_bu').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[input_df_sell_bu_top_70.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_input_sell_bu_top_70.columns = [col if col == 'sell_bu' else col + '_SOS' for col in summary_input_sell_bu_top_70.columns]

    # Non-Input SET Summary
    non_input_df_sell_bu_top_70 = filtered_df[filtered_df['SOS / FOS Flag'] == 'FOS']
    summary_non_input_sell_bu_top_70 = non_input_df_sell_bu_top_70.groupby('sell_bu').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[non_input_df_sell_bu_top_70.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_non_input_sell_bu_top_70.columns = [col if col == 'sell_bu' else col + '_FOS' for col in summary_non_input_sell_bu_top_70.columns]

    # Overall Summary
    summary_overall_sell_bu_top_70 = filtered_df.groupby('sell_bu').agg(
        orders=('orders', 'sum'),
        # FIXED: Replaced df_1_edited with filtered_df
        opted_in_orders=('orders', lambda x: (x[filtered_df.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_overall_sell_bu_top_70.columns = [col if col == 'sell_bu' else col + '_overall' for col in summary_overall_sell_bu_top_70.columns]

    # Final merge
    summary_final_sell_bu_top_70 = summary_input_sell_bu_top_70.merge(summary_non_input_sell_bu_top_70, on='sell_bu', how='outer')
    summary_final_sell_bu_top_70 = summary_final_sell_bu_top_70.merge(summary_overall_sell_bu_top_70, on='sell_bu', how='outer')

    unique_listing_ids_df_summary_sell_bu_top_70 = summary_final_sell_bu_top_70

    import numpy as np
    
    unique_listing_ids_df_summary_sell_bu_top_70['DW SOS %'] = np.where(unique_listing_ids_df_summary_sell_bu_top_70['orders_SOS']==0, 0, unique_listing_ids_df_summary_sell_bu_top_70['opted_in_orders_SOS']/unique_listing_ids_df_summary_sell_bu_top_70['orders_SOS']).round(2)
    unique_listing_ids_df_summary_sell_bu_top_70['DW SOS %'] = (unique_listing_ids_df_summary_sell_bu_top_70['DW SOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_sell_bu_top_70['DW FOS %'] = np.where(unique_listing_ids_df_summary_sell_bu_top_70['orders_FOS']==0, 0, unique_listing_ids_df_summary_sell_bu_top_70['opted_in_orders_FOS']/unique_listing_ids_df_summary_sell_bu_top_70['orders_FOS']).round(2)
    unique_listing_ids_df_summary_sell_bu_top_70['DW FOS %'] = (unique_listing_ids_df_summary_sell_bu_top_70['DW FOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_sell_bu_top_70['DW Overall %'] = np.where(unique_listing_ids_df_summary_sell_bu_top_70['orders_overall']==0, 0, unique_listing_ids_df_summary_sell_bu_top_70['opted_in_orders_overall']/unique_listing_ids_df_summary_sell_bu_top_70['orders_overall']).round(2)
    unique_listing_ids_df_summary_sell_bu_top_70['DW Overall %'] = (unique_listing_ids_df_summary_sell_bu_top_70['DW Overall %'] * 100).round(2).astype(str) + '%'

    total_row_sell_bu_top_70 = pd.Series({
        'sell_bu': 'Total',
        'orders_SOS': input_df_sell_bu_top_70['orders'].sum(),
        'opted_in_orders_SOS': input_df_sell_bu_top_70[input_df_sell_bu_top_70['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_FOS': non_input_df_sell_bu_top_70['orders'].sum(),
        'opted_in_orders_FOS': non_input_df_sell_bu_top_70[non_input_df_sell_bu_top_70['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        # FIXED: Replaced df_1_edited with filtered_df
        'orders_overall': filtered_df['orders'].sum(),
        'opted_in_orders_overall': filtered_df[filtered_df['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_sell_bu_top_70_df = pd.DataFrame([total_row_sell_bu_top_70])

    total_row_sell_bu_top_70_df['DW SOS %'] = np.where(
        total_row_sell_bu_top_70_df['orders_SOS'] == 0, 0, 
        total_row_sell_bu_top_70_df['opted_in_orders_SOS'] / total_row_sell_bu_top_70_df['orders_SOS']
    ).round(2)

    total_row_sell_bu_top_70_df['DW FOS %'] = np.where(
        total_row_sell_bu_top_70_df['orders_FOS'] == 0, 0, 
        total_row_sell_bu_top_70_df['opted_in_orders_FOS'] / total_row_sell_bu_top_70_df['orders_FOS']
    ).round(2)

    total_row_sell_bu_top_70_df['DW Overall %'] = np.where(
        total_row_sell_bu_top_70_df['orders_overall'] == 0, 0, 
        total_row_sell_bu_top_70_df['opted_in_orders_overall'] / total_row_sell_bu_top_70_df['orders_overall']
    ).round(2)

    for col in ['DW SOS %', 'DW FOS %', 'DW Overall %']:
        total_row_sell_bu_top_70_df[col] = (total_row_sell_bu_top_70_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_sell_bu_top_70_2 = pd.concat([total_row_sell_bu_top_70_df, unique_listing_ids_df_summary_sell_bu_top_70], ignore_index=True)
    unique_listing_ids_df_summary_sell_bu_top_70_2 = unique_listing_ids_df_summary_sell_bu_top_70_2.fillna(0)
    
    html_table_sell_bu_top_70 = f'''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">sell_bu</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders Overall</th> 
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS) %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS) %</th>
        </tr>
    </thead>
    <tbody>
'''

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_sell_bu_top_70_2.iterrows():
        if 'total' in str(row['sell_bu']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
        html_table_sell_bu_top_70 += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['sell_bu']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW SOS %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW FOS %']}</td>
            </tr>
        '''

    html_table_sell_bu_top_70 += '</tbody></table>'
    
    # Add custom CSS for the table (optional styling like borders)
    css_style_sell_bu_top_70 = """
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
    html_table_sell_bu_top_70 = css_style_sell_bu_top_70 + html_table_sell_bu_top_70

#========================SELL_BU========TOP-70%============KAM==CUT================================
    # Filter rows where top_70_flag == 1
#======================================TOp--70%-----CUT==========================================
#=====================================sell_bu======================================================
if df_optin.empty:
    print("Empty set error – df_optin has no data.")
else:

# sell_bu ---

    # Input SET Summary
    filtered_df = df_1_edited[(df_1_edited['top_70_flag'] == 1) & (df_1_edited['owner'].astype(str).str.strip().str.upper() == 'KAM')].copy()

    input_df_sell_bu_top_70_kam_cut = filtered_df[filtered_df['SOS / FOS Flag'] == 'SOS']
    summary_input_sell_bu_top_70_kam_cut = input_df_sell_bu_top_70_kam_cut.groupby('sell_bu').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[input_df_sell_bu_top_70_kam_cut.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_input_sell_bu_top_70_kam_cut.columns = [col if col == 'sell_bu' else col + '_SOS' for col in summary_input_sell_bu_top_70_kam_cut.columns]

    # Non-Input SET Summary
    non_input_df_sell_bu_top_70_kam_cut = filtered_df[filtered_df['SOS / FOS Flag'] == 'FOS']
    summary_non_input_sell_bu_top_70_kam_cut = non_input_df_sell_bu_top_70_kam_cut.groupby('sell_bu').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[non_input_df_sell_bu_top_70_kam_cut.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_non_input_sell_bu_top_70_kam_cut.columns = [col if col == 'sell_bu' else col + '_FOS' for col in summary_non_input_sell_bu_top_70_kam_cut.columns]

    # Overall Summary
    summary_overall_sell_bu_top_70_kam_cut = filtered_df.groupby('sell_bu').agg(
        orders=('orders', 'sum'),
        # FIXED: Replaced df_1_edited with filtered_df
        opted_in_orders=('orders', lambda x: (x[filtered_df.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_overall_sell_bu_top_70_kam_cut.columns = [col if col == 'sell_bu' else col + '_overall' for col in summary_overall_sell_bu_top_70_kam_cut.columns]

    # Final merge
    summary_final_sell_bu_top_70_kam_cut = summary_input_sell_bu_top_70_kam_cut.merge(summary_non_input_sell_bu_top_70_kam_cut, on='sell_bu', how='outer')
    summary_final_sell_bu_top_70_kam_cut = summary_final_sell_bu_top_70_kam_cut.merge(summary_overall_sell_bu_top_70_kam_cut, on='sell_bu', how='outer')

    unique_listing_ids_df_summary_sell_bu_top_70_kam_cut = summary_final_sell_bu_top_70_kam_cut

    import numpy as np
    
    unique_listing_ids_df_summary_sell_bu_top_70_kam_cut['DW SOS %'] = np.where(unique_listing_ids_df_summary_sell_bu_top_70_kam_cut['orders_SOS']==0, 0, unique_listing_ids_df_summary_sell_bu_top_70_kam_cut['opted_in_orders_SOS']/unique_listing_ids_df_summary_sell_bu_top_70_kam_cut['orders_SOS']).round(2)
    unique_listing_ids_df_summary_sell_bu_top_70_kam_cut['DW SOS %'] = (unique_listing_ids_df_summary_sell_bu_top_70_kam_cut['DW SOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_sell_bu_top_70_kam_cut['DW FOS %'] = np.where(unique_listing_ids_df_summary_sell_bu_top_70_kam_cut['orders_FOS']==0, 0, unique_listing_ids_df_summary_sell_bu_top_70_kam_cut['opted_in_orders_FOS']/unique_listing_ids_df_summary_sell_bu_top_70_kam_cut['orders_FOS']).round(2)
    unique_listing_ids_df_summary_sell_bu_top_70_kam_cut['DW FOS %'] = (unique_listing_ids_df_summary_sell_bu_top_70_kam_cut['DW FOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_sell_bu_top_70_kam_cut['DW Overall %'] = np.where(unique_listing_ids_df_summary_sell_bu_top_70_kam_cut['orders_overall']==0, 0, unique_listing_ids_df_summary_sell_bu_top_70_kam_cut['opted_in_orders_overall']/unique_listing_ids_df_summary_sell_bu_top_70_kam_cut['orders_overall']).round(2)
    unique_listing_ids_df_summary_sell_bu_top_70_kam_cut['DW Overall %'] = (unique_listing_ids_df_summary_sell_bu_top_70_kam_cut['DW Overall %'] * 100).round(2).astype(str) + '%'

    total_row_sell_bu_top_70_kam_cut = pd.Series({
        'sell_bu': 'Total',
        'orders_SOS': input_df_sell_bu_top_70_kam_cut['orders'].sum(),
        'opted_in_orders_SOS': input_df_sell_bu_top_70_kam_cut[input_df_sell_bu_top_70_kam_cut['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_FOS': non_input_df_sell_bu_top_70_kam_cut['orders'].sum(),
        'opted_in_orders_FOS': non_input_df_sell_bu_top_70_kam_cut[non_input_df_sell_bu_top_70_kam_cut['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        # FIXED: Replaced df_1_edited with filtered_df
        'orders_overall': filtered_df['orders'].sum(),
        'opted_in_orders_overall': filtered_df[filtered_df['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_sell_bu_top_70_kam_cut_df = pd.DataFrame([total_row_sell_bu_top_70_kam_cut])

    total_row_sell_bu_top_70_kam_cut_df['DW SOS %'] = np.where(
        total_row_sell_bu_top_70_kam_cut_df['orders_SOS'] == 0, 0, 
        total_row_sell_bu_top_70_kam_cut_df['opted_in_orders_SOS'] / total_row_sell_bu_top_70_kam_cut_df['orders_SOS']
    ).round(2)

    total_row_sell_bu_top_70_kam_cut_df['DW FOS %'] = np.where(
        total_row_sell_bu_top_70_kam_cut_df['orders_FOS'] == 0, 0, 
        total_row_sell_bu_top_70_kam_cut_df['opted_in_orders_FOS'] / total_row_sell_bu_top_70_kam_cut_df['orders_FOS']
    ).round(2)

    total_row_sell_bu_top_70_kam_cut_df['DW Overall %'] = np.where(
        total_row_sell_bu_top_70_kam_cut_df['orders_overall'] == 0, 0, 
        total_row_sell_bu_top_70_kam_cut_df['opted_in_orders_overall'] / total_row_sell_bu_top_70_kam_cut_df['orders_overall']
    ).round(2)

    for col in ['DW SOS %', 'DW FOS %', 'DW Overall %']:
        total_row_sell_bu_top_70_kam_cut_df[col] = (total_row_sell_bu_top_70_kam_cut_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_sell_bu_top_70_kam_cut_2 = pd.concat([total_row_sell_bu_top_70_kam_cut_df, unique_listing_ids_df_summary_sell_bu_top_70_kam_cut], ignore_index=True)
    unique_listing_ids_df_summary_sell_bu_top_70_kam_cut_2 = unique_listing_ids_df_summary_sell_bu_top_70_kam_cut_2.fillna(0)
    
    html_table_sell_bu_top_70_kam_cut = f'''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">sell_bu</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders Overall</th> 
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS) %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS) %</th>
        </tr>
    </thead>
    <tbody>
'''

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_sell_bu_top_70_kam_cut_2.iterrows():
        if 'total' in str(row['sell_bu']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
        html_table_sell_bu_top_70_kam_cut += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['sell_bu']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW SOS %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW FOS %']}</td>
            </tr>
        '''

    html_table_sell_bu_top_70_kam_cut += '</tbody></table>'
    
    # Add custom CSS for the table (optional styling like borders)
    css_style_sell_bu_top_70_kam_cut = """
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
    html_table_sell_bu_top_70_kam_cut = css_style_sell_bu_top_70_kam_cut + html_table_sell_bu_top_70_kam_cut

#================TOP-70%=============KAM==LEVEL=======================================================
    # Filter rows where top_70_flag == 1
#======================================TOp--70%-----CUT==========================================
#=====================================KAM======================================================
if df_optin.empty:
    print("Empty set error – df_optin has no data.")
else:

# KAM ---

    # Input SET Summary
    filtered_df = df_1_edited[(df_1_edited['top_70_flag'] == 1) & (df_1_edited['owner'].astype(str).str.strip().str.upper() == 'KAM')].copy()

    input_df_sell_top_70_kam = filtered_df[filtered_df['SOS / FOS Flag'] == 'SOS']
    summary_input_sell_top_70_kam = input_df_sell_top_70_kam.groupby('KAM').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[input_df_sell_top_70_kam.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_input_sell_top_70_kam.columns = [col if col == 'KAM' else col + '_SOS' for col in summary_input_sell_top_70_kam.columns]

    # Non-Input SET Summary
    non_input_df_sell_top_70_kam = filtered_df[filtered_df['SOS / FOS Flag'] == 'FOS']
    summary_non_input_sell_top_70_kam = non_input_df_sell_top_70_kam.groupby('KAM').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[non_input_df_sell_top_70_kam.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_non_input_sell_top_70_kam.columns = [col if col == 'KAM' else col + '_FOS' for col in summary_non_input_sell_top_70_kam.columns]

    # Overall Summary
    summary_overall_sell_top_70_kam = filtered_df.groupby('KAM').agg(
        orders=('orders', 'sum'),
        # FIXED: Replaced df_1_edited with filtered_df
        opted_in_orders=('orders', lambda x: (x[filtered_df.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_overall_sell_top_70_kam.columns = [col if col == 'KAM' else col + '_overall' for col in summary_overall_sell_top_70_kam.columns]

    # Final merge
    summary_final_sell_top_70_kam = summary_input_sell_top_70_kam.merge(summary_non_input_sell_top_70_kam, on='KAM', how='outer')
    summary_final_sell_top_70_kam = summary_final_sell_top_70_kam.merge(summary_overall_sell_top_70_kam, on='KAM', how='outer')

    unique_listing_ids_df_summary_sell_top_70_kam = summary_final_sell_top_70_kam

    import numpy as np
    
    unique_listing_ids_df_summary_sell_top_70_kam['DW SOS %'] = np.where(unique_listing_ids_df_summary_sell_top_70_kam['orders_SOS']==0, 0, unique_listing_ids_df_summary_sell_top_70_kam['opted_in_orders_SOS']/unique_listing_ids_df_summary_sell_top_70_kam['orders_SOS']).round(2)
    unique_listing_ids_df_summary_sell_top_70_kam['DW SOS %'] = (unique_listing_ids_df_summary_sell_top_70_kam['DW SOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_sell_top_70_kam['DW FOS %'] = np.where(unique_listing_ids_df_summary_sell_top_70_kam['orders_FOS']==0, 0, unique_listing_ids_df_summary_sell_top_70_kam['opted_in_orders_FOS']/unique_listing_ids_df_summary_sell_top_70_kam['orders_FOS']).round(2)
    unique_listing_ids_df_summary_sell_top_70_kam['DW FOS %'] = (unique_listing_ids_df_summary_sell_top_70_kam['DW FOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_sell_top_70_kam['DW Overall %'] = np.where(unique_listing_ids_df_summary_sell_top_70_kam['orders_overall']==0, 0, unique_listing_ids_df_summary_sell_top_70_kam['opted_in_orders_overall']/unique_listing_ids_df_summary_sell_top_70_kam['orders_overall']).round(2)
    unique_listing_ids_df_summary_sell_top_70_kam['DW Overall %'] = (unique_listing_ids_df_summary_sell_top_70_kam['DW Overall %'] * 100).round(2).astype(str) + '%'

    total_row_sell_top_70_kam = pd.Series({
        'KAM': 'Total',
        'orders_SOS': input_df_sell_top_70_kam['orders'].sum(),
        'opted_in_orders_SOS': input_df_sell_top_70_kam[input_df_sell_top_70_kam['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_FOS': non_input_df_sell_top_70_kam['orders'].sum(),
        'opted_in_orders_FOS': non_input_df_sell_top_70_kam[non_input_df_sell_top_70_kam['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        # FIXED: Replaced df_1_edited with filtered_df
        'orders_overall': filtered_df['orders'].sum(),
        'opted_in_orders_overall': filtered_df[filtered_df['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_sell_top_70_kam_df = pd.DataFrame([total_row_sell_top_70_kam])

    total_row_sell_top_70_kam_df['DW SOS %'] = np.where(
        total_row_sell_top_70_kam_df['orders_SOS'] == 0, 0, 
        total_row_sell_top_70_kam_df['opted_in_orders_SOS'] / total_row_sell_top_70_kam_df['orders_SOS']
    ).round(2)

    total_row_sell_top_70_kam_df['DW FOS %'] = np.where(
        total_row_sell_top_70_kam_df['orders_FOS'] == 0, 0, 
        total_row_sell_top_70_kam_df['opted_in_orders_FOS'] / total_row_sell_top_70_kam_df['orders_FOS']
    ).round(2)

    total_row_sell_top_70_kam_df['DW Overall %'] = np.where(
        total_row_sell_top_70_kam_df['orders_overall'] == 0, 0, 
        total_row_sell_top_70_kam_df['opted_in_orders_overall'] / total_row_sell_top_70_kam_df['orders_overall']
    ).round(2)

    for col in ['DW SOS %', 'DW FOS %', 'DW Overall %']:
        total_row_sell_top_70_kam_df[col] = (total_row_sell_top_70_kam_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_sell_top_70_kam_2 = pd.concat([total_row_sell_top_70_kam_df, unique_listing_ids_df_summary_sell_top_70_kam], ignore_index=True)
    unique_listing_ids_df_summary_sell_top_70_kam_2 = unique_listing_ids_df_summary_sell_top_70_kam_2.fillna(0)
    
    html_table_sell_top_70_kam = f'''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">KAM</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders Overall</th> 
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS) %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS) %</th>
        </tr>
    </thead>
    <tbody>
'''

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_sell_top_70_kam_2.iterrows():
        if 'total' in str(row['KAM']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
        html_table_sell_top_70_kam += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['KAM']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW SOS %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW FOS %']}</td>
            </tr>
        '''

    html_table_sell_top_70_kam += '</tbody></table>'
    
    # Add custom CSS for the table (optional styling like borders)
    css_style_sell_top_70_kam = """
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
    html_table_sell_top_70_kam = css_style_sell_top_70_kam + html_table_sell_top_70_kam

#==========================ASP==Bucket==Overall=================================================
if df_optin.empty:
    print("Empty set error – df_optin has no data.")
else:

# asp_bucket ---

    # Input SET Summary
    input_df_asp_bucket_over_all = df_1_edited[df_1_edited['SOS / FOS Flag'] == 'SOS']
    summary_input_asp_bucket_over_all = input_df_asp_bucket_over_all.groupby('asp_bucket').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[input_df_asp_bucket_over_all.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_input_asp_bucket_over_all.rename(columns={'asp_bucket': 'KAM'}, inplace=True)
    summary_input_asp_bucket_over_all.columns = [col if col == 'asp_bucket' else col + '_SOS' for col in summary_input_asp_bucket_over_all.columns]

    # Non-Input SET Summary
    non_input_df_asp_bucket_over_all = df_1_edited[df_1_edited['SOS / FOS Flag'] == 'FOS']
    summary_non_input_asp_bucket_over_all = non_input_df_asp_bucket_over_all.groupby('asp_bucket').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[non_input_df_asp_bucket_over_all.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_non_input_asp_bucket_over_all.rename(columns={'asp_bucket': 'KAM'}, inplace=True)
    summary_non_input_asp_bucket_over_all.columns = [col if col == 'asp_bucket' else col + '_FOS' for col in summary_non_input_asp_bucket_over_all.columns]

    # Overall Summary
    summary_overall_asp_bucket_over_all = df_1_edited.groupby('asp_bucket').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    #summary_overall_asp_bucket_over_all.rename(columns={'asp_bucket': 'KAM'}, inplace=True)
    summary_overall_asp_bucket_over_all.columns = [col if col == 'asp_bucket' else col + '_overall' for col in summary_overall_asp_bucket_over_all.columns]

    # Final merge
    summary_final_asp_bucket_over_all = summary_input_asp_bucket_over_all.merge(summary_non_input_asp_bucket_over_all, on='asp_bucket', how='outer')
    summary_final_asp_bucket_over_all = summary_final_asp_bucket_over_all.merge(summary_overall_asp_bucket_over_all, on='asp_bucket', how='outer')

    unique_listing_ids_df_summary_asp_bucket_over_all = summary_final_asp_bucket_over_all

    import numpy as np
    # unique_listing_ids_df_summary_asp_bucket_over_all['Opt In LID %']=np.where(unique_listing_ids_df_summary_asp_bucket_over_all['listing_id']==0, 0, unique_listing_ids_df_summary_asp_bucket_over_all['opted_in_listing_id']/unique_listing_ids_df_summary_asp_bucket_over_all['listing_id']).round(2)
    # unique_listing_ids_df_summary_asp_bucket_over_all['Opt In SID %']=np.where(unique_listing_ids_df_summary_asp_bucket_over_all['seller_id']==0, 0, unique_listing_ids_df_summary_asp_bucket_over_all['opted_in_seller_id']/unique_listing_ids_df_summary_asp_bucket_over_all['seller_id']).round(2)
    unique_listing_ids_df_summary_asp_bucket_over_all['DW SOS %']=np.where(unique_listing_ids_df_summary_asp_bucket_over_all['orders_SOS']==0, 0, unique_listing_ids_df_summary_asp_bucket_over_all['opted_in_orders_SOS']/unique_listing_ids_df_summary_asp_bucket_over_all['orders_SOS']).round(2)

    # unique_listing_ids_df_summary_asp_bucket_over_all['Opt In LID %'] = (unique_listing_ids_df_summary_asp_bucket_over_all['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_asp_bucket_over_all['Opt In SID %'] = (unique_listing_ids_df_summary_asp_bucket_over_all['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_asp_bucket_over_all['DW SOS %'] = (unique_listing_ids_df_summary_asp_bucket_over_all['DW SOS %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_asp_bucket_over_all
    
    

    unique_listing_ids_df_summary_asp_bucket_over_all['DW FOS %']=np.where(unique_listing_ids_df_summary_asp_bucket_over_all['orders_FOS']==0, 0, unique_listing_ids_df_summary_asp_bucket_over_all['opted_in_orders_FOS']/unique_listing_ids_df_summary_asp_bucket_over_all['orders_FOS']).round(2)
    
    # unique_listing_ids_df_summary_asp_bucket_over_all['Opt In LID %'] = (unique_listing_ids_df_summary_asp_bucket_over_all['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_asp_bucket_over_all['Opt In SID %'] = (unique_listing_ids_df_summary_asp_bucket_over_all['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_asp_bucket_over_all['DW FOS %'] = (unique_listing_ids_df_summary_asp_bucket_over_all['DW FOS %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_asp_bucket_over_all

    
    unique_listing_ids_df_summary_asp_bucket_over_all['DW Overall %']=np.where(unique_listing_ids_df_summary_asp_bucket_over_all['orders_overall']==0, 0, unique_listing_ids_df_summary_asp_bucket_over_all['opted_in_orders_overall']/unique_listing_ids_df_summary_asp_bucket_over_all['orders_overall']).round(2)
    
    # unique_listing_ids_df_summary_asp_bucket_over_all['Opt In LID %'] = (unique_listing_ids_df_summary_asp_bucket_over_all['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary_asp_bucket_over_all['Opt In SID %'] = (unique_listing_ids_df_summary_asp_bucket_over_all['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_asp_bucket_over_all['DW Overall %'] = (unique_listing_ids_df_summary_asp_bucket_over_all['DW Overall %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary_asp_bucket_over_all
  

    total_row_asp_bucket_over_all = pd.Series({
        'asp_bucket': 'Total',
        # 'listing_id': df_1_edited['LID'].nunique(),
        # 'opted_in_listing_id': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['LID'].nunique(),
        # 'seller_id': df_1_edited['Seller Id'].nunique(),
        # 'opted_in_seller_id': df_1_edited[df_1_edited['Opted-in seller_id'] == 'Opted-in']['Seller Id'].nunique(),
        'orders_SOS': input_df_asp_bucket_over_all['orders'].sum(),
        'opted_in_orders_SOS': input_df_asp_bucket_over_all[input_df_asp_bucket_over_all['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_FOS': non_input_df_asp_bucket_over_all['orders'].sum(),
        'opted_in_orders_FOS': non_input_df_asp_bucket_over_all[non_input_df_asp_bucket_over_all['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_overall': df_1_edited['orders'].sum(),
        'opted_in_orders_overall': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_asp_bucket_over_all_df = pd.DataFrame([total_row_asp_bucket_over_all])

    total_row_asp_bucket_over_all_df['DW SOS %'] = np.where(
        total_row_asp_bucket_over_all_df['orders_SOS'] == 0, 0, 
        total_row_asp_bucket_over_all_df['opted_in_orders_SOS'] / total_row_asp_bucket_over_all_df['orders_SOS']
    ).round(2)

    total_row_asp_bucket_over_all_df['DW FOS %'] = np.where(
        total_row_asp_bucket_over_all_df['orders_FOS'] == 0, 0, 
        total_row_asp_bucket_over_all_df['opted_in_orders_FOS'] / total_row_asp_bucket_over_all_df['orders_FOS']
    ).round(2)

    total_row_asp_bucket_over_all_df['DW Overall %'] = np.where(
        total_row_asp_bucket_over_all_df['orders_overall'] == 0, 0, 
        total_row_asp_bucket_over_all_df['opted_in_orders_overall'] / total_row_asp_bucket_over_all_df['orders_overall']
    ).round(2)

    for col in ['DW SOS %', 'DW FOS %', 'DW Overall %']:
        total_row_asp_bucket_over_all_df[col] = (total_row_asp_bucket_over_all_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_asp_bucket_over_all_2 = pd.concat([total_row_asp_bucket_over_all_df, unique_listing_ids_df_summary_asp_bucket_over_all], ignore_index=True)
    unique_listing_ids_df_summary_asp_bucket_over_all_2

    unique_listing_ids_df_summary_asp_bucket_over_all_2=unique_listing_ids_df_summary_asp_bucket_over_all_2.fillna(0)
    
    html_table_asp_bucket_over_all = f'''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">asp_bucket</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders Overall</th> 
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS) %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS) %</th>
        </tr>
    </thead>
    <tbody>
'''

    html_table_asp_bucket_over_all += '<tbody>'

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_asp_bucket_over_all_2.iterrows():
        if 'total' in str(row['asp_bucket']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
        html_table_asp_bucket_over_all += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['asp_bucket']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW SOS %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW FOS %']}</td>
            </tr>
        '''

    html_table_asp_bucket_over_all += '</tbody></table>'
    

    # Add custom CSS for the table (optional styling like borders)
    css_style_asp_bucket_over_all = """
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
    html_table_asp_bucket_over_all = css_style_asp_bucket_over_all + html_table_asp_bucket_over_all

#=================================ASP==Bucket=Overall=KAM==LEVEL========================================
import numpy as np
import pandas as pd

if df_optin.empty:
    print("Empty set error – df_optin has no data.")
else:

# asp_bucket ---

    # CORRECTED KAM FILTER: Handles case sensitivity, spaces, and nulls
    kam_df = df_1_edited[df_1_edited['owner'].astype(str).str.strip().str.upper() == 'KAM'].copy()
    
    # Note: If your data contains values like "KAM 1" or "KAM North", use this line instead:
    # kam_df = df_1_edited[df_1_edited['owner'].astype(str).str.contains('KAM', case=False, na=False)].copy()

    # Input SET Summary
    input_df_asp_bucket_kam_cut = kam_df[kam_df['SOS / FOS Flag'].astype(str).str.strip().str.upper() == 'SOS']
    summary_input_asp_bucket_kam_cut = input_df_asp_bucket_kam_cut.groupby('asp_bucket').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[input_df_asp_bucket_kam_cut.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_input_asp_bucket_kam_cut.columns = [col if col == 'asp_bucket' else col + '_SOS' for col in summary_input_asp_bucket_kam_cut.columns]

    # Non-Input SET Summary
    non_input_df_asp_bucket_kam_cut = kam_df[kam_df['SOS / FOS Flag'].astype(str).str.strip().str.upper() == 'FOS']
    summary_non_input_asp_bucket_kam_cut = non_input_df_asp_bucket_kam_cut.groupby('asp_bucket').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[non_input_df_asp_bucket_kam_cut.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_non_input_asp_bucket_kam_cut.columns = [col if col == 'asp_bucket' else col + '_FOS' for col in summary_non_input_asp_bucket_kam_cut.columns]

    # Overall Summary
    summary_overall_asp_bucket_kam_cut = kam_df.groupby('asp_bucket').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[kam_df.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_overall_asp_bucket_kam_cut.columns = [col if col == 'asp_bucket' else col + '_overall' for col in summary_overall_asp_bucket_kam_cut.columns]

    # Final merge
    summary_final_asp_bucket_kam_cut = summary_input_asp_bucket_kam_cut.merge(summary_non_input_asp_bucket_kam_cut, on='asp_bucket', how='outer')
    summary_final_asp_bucket_kam_cut = summary_final_asp_bucket_kam_cut.merge(summary_overall_asp_bucket_kam_cut, on='asp_bucket', how='outer')

    unique_listing_ids_df_summary_asp_bucket_kam_cut = summary_final_asp_bucket_kam_cut

    # Percentages
    unique_listing_ids_df_summary_asp_bucket_kam_cut['DW SOS %'] = np.where(unique_listing_ids_df_summary_asp_bucket_kam_cut['orders_SOS']==0, 0, unique_listing_ids_df_summary_asp_bucket_kam_cut['opted_in_orders_SOS']/unique_listing_ids_df_summary_asp_bucket_kam_cut['orders_SOS']).round(2)
    unique_listing_ids_df_summary_asp_bucket_kam_cut['DW SOS %'] = (unique_listing_ids_df_summary_asp_bucket_kam_cut['DW SOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_asp_bucket_kam_cut['DW FOS %'] = np.where(unique_listing_ids_df_summary_asp_bucket_kam_cut['orders_FOS']==0, 0, unique_listing_ids_df_summary_asp_bucket_kam_cut['opted_in_orders_FOS']/unique_listing_ids_df_summary_asp_bucket_kam_cut['orders_FOS']).round(2)
    unique_listing_ids_df_summary_asp_bucket_kam_cut['DW FOS %'] = (unique_listing_ids_df_summary_asp_bucket_kam_cut['DW FOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_asp_bucket_kam_cut['DW Overall %'] = np.where(unique_listing_ids_df_summary_asp_bucket_kam_cut['orders_overall']==0, 0, unique_listing_ids_df_summary_asp_bucket_kam_cut['opted_in_orders_overall']/unique_listing_ids_df_summary_asp_bucket_kam_cut['orders_overall']).round(2)
    unique_listing_ids_df_summary_asp_bucket_kam_cut['DW Overall %'] = (unique_listing_ids_df_summary_asp_bucket_kam_cut['DW Overall %'] * 100).round(2).astype(str) + '%'

    # Total Row Calculation
    total_row_asp_bucket_kam_cut = pd.Series({
        'asp_bucket': 'Total',
        'orders_SOS': input_df_asp_bucket_kam_cut['orders'].sum(),
        'opted_in_orders_SOS': input_df_asp_bucket_kam_cut[input_df_asp_bucket_kam_cut['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_FOS': non_input_df_asp_bucket_kam_cut['orders'].sum(),
        'opted_in_orders_FOS': non_input_df_asp_bucket_kam_cut[non_input_df_asp_bucket_kam_cut['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_overall': kam_df['orders'].sum(), # Fixed to kam_df to match the KAM cut correctly
        'opted_in_orders_overall': kam_df[kam_df['Opted-in listing_id'] == 'Opted-in']['orders'].sum() # Fixed to kam_df
    })

    total_row_asp_bucket_kam_cut_df = pd.DataFrame([total_row_asp_bucket_kam_cut])

    total_row_asp_bucket_kam_cut_df['DW SOS %'] = np.where(
        total_row_asp_bucket_kam_cut_df['orders_SOS'] == 0, 0, 
        total_row_asp_bucket_kam_cut_df['opted_in_orders_SOS'] / total_row_asp_bucket_kam_cut_df['orders_SOS']
    ).round(2)

    total_row_asp_bucket_kam_cut_df['DW FOS %'] = np.where(
        total_row_asp_bucket_kam_cut_df['orders_FOS'] == 0, 0, 
        total_row_asp_bucket_kam_cut_df['opted_in_orders_FOS'] / total_row_asp_bucket_kam_cut_df['orders_FOS']
    ).round(2)

    total_row_asp_bucket_kam_cut_df['DW Overall %'] = np.where(
        total_row_asp_bucket_kam_cut_df['orders_overall'] == 0, 0, 
        total_row_asp_bucket_kam_cut_df['opted_in_orders_overall'] / total_row_asp_bucket_kam_cut_df['orders_overall']
    ).round(2)

    for col in ['DW SOS %', 'DW FOS %', 'DW Overall %']:
        total_row_asp_bucket_kam_cut_df[col] = (total_row_asp_bucket_kam_cut_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_asp_bucket_kam_cut_2 = pd.concat([total_row_asp_bucket_kam_cut_df, unique_listing_ids_df_summary_asp_bucket_kam_cut], ignore_index=True)
    unique_listing_ids_df_summary_asp_bucket_kam_cut_2 = unique_listing_ids_df_summary_asp_bucket_kam_cut_2.fillna(0)
    
    # HTML Table Generation
    html_table_asp_bucket_kam_cut = '''
    <table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
        <thead>
            <tr>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">asp_bucket(KAM)</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders Overall</th> 
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall %</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (SOS)</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS)</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS) %</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (FOS)</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS)</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS) %</th>
            </tr>
        </thead>
        <tbody>
    '''

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_asp_bucket_kam_cut_2.iterrows():
        if 'total' in str(row['asp_bucket']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
            
        html_table_asp_bucket_kam_cut += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['asp_bucket']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW SOS %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW FOS %']}</td>
            </tr>
        '''

    html_table_asp_bucket_kam_cut += '</tbody></table>'
    
    # Add custom CSS for the table
    css_style_asp_bucket_kam_cut = """
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
    html_table_asp_bucket_kam_cut = css_style_asp_bucket_kam_cut + html_table_asp_bucket_kam_cut

#=======================ASP===TOP 70%===========Over-ALL======================================
if df_optin.empty:
    print("Empty set error – df_optin has no data.")
else:

# asp_bucket ---

    # Input SET Summary
    filtered_df = df_1_edited[df_1_edited['top_70_flag'] == 1].copy()
    
    input_df_asp_bucket_top_70 = filtered_df[filtered_df['SOS / FOS Flag'] == 'SOS']
    summary_input_asp_bucket_top_70 = input_df_asp_bucket_top_70.groupby('asp_bucket').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[input_df_asp_bucket_top_70.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_input_asp_bucket_top_70.columns = [col if col == 'asp_bucket' else col + '_SOS' for col in summary_input_asp_bucket_top_70.columns]

    # Non-Input SET Summary
    non_input_df_asp_bucket_top_70 = filtered_df[filtered_df['SOS / FOS Flag'] == 'FOS']
    summary_non_input_asp_bucket_top_70 = non_input_df_asp_bucket_top_70.groupby('asp_bucket').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[non_input_df_asp_bucket_top_70.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_non_input_asp_bucket_top_70.columns = [col if col == 'asp_bucket' else col + '_FOS' for col in summary_non_input_asp_bucket_top_70.columns]

    # Overall Summary
    summary_overall_asp_bucket_top_70 = filtered_df.groupby('asp_bucket').agg(
        orders=('orders', 'sum'),
        # FIXED: Replaced df_1_edited with filtered_df
        opted_in_orders=('orders', lambda x: (x[filtered_df.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_overall_asp_bucket_top_70.columns = [col if col == 'asp_bucket' else col + '_overall' for col in summary_overall_asp_bucket_top_70.columns]

    # Final merge
    summary_final_asp_bucket_top_70 = summary_input_asp_bucket_top_70.merge(summary_non_input_asp_bucket_top_70, on='asp_bucket', how='outer')
    summary_final_asp_bucket_top_70 = summary_final_asp_bucket_top_70.merge(summary_overall_asp_bucket_top_70, on='asp_bucket', how='outer')

    unique_listing_ids_df_summary_asp_bucket_top_70 = summary_final_asp_bucket_top_70

    import numpy as np
    
    unique_listing_ids_df_summary_asp_bucket_top_70['DW SOS %'] = np.where(unique_listing_ids_df_summary_asp_bucket_top_70['orders_SOS']==0, 0, unique_listing_ids_df_summary_asp_bucket_top_70['opted_in_orders_SOS']/unique_listing_ids_df_summary_asp_bucket_top_70['orders_SOS']).round(2)
    unique_listing_ids_df_summary_asp_bucket_top_70['DW SOS %'] = (unique_listing_ids_df_summary_asp_bucket_top_70['DW SOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_asp_bucket_top_70['DW FOS %'] = np.where(unique_listing_ids_df_summary_asp_bucket_top_70['orders_FOS']==0, 0, unique_listing_ids_df_summary_asp_bucket_top_70['opted_in_orders_FOS']/unique_listing_ids_df_summary_asp_bucket_top_70['orders_FOS']).round(2)
    unique_listing_ids_df_summary_asp_bucket_top_70['DW FOS %'] = (unique_listing_ids_df_summary_asp_bucket_top_70['DW FOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_asp_bucket_top_70['DW Overall %'] = np.where(unique_listing_ids_df_summary_asp_bucket_top_70['orders_overall']==0, 0, unique_listing_ids_df_summary_asp_bucket_top_70['opted_in_orders_overall']/unique_listing_ids_df_summary_asp_bucket_top_70['orders_overall']).round(2)
    unique_listing_ids_df_summary_asp_bucket_top_70['DW Overall %'] = (unique_listing_ids_df_summary_asp_bucket_top_70['DW Overall %'] * 100).round(2).astype(str) + '%'

    total_row_asp_bucket_top_70 = pd.Series({
        'asp_bucket': 'Total',
        'orders_SOS': input_df_asp_bucket_top_70['orders'].sum(),
        'opted_in_orders_SOS': input_df_asp_bucket_top_70[input_df_asp_bucket_top_70['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_FOS': non_input_df_asp_bucket_top_70['orders'].sum(),
        'opted_in_orders_FOS': non_input_df_asp_bucket_top_70[non_input_df_asp_bucket_top_70['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        # FIXED: Replaced df_1_edited with filtered_df
        'orders_overall': filtered_df['orders'].sum(),
        'opted_in_orders_overall': filtered_df[filtered_df['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_asp_bucket_top_70_df = pd.DataFrame([total_row_asp_bucket_top_70])

    total_row_asp_bucket_top_70_df['DW SOS %'] = np.where(
        total_row_asp_bucket_top_70_df['orders_SOS'] == 0, 0, 
        total_row_asp_bucket_top_70_df['opted_in_orders_SOS'] / total_row_asp_bucket_top_70_df['orders_SOS']
    ).round(2)

    total_row_asp_bucket_top_70_df['DW FOS %'] = np.where(
        total_row_asp_bucket_top_70_df['orders_FOS'] == 0, 0, 
        total_row_asp_bucket_top_70_df['opted_in_orders_FOS'] / total_row_asp_bucket_top_70_df['orders_FOS']
    ).round(2)

    total_row_asp_bucket_top_70_df['DW Overall %'] = np.where(
        total_row_asp_bucket_top_70_df['orders_overall'] == 0, 0, 
        total_row_asp_bucket_top_70_df['opted_in_orders_overall'] / total_row_asp_bucket_top_70_df['orders_overall']
    ).round(2)

    for col in ['DW SOS %', 'DW FOS %', 'DW Overall %']:
        total_row_asp_bucket_top_70_df[col] = (total_row_asp_bucket_top_70_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_asp_bucket_top_70_2 = pd.concat([total_row_asp_bucket_top_70_df, unique_listing_ids_df_summary_asp_bucket_top_70], ignore_index=True)
    unique_listing_ids_df_summary_asp_bucket_top_70_2 = unique_listing_ids_df_summary_asp_bucket_top_70_2.fillna(0)
    
    html_table_asp_bucket_top_70 = f'''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">asp_bucket</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders Overall</th> 
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS) %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS) %</th>
        </tr>
    </thead>
    <tbody>
'''

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_asp_bucket_top_70_2.iterrows():
        if 'total' in str(row['asp_bucket']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
        html_table_asp_bucket_top_70 += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['asp_bucket']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW SOS %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW FOS %']}</td>
            </tr>
        '''

    html_table_asp_bucket_top_70 += '</tbody></table>'
    
    # Add custom CSS for the table (optional styling like borders)
    css_style_asp_bucket_top_70 = """
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
    html_table_asp_bucket_top_70 = css_style_asp_bucket_top_70 + html_table_asp_bucket_top_70

#======================ASP=TOP=70%=========KAM===CUT==============================================
# Filter rows where top_70_flag == 1
#======================================TOp--70%---KAM--CUT==========================================
#=====================================asp_bucket======================================================
if df_optin.empty:
    print("Empty set error – df_optin has no data.")
else:

# asp_bucket ---

    # Input SET Summary
    filtered_df = df_1_edited[(df_1_edited['top_70_flag'] == 1) & (df_1_edited['owner'].astype(str).str.strip().str.upper() == 'KAM')].copy()

    input_df_asp_bucket_top_70_kam_cut = filtered_df[filtered_df['SOS / FOS Flag'] == 'SOS']
    summary_input_asp_bucket_top_70_kam_cut = input_df_asp_bucket_top_70_kam_cut.groupby('asp_bucket').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[input_df_asp_bucket_top_70_kam_cut.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_input_asp_bucket_top_70_kam_cut.columns = [col if col == 'asp_bucket' else col + '_SOS' for col in summary_input_asp_bucket_top_70_kam_cut.columns]

    # Non-Input SET Summary
    non_input_df_asp_bucket_top_70_kam_cut = filtered_df[filtered_df['SOS / FOS Flag'] == 'FOS']
    summary_non_input_asp_bucket_top_70_kam_cut = non_input_df_asp_bucket_top_70_kam_cut.groupby('asp_bucket').agg(
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[non_input_df_asp_bucket_top_70_kam_cut.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_non_input_asp_bucket_top_70_kam_cut.columns = [col if col == 'asp_bucket' else col + '_FOS' for col in summary_non_input_asp_bucket_top_70_kam_cut.columns]

    # Overall Summary
    summary_overall_asp_bucket_top_70_kam_cut = filtered_df.groupby('asp_bucket').agg(
        orders=('orders', 'sum'),
        # FIXED: Replaced df_1_edited with filtered_df
        opted_in_orders=('orders', lambda x: (x[filtered_df.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_overall_asp_bucket_top_70_kam_cut.columns = [col if col == 'asp_bucket' else col + '_overall' for col in summary_overall_asp_bucket_top_70_kam_cut.columns]

    # Final merge
    summary_final_asp_bucket_top_70_kam_cut = summary_input_asp_bucket_top_70_kam_cut.merge(summary_non_input_asp_bucket_top_70_kam_cut, on='asp_bucket', how='outer')
    summary_final_asp_bucket_top_70_kam_cut = summary_final_asp_bucket_top_70_kam_cut.merge(summary_overall_asp_bucket_top_70_kam_cut, on='asp_bucket', how='outer')

    unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut = summary_final_asp_bucket_top_70_kam_cut

    import numpy as np
    
    unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut['DW SOS %'] = np.where(unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut['orders_SOS']==0, 0, unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut['opted_in_orders_SOS']/unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut['orders_SOS']).round(2)
    unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut['DW SOS %'] = (unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut['DW SOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut['DW FOS %'] = np.where(unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut['orders_FOS']==0, 0, unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut['opted_in_orders_FOS']/unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut['orders_FOS']).round(2)
    unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut['DW FOS %'] = (unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut['DW FOS %'] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut['DW Overall %'] = np.where(unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut['orders_overall']==0, 0, unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut['opted_in_orders_overall']/unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut['orders_overall']).round(2)
    unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut['DW Overall %'] = (unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut['DW Overall %'] * 100).round(2).astype(str) + '%'

    total_row_asp_bucket_top_70_kam_cut = pd.Series({
        'asp_bucket': 'Total',
        'orders_SOS': input_df_asp_bucket_top_70_kam_cut['orders'].sum(),
        'opted_in_orders_SOS': input_df_asp_bucket_top_70_kam_cut[input_df_asp_bucket_top_70_kam_cut['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_FOS': non_input_df_asp_bucket_top_70_kam_cut['orders'].sum(),
        'opted_in_orders_FOS': non_input_df_asp_bucket_top_70_kam_cut[non_input_df_asp_bucket_top_70_kam_cut['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        # FIXED: Replaced df_1_edited with filtered_df
        'orders_overall': filtered_df['orders'].sum(),
        'opted_in_orders_overall': filtered_df[filtered_df['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_asp_bucket_top_70_kam_cut_df = pd.DataFrame([total_row_asp_bucket_top_70_kam_cut])

    total_row_asp_bucket_top_70_kam_cut_df['DW SOS %'] = np.where(
        total_row_asp_bucket_top_70_kam_cut_df['orders_SOS'] == 0, 0, 
        total_row_asp_bucket_top_70_kam_cut_df['opted_in_orders_SOS'] / total_row_asp_bucket_top_70_kam_cut_df['orders_SOS']
    ).round(2)

    total_row_asp_bucket_top_70_kam_cut_df['DW FOS %'] = np.where(
        total_row_asp_bucket_top_70_kam_cut_df['orders_FOS'] == 0, 0, 
        total_row_asp_bucket_top_70_kam_cut_df['opted_in_orders_FOS'] / total_row_asp_bucket_top_70_kam_cut_df['orders_FOS']
    ).round(2)

    total_row_asp_bucket_top_70_kam_cut_df['DW Overall %'] = np.where(
        total_row_asp_bucket_top_70_kam_cut_df['orders_overall'] == 0, 0, 
        total_row_asp_bucket_top_70_kam_cut_df['opted_in_orders_overall'] / total_row_asp_bucket_top_70_kam_cut_df['orders_overall']
    ).round(2)

    for col in ['DW SOS %', 'DW FOS %', 'DW Overall %']:
        total_row_asp_bucket_top_70_kam_cut_df[col] = (total_row_asp_bucket_top_70_kam_cut_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut_2 = pd.concat([total_row_asp_bucket_top_70_kam_cut_df, unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut], ignore_index=True)
    unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut_2 = unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut_2.fillna(0)
    
    html_table_asp_bucket_top_70_kam_cut = f'''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkred; color: white;">asp_bucket</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders Overall</th> 
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders Overall %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (SOS) %</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS)</th>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted-in Orders (FOS) %</th>
        </tr>
    </thead>
    <tbody>
'''

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_asp_bucket_top_70_kam_cut_2.iterrows():
        if 'total' in str(row['asp_bucket']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style
        html_table_asp_bucket_top_70_kam_cut += f'''
            <tr style="{row_style}">
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['asp_bucket']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_SOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW SOS %']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_FOS']}</td>
            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW FOS %']}</td>
            </tr>
        '''

    html_table_asp_bucket_top_70_kam_cut += '</tbody></table>'
    
    # Add custom CSS for the table (optional styling like borders)
    css_style_asp_bucket_top_70_kam_cut = """
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
    html_table_asp_bucket_top_70_kam_cut = css_style_asp_bucket_top_70_kam_cut + html_table_asp_bucket_top_70_kam_cut


    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.application import MIMEApplication
    from datetime import datetime as dt
    import os

    sender_email = 'thotat.vc@flipkart.com'
    sender_password = 'kiV1thai!@#%'
        
    link = "https://drive.google.com/drive/folders/1QX7_hfd7NiXgLAMU47t84laXB_jT5FiC?usp=drive_link"
    link_sheet = "https://docs.google.com/spreadsheets/d/1SJs-6Of5vLOMFDH3032ZkGncWhrzhYvNWguV_DdmaqU/edit?usp=sharing"

    # Original message metadata
    original_from = 'thotat.vc@flipkart.com'
    # original_to = ['thotat.vc@flipkart.com','h.kishandasmenon@flipkart.com']
    original_to = ['h.kishandasmenon@flipkart.com','shopsy_buy@flipkart.com','shopsy_buy_cr@flipkart.com','shopsysell@flipkart.com','shopsypricing@flipkart.com','nakul.gulati@flipkart.com','sriram.m@flipkart.com','shopsy_re_partner@flipkart.com']
    #original_to = ['thotat.vc@flipkart.com','vishnuac.vc@flipkart.com','shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com','nakul.gulati@flipkart.com','abhishek.patwari@flipkart.com','s.abhishek@flipkart.com','avijit.mohapatra@flipkart.com','bharambe.pradnya@flipkart.com']
    # 'upasanaupadhak.vc@flipkart.com','thotat.vc@flipkart.com','vishnuac.vc@flipkart.com','shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com'
    #'shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com']
    #'shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com']
    #'shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com'
    original_cc = ['thotat.vc@flipkart.com']
    #original_cc = ['thotat.vc@flipkart.com']
    original_message_id = '<CADLy_NGfFOiT_PYm4XjX_-_NH+Yi=SuSM1n3315TxmAQ1mc+dA@mail.gmail.com>'

    all_recipients = set([original_from] + original_to + original_cc)

    from datetime import datetime
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(original_to)
    msg['Cc'] = ', '.join(original_cc)
    msg['Subject'] = f"Shopsy_Extra 15% (50/50 Sharing) Optin Report"
    msg['In-Reply-To'] = original_message_id
    msg['References'] = original_message_id

    body = f"""
    Dear Team,<br><br>
    This tracker consolidates opt-in status for the period April 1 to April 12, based solely on orders.<br><br>
    Key points for calculation and review:<br><br>
    Orders are taken directly.<br>
    Contributions are tracked at the new co-funded levels:<br>
    • FOS level<br>
    • SOS level<br>
    Top 70% contribution is highlighted and factored into the consolidated view.<br><br>
    For any clarifications regarding the calculation methodology, please feel free to reach out.<br><br>
    Please find the status as of {current_time} <br><br>
    Please find the Owner Level Summary:<br><br>
    {html_table_owner}<br><br>
    Please find the BU Level Summary:<br><br>
    {html_table_sell_bu}<br><br>
    Please find the BU(KAM) Level Summary:<br><br>
    {html_table_sell_bu_kam_cut}<br><br>
    Please find the ASP Bucket Level Summary:<br><br>
    {html_table_asp_bucket_over_all}<br><br>  
    Please find the ASP Bucket(KAM) Level Summary:<br><br>
    {html_table_asp_bucket_kam_cut}<br><br>                
    Please find the KAM Level Summary:<br><br>
    {html_table_KAM_}<br><br>
    Please find the Owner(Top 70%) Level Summary:<br><br>
    {html_table_owner_top_70}<br><br>
    Please find the BU(Top 70%) Level Summary:<br><br>
    {html_table_sell_bu_top_70}<br><br>
    Please find the BU(KAM - Top 70%) Level Summary:<br><br>
    {html_table_sell_bu_top_70_kam_cut}<br><br>
    Please find the ASP Bucket(TOP 70%) Level Summary:<br><br>
    {html_table_asp_bucket_top_70}<br><br>
    Please find the ASP Bucket(KAM TOP 70%) Level Summary:<br><br>
    {html_table_asp_bucket_top_70_kam_cut}
    Please find the KAM(Top 70%) Level Summary:<br><br>
    {html_table_sell_top_70_kam}<br><br>
    Please find the Analytic_category Level Summary:<br><br>
    {html_table_analytic_category_cut}<br><br>
    Please find the Analytic_category(KAM) Level Summary:<br><br>
    {html_table_analytic_category_kam_cut}<br><br>
    The attached drive link contains the raw and summary data for your reference: {link}<br><br>
    The attached drive link contains the all offer_ids consider in the report for your reference: {link_sheet}<br><br>
    In the drive file name mentioned as (Cofunded_Optin_Report_SOS_FOS_Fifteen_Percent_)<br><br>
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


    from datetime import datetime as dt2
    today=dt2.now().strftime('%d_%b_%Y')

    df_1_edited.to_csv(f"/home/thotat.vc/Downloads/Cofunded_Optin_Report_SOS_FOS_Fifteen_Percent_{today}.csv", index=False)

    df_1_edited[df_1_edited['owner']=='KAM'].to_csv(f"/home/thotat.vc/Downloads/Cofunded_Optin_Report_SOS_FOS_Fifteen_Percent_{today}_KAM.csv")
    df_1_edited[df_1_edited['owner']=='RE'].to_csv(f"/home/thotat.vc/Downloads/Cofunded_Optin_Report_SOS_FOS_Fifteen_Percent_{today}_RE.csv")
    df_1_edited[df_1_edited['owner']=='UM'].to_csv(f"/home/thotat.vc/Downloads/Cofunded_Optin_Report_SOS_FOS_Fifteen_Percent_{today}_UM.csv")


        # with pd.ExcelWriter(f"/home/thotat.vc/Downloads/Cofunded_Report_Fifteen_Percent_{today}.xlsx") as writer:
        #     unique_listing_ids_df_summary_supercat_2.to_excel(writer, sheet_name='Summary Supercategory',index=False)
        #     df_1_edited.to_excel(writer, sheet_name='Raw',index=False)
        #     unique_listing_ids_df_summary_2.to_excel(writer, sheet_name='Summary',index=False)

    filename =f"Cofunded_Optin_Report_SOS_FOS_Fifteen_Percent_{today}.csv"
    file_path =f"/home/thotat.vc/Downloads/Cofunded_Optin_Report_SOS_FOS_Fifteen_Percent_{today}.csv" 
    file_path_kam=f"/home/thotat.vc/Downloads/Cofunded_Optin_Report_SOS_FOS_Fifteen_Percent_{today}_KAM.csv"
    file_path_re=f"/home/thotat.vc/Downloads/Cofunded_Optin_Report_SOS_FOS_Fifteen_Percent_{today}_RE.csv"
    file_path_um=f"/home/thotat.vc/Downloads/Cofunded_Optin_Report_SOS_FOS_Fifteen_Percent_{today}_UM.csv"

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
        folder_id = '1QX7_hfd7NiXgLAMU47t84laXB_jT5FiC'  # Replace with actual folder ID
        file_path = f"/home/thotat.vc/Downloads/Cofunded_Optin_Report_SOS_FOS_Fifteen_Percent_{today}.csv"
        file_path_kam=f"/home/thotat.vc/Downloads/Cofunded_Optin_Report_SOS_FOS_Fifteen_Percent_{today}_KAM.csv"
        file_path_re=f"/home/thotat.vc/Downloads/Cofunded_Optin_Report_SOS_FOS_Fifteen_Percent_{today}_RE.csv"
        file_path_um=f"/home/thotat.vc/Downloads/Cofunded_Optin_Report_SOS_FOS_Fifteen_Percent_{today}_UM.csv" 

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
