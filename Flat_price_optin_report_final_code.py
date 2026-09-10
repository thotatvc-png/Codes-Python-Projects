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

# conn = jaydebeapi.connect("org.apache.hive.jdbc.HiveDriver", url,{'user': "thotat.vc", 'password': "kiV1thai!@#%"})

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
# fdp_uploads.ds_fkint_2gud_partner_shopsy_optin_1_0 a
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
df_listing=pd.read_csv(f'/home/thotat.vc/analytics_scripts/Hourly_Fact_Raw_FIle.csv') # Run this when code fails so that you pickup the last query output
df_listing=df_listing.rename(columns={"b.analytic_category":"analytic_category"})


# import gspread
# from oauth2client.service_account import ServiceAccountCredentials
# from gspread_dataframe import set_with_dataframe


# scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
# creds = ServiceAccountCredentials.from_json_keyfile_name('/home/thotat.vc/analytics_scripts/creds.json', scope)
# client = gspread.authorize(creds)

# wks = client.open("All Offer - Shopsy GSM").sheet1
# ws = client.open("All Offer - Shopsy GSM").worksheet("Offer Details")

# data = ws.get_all_records()
# df = pd.DataFrame(data)
# print(df)

# df.to_csv('/home/thotat.vc/Downloads/consolidated_GSM_All_Offers.csv')


# csv_path = r'/home/thotat.vc/Downloads/Consolidated_Offers.csv'

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe


# scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
# creds = ServiceAccountCredentials.from_json_keyfile_name('/home/thotat.vc/analytics_scripts/creds.json', scope)
# client = gspread.authorize(creds)

# wks = client.open("KAM Mapping - Cleanup - Consolidated Optins").sheet1
# ws = client.open("KAM Mapping - Cleanup - Consolidated Optins").worksheet("Final DF")

# data = ws.get_all_records()
# df_lead = pd.DataFrame(data)
# print(df_lead)
# df_lead.to_csv('/home/thotat.vc/Downloads/KAM_Leads_Mapping.csv')



# # Read the CSV
# import pandas as pd

# # # Load the CSV and prepare offer_ids list
# df = pd.read_csv(r'/home/thotat.vc/Downloads/consolidated_GSM_All_Offers.csv', encoding='cp1252')
# offer_ids = df['Offer IDs'].dropna().unique().tolist()
# offer_ids_cleaned = [f"'{str(oid)}'" for oid in offer_ids]
# offer_ids_clause = ', '.join(offer_ids_cleaned)

# # # Inject offer_ids_clause directly into the SQL string
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
#         WHERE day > lookup_date(date_sub(CURRENT_DATE, 60))
#           AND data.offerid IN ({offer_ids_clause})
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
df_optin=pd.read_csv(f'/home/thotat.vc/analytics_scripts/Hourly_offers_Raw_FIle.csv') # Run this when code fails so that you pickup the last query output



if df_optin.empty:
    print("Empty set error – df_optin has no data beyond headers.")

else:
    # Read Input File - Which is L3M Transacting

    # PROCESS BASE

    import pandas as pd
    file_path_1 = '/home/thotat.vc/analytics_scripts/Flat_Price_Offer_Deals_Optin_Views_5th_Sep_2025.csv'
    # df_1 = pd.read_csv(file_path_1, encoding='cp1252')
    df_1 = pd.read_csv(file_path_1, encoding='ISO-8859-1')
    df_1 = df_1.drop_duplicates()
    print(df_1.columns)

    # Equate to today for uniqueness of files
    from datetime import datetime as dt2
    today=dt2.now().strftime('%d_%b_%Y')

    # Temp assignment
    df_1_1=df_1
    #df_1_1['LID'] = df_1_1['LID'].astype(str).str.strip()
    df_1_1['listing_id'] = df_1_1['listing_id'].astype(str).str.strip()
    df_1_1['Seller Id'] = df_1_1['Seller Id'].astype(str).str.strip()
    #df_1_1['Offer ID'] = df_1_1['Offer ID'].astype(str).str.strip()
    df_1_1['Correct cms_vertical'] = df_1_1['Correct cms_vertical'].astype(str).str.strip()
    # df_1['KAM']='' # Comment if KAM is blank
    df_1_1['KAM'] = df_1_1['KAM'].str.upper()


    # Read from listing_status and remove inactive LIDs from the base by comparing it with current listing_status

    removal_df_1 = df_listing.drop_duplicates()
    removal_df_1 = removal_df_1[removal_df_1['listing_status'] == 'ACTIVE']
    removal_df_1['listing_id'] = removal_df_1['listing_id'].astype(str).str.strip()
    removal_ids_1 = set(removal_df_1['listing_id'])


    # df_1_1['listing_status'] = df_1_1['listing_id'].apply(lambda x: 'ACTIVE' if x in removal_ids_1 else 'INACTIVE')
    # Ensure 'listing_id' is in the list of selected columns
    
    cols=['listing_id','listing_status']
    df_listing_1=df_listing[cols]
    print(df_listing.columns.tolist())
    print(df_listing_1.columns.tolist())

    df_1_1=df_1_1.merge(df_listing_1, left_on='listing_id', right_on='listing_id',how='left')
    df_1_1['listing_status'] = df_1_1['listing_status'].astype(str).str.strip()
    df_1_1['analytic_super_category_l2'] = df_1_1['analytic_super_category_l2'].astype(str).str.strip()
    df_1_1['analytic_category'] = df_1_1['analytic_category'].astype(str).str.strip()
    df_1_1 = df_1_1[df_1_1['listing_status'] == 'ACTIVE']

    ## Read Input - which is current offer universe

    #df_universe=pd.read_csv('/home/thotat.vc/analytics_scripts/updated_Consolidated_Cleaned_Output_Without_Offers_.csv',encoding='ISO-8859-1')
    
    # df_universe['LID'] = df_universe['LID'].astype(str).str.strip()
    # df_universe['Seller Id'] = df_universe['Seller Id'].astype(str).str.strip()
    # #df_1_1['Offer ID'] = df_1_1['Offer ID'].astype(str).str.strip()
    # df_universe['Correct cms_vertical'] = df_universe['Correct cms_vertical'].astype(str).str.strip()
    # # df_1['KAM']='' # Comment if KAM is blank
    # df_universe['KAM'] = df_universe['KAM'].str.upper()
    # # df_universe['KAM'] = df_universe['KAM'].apply(lambda x: 'GEETASHRI' if str(x).strip().lower() == 'GEETASHREE' else x)
    # df_universe['LID'] = df_universe['LID'].astype(str).str.strip()
    #input_lids = set(df_universe['LID'])

    #df_1_1['Set'] = df_1_1['listing_id'].apply(lambda x: 'INPUT SET' if x in input_lids else 'NON-INPUT SET')
    
    #df_final_mapping=pd.read_csv('/home/thotat.vc/Downloads/Input KAM Mapping.csv',encoding='cp1252')
    # df_1_1['listing_id']=df_1_1['listing_id'].astype(str).str.strip()
    # #df_final_mapping['listing_id']=df_final_mapping['listing_id'].astype(str).str.strip()
    # df_1_1=df_1_1.merge(df_final_mapping, on=['listing_id'],how='left')
    # df_1_1['KAM Name - Based On input'] = df_1_1['KAM Name - Based On input'].fillna(df_1_1['owner'])
    # df_1_1['Buy Leads'] = df_1_1['Buy Leads'].fillna(df_1_1['owner'])
    # df_1_1=df_1_1.rename(columns={'KAM':'Primary KAM'})
    # df_1_1=df_1_1.rename(columns={'KAM Name - Based On input':'KAM'})
    # df_1_1=df_1_1.rename(columns={'Buy Leads':'Lead'})
    # df_1_1=df_1_1.merge(df_listing_1, left_on='listing_id', right_on='listing_id',how='left')

    # Replace where input KAM is null with current primary KAM 
    # cols=['LID','KAM']
    # df_universe_1=df_universe[cols]
    # df_universe_1=df_universe_1.rename(columns={'KAM':'Input KAM'}) # Current Running Offers
    # df_1_1=df_1_1.rename(columns={'KAM':'Primary KAM'}) # L3M Transacting
    # df_1_1=df_1_1.merge(df_universe_1, left_on=['listing_id'], right_on=['LID'], how='left')
    # df_1_1['KAM']=df_1_1['Input KAM'].fillna(df_1_1['Primary KAM'])
    # df_1_1['Primary owner']=df_1_1['owner']
    # # df_1_1['owner'] = df_1_1['owner'].apply(lambda x: 'RE' if x in {'RE', 'RE - Arafat'} else x)
    # # df_1_1['owner'] = df_1_1['owner'].apply(lambda x: 'RE' if x.strip() in {'RE', 'RE - Arafat'} else 'UM' if x.strip() in {'UM'} else x)# Add any other UM variants here
    # df_1_1['owner'] = df_1_1['KAM'].apply(lambda x: 'RE' if x.strip() in {'RE', 'RE - Arafat'} else 'UM' if x.strip() == 'UM' else 'KAM')
    # df_1_1['KAM'] = df_1_1['KAM'].apply(lambda x: 'GEETASHRI' if str(x).strip().lower() == 'GEETASHREE' else x)
    # df_lead['KAM']=df_lead['KAM'].str.strip()
    # df_1_1['KAM']=df_1_1['KAM'].str.strip()
    # df_1_1=df_1_1.merge(df_lead, on=['KAM'], how='left')
    # df_1_1['Lead']=df_1_1['Lead'].fillna('No Lead')



    # # Exclude LIDs/FSNs etc

    #excluded_fsns=['SJXGSQXMM6HEZPDW','SJXH2HK53HNN3JV4','SJXH93F6WGHEYUPW','SJXGSU4XXJZUCPNU','SJXGSQX4RGR8SQ6W','SJXH47FERTU8NVGU','SJXH58KJPHPNE2KF','SJXH78MGKESCUVGU','SJXH94D7YPVVJFHX','SJXH94CXPYHPYYHT','SJXH8UN7JNSQZYAF','SJXH2YZXTYJZ8ANK','SJXGSRXY5RGM2N7F','SJXHY57UW2BXAYXZ','SJXH59XDSQRRXHGC','SJXH5AZZ375FHEKH','SJXGYSKFYQHVSNAH','SJXGSU4XGFH9B7DD','SJXH6M7HH4AWRBRX','SJXGYBMECYHRG25C','SJXH6M7HGJFDUAXH','SJXH6M7HUBH8KMPN','SJXH6M7HHDHZHEBE','SJXH2E5RK5CYBUFG','SJXHY9EZVPPAEMHW','SJXH4EME7SHNGBRV','SJXH58MGCZJMYGAU','SJXH8NY4SVC4NKNP','SJXH8Z32FJKANDNF','SJXH8349KUE7AUPZ','SJXH4EMGZPRYFQXQ','SJXH8TBPZKX6EWNT','SJXH5H3BKZ7THGNP','SJXH7A5ZXD7KHSWG','SJXGSU4RHXABT7RM','SJXH6NAWRDUZDCNK']
    #df_1_1=df_1_1[~df_1_1['FSN'].isin(excluded_fsns)]
    #df_1_1 = df_1_1.drop_duplicates()
    

    #df_1_1 = df_1_1[~df_1_1.apply(lambda row: (row['Seller Id'], row['Correct cms_vertical']) in exclude_set, axis=1)]

     #Exclude specific seller IDs
    #excluded_seller_ids = ['ed1096fdb7bf4257', '0be32a51153c4721']
    # df_1_1 = df_1_1[~df_1_1['Seller Id'].str.strip().isin(excluded_seller_ids)]
    # Return the optin report
    #df_1_1['KAM'] = df_1_1['KAM'].apply(lambda x: 'PRATIK NAG' if str(x).strip().lower() == 'PRATIK' else x)
    # df_1_1['KAM'] = df_1_1['KAM'].astype(str).str.strip().str.upper()
    # df_1_1['KAM'] = df_1_1['KAM'].apply(lambda x: 'PULKIT' if str(x).strip().lower() == 'pulkit' else x)
    
    df_optin=df_optin
    df_optin=df_optin.drop_duplicates()
    df_optin
    df_optin['listing_id'] = df_optin['listing_id'].astype(str).str.strip()
    df_optin['offer_id'] = df_optin['offer_id'].astype(str).str.strip()
    df_optin['seller_id'] = df_optin['seller_id'].astype(str).str.strip()

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
        lambda row: 'Opted-in' if (row['Seller Id'], row['Correct cms_vertical']) in optin_combinations else 'Not opted-in', 
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

    df_1_raw['order_drr']=(df_1_raw['orders'].fillna(0))/90
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
    #df_1_edited_kam=df_1_edited[df_1_edited['owner']=='KAM']

    

    df_1_edited_pp_cut = df_1_edited.groupby('ASP Bucket - Hourly').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum()),
    listing_id=('listing_id', 'nunique')
    ).reset_index()

    df_1_edited_pp_cut['DW %']=np.where(df_1_edited_pp_cut['orders']==0, 0, df_1_edited_pp_cut['opted_in_orders']/df_1_edited_pp_cut['orders']).round(2)
    df_1_edited_pp_cut['DW %'] = (df_1_edited_pp_cut['DW %'] * 100).round(2).astype(str) + '%'
    
    total_row_pp_cut = pd.Series({
        'ASP Bucket - Hourly': 'Total',
        'listing_id': df_1_edited['listing_id'].nunique(),
        'orders': df_1_edited['orders'].sum(),
        'opted_in_orders': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_pp_cut_df = pd.DataFrame([total_row_pp_cut])

    total_pp_orders=total_row_pp_cut_df['orders'].sum()

    total_row_pp_cut_df['DW %']=np.where(total_row_pp_cut_df['orders']==0, 0, total_row_pp_cut_df['opted_in_orders']/total_row_pp_cut_df['orders']).round(2)
    total_row_pp_cut_df['DW %'] = (total_row_pp_cut_df['DW %'] * 100).round(2).astype(str) + '%'

    df_1_edited_pp_cut_summary = pd.concat([total_row_pp_cut_df, df_1_edited_pp_cut], ignore_index=True)

    df_1_edited_pp_cut_summary['PP DW %']=np.where(total_pp_orders==0, 0, df_1_edited_pp_cut_summary['orders']/total_pp_orders).round(2)
    df_1_edited_pp_cut_summary['PP DW %'] = (df_1_edited_pp_cut_summary['PP DW %'] * 100).round(2).astype(str) + '%'

    print(df_1_edited_pp_cut_summary)

    #--Super_Category_cut

    df_1_edited_sc_cut = df_1_edited.groupby('analytic_super_category_l2').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum()),
    listing_id=('listing_id', 'nunique')
    ).reset_index()

    df_1_edited_sc_cut['DW %']=np.where(df_1_edited_sc_cut['orders']==0, 0, df_1_edited_sc_cut['opted_in_orders']/df_1_edited_sc_cut['orders']).round(2)
    df_1_edited_sc_cut['DW %'] = (df_1_edited_sc_cut['DW %'] * 100).round(2).astype(str) + '%'
    
    total_row_sc_cut = pd.Series({
        'analytic_super_category_l2': 'Total',
        'listing_id': df_1_edited['listing_id'].nunique(),
        'orders': df_1_edited['orders'].sum(),
        'opted_in_orders': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_sc_cut_df = pd.DataFrame([total_row_sc_cut])

    total_sc_orders=total_row_sc_cut_df['orders'].sum()

    total_row_sc_cut_df['DW %']=np.where(total_row_sc_cut_df['orders']==0, 0, total_row_sc_cut_df['opted_in_orders']/total_row_sc_cut_df['orders']).round(2)
    total_row_sc_cut_df['DW %'] = (total_row_sc_cut_df['DW %'] * 100).round(2).astype(str) + '%'

    df_1_edited_sc_cut_summary = pd.concat([total_row_sc_cut_df, df_1_edited_sc_cut], ignore_index=True)

    df_1_edited_sc_cut_summary['PP DW %']=np.where(total_sc_orders==0, 0, df_1_edited_sc_cut_summary['orders']/total_sc_orders).round(2)
    df_1_edited_sc_cut_summary['PP DW %'] = (df_1_edited_sc_cut_summary['PP DW %'] * 100).round(2).astype(str) + '%'
    print(df_1_edited_sc_cut_summary)
    
#--Lead-CUT
   
    df_1_edited_lead_cut = df_1_edited.groupby('Buy Leads').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum()),
    listing_id=('listing_id', 'nunique')
    ).reset_index()

    df_1_edited_lead_cut['DW %']=np.where(df_1_edited_lead_cut['orders']==0, 0, df_1_edited_lead_cut['opted_in_orders']/df_1_edited_lead_cut['orders']).round(2)
    df_1_edited_lead_cut['DW %'] = (df_1_edited_lead_cut['DW %'] * 100).round(2).astype(str) + '%'
    
    total_row_lead_cut = pd.Series({
        'Buy Leads': 'Total',
        'listing_id': df_1_edited['listing_id'].nunique(),
        'orders': df_1_edited['orders'].sum(),
        'opted_in_orders': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_lead_cut_df = pd.DataFrame([total_row_lead_cut])

    total_lead_orders=total_row_lead_cut_df['orders'].sum()

    total_row_lead_cut_df['DW %']=np.where(total_row_lead_cut_df['orders']==0, 0, total_row_lead_cut_df['opted_in_orders']/total_row_lead_cut_df['orders']).round(2)
    total_row_lead_cut_df['DW %'] = (total_row_lead_cut_df['DW %'] * 100).round(2).astype(str) + '%'

    df_1_edited_lead_cut_summary = pd.concat([total_row_lead_cut_df, df_1_edited_lead_cut], ignore_index=True)

    df_1_edited_lead_cut_summary['PP DW %']=np.where(total_lead_orders==0, 0, df_1_edited_lead_cut_summary['orders']/total_lead_orders).round(2)
    df_1_edited_lead_cut_summary['PP DW %'] = (df_1_edited_lead_cut_summary['PP DW %'] * 100).round(2).astype(str) + '%'
    print(df_1_edited_lead_cut_summary)


    #---Owner---Cut
    df_1_edited_owner_cut = df_1_edited.groupby('owner').agg(
    orders=('orders', 'sum'),
    opted_in_orders=('orders', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum()),
    listing_id=('listing_id', 'nunique')
    ).reset_index()

    df_1_edited_owner_cut['DW %']=np.where(df_1_edited_owner_cut['orders']==0, 0, df_1_edited_owner_cut['opted_in_orders']/df_1_edited_owner_cut['orders']).round(2)
    df_1_edited_owner_cut['DW %'] = (df_1_edited_owner_cut['DW %'] * 100).round(2).astype(str) + '%'
    
    total_row_owner_cut = pd.Series({
        'owner': 'Total',
        'listing_id': df_1_edited['listing_id'].nunique(),
        'orders': df_1_edited['orders'].sum(),
        'opted_in_orders': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_owner_cut_df = pd.DataFrame([total_row_owner_cut])

    total_owner_orders=total_row_owner_cut_df['orders'].sum()

    total_row_owner_cut_df['DW %']=np.where(total_row_owner_cut_df['orders']==0, 0, total_row_owner_cut_df['opted_in_orders']/total_row_owner_cut_df['orders']).round(2)
    total_row_owner_cut_df['DW %'] = (total_row_owner_cut_df['DW %'] * 100).round(2).astype(str) + '%'

    df_1_edited_owner_cut_summary = pd.concat([total_row_owner_cut_df, df_1_edited_owner_cut], ignore_index=True)

    df_1_edited_owner_cut_summary['PP DW %']=np.where(total_owner_orders==0, 0, df_1_edited_owner_cut_summary['orders']/total_owner_orders).round(2)
    df_1_edited_owner_cut_summary['PP DW %'] = (df_1_edited_owner_cut_summary['PP DW %'] * 100).round(2).astype(str) + '%'
    print(df_1_edited_owner_cut_summary)


    with pd.ExcelWriter(f"/home/thotat.vc/Downloads/Flat Price Offer Deals - Optin Views {today}.xlsx") as writer:
        df_1_edited_sc_cut_summary.to_excel(writer, sheet_name='Summary Supercategory',index=False)
        df_1_edited.to_excel(writer, sheet_name='Raw',index=False)
        df_1_edited_pp_cut_summary.to_excel(writer, sheet_name='Asp-bucket',index=False)
        df_1_edited_lead_cut_summary.to_excel(writer, sheet_name='Lead-Level',index=False)
        df_1_edited_owner_cut_summary.to_excel(writer, sheet_name='Owner-level',index=False)

    
    filename = f"Flat Price Offer Deals - Optin Views {today}.xlsx"
    file_path = f"/home/thotat.vc/Downloads/Flat Price Offer Deals - Optin Views {today}.xlsx" 


    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name('/home/thotat.vc/analytics_scripts/creds.json', scope)
    # client = gspread.authorize(creds)
    http = creds.authorize(httplib2.Http(timeout=600))
    drive_service = build('drive', 'v3', http=http)


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
    def upload_file_to_shared_drive_folder(file_path, folder_id):
        """Upload a file to a folder inside a shared drive."""
        file_metadata = {
            'name': os.path.basename(file_path),
            'parents': [folder_id]
        }
        media = MediaFileUpload(file_path, mimetype='application/octet-stream')

        file = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id',
            supportsAllDrives=True
        ).execute()

        print(f"File uploaded to folder ID: {folder_id} with file ID: {file['id']}.")
        return file['id']

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
        folder_id = '1PA_aQ7ZaZy1lz9HFwfODDQa9WVr3vm3p'  # Replace with actual folder ID
        file_path = f"/home/thotat.vc/Downloads/Flat Price Offer Deals - Optin Views {today}.xlsx"
    # Replace with actual file path

        upload_to_shared_drive_folder(drive_id, folder_id,file_path) #file_path
        link = "https://drive.google.com/drive/folders/1W8aP5YP4ViJgUV1iZbPStaWmrOnkhs9r"

#---HTML-CODE--for--Price_cut
    html_table_pp_cut_summary = '''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">ASP-Bucket-hourly</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;"># listing_id</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted In Orders</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">DW %</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Platform Price DW %</th>
        </tr>
    </thead>
    <tbody>
'''

    html_table_pp_cut_summary += '<tbody>'

    for index, row in df_1_edited_pp_cut_summary.iterrows():
        # Check if 'analytic_super_category_l2' contains 'total' (case insensitive)
        if 'total' in str(row['ASP Bucket - Hourly']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style

        html_table_pp_cut_summary += f'''
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['ASP Bucket - Hourly']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['listing_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['orders']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['opted_in_orders']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['DW %']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['PP DW %']}</td>                                                                     
            </tr>
        '''

    html_table_pp_cut_summary += '</tbody></table>'

    # Add custom CSS for the table (optional styling like borders)
    css_style_pp_cut_summary = """
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
    html_table_pp_cut_summary = css_style_pp_cut_summary + html_table_pp_cut_summary


#--HTML--CODE__Super_cat_Cut

    html_table_super_category_cut_summary = '''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Analytic_super_category</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;"># listing_id</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted In Orders</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">DW %</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Platform Price DW %</th>
        </tr>
    </thead>
    <tbody>
'''

    html_table_super_category_cut_summary += '<tbody>'

    for index, row in df_1_edited_sc_cut_summary.iterrows():
        # Check if 'analytic_super_category_l2' contains 'total' (case insensitive)
        if 'total' in str(row['analytic_super_category_l2']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style

        html_table_super_category_cut_summary += f'''
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['analytic_super_category_l2']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['listing_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['orders']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['opted_in_orders']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['DW %']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['PP DW %']}</td>                                                                     
            </tr>
        '''

    html_table_super_category_cut_summary += '</tbody></table>'

    # Add custom CSS for the table (optional styling like borders)
    css_style_super_category_cut_summary = """
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
    html_table_super_category_cut_summary = css_style_super_category_cut_summary + html_table_super_category_cut_summary


#--Lead--CUt

    html_table_lead_cut_summary = '''
<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
    <thead>
        <tr>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Lead</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;"># listing_id</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted In Orders</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">DW %</th>
            <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Platform Price DW %</th>
        </tr>
    </thead>
    <tbody>
'''

    html_table_lead_cut_summary += '<tbody>'

    for index, row in df_1_edited_lead_cut_summary.iterrows():
        # Check if 'analytic_super_category_l2' contains 'total' (case insensitive)
        if 'total' in str(row['Buy Leads']).lower():
            row_style = 'background-color: lightskyblue; font-weight: bold;'
        else:
            row_style = ''  # no special style

        html_table_lead_cut_summary += f'''
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['Buy Leads']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['listing_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['orders']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['opted_in_orders']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['DW %']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['PP DW %']}</td>                                                                     
            </tr>
        '''

    html_table_lead_cut_summary += '</tbody></table>'

    # Add custom CSS for the table (optional styling like borders)
    css_style_lead_cut_summary = """
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
    html_table_lead_cut_summary = css_style_lead_cut_summary + html_table_lead_cut_summary

#-----Owner--------Cut

    html_table_owner_cut_summary = '''
    <table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
        <thead>
            <tr>
                <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Owner</th>
                <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;"> # listing_id</th>
                <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Orders</th>
                <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opted In Orders</th>
                <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">DW %</th>
                <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Platform Price DW %</th>
            </tr>
        </thead>
        <tbody>
    '''

    html_table_owner_cut_summary += '<tbody>'

    for index, row in df_1_edited_owner_cut_summary.iterrows():
            # Check if 'analytic_super_category_l2' contains 'total' (case insensitive)
            if 'total' in str(row['owner']).lower():
                row_style = 'background-color: lightskyblue; font-weight: bold;'
            else:
                row_style = ''  # no special style

            html_table_owner_cut_summary += f'''
                <tr>
                    <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['owner']}</td>
                    <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['listing_id']}</td>
                    <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['orders']}</td>
                    <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['opted_in_orders']}</td>
                    <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['DW %']}</td>
                    <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['PP DW %']}</td>                                                                     
                </tr>
            '''

    html_table_owner_cut_summary += '</tbody></table>'

        # Add custom CSS for the table (optional styling like borders)
    css_style_owner_cut_summary = """
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
    html_table_owner_cut_summary = css_style_owner_cut_summary + html_table_owner_cut_summary




#-----MAIL--code
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
    original_to=['thotat.vc@flipkart.com']
    # original_to = ['upasanaupadhak.vc@flipkart.com','h.kishandasmenon@flipkart.com','vishnuac.vc@flipkart.com','shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com']
    # 'upasanaupadhak.vc@flipkart.com','h.kishandasmenon@flipkart.com','vishnuac.vc@flipkart.com','shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com'
    #'shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com']
    #'shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com']
    #'shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com'
    original_cc = ['thotat.vc@flipkart.com']
    original_message_id = '<CACZpucyYQ5HWigruynUtA7nesd9tucPTPLQSqhe_E1y=EVpUuQ@mail.gmail.com>'

    all_recipients = set([original_from] + original_to + original_cc)

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(original_to)
    msg['Cc'] = ', '.join(original_cc)
    msg['Subject'] = f"FLAT PRICE OPT-IN REPORT (DW bw 29th Aug to 3rd Sep)"
    msg['In-Reply-To'] = original_message_id
    msg['References'] = original_message_id

    body = f"""
    Hi Team,<br><br>
    Please find the Owner summary for the same:<br><br>
    {html_table_owner_cut_summary}
    Please find the Price Point summary for the same:<br><br>
    {html_table_pp_cut_summary}<br><br>
    Please find the Supercategory summary for the same:<br><br>
    {html_table_super_category_cut_summary}
    Please find the Lead summary for the same:<br><br>
    {html_table_lead_cut_summary}
    The attached drive file contains the raw and summary data for your reference: {link}.<br><br>
    In drive file_name is date<br><br>
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

    



    
