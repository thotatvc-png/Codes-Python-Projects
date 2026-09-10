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
sql1='''
select
a.listing_id as listing_id,
b.listing_status as listing_status,
c.display_name as display_name,
CASE
  WHEN d.analytic_super_category in ('ShopsyLifeStyle') and lower(d.analytic_category) IN ('na', 'shopsykidclothing','shopsymensclothingcasualtopwear','shopsymensclothingessentialsandethnic','shopsymensclothingjeans','shopsymensclothingsmartandformals','shopsywomenethniccontemporary','shopsywomenethniccore','shopsywomenwesterncore') THEN 'ShopsyLifeStyle Apparel'
  WHEN d.analytic_super_category in ('ShopsyLifeStyle') and lower(d.analytic_category) not IN ('na', 'shopsykidclothing','shopsymensclothingcasualtopwear','shopsymensclothingessentialsandethnic','shopsymensclothingjeans','shopsymensclothingsmartandformals','shopsywomenethniccontemporary','shopsywomenethniccore','shopsywomenwesterncore') THEN 'ShopsyLifeStyle Non-Apparel'
  ELSE d.analytic_super_category END AS analytic_super_category_l2
from
fdp_uploads.ds_fkint_2gud_partner_vishnu_optin_3_lids_1_0 a
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

# from datetime import datetime as dt2
# today=dt2.now().strftime('%d_%b_%Y')
# # df_listing.to_csv(f'/home/thotat.vc/analytics_scripts/listing_raw_{today}.csv') 
# df_listing=pd.read_csv(f'/home/thotat.vc/analytics_scripts/listing_raw_{today}.csv') # Run this when code fails so that you pickup the last query output


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
    and data.offerid in ('nb:mp:052c462719', 'nb:mp:0521e1c719', 'nb:mp:05adf19619', 'nb:mp:05e9555d24', 'nb:mp:0545700221', 'nb:mp:050cdd7825', 'nb:mp:0515db6d25', 'nb:mp:0537af9725', 'nb:mp:0598873d24', 'nb:mp:050ba37325', 'nb:mp:05d0aedf25', 'nb:mp:05fc1f3324', 'nb:mp:0544c98a25', 'nb:mp:053048d725', 'nb:mp:059eef3e24', 'nb:mp:0560c7d624', 'nb:mp:05148fa025', 'Manual Pricing', 'nb:mp:059ab9aa24', 'nb:mp:05619bc925', 'nb:mp:054e47e121', 'nb:mp:0540c5ee24', 'nb:mp:05103f3e24', 'nb:mp:052cdc1b25', 'nb:mp:05b2230619', 'nb:mp:05711eea19', 'nb:mp:057ecb0921', 'nb:mp:042b7c5729', 'nb:mp:054282f224', 'nb:mp:05594c7a25', 'nb:mp:0500653025', 'nb:mp:053c747319', 'nb:mp:057074fb24', 'nb:mp:05e619d224', 'nb:mp:0567b12d24', 'nb:mp:050b73f525', 'nb:mp:055a18b925', 'nb:mp:059a477019', 'nb:mp:050df48721', 'nb:mp:05cde2db25', 'nb:mp:05d99e1925', 'nb:mp:05d4d72425', 'nb:mp:057e411419', 'nb:mp:0537b23a25', 'nb:mp:05d73eb327', 'nb:mp:05181f5424', 'nb:mp:055277af25', 'nb:mp:05c0399624', 'nb:mp:0507665c25', 'nb:mp:05e2c09725', 'nb:mp:059fd54a25', 'nb:mp:05ae925721', 'nb:mp:05d39d9621', 'nb:mp:05c9355d25', 'nb:mp:044dd9dd29', 'nb:mp:05b53f9019', 'nb:mp:059844fb25', 'nb:mp:057ca55724', 'nb:mp:05dff28525', 'nb:mp:05205b9125', 'nb:mp:0560b0ff25', 'nb:mp:05265b4f24', 'nb:mp:05c79dc724', 'nb:mp:05caf00425', 'nb:mp:05fe0ed221', 'nb:mp:056d6ab425', 'nb:mp:056726ef24', 'nb:mp:051521ac25', 'nb:mp:05df3c9025', 'nb:mp:05c2d2b616', 'nb:mp:05d0094116', 'nb:mp:05146a9f16',
'nb:mp:05476fc816', 'nb:mp:056963eb16', 'nb:mp:05eaae8616',
       'nb:mp:05de5ed416', 'nb:mp:05817c2616', 'nb:mp:0565c6b816',
       'nb:mp:05b2559916', 'nb:mp:05afb74416', 'nb:mp:05d189c916',
       'nb:mp:051aceb816', 'nb:mp:05f99ffd16', 'nb:mp:05456afa16',
       'nb:mp:05e83d8c16', 'nb:mp:05dfd95c16', 'nb:mp:056ef65d16',
       'nb:mp:05e5b9ae16', 'nb:mp:057e2e5916', 'nb:mp:05435ba516',
       'nb:mp:05d33fdc16', 'nb:mp:0542ead316', 'nb:mp:052d5d0716',
       'nb:mp:0574b86116', 'nb:mp:0502588616', 'nb:mp:0548e72d16',
       'nb:mp:05ba432016', 'nb:mp:0556e8f116', 'nb:mp:05cb270c16',
       'nb:mp:05fd58a916', 'nb:mp:05c4236f16', 'nb:mp:054a5e2c16',
       'nb:mp:05af34df16', 'nb:mp:0578b21416', 'nb:mp:05229aaa16',
       'nb:mp:05a617af16', 'nb:mp:059c52c216', 'nb:mp:0577530f16',
       'nb:mp:05b8161b16', 'nb:mp:05aa2a9616', 'nb:mp:057fa80016',
       'nb:mp:05116ae016', 'nb:mp:0574dd5716', 'nb:mp:05b7799716',
       'nb:mp:05041a8516', 'nb:mp:055f1e4016', 'nb:mp:05a6fdb916',
       'nb:mp:05920d4d16', 'nb:mp:0543ff8616', 'nb:mp:0514f58616',
       'nb:mp:0542347416', 'nb:mp:05bc52c016', 'nb:mp:053319c516',
       'nb:mp:05777afb16', 'nb:mp:0545819116', 'nb:mp:05ef8d5916',
       'nb:mp:0558b08a16','nb:mp:0577f95f21', 'nb:mp:0515cfc021', 'nb:mp:05879dec21',
       'Manual Pricing', 'nb:mp:0539d26d21', 'nb:mp:055e0cf421',
       'nb:mp:059aad7621', 'nb:mp:05f675c621', 'nb:mp:0504f2eb21',
       'nb:mp:05cc1cb221', 'nb:mp:056316eb21', 'nb:mp:05a56cf821',
       'nb:mp:05b17dcd21','nb:mp:0592c82f19', 'nb:mp:05c9aa4019', 'nb:mp:05606bf319',
       'nb:mp:05bdce4e19', 'nb:mp:055573ff19', 'nb:mp:05bad36919',
       'nb:mp:0503ae5619', 'nb:mp:0529936619', 'nb:mp:0592b1fc19',
       'nb:mp:054139e519', 'nb:mp:05e9381219', 'nb:mp:05712b1619',
       'nb:mp:05d39c8e19', 'nb:mp:05dae99d19', 'nb:mp:0523752819',
       'nb:mp:0525eb7519', 'nb:mp:05e0224f19', 'nb:mp:05b4afd419',
       'nb:mp:050fb3df19', 'nb:mp:057205da19', 'nb:mp:052bd23119',
       'nb:mp:0541b35519', 'nb:mp:0569eee219', 'nb:mp:050a9a4e19',
       'nb:mp:056fe0f419', 'nb:mp:05e095d619', 'nb:mp:0505d69019',
       'nb:mp:05a30e5a19', 'nb:mp:05013b1419', 'nb:mp:05b83c0a19',
       'nb:mp:0519ee0319', 'nb:mp:05c85ae819', 'nb:mp:059e2b4b19',
       'nb:mp:0564174519', 'nb:mp:05980b6e19', 'nb:mp:05ba9d1219',
       'nb:mp:0514e8e319', 'nb:mp:0524c8c819', 'nb:mp:0576a68919',
       'nb:mp:0579fae119', 'nb:mp:0580831e19', 'nb:mp:052e132d19',
       'nb:mp:05b8d7a619', 'nb:mp:0582261919', 'nb:mp:05d6f49319',
       'nb:mp:05e7d43619', 'nb:mp:05cce5a219', 'nb:mp:0538495719',
       'nb:mp:0566e63219', 'nb:mp:05d4474619', 'nb:mp:05c4dcf619',
       'nb:mp:051f0f4319', 'nb:mp:05993a3219', 'nb:mp:05aeae5d19',
       'nb:mp:05091f2d19', 'nb:mp:05ccfd4f19', 'nb:mp:05beb59619',
       'nb:mp:05fef10519', 'nb:mp:0574632a19', 'nb:mp:05d427ce19',
       'nb:mp:052b6b6d19', 'nb:mp:05d821e419')
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
df_optin=pd.DataFrame(data2)
print(df_optin)

# df_optin.to_csv(f'/home/thotat.vc/analytics_scripts/optin_raw_{today}.csv')
#df_optin=pd.read_csv(f'/home/thotat.vc/analytics_scripts/optin_raw_{today}.csv') # Run this when code fails so that you pickup the last query output

if df_optin.empty:
    print("Empty set error – df_optin has no data beyond headers.")

else:


    # Read Input File
    import pandas as pd
    file_path_1 = '/home/thotat.vc/analytics_scripts/update_code_Optin_L3M_Consolidated_Base_File (2).csv'
    df_1 = pd.read_csv(file_path_1, encoding='cp1252')
    df_1 = df_1.drop_duplicates()

    # Equate to today for uniqueness of files
    from datetime import datetime as dt2
    today=dt2.now().strftime('%d_%b_%Y')

    # Temp assignment
    df_1_1=df_1
    #df_1_1['LID'] = df_1_1['LID'].astype(str).str.strip()
    df_1_1['listing_id'] = df_1_1['listing_id'].astype(str).str.strip()
    # df_1['KAM']='' # Comment if KAM is blank
    df_1_1['KAM'] = df_1_1['KAM'].str.upper()


    # Read from listing_status and remove inactive LIDs from the base by comparing it with current listing_status

    removal_df_1 = df_listing.drop_duplicates()
    removal_df_1 = removal_df_1[removal_df_1['listing_status'] == 'ACTIVE']
    removal_df_1['listing_id'] = removal_df_1['listing_id'].astype(str).str.strip()
    removal_ids_1 = set(removal_df_1['listing_id'])

    #changed lid to listing_id
    df_1_1['listing_id'] = df_1_1['listing_id'].astype(str).str.strip()
    df_1_1['Seller Id'] = df_1_1['Seller Id'].astype(str).str.strip()
    #df_1_1['Offer ID'] = df_1_1['Offer ID'].astype(str).str.strip()
    df_1_1['Correct cms_vertical'] = df_1_1['Correct cms_vertical'].astype(str).str.strip()

    df_1_1['listing_status'] = df_1_1['listing_id'].apply(lambda x: 'ACTIVE' if x in removal_ids_1 else 'INACTIVE')
    # Ensure 'listing_id' is in the list of selected columns
    
    cols=['listing_id','analytic_super_category_l2']
    df_listing_1=df_listing[cols]

    df_1_1=df_1_1.merge(df_listing_1, left_on='listing_id', right_on='listing_id',how='left')
    df_1_1['analytic_super_category_l2'] = df_1_1['analytic_super_category_l2'].astype(str).str.strip()
    df_1_1 = df_1_1[df_1_1['listing_status'] == 'ACTIVE']

    # # Exclude LIDs/FSNs etc

    #excluded_fsns=['SJXGSQXMM6HEZPDW','SJXH2HK53HNN3JV4','SJXH93F6WGHEYUPW','SJXGSU4XXJZUCPNU','SJXGSQX4RGR8SQ6W','SJXH47FERTU8NVGU','SJXH58KJPHPNE2KF','SJXH78MGKESCUVGU','SJXH94D7YPVVJFHX','SJXH94CXPYHPYYHT','SJXH8UN7JNSQZYAF','SJXH2YZXTYJZ8ANK','SJXGSRXY5RGM2N7F','SJXHY57UW2BXAYXZ','SJXH59XDSQRRXHGC','SJXH5AZZ375FHEKH','SJXGYSKFYQHVSNAH','SJXGSU4XGFH9B7DD','SJXH6M7HH4AWRBRX','SJXGYBMECYHRG25C','SJXH6M7HGJFDUAXH','SJXH6M7HUBH8KMPN','SJXH6M7HHDHZHEBE','SJXH2E5RK5CYBUFG','SJXHY9EZVPPAEMHW','SJXH4EME7SHNGBRV','SJXH58MGCZJMYGAU','SJXH8NY4SVC4NKNP','SJXH8Z32FJKANDNF','SJXH8349KUE7AUPZ','SJXH4EMGZPRYFQXQ','SJXH8TBPZKX6EWNT','SJXH5H3BKZ7THGNP','SJXH7A5ZXD7KHSWG','SJXGSU4RHXABT7RM','SJXH6NAWRDUZDCNK']
    #df_1_1=df_1_1[~df_1_1['FSN'].isin(excluded_fsns)]
    #df_1_1 = df_1_1.drop_duplicates()
    
    #exclude_set = {
        #('de30cb22cb61480a', 'shopsy_jewellery_set'),
        #('pj2qynwv83mclisk', 'shopsy_sanitary_pad_pantyliner'),
        #('8bb6531aa0504a05', 'shopsy_watch'),
        #('668ef4d4129e4b9f', 'shopsy_sanitary_pad_pantyliner'),
        #('8b98358a6aed459e', 'shopsy_bangle_bracelet_armlet'),
        #('8b98358a6aed459e', 'shopsy_necklace_chain'),
        #('f407f242ad5142be', 'shopsy_necklace_chain'),
        #('7c8d23d7e5b742c4', 'shopsy_sanitary_pad_pantyliner'),
        #('0ece4286cec24ed1', 'shopsy_sanitary_pad_pantyliner'),
        #('465ed2dbbe2c4084', 'shopsy_backpack'),
        #('465ed2dbbe2c4084', 'shopsy_duffel_bag'),
        #('465ed2dbbe2c4084', 'shopsy_sling_bag'),
        #('465ed2dbbe2c4084', 'shopsy_backpack'),
        #('465ed2dbbe2c4084', 'shopsy_bag'),
        #('815a2b0c719c4b76', 'shopsy_backpack'),
        #'815a2b0c719c4b76', 'shopsy_bag'),
        #('d62d81053cc045da', 'shopsy_duffel_bag'),
        #('dfd6041d11194f75', 'shopsy_backpack'),
        #('dfd6041d11194f75', 'shopsy_backpack'),
        #('dfd6041d11194f75', 'shopsy_bag'),
        #('cdf4a8036efe4fe5', 'shopsy_backpack'),
        #('cdf4a8036efe4fe5', 'shopsy_sling_bag'),
        #('afd5b46b6b474ec5', 'shopsy_chopper'),
        #('163f944d6cba4d81', 'shopsy_chopper'),
        #('8ea8080c4ec4474f', 'shopsy_shoe'),
        #('376690b2c65348b0', 'shopsy_t_shirt'),
        #('376690b2c65348b0', 'shopsy_top'),
        #('669fea4c22c24bea', 'shopsy_coin_bank'),
        #('6a88509a94b24258', 'shopsy_lipstick'),
        #('cb7b1d3d1129478d', 'shopsy_trimmer'),
        #('35a39c944fc14910', 'shopsy_lipstick'),
        #('81d8b4cdf4894db0', 'shopsy_t_shirt')
        #}
    

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

    df_1_raw['orders']=(df_1_raw['orders'].fillna(0))/60
    df_1_raw['orders']=df_1_raw['orders'].round(2).astype(int)

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

    input_df = df_1_edited[df_1_edited['Set'] == 'INPUT SET']

    summary_input = input_df.groupby('KAM').agg(
        # listing_id=('LID', 'nunique'),
        # opted_in_listing_id=('LID', lambda x: (x[input_df.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).nunique()),
        # seller_id=('Seller Id', 'nunique'),
        # opted_in_seller_id=('Seller Id', lambda x: (x[input_df.loc[x.index, 'Opted-in seller_id'] == 'Opted-in']).nunique()),
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[input_df.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_input.columns = ['KAM'] + [col + '_input' for col in summary_input.columns if col != 'KAM']

    # Non-Input Summary

    non_input_df = df_1_edited[df_1_edited['Set'] == 'NON-INPUT SET']

    summary_non_input = non_input_df.groupby('KAM').agg(
        # listing_id=('LID', 'nunique'),
        # opted_in_listing_id=('LID', lambda x: (x[non_input_df.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).nunique()),
        # seller_id=('Seller Id', 'nunique'),
        # opted_in_seller_id=('Seller Id', lambda x: (x[non_input_df.loc[x.index, 'Opted-in seller_id'] == 'Opted-in']).nunique()),
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[non_input_df.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_non_input.columns = ['KAM'] + [col + '_non_input' for col in summary_non_input.columns if col != 'KAM']

    # Overall Summary

    summary_overall = df_1_edited.groupby('KAM').agg(
        # listing_id=('LID', 'nunique'),
        # opted_in_listing_id=('LID', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).nunique()),
        # seller_id=('Seller Id', 'nunique'),
        # opted_in_seller_id=('Seller Id', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in seller_id'] == 'Opted-in']).nunique()),
        orders=('orders', 'sum'),
        opted_in_orders=('orders', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    ).reset_index()

    summary_overall.columns = ['KAM'] + [col + '_overall' for col in summary_overall.columns if col != 'KAM']

    summary_final = summary_input.merge(summary_non_input, on='KAM', how='outer')
    summary_final = summary_final.merge(summary_overall, on='KAM', how='outer')

    unique_listing_ids_df_summary=summary_final

    import numpy as np
    # unique_listing_ids_df_summary['Opt In LID %']=np.where(unique_listing_ids_df_summary['listing_id']==0, 0, unique_listing_ids_df_summary['opted_in_listing_id']/unique_listing_ids_df_summary['listing_id']).round(2)
    # unique_listing_ids_df_summary['Opt In SID %']=np.where(unique_listing_ids_df_summary['seller_id']==0, 0, unique_listing_ids_df_summary['opted_in_seller_id']/unique_listing_ids_df_summary['seller_id']).round(2)
    unique_listing_ids_df_summary['DW Input %']=np.where(unique_listing_ids_df_summary['orders_input']==0, 0, unique_listing_ids_df_summary['opted_in_orders_input']/unique_listing_ids_df_summary['orders_input']).round(2)

    # unique_listing_ids_df_summary['Opt In LID %'] = (unique_listing_ids_df_summary['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary['Opt In SID %'] = (unique_listing_ids_df_summary['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary['DW Input %'] = (unique_listing_ids_df_summary['DW Input %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary
    
    

    unique_listing_ids_df_summary['DW Non Input %']=np.where(unique_listing_ids_df_summary['orders_non_input']==0, 0, unique_listing_ids_df_summary['opted_in_orders_non_input']/unique_listing_ids_df_summary['orders_non_input']).round(2)
    
    # unique_listing_ids_df_summary['Opt In LID %'] = (unique_listing_ids_df_summary['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary['Opt In SID %'] = (unique_listing_ids_df_summary['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary['DW Non Input %'] = (unique_listing_ids_df_summary['DW Non Input %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary

    
    unique_listing_ids_df_summary['DW Overall %']=np.where(unique_listing_ids_df_summary['orders_overall']==0, 0, unique_listing_ids_df_summary['opted_in_orders_overall']/unique_listing_ids_df_summary['orders_overall']).round(2)
    
    # unique_listing_ids_df_summary['Opt In LID %'] = (unique_listing_ids_df_summary['Opt In LID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary['Opt In SID %'] = (unique_listing_ids_df_summary['Opt In SID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary['DW Overall %'] = (unique_listing_ids_df_summary['DW Overall %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary
  

    total_row = pd.Series({
        'KAM': 'Total',
        # 'listing_id': df_1_edited['LID'].nunique(),
        # 'opted_in_listing_id': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['LID'].nunique(),
        # 'seller_id': df_1_edited['Seller Id'].nunique(),
        # 'opted_in_seller_id': df_1_edited[df_1_edited['Opted-in seller_id'] == 'Opted-in']['Seller Id'].nunique(),
        'orders_input': input_df['orders'].sum(),
        'opted_in_orders_input': input_df[input_df['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_non_input': non_input_df['orders'].sum(),
        'opted_in_orders_non_input': non_input_df[non_input_df['Opted-in listing_id'] == 'Opted-in']['orders'].sum(),
        
        'orders_overall': df_1_edited['orders'].sum(),
        'opted_in_orders_overall': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['orders'].sum()
    })

    total_row_df = pd.DataFrame([total_row])

    total_row_df['DW Input %'] = np.where(
        total_row_df['orders_input'] == 0, 0, 
        total_row_df['opted_in_orders_input'] / total_row_df['orders_input']
    ).round(2)

    total_row_df['DW Non Input %'] = np.where(
        total_row_df['orders_non_input'] == 0, 0, 
        total_row_df['opted_in_orders_non_input'] / total_row_df['orders_non_input']
    ).round(2)

    total_row_df['DW Overall %'] = np.where(
        total_row_df['orders_overall'] == 0, 0, 
        total_row_df['opted_in_orders_overall'] / total_row_df['orders_overall']
    ).round(2)

    for col in ['DW Input %', 'DW Non Input %', 'DW Overall %']:
        total_row_df[col] = (total_row_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_2 = pd.concat([total_row_df, unique_listing_ids_df_summary], ignore_index=True)
    unique_listing_ids_df_summary_2

    unique_listing_ids_df_summary_2=unique_listing_ids_df_summary_2.fillna(0)

    # Uncomment if KAM column is blank
    # unique_listing_ids_df_summary_2=unique_listing_ids_df_summary_2[unique_listing_ids_df_summary_2['KAM']=='Total']

    # Build the HTML table with grid lines
    
    html_table = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">'
    html_table += '<thead><tr><th style="border: 1px solid #ddd; padding: 8px;">KAM</th><th style="border: 1px solid #ddd; padding: 8px;">orders_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">orders_non_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_non_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">orders_overall_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_overall_drr</th><th style="border: 1px solid #ddd; padding: 8px;">DW Input %</th><th style="border: 1px solid #ddd; padding: 8px;">DW Non Input %</th><th style="border: 1px solid #ddd; padding: 8px;">DW Overall %</th></tr></thead>'
    html_table += '<tbody>'

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_2.iterrows():
        html_table += f'''
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['KAM']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_input']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_input']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_non_input']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_non_input']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Input %']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Non Input %']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>                                                                               
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

    # Input Summary

    # input_df_supercat = df_1_edited[df_1_edited['Set'] == 'INPUT SET']

    # summary_input_supercat = input_df_supercat.groupby('analytic_super_category_l2').agg(
    #     # listing_id=('LID', 'nunique'),
    #     # opted_in_listing_id=('LID', lambda x: (x[input_df_supercat.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).nunique()),
    #     # seller_id=('Seller Id', 'nunique'),
    #     # opted_in_seller_id=('Seller Id', lambda x: (x[input_df_supercat.loc[x.index, 'Opted-in seller_id'] == 'Opted-in']).nunique()),
    #     orders=('orders', 'sum'),
    #     opted_in_orders=('orders', lambda x: (x[input_df_supercat.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    # ).reset_index()

    # summary_input_supercat.columns = ['KAM'] + [col + '_input' for col in summary_input_supercat.columns if col != 'KAM']

    # # Non-Input Summary

    # non_input_df_supercat = df_1_edited[df_1_edited['Set'] == 'NON-INPUT SET']

    # summary_non_input_supercat = non_input_df_supercat.groupby('analytic_super_category_l2').agg(
    #     # listing_id=('LID', 'nunique'),
    #     # opted_in_listing_id=('LID', lambda x: (x[non_input_df_supercat.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).nunique()),
    #     # seller_id=('Seller Id', 'nunique'),
    #     # opted_in_seller_id=('Seller Id', lambda x: (x[non_input_df_supercat.loc[x.index, 'Opted-in seller_id'] == 'Opted-in']).nunique()),
    #     orders=('orders', 'sum'),
    #     opted_in_orders=('orders', lambda x: (x[non_input_df_supercat.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    # ).reset_index()

    # summary_non_input_supercat.columns = ['KAM'] + [col + '_non_input' for col in summary_non_input_supercat.columns if col != 'KAM']

    # # Overall Summary

    # summary_overall_supercat = df_1_edited.groupby('analytic_super_category_l2').agg(
    #     # listing_id=('LID', 'nunique'),
    #     # opted_in_listing_id=('LID', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).nunique()),
    #     # seller_id=('Seller Id', 'nunique'),
    #     # opted_in_seller_id=('Seller Id', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in seller_id'] == 'Opted-in']).nunique()),
    #     orders=('orders', 'sum'),
    #     opted_in_orders=('orders', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).sum())
    # ).reset_index()

    # summary_overall_supercat.columns = ['KAM'] + [col + '_overall' for col in summary_overall_supercat.columns if col != 'KAM']

    # summary_final_supercat = summary_input_supercat.merge(summary_non_input_supercat, on='KAM', how='outer')
    # summary_final_supercat = summary_final_supercat.merge(summary_overall_supercat, on='KAM', how='outer')

    # unique_listing_ids_df_summary_supercat=summary_final_supercat
    
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
    import pandas as pd
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

    # total_row_supercat_df['DW Input %'] = np.where(
    #     total_row_supercat_df['orders_input'] == 0, 0, 
    #     total_row_supercat_df['opted_in_orders_input'] / total_row_supercat_df['orders_input']
    # ).round(2).astype(float).fillna(0)

    total_row_supercat_df.loc[:, 'DW Input %'] = pd.Series(
    np.where(
        total_row_supercat_df['orders_input'] == 0, 0, 
        total_row_supercat_df['opted_in_orders_input'] / total_row_supercat_df['orders_input']
    ),
    index=total_row_supercat_df.index).round(2).fillna(0)

    #total_row_supercat_df['DW Input %'] = total_row_supercat_df['DW Input %'].fillna(0)

   

    total_row_supercat_df['DW Non Input %'] = np.where(
        total_row_supercat_df['orders_non_input'] == 0, 0, 
        total_row_supercat_df['opted_in_orders_non_input'] / total_row_supercat_df['orders_non_input']
    ).round(2)

    #total_row_supercat_df['DW Non Input %'] = total_row_supercat_df['DW Non Input %'].fillna(0)

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
    html_table_supercat = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">'
    html_table_supercat += '<thead><tr><th style="border: 1px solid #ddd; padding: 8px;">analytic_super_category_l2</th><th style="border: 1px solid #ddd; padding: 8px;">orders_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">orders_non_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_non_input_drr</th><th style="border: 1px solid #ddd; padding: 8px;">orders_overall_drr</th><th style="border: 1px solid #ddd; padding: 8px;">opted_in_orders_overall_drr</th><th style="border: 1px solid #ddd; padding: 8px;">DW Input %</th><th style="border: 1px solid #ddd; padding: 8px;">DW Non Input %</th><th style="border: 1px solid #ddd; padding: 8px;">DW Overall %</th></tr></thead>'
    html_table_supercat += '<tbody>'

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_supercat_2.iterrows():
        html_table_supercat += f'''
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['analytic_super_category_l2']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_input']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_input']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_non_input']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_non_input']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['orders_overall']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_orders_overall']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Input %']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Non Input %']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['DW Overall %']}</td>                                                                               
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

    unique_listing_ids_df_summary_2=unique_listing_ids_df_summary_2.rename(columns={'opted_in_orders':'opted_in_orders_drr', 'orders':'orders_drr'})
    unique_listing_ids_df_summary_supercat_2=unique_listing_ids_df_summary_supercat_2.rename(columns={'opted_in_orders':'opted_in_orders_drr', 'orders':'orders_drr'})
    
    df_1_edited.to_csv(f"/home/thotat.vc/Downloads/Consolidated_report_{today}.csv")

    # with pd.ExcelWriter(f"/home/thotat.vc/Downloads/Consolidated_report_{today}.xlsx") as writer:
    #     unique_listing_ids_df_summary_supercat_2.to_excel(writer, sheet_name='Summary Supercategory',index=False)
    #     df_1_edited.to_excel(writer, sheet_name='Raw',index=False)
    #     unique_listing_ids_df_summary_2.to_excel(writer, sheet_name='Summary',index=False)

    filename = f"Consolidated_report_{today}.csv"
    file_path = f"/home/thotat.vc/Downloads/Consolidated_report_{today}.csv" 

    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name('/home/thotat.vc/analytics_scripts/creds.json', scope)
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

    file_id = navigate_to_folder_and_upload_file('Optin Reports - Output', file_path)
    link = "https://drive.google.com/drive/u/0/folders/1Nei8rY9HFUji4rZ7uyRHQtDO3yuNHb7b"

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
    original_to = ['upasanaupadhak.vc@flipkart.com','vishnuac.vc@flipkart.com']
    # 'upasanaupadhak.vc@flipkart.com','h.kishandasmenon@flipkart.com','vishnuac.vc@flipkart.com','shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com'
    #'shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com']
    #'shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com']
    #'shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com'
    original_cc = ['thotat.vc@flipkart.com']
    original_message_id = '<CACZpucxu72Mi53xeurcXHuHr3W2Tjy-t7mw=_N2GnJ42GRkOGQ@mail.gmail.com>'

    all_recipients = set([original_from] + original_to + original_cc)

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(original_to)
    msg['Cc'] = ', '.join(original_cc)
    msg['Subject'] = f"Consolidated Optin (L3M) Thread"
    msg['In-Reply-To'] = original_message_id
    msg['References'] = original_message_id

    body = f"""
    Hi Team,<br><br>
    Please find below the optin status for Consolidated Optin (L3M) Thread.:<br><br>
    {html_table}<br><br>
    Please find the Supercategory summary for the same:<br><br>
    {html_table_supercat}<br><br>
    The attached drive file contains the raw and summary data for your reference: {link}.<br><br>
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
