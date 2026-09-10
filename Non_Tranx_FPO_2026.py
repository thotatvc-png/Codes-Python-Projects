#! /usr/share/hunch/bin/python 

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
# map2.mapping as new_bu,
# CASE
#   WHEN d.analytic_super_category in ('ShopsyLifeStyle') and lower(d.analytic_category) IN ('na', 'shopsykidclothing','shopsymensclothingcasualtopwear','shopsymensclothingessentialsandethnic','shopsymensclothingjeans','shopsymensclothingsmartandf_reormals','shopsywomenethniccontemporary','shopsywomenethniccore','shopsywomenwesterncore') THEN 'ShopsyLifeStyle Apparel'
#   WHEN d.analytic_super_category in ('ShopsyLifeStyle') and lower(d.analytic_category) not IN ('na', 'shopsykidclothing','shopsymensclothingcasualtopwear','shopsymensclothingessentialsandethnic','shopsymensclothingjeans','shopsymensclothingsmartandf_reormals','shopsywomenethniccontemporary','shopsywomenethniccore','shopsywomenwesterncore') THEN 'ShopsyLifeStyle Non-Apparel'
#   WHEN d.analytic_super_category in ('ShopsyBGM') AND LOWER(d.analytic_category) IN ('shopsygrooming','shopsymakeupfragrances') THEN 'ShopsyBGM BPC'
#   WHEN d.analytic_super_category in ('ShopsyBGM') AND LOWER(d.analytic_category) NOT IN ('shopsygrooming','shopsymakeupfragrances') THEN 'ShopsyBGM GM'
#   ELSE d.analytic_super_category END AS analytic_super_category_l2
# from
# fdp_uploads.ds_fkint_2gud_partner_vamsi_lid_shopsy_1_0 a
# left join
# bigfoot_external_neo.sp_product__listing_hive_dim b
# on a.listing_id = b.listing_id
# left join 
# fdp_uploads.ds_fkint_2gud_partner_new_bu_mapping_shopsy_incl_hat_1_0 map2
# on b.analytic_category=map2.analytic_category
# left join
# bigfoot_external_neo.sp_seller__seller_hive_dim c
# on b.seller_id=c.seller_id
# left join
# bigfoot_external_neo.sp_product__product_categorization_hive_dim d
# on b.product_id = d.product_id
# '''
# data = pd.read_sql_query(sql1, con=conn)
# df_re_listing=pd.DataFrame(data)
# print(df_re_listing)

# from datetime import datetime as dt2
# today=dt2.now().strftime('%d_%b_%Y')

# df_re_listing.to_csv(f'/home/thotat.vc/analytics_scripts/FEB_FPO_POST_RC_OFFER_listing.csv') 
df_re_listing=pd.read_csv(f'/home/thotat.vc/analytics_scripts/non_Fpo_optin_listing.csv') 

# sql2='''
# select distinct 
# a.offer_id as offer_id, 
# b.product_id as product_id,
# a.listing_id as listing_id, 
# b.seller_id as seller_id, 
# c.cms_vertical as cms_vertical, 
# b.listing_status as listing_status
# from 
# (
#   select offer_id, listing_id
#   from 
#   (
#     SELECT
#     data.listingid AS listing_id,
#     data.offerid AS offer_id,
#     MAX(case when data.action = 'Mapped' then eventtime end) as opt_in_time,
#     MAX(case when data.action = 'Unmapped' then eventtime end) as opt_out_time
#     FROM bigfoot_journal.dart_fkint_cp_santa_offerlistingmappingchange_5
#     WHERE day > 20251201
#     and data.offerid in ('nb:mp:04f34cbb06')
#     GROUP BY
#     data.listingid,
#     data.offerid
#   ) a 
#   where opt_out_time is null  or opt_in_time > opt_out_time  
# ) a 
# join bigfoot_external_neo.sp_product__listing_hive_dim b 
# on a.listing_id = b.listing_id
# JOIN bigfoot_external_neo.sp_product__product_categorization_hive_dim c
#  ON b.product_id =  c.product_id
# '''
# data2 = pd.read_sql_query(sql2, con=conn)
# df_re_optin=pd.DataFrame(data2)
# print(df_re_optin)

# df_re_optin.to_csv(f'/home/thotat.vc/analytics_scripts/FEB_FPO_POST_RC_OFFER_optin.csv')
df_re_optin=pd.read_csv(f'/home/thotat.vc/analytics_scripts/non_Fpo_optin_raw.csv') 

if df_re_optin.empty:
    print("Empty set error – df_re_optin has no data beyond headers.")

else:
    # Read Input File
    import pandas as pd
    file_path_re_1 = '/home/thotat.vc/analytics_scripts/Non_Final_deals_fpo_may_2026.csv'
    df_re_1 = pd.read_csv(file_path_re_1, encoding='cp1252')
    df_re_1 = df_re_1.drop_duplicates()

    from datetime import datetime as dt2
    today=dt2.now().strftime('%d_%b_%Y')

    df_re_1_1=df_re_1
    df_re_1_1['LID'] = df_re_1_1['LID'].astype(str).str.strip()
    df_re_1_1['KAM'] = df_re_1_1['KAM'].str.upper()

    removal_df_re_1 = df_re_listing.drop_duplicates()
    # removal_df_re_1 = removal_df_re_1[removal_df_re_1['listing_status'] == 'ACTIVE']
    removal_df_re_1['listing_id'] = removal_df_re_1['listing_id'].astype(str).str.strip()
    removal_ids_1 = set(removal_df_re_1['listing_id'])

    df_re_1_1['LID'] = df_re_1_1['LID'].astype(str).str.strip()
    df_re_1_1['Seller Id'] = df_re_1_1['Seller Id'].astype(str).str.strip()
    df_re_1_1['Offer ID'] = df_re_1_1['Offer ID'].astype(str).str.strip()

    cols=['listing_id', 'analytic_super_category_l2','new_bu']
    df_re_listing_1=df_re_listing[cols]

    df_re_1_1=df_re_1_1.merge(df_re_listing_1, left_on='LID', right_on='listing_id',how='left')
    df_re_1_1['analytic_super_category_l2'] = df_re_1_1['analytic_super_category_l2'].astype(str).str.strip()

    df_re_1_1['listing_status'] = df_re_1_1['LID'].apply(lambda x: 'ACTIVE' if x in removal_ids_1 else 'INACTIVE')
    
    df_re_1_1 = df_re_1_1[df_re_1_1['listing_status'] == 'ACTIVE']

    df_re_1_1['KAM'] = df_re_1_1['KAM'].astype(str).str.strip().str.upper()
    df_re_1_1['KAM'] = df_re_1_1['KAM'].apply(lambda x: 'HARSHITA PANDEY' if str(x).strip().lower() == 'harshita' else x)
    df_re_1_1['KAM'] = df_re_1_1['KAM'].apply(lambda x: 'SHUBHANKAR' if str(x).strip().lower() == 'sagar bansal' else x)
    df_re_1_1['KAM'] = df_re_1_1['KAM'].apply(lambda x: 'PRATIK NAG' if str(x).strip().lower() == 'pratik' else x)
    df_re_1_1['KAM'] = df_re_1_1['KAM'].apply(lambda x: 'VISHAL SINGHA' if str(x).strip().lower() == 'jerin' else x)
    df_re_1_1['KAM'] = df_re_1_1['KAM'].apply(lambda x: 'AFFAN NADEEM' if str(x).strip().lower() == 'affan' else x)
    df_re_1_1['KAM'] = df_re_1_1['KAM'].apply(lambda x: 'AISHWARYA SAWANT' if str(x).strip().lower() == 'aishwarya' else x)  
    df_re_1_1['KAM'] = df_re_1_1['KAM'].apply(lambda x: 'AKASH PATTANAYAK' if str(x).strip().lower() == 'akash patanayak' else x) 
    df_re_1_1['KAM'] = df_re_1_1['KAM'].apply(lambda x: 'ANIMESH JAIN' if str(x).strip().lower() == 'animesh' else x) 
    df_re_1_1['KAM'] = df_re_1_1['KAM'].apply(lambda x: 'EKTA KHATARKAR' if str(x).strip().lower() == 'ekta' else x)
    df_re_1_1['KAM'] = df_re_1_1['KAM'].apply(lambda x: 'RIYA GUPTA' if str(x).strip().lower() == 'riya' else x)
    df_re_1_1['KAM'] = df_re_1_1['KAM'].apply(lambda x: 'SANSRITI BAJORIA' if str(x).strip().lower() == 'sansriti' else x)
    df_re_1_1['KAM'] = df_re_1_1['KAM'].apply(lambda x: 'POOJA RAMESH' if str(x).strip().lower() == 'pooja' else x)
    df_re_1_1['KAM'] = df_re_1_1['KAM'].apply(lambda x: 'VANDANA RANA' if str(x).strip().lower() == 'vandana' else x)

    df_re_optin=df_re_optin.drop_duplicates()
    df_re_optin['listing_id'] = df_re_optin['listing_id'].astype(str).str.strip()
    df_re_optin['offer_id'] = df_re_optin['offer_id'].astype(str).str.strip()
    df_re_optin['seller_id'] = df_re_optin['seller_id'].astype(str).str.strip()

    optin_combinations = set(zip(df_re_optin['listing_id'], df_re_optin['offer_id']))
    df_re_1_raw=df_re_1_1
    df_re_1_raw['Opted-in listing_id'] = df_re_1_raw.apply(
        lambda row: 'Opted-in' if (row['LID'], row['Offer ID']) in optin_combinations else 'Not opted-in', 
        axis=1
    )

    optin_combinations_seller = set(zip(df_re_optin['seller_id'], df_re_optin['cms_vertical']))
    df_re_1_raw['Opted-in seller_id'] = df_re_1_raw.apply(
        lambda row: 'Opted-in' if (row['Seller Id'], row['Correct cms_vertical']) in optin_combinations_seller else 'Not opted-in', 
        axis=1
    )
    df_re_1_raw=df_re_1_raw.drop_duplicates()

    # REMOVED ORDERS PORTION PER REQUEST
    df_re_1_edited=df_re_1_raw[df_re_1_raw['Offer ID']!='Manual Pricing']

    # Aggregation logic fixed to work without Orders
    unique_listing_ids_df_re_summary=df_re_1_edited.groupby('KAM').agg(
        listing_id=('LID', 'nunique'),
        opted_in_listing_id=('LID', lambda x: (x[df_re_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).nunique()),
        seller_id=('Seller Id','nunique'),
        opted_in_seller_id=('Seller Id', lambda x: (x[df_re_1_edited.loc[x.index, 'Opted-in seller_id'] == 'Opted-in']).nunique())
    ).reset_index()

    unique_listing_ids_df_re_summary['Opt In LID %']=np.where(unique_listing_ids_df_re_summary['listing_id']==0, 0, unique_listing_ids_df_re_summary['opted_in_listing_id']/unique_listing_ids_df_re_summary['listing_id']).round(2)
    unique_listing_ids_df_re_summary['Opt In SID %']=np.where(unique_listing_ids_df_re_summary['seller_id']==0, 0, unique_listing_ids_df_re_summary['opted_in_seller_id']/unique_listing_ids_df_re_summary['seller_id']).round(2)
    unique_listing_ids_df_re_summary['Opt In LID %'] = (unique_listing_ids_df_re_summary['Opt In LID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_re_summary['Opt In SID %'] = (unique_listing_ids_df_re_summary['Opt In SID %'] * 100).round(2).astype(str) + '%'

    total_row = pd.Series({
        'KAM': 'Total',
        'listing_id': df_re_1_edited['LID'].nunique(),
        'opted_in_listing_id': df_re_1_edited[df_re_1_edited['Opted-in listing_id'] == 'Opted-in']['LID'].nunique(),
        'seller_id': df_re_1_edited['Seller Id'].nunique(),
        'opted_in_seller_id': df_re_1_edited[df_re_1_edited['Opted-in seller_id'] == 'Opted-in']['Seller Id'].nunique()
    })

    total_row_df_re = pd.DataFrame([total_row])
    total_row_df_re['Opt In LID %'] = np.where(total_row_df_re['listing_id'] == 0, 0, total_row_df_re['opted_in_listing_id'] / total_row_df_re['listing_id']).round(2)
    total_row_df_re['Opt In SID %'] = np.where(total_row_df_re['seller_id'] == 0, 0, total_row_df_re['opted_in_seller_id'] / total_row_df_re['seller_id']).round(2)
    for col in ['Opt In LID %', 'Opt In SID %']:
        total_row_df_re[col] = (total_row_df_re[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_re_summary_2 = pd.concat([total_row_df_re, unique_listing_ids_df_re_summary], ignore_index=True)

    # HTML TABLE PART (RESTORED FROM INITIAL SHARE)
    html_re_table = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">'
    html_re_table += '''<thead><tr>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">KAM</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">listing_id</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">opted_in_listing_id</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">seller_id</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">opted_in_seller_id</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opt In LID %</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opt In SID %</th>
    </tr></thead>'''
    html_re_table += '<tbody>'

    for index, row in unique_listing_ids_df_re_summary_2.iterrows():
        row_style = 'background-color: lightskyblue; font-weight: bold;' if 'total' in str(row['KAM']).lower() else ''
        html_re_table += f'''
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['KAM']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['listing_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['opted_in_listing_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['seller_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['opted_in_seller_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['Opt In LID %']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['Opt In SID %']}</td>                                                                                                   
            </tr>'''
    html_re_table += '</tbody></table>'

    # SUPERCAT SUMMARY PART
    unique_listing_ids_df_re_summary_supercat=df_re_1_edited.groupby('analytic_super_category_l2').agg(
        listing_id=('LID', 'nunique'),
        opted_in_listing_id_overall=('LID', lambda x: (x[df_re_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).nunique()),
        seller_id=('Seller Id','nunique'),
        opted_in_seller_id=('Seller Id', lambda x: (x[df_re_1_edited.loc[x.index, 'Opted-in seller_id'] == 'Opted-in']).nunique())
    ).reset_index()

    unique_listing_ids_df_re_summary_supercat['Opt In LID %']=np.where(unique_listing_ids_df_re_summary_supercat['listing_id']==0, 0, unique_listing_ids_df_re_summary_supercat['opted_in_listing_id_overall']/unique_listing_ids_df_re_summary_supercat['listing_id']).round(2)
    unique_listing_ids_df_re_summary_supercat['Opt In SID %']=np.where(unique_listing_ids_df_re_summary_supercat['seller_id']==0, 0, unique_listing_ids_df_re_summary_supercat['opted_in_seller_id']/unique_listing_ids_df_re_summary_supercat['seller_id']).round(2)
    unique_listing_ids_df_re_summary_supercat['Opt In LID %'] = (unique_listing_ids_df_re_summary_supercat['Opt In LID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_re_summary_supercat['Opt In SID %'] = (unique_listing_ids_df_re_summary_supercat['Opt In SID %'] * 100).round(2).astype(str) + '%'

    total_row_supercat = pd.Series({
        'analytic_super_category_l2': 'Total',
        'listing_id': df_re_1_edited['LID'].nunique(),
        'opted_in_listing_id_overall': df_re_1_edited[df_re_1_edited['Opted-in listing_id'] == 'Opted-in']['LID'].nunique(),
        'seller_id': df_re_1_edited['Seller Id'].nunique(),
        'opted_in_seller_id': df_re_1_edited[df_re_1_edited['Opted-in seller_id'] == 'Opted-in']['Seller Id'].nunique()
    })

    total_row_supercat_df_re = pd.DataFrame([total_row_supercat])
    total_row_supercat_df_re['Opt In LID %'] = np.where(total_row_supercat_df_re['listing_id'] == 0, 0, total_row_supercat_df_re['opted_in_listing_id_overall'] / total_row_supercat_df_re['listing_id']).round(2)
    total_row_supercat_df_re['Opt In SID %'] = np.where(total_row_supercat_df_re['seller_id'] == 0, 0, total_row_supercat_df_re['opted_in_seller_id'] / total_row_supercat_df_re['seller_id']).round(2)
    for col in ['Opt In LID %', 'Opt In SID %']:
        total_row_supercat_df_re[col] = (total_row_supercat_df_re[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_re_summary_supercat_2 = pd.concat([total_row_supercat_df_re, unique_listing_ids_df_re_summary_supercat], ignore_index=True)

    # SUPERCAT HTML TABLE
    html_re_table_supercat = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">'
    html_re_table_supercat += '''<thead><tr>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">analytic_super_category_l2</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">listing_id</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">opted_in_listing_id</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">seller_id</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">opted_in_seller_id</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opt In LID %</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opt In SID %</th>
    </tr></thead>'''
    html_re_table_supercat += '<tbody>'

    for index, row in unique_listing_ids_df_re_summary_supercat_2.iterrows():
        row_style = 'background-color: lightskyblue; font-weight: bold;' if 'total' in str(row['analytic_super_category_l2']).lower() else ''
        html_re_table_supercat += f'''
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['analytic_super_category_l2']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['listing_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['opted_in_listing_id_overall'] if 'opted_in_listing_id_overall' in row else row['opted_in_listing_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['seller_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['opted_in_seller_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['Opt In LID %']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['Opt In SID %']}</td>                                                                                                                
            </tr>'''
    html_re_table_supercat += '</tbody></table>'

    #---------BU---CUT---

    unique_listing_ids_df_re_summary_New_BU=df_re_1_edited.groupby('new_bu').agg(
        listing_id=('LID', 'nunique'),
        opted_in_listing_id_overall=('LID', lambda x: (x[df_re_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).nunique()),
        seller_id=('Seller Id','nunique'),
        opted_in_seller_id=('Seller Id', lambda x: (x[df_re_1_edited.loc[x.index, 'Opted-in seller_id'] == 'Opted-in']).nunique())
    ).reset_index()

    unique_listing_ids_df_re_summary_New_BU['Opt In LID %']=np.where(unique_listing_ids_df_re_summary_New_BU['listing_id']==0, 0, unique_listing_ids_df_re_summary_New_BU['opted_in_listing_id_overall']/unique_listing_ids_df_re_summary_New_BU['listing_id']).round(2)
    unique_listing_ids_df_re_summary_New_BU['Opt In SID %']=np.where(unique_listing_ids_df_re_summary_New_BU['seller_id']==0, 0, unique_listing_ids_df_re_summary_New_BU['opted_in_seller_id']/unique_listing_ids_df_re_summary_New_BU['seller_id']).round(2)
    unique_listing_ids_df_re_summary_New_BU['Opt In LID %'] = (unique_listing_ids_df_re_summary_New_BU['Opt In LID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_re_summary_New_BU['Opt In SID %'] = (unique_listing_ids_df_re_summary_New_BU['Opt In SID %'] * 100).round(2).astype(str) + '%'

    total_row_New_BU = pd.Series({
        'new_bu': 'Total',
        'listing_id': df_re_1_edited['LID'].nunique(),
        'opted_in_listing_id_overall': df_re_1_edited[df_re_1_edited['Opted-in listing_id'] == 'Opted-in']['LID'].nunique(),
        'seller_id': df_re_1_edited['Seller Id'].nunique(),
        'opted_in_seller_id': df_re_1_edited[df_re_1_edited['Opted-in seller_id'] == 'Opted-in']['Seller Id'].nunique()
    })

    total_row_New_BU_df_re = pd.DataFrame([total_row_New_BU])
    total_row_New_BU_df_re['Opt In LID %'] = np.where(total_row_New_BU_df_re['listing_id'] == 0, 0, total_row_New_BU_df_re['opted_in_listing_id_overall'] / total_row_New_BU_df_re['listing_id']).round(2)
    total_row_New_BU_df_re['Opt In SID %'] = np.where(total_row_New_BU_df_re['seller_id'] == 0, 0, total_row_New_BU_df_re['opted_in_seller_id'] / total_row_New_BU_df_re['seller_id']).round(2)
    for col in ['Opt In LID %', 'Opt In SID %']:
        total_row_New_BU_df_re[col] = (total_row_New_BU_df_re[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_re_summary_New_BU_2 = pd.concat([total_row_New_BU_df_re, unique_listing_ids_df_re_summary_New_BU], ignore_index=True)

    # New_BU HTML TABLE
    html_re_table_New_BU = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">'
    html_re_table_New_BU += '''<thead><tr>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">new_bu</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">listing_id</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">opted_in_listing_id</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">seller_id</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">opted_in_seller_id</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opt In LID %</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opt In SID %</th>
    </tr></thead>'''
    html_re_table_New_BU += '<tbody>'

    for index, row in unique_listing_ids_df_re_summary_New_BU_2.iterrows():
        row_style = 'background-color: lightskyblue; font-weight: bold;' if 'total' in str(row['new_bu']).lower() else ''
        html_re_table_New_BU += f'''
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['new_bu']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['listing_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['opted_in_listing_id_overall'] if 'opted_in_listing_id_overall' in row else row['opted_in_listing_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['seller_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['opted_in_seller_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['Opt In LID %']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['Opt In SID %']}</td>                                                                                                                
            </tr>'''
    html_re_table_New_BU += '</tbody></table>'

    unique_listing_ids_df_re_summary_owner=df_re_1_edited.groupby('owner').agg(
        listing_id=('LID', 'nunique'),
        opted_in_listing_id_overall=('LID', lambda x: (x[df_re_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).nunique()),
        seller_id=('Seller Id','nunique'),
        opted_in_seller_id=('Seller Id', lambda x: (x[df_re_1_edited.loc[x.index, 'Opted-in seller_id'] == 'Opted-in']).nunique())
    ).reset_index()

    unique_listing_ids_df_re_summary_owner['Opt In LID %']=np.where(unique_listing_ids_df_re_summary_owner['listing_id']==0, 0, unique_listing_ids_df_re_summary_owner['opted_in_listing_id_overall']/unique_listing_ids_df_re_summary_owner['listing_id']).round(2)
    unique_listing_ids_df_re_summary_owner['Opt In SID %']=np.where(unique_listing_ids_df_re_summary_owner['seller_id']==0, 0, unique_listing_ids_df_re_summary_owner['opted_in_seller_id']/unique_listing_ids_df_re_summary_owner['seller_id']).round(2)
    unique_listing_ids_df_re_summary_owner['Opt In LID %'] = (unique_listing_ids_df_re_summary_owner['Opt In LID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_re_summary_owner['Opt In SID %'] = (unique_listing_ids_df_re_summary_owner['Opt In SID %'] * 100).round(2).astype(str) + '%'

    total_row_owner = pd.Series({
        'owner': 'Total',
        'listing_id': df_re_1_edited['LID'].nunique(),
        'opted_in_listing_id_overall': df_re_1_edited[df_re_1_edited['Opted-in listing_id'] == 'Opted-in']['LID'].nunique(),
        'seller_id': df_re_1_edited['Seller Id'].nunique(),
        'opted_in_seller_id': df_re_1_edited[df_re_1_edited['Opted-in seller_id'] == 'Opted-in']['Seller Id'].nunique()
    })

    total_row_owner_df_re = pd.DataFrame([total_row_owner])
    total_row_owner_df_re['Opt In LID %'] = np.where(total_row_owner_df_re['listing_id'] == 0, 0, total_row_owner_df_re['opted_in_listing_id_overall'] / total_row_owner_df_re['listing_id']).round(2)
    total_row_owner_df_re['Opt In SID %'] = np.where(total_row_owner_df_re['seller_id'] == 0, 0, total_row_owner_df_re['opted_in_seller_id'] / total_row_owner_df_re['seller_id']).round(2)
    for col in ['Opt In LID %', 'Opt In SID %']:
        total_row_owner_df_re[col] = (total_row_owner_df_re[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_re_summary_owner_2 = pd.concat([total_row_owner_df_re, unique_listing_ids_df_re_summary_owner], ignore_index=True)

    # owner HTML TABLE
    html_re_table_owner = '<table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">'
    html_re_table_owner += '''<thead><tr>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">owner</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">listing_id</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">opted_in_listing_id</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">seller_id</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">opted_in_seller_id</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opt In LID %</th>
    <th style="border: 1px solid #ddd; padding: 8px; background-color: darkblue; color: white;">Opt In SID %</th>
    </tr></thead>'''
    html_re_table_owner += '<tbody>'

    for index, row in unique_listing_ids_df_re_summary_owner_2.iterrows():
        row_style = 'background-color: lightskyblue; font-weight: bold;' if 'total' in str(row['owner']).lower() else ''
        html_re_table_owner += f'''
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['owner']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['listing_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['opted_in_listing_id_overall'] if 'opted_in_listing_id_overall' in row else row['opted_in_listing_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['seller_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['opted_in_seller_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['Opt In LID %']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left; {row_style}">{row['Opt In SID %']}</td>                                                                                                                
            </tr>'''
    html_re_table_owner += '</tbody></table>'


    



    # FINAL EXCEL EXPORT
    with pd.ExcelWriter(f"/home/thotat.vc/Downloads/FPO_Adotption_{today}.xlsx") as writer:
        df_re_1_edited.to_excel(writer, sheet_name='Raw',index=False)
        unique_listing_ids_df_re_summary_supercat_2.to_excel(writer, sheet_name='Summary Supercategory',index=False)
        unique_listing_ids_df_re_summary_2.to_excel(writer, sheet_name='Summary',index=False)
        unique_listing_ids_df_re_summary_New_BU.to_excel(writer, sheet_name='BU_summary',index=False)
    # DRIVE UPLOAD (RESTORED FROM INITIAL SHARE)
    filename = f"FPO_Adotption_{today}.xlsx"
    file_path = f"/home/thotat.vc/Downloads/FPO_Adotption_{today}.xlsx"  

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
        folder_id = '18bpuWUyFJs_izQYOfO_sgH7nzDbu1mjn'  # Replace with actual folder ID
        file_path = f"/home/thotat.vc/Downloads/FPO_Adotption_{today}.xlsx"
    # Replace with actual file path

        upload_to_shared_drive_folder(drive_id, folder_id,file_path) #file_path
        link = "https://drive.google.com/drive/folders/1gP0sFSP5jejqoNXnBnRVMm9xSJ8PIc1E"
 

    # import os
    # import urllib.parse
    # from datetime import datetime, timedelta
    # from googleapiclient.errors import HttpError
    # from googleapiclient.http import MediaFileUpload

    # # ---- Function to upload file to a specific Google Drive folder ----
    # def upload_file_to_folder(file_path, folder_id):
    #     """Upload a file to a Google Drive folder."""
    #     file_metadata = {
    #         'name': os.path.basename(file_path),
    #         'parents': [folder_id]
    #     }
    #     media = MediaFileUpload(file_path, mimetype='application/octet-stream')
        
    #     file = drive_service.files().create(
    #         body=file_metadata,
    #         media_body=media,
    #         fields='id'
    #     ).execute()
        
    #     print(f"File uploaded to folder ID: {folder_id} with file ID: {file['id']}.")
    #     return file['id']

    # # ---- Function to find a shared folder by exact name and upload file to it ----
    # def navigate_to_folder_and_upload_file(folder_name, file_path):
    #     """Find a shared folder by name and upload a file to it."""
    #     try:
    #         # Get shared folders
    #         results = drive_service.files().list(
    #             q="sharedWithMe and mimeType='application/vnd.google-apps.folder'",
    #             fields="files(id, name, mimeType)"
    #         ).execute()
            
    #         items = results.get('files', [])
    #         target_folder = None
            
    #         for item in items:
    #             if item.get('mimeType') == 'application/vnd.google-apps.folder' and item.get('name') == folder_name:
    #                 target_folder = item
    #                 print(f"Found folder: {target_folder['name']} (ID: {target_folder['id']})")
    #                 break
            
    #         if not target_folder:
    #             print(f"No folder found with the name '{folder_name}'.")
    #             return None
            
    #         # Upload the file directly to the found folder
    #         file_id = upload_file_to_folder(file_path, target_folder['id'])
    #         if file_id:
    #             print(f"File uploaded successfully with ID: {file_id}")
    #             return file_id
    #         else:
    #             print("File upload failed.")
    #             return None

    #     except HttpError as error:
    #         print(f"An error occurred: {error}")
    #         return None

    # # # ---- Function to create a shareable link for a file ----
    # # def create_shareable_link(drive_service, file_id):
    # #     """Create a public shareable link for a Google Drive file."""
    # #     permission = {
    # #         'type': 'anyone',
    # #         'role': 'reader',
    # #     }
        
    # #     drive_service.permissions().create(fileId=file_id, body=permission).execute()
        
    # #     link = f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
    # #     return link

    # file_id = navigate_to_folder_and_upload_file('Optin Reports - Output', file_path)
    # link = "https://drive.google.com/drive/u/0/folders/1Nei8rY9HFUji4rZ7uyRHQtDO3yuNHb7b"

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
    # original_to = ['thotat.vc@flipkart.com','h.kishandasmenon@flipkart.com', 'shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com']
    
    original_to =['thotat.vc@flipkart.com']
    
    #['h.kishandasmenon@flipkart.com', 'shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'shopsysell@flipkart.com', 'shopsypricing@flipkart.com']
    
    
    original_cc = ['thotat.vc@flipkart.com']
    original_message_id = '<CADLy_NEvZq+Q6N4yek+q0vPkPEuzmMDk3utyVn0bPcZnqUv2bA@mail.gmail.com>'


    all_recipients = set([original_from] + original_to + original_cc)

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(original_to)
    msg['Cc'] = ', '.join(original_cc)
    msg['Subject'] = f"FPO Adotpion Optin"
    msg['In-Reply-To'] = original_message_id
    msg['References'] = original_message_id

    body = f"""
    Hi Team,<br><br>
    Please find below the optin status for  FPO .<br><br>
    Please find the Owner summary:<br><br>
    {html_re_table_owner}
    Please find the BU summary:<br><br>
    {html_re_table_New_BU}
    Please find the Supercategory summary:<br><br>
    {html_re_table_supercat}<br><br>
    Please find the KAM summary for the same:<br><br>
    {html_re_table}<br><br>

    The drive link contains the raw and summary data for your reference: {link}<br><br>
    (File name on drive: FEB_Adoption)<br><br>
    <b>Regards,<br>
    Vamsi </b>
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
