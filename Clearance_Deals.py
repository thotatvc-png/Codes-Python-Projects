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

import pandas as pd
file_path_1 = '/home/thotat.vc/analytics_scripts/Clearence_Updated_Combined_Deals_Base_file.csv'
df = pd.read_csv(file_path_1, encoding='cp1252')

offer_ids = df['Offer ID'].dropna().unique().tolist()
offer_ids_cleaned = [f"'{str(oid)}'" for oid in offer_ids]
offer_ids_clause = ', '.join(offer_ids_cleaned)
existing_offer_ids = set([str(oid).strip() for oid in offer_ids])
print("Offer IDs Considered")
print(offer_ids_clause)

listing_ids = df['listing_id'].dropna().unique().tolist()
listing_ids_cleaned = [f"'{str(oid)}'" for oid in listing_ids]
listing_ids_clause = ', '.join(listing_ids_cleaned)
existing_listing_ids = set([str(oid).strip() for oid in listing_ids_cleaned])
print("Listing IDs Considered")
print(listing_ids_clause)

from datetime import datetime as dt2
today=dt2.now().strftime('%d_%b_%Y')

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
# sql1=f"""
# select
# b.listing_id as listing_id,
# b.listing_status as listing_status,
# c.display_name as display_name
# from bigfoot_external_neo.sp_product__listing_hive_dim b
# left join
# bigfoot_external_neo.sp_seller__seller_hive_dim c
# on b.seller_id=c.seller_id
# where b.listing_id in ({listing_ids_clause})
# """
# data = pd.read_sql_query(sql1, con=conn)
# df_listing=pd.DataFrame(data)
# print(df_listing)

# df_listing.to_csv(f'/home/thotat.vc/analytics_scripts/listing_raw_new_clearance_deal.csv') 
df_listing=pd.read_csv(f'/home/thotat.vc/analytics_scripts/listing_raw_new_clearance_deal.csv')

# sql2=f"""
# select distinct a.offer_id as offer_id, b.product_id as product_id,a.listing_id as listing_id, b.seller_id as seller_id, c.cms_vertical as cms_vertical, b.listing_status as listing_status
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
#     WHERE day > lookup_date(date_sub(CURRENT_DATE, 60))
#     and data.offerid in ({offer_ids_clause})
#     GROUP BY
#     data.listingid,
#     data.offerid
#   )a 
#   where opt_out_time is null  or opt_in_time > opt_out_time  
# )a 
# join bigfoot_external_neo.sp_product__listing_hive_dim b 
# on a.listing_id = b.listing_id
# JOIN bigfoot_external_neo.sp_product__product_categorization_hive_dim c
#  ON b.product_id =  c.product_id
# """
# data2 = pd.read_sql_query(sql2, con=conn)
# df_optin=pd.DataFrame(data2)
# print(df_optin)

# df_optin.to_csv(f'/home/thotat.vc/analytics_scripts/optin_raw_new_clearance_deal.csv') 
df_optin=pd.read_csv(f'/home/thotat.vc/analytics_scripts/optin_raw_new_clearance_deal.csv')

if df_optin.empty:
    print("Empty set error – df_optin has no data beyond headers.")

else:


    # Read Input File
    import pandas as pd
    file_path_1 = '/home/thotat.vc/analytics_scripts/Clearence_Updated_Combined_Deals_Base_file.csv'
    df_1 = pd.read_csv(file_path_1, encoding='cp1252')


    # Equate to today for uniqueness of files
    from datetime import datetime as dt2
    today=dt2.now().strftime('%d_%b_%Y')

    # Temp assignment
    df_1_1=df_1.copy()
    df_1_1=df_1_1.drop(columns=['LID'])
    df_1_1['LID']=df_1_1['listing_id']
    df_1_1['LID'] = df_1_1['LID'].astype(str).str.strip()
    #df_1['KAM']=''

    # Read from listing_status and remove inactive LIDs from the base by comparing it with current listing_status

    removal_df_1 = df_listing.drop_duplicates()
    removal_df_1 = removal_df_1[removal_df_1['listing_status'] == 'ACTIVE']
    removal_df_1['listing_id'] = removal_df_1['listing_id'].astype(str).str.strip()
    removal_ids_1 = set(removal_df_1['listing_id'])

    df_1_1['LID'] = df_1_1['LID'].astype(str).str.strip()
    df_1_1['Seller Id'] = df_1_1['Seller Id'].astype(str).str.strip()
    df_1_1['Offer ID'] = df_1_1['Offer ID'].astype(str).str.strip()

    df_1_1['listing_status'] = df_1_1['LID'].apply(lambda x: 'ACTIVE' if x in removal_ids_1 else 'INACTIVE')
    # df_1_1 = df_1_1[df_1_1['listing_status'] == 'ACTIVE']

    # # Exclude LIDs/FSNs etc

    # excluded_fsns=['SJXGSQXMM6HEZPDW','SJXH2HK53HNN3JV4','SJXH93F6WGHEYUPW','SJXGSU4XXJZUCPNU','SJXGSQX4RGR8SQ6W','SJXH47FERTU8NVGU','SJXH58KJPHPNE2KF','SJXH78MGKESCUVGU','SJXH94D7YPVVJFHX','SJXH94CXPYHPYYHT','SJXH8UN7JNSQZYAF','SJXH2YZXTYJZ8ANK','SJXGSRXY5RGM2N7F','SJXHY57UW2BXAYXZ','SJXH59XDSQRRXHGC','SJXH5AZZ375FHEKH','SJXGYSKFYQHVSNAH','SJXGSU4XGFH9B7DD','SJXH6M7HH4AWRBRX','SJXGYBMECYHRG25C','SJXH6M7HGJFDUAXH','SJXH6M7HUBH8KMPN','SJXH6M7HHDHZHEBE','SJXH2E5RK5CYBUFG','SJXHY9EZVPPAEMHW','SJXH4EME7SHNGBRV','SJXH58MGCZJMYGAU','SJXH8NY4SVC4NKNP','SJXH8Z32FJKANDNF','SJXH8349KUE7AUPZ','SJXH4EMGZPRYFQXQ','SJXH8TBPZKX6EWNT','SJXH5H3BKZ7THGNP','SJXH7A5ZXD7KHSWG','SJXGSU4RHXABT7RM','SJXH6NAWRDUZDCNK']
    # df_1_1=df_1_1[~df_1_1['FSN'].isin(excluded_fsns)]
    # df_1_1 = df_1_1.drop_duplicates()


    # Return the optin report
    df_optin=df_optin
    df_optin=df_optin.drop_duplicates()
    df_optin

    df_optin['listing_id'] = df_optin['listing_id'].astype(str).str.strip()
    df_optin['offer_id'] = df_optin['offer_id'].astype(str).str.strip()
    df_optin['seller_id'] = df_optin['seller_id'].astype(str).str.strip()

    # Cross check if unique listing_id from the base file is present in the optin report.
    optin_combinations = set(zip(df_optin['listing_id'], df_optin['offer_id']))
    df_1_raw=df_1_1
    df_1_raw['Opted-in listing_id'] = df_1_raw.apply(
        lambda row: 'Opted-in' if (row['LID'], row['Offer ID']) in optin_combinations else 'Not opted-in', 
        axis=1
    )
    df_1_raw=df_1_raw.drop_duplicates()
    df_1_raw

    # Make unique combinations of seller_id and cms_vertical from the base file and cross check in the optin report
    optin_combinations = set(df_optin['seller_id'])
    df_1_raw['Opted-in seller_id'] = df_1_raw.apply(
        lambda row: 'Opted-in' if (row['Seller Id']) in optin_combinations else 'Not opted-in', 
        axis=1
    )
    df_1_raw=df_1_raw.drop_duplicates()
    df_1_raw

    # # Map orders for DW - this is a one time run only.
    # df_orders=pd.read_csv('/home/thotat.vc/analytics_scripts/FPO_GSMTP_MFR_MSELLER_UPDATED_ORDERS.csv')
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

    # df_1_edited=df_1_raw[df_1_raw['Offer ID']!='Manual Pricing']
    df_1_edited = df_1_raw

    # Make summary at KAM level from result - which contains LID, SID, CMS vert, Orders, Optin of both seller and LID and add the necessary aggregations.
    unique_listing_ids_df_summary=df_1_edited.groupby(['Deal Callout','Day']).agg(
        listing_id=('LID', 'nunique'),
        opted_in_listing_id=('LID', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in listing_id'] == 'Opted-in']).nunique()),
        seller_id=('Seller Id','nunique'),
        opted_in_seller_id=('Seller Id', lambda x: (x[df_1_edited.loc[x.index, 'Opted-in seller_id'] == 'Opted-in']).nunique())
    ).reset_index()

    import numpy as np
    unique_listing_ids_df_summary['Opt In LID %']=np.where(unique_listing_ids_df_summary['listing_id']==0, 0, unique_listing_ids_df_summary['opted_in_listing_id']/unique_listing_ids_df_summary['listing_id']).round(2)
    unique_listing_ids_df_summary['Opt In SID %']=np.where(unique_listing_ids_df_summary['seller_id']==0, 0, unique_listing_ids_df_summary['opted_in_seller_id']/unique_listing_ids_df_summary['seller_id']).round(2)
    # unique_listing_ids_df_summary['DW %']=np.where(unique_listing_ids_df_summary['orders']==0, 0, unique_listing_ids_df_summary['opted_in_orders']/unique_listing_ids_df_summary['orders']).round(2)
    unique_listing_ids_df_summary['Opt In LID %'] = (unique_listing_ids_df_summary['Opt In LID %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary['Opt In SID %'] = (unique_listing_ids_df_summary['Opt In SID %'] * 100).round(2).astype(str) + '%'
    # unique_listing_ids_df_summary['DW %'] = (unique_listing_ids_df_summary['DW %'] * 100).round(2).astype(str) + '%'
    unique_listing_ids_df_summary

    total_row = pd.Series({
        'Deal Callout': 'Total',
        'Day':'',
        'listing_id': df_1_edited['LID'].nunique(),
        'opted_in_listing_id': df_1_edited[df_1_edited['Opted-in listing_id'] == 'Opted-in']['LID'].nunique(),
        'seller_id': df_1_edited['Seller Id'].nunique(),
        'opted_in_seller_id': df_1_edited[df_1_edited['Opted-in seller_id'] == 'Opted-in']['Seller Id'].nunique()
    })

    total_row_df = pd.DataFrame([total_row])

    total_row_df['Opt In LID %'] = np.where(
        total_row_df['listing_id'] == 0, 0, 
        total_row_df['opted_in_listing_id'] / total_row_df['listing_id']
    ).round(2)

    total_row_df['Opt In SID %'] = np.where(
        total_row_df['seller_id'] == 0, 0, 
        total_row_df['opted_in_seller_id'] / total_row_df['seller_id']
    ).round(2)

    # total_row_df['DW %'] = np.where(
    #     total_row_df['orders'] == 0, 0, 
    #     total_row_df['opted_in_orders'] / total_row_df['orders']
    # ).round(2)

    for col in ['Opt In LID %', 'Opt In SID %']:
        total_row_df[col] = (total_row_df[col] * 100).round(2).astype(str) + '%'

    unique_listing_ids_df_summary_2 = pd.concat([total_row_df, unique_listing_ids_df_summary], ignore_index=True)
    unique_listing_ids_df_summary_2

    # # Add if KAM column is blank
    # unique_listing_ids_df_summary_2=unique_listing_ids_df_summary_2[unique_listing_ids_df_summary_2['Deal Callout']=='Total']

    # Build the HTML table with grid lines
    html_table = """
    <table class="left-aligned-table" style="width:100%; border-collapse: collapse; border: 1px solid #ddd;">
        <thead>
            <tr>
                <th style="border: 1px solid #ddd; padding: 8px;">Deal Callout</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Day</th>
                <th style="border: 1px solid #ddd; padding: 8px;">listing_id</th>
                <th style="border: 1px solid #ddd; padding: 8px;">opted_in_listing_id</th>
                <th style="border: 1px solid #ddd; padding: 8px;">seller_id</th>
                <th style="border: 1px solid #ddd; padding: 8px;">opted_in_seller_id</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Opt In LID %</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Opt In SID %</th>
            </tr>
        </thead>
        <tbody>
    """

    # Loop through the dataframe to add rows with conditional formatting
    for index, row in unique_listing_ids_df_summary_2.iterrows():
        html_table += f'''
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['Deal Callout']}</td>
                 <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['Day']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['listing_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_listing_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['seller_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['opted_in_seller_id']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['Opt In LID %']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{row['Opt In SID %']}</td>
                                                                                            
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

    with pd.ExcelWriter(f"/home/thotat.vc/Downloads/Clearance_Deal_Offers_{today}.xlsx") as writer:
        df_1_edited.to_excel(writer, sheet_name='Raw',index=False)
        unique_listing_ids_df_summary_2.to_excel(writer, sheet_name='Summary',index=False)

    filename = f"Clearance_Deal_Offers_{today}.xlsx"
    file_path = f"/home/thotat.vc/Downloads/Clearance_Deal_Offers_{today}.xlsx"   

    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name('/home/thotat.vc/analytics_scripts/creds.json', scope)
    # client = gspread.authorize(creds)
    http = creds.authorize(httplib2.Http(timeout=600))
    drive_service = build('drive', 'v3', http=http)

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

    # file_id = navigate_to_folder_and_upload_file('Optin Reports - Output', file_path)
    # link = create_shareable_link(drive_service, file_id)

    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.application import MIMEApplication
    from datetime import datetime as dt
    import os

    sender_email = 'thotat.vc@flipkart.com'
    sender_password = 'jljmydqrxxmtdtvz'

    # Original message metadata
    original_from = 'thotat.vc@flipkart.com'
    # original_to = ['shopsy_buy@flipkart.com', 'shopsy_buy_cr@flipkart.com', 'jain.dhruv@flipkart.com']
    original_to = ['thotat.vc@flipkart.com']
    original_cc = ['thotat.vc@flipkart.com']
    original_message_id = '<CAFTNLLYy7y4j90qYt5fp3TYbkqW4BEVLifmXB8=20T1R9NtfGA@mail.gmail.com>'
    

    all_recipients = set([original_from] + original_to + original_cc)

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(original_to)
    msg['Cc'] = ', '.join(original_cc)
    msg['Subject'] = f"Clearance FPO Offers for May-June"
    msg['In-Reply-To'] = original_message_id
    msg['References'] = original_message_id

    body = f"""
    Hi Team,<br><br>
    Please find below the optin status for Clearance Deals:<br><br>
    {html_table}<br><br>
    The attached Excel file contains the raw and summary data for your reference.<br><br>
    <b>Regards,<br>
    Vamsi</b>
    """
    msg.attach(MIMEText(body, 'html'))

    if os.path.exists(file_path):
        with open(file_path, 'rb') as f:
            file_attachment = MIMEApplication(f.read(), _subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            file_attachment.add_header('Content-Disposition', 'attachment', filename=filename)
            msg.attach(file_attachment)
    else:
        print(f"File not found: {file_path}")


    try:
        server = smtplib.SMTP('127.0.0.1', 25)
        server.set_debuglevel(1)
        server.sendmail(sender_email, list(all_recipients), msg.as_string())
        print("Reply-All email with HTML table and Excel attachment sent successfully!")
    except smtplib.SMTPException as e:
        print(f"Error sending email: {e}")
    finally:
        server.quit()
