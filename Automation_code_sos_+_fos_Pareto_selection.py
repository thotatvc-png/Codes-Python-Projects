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

from datetime import datetime

current_month = datetime.today().month


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



sql1=f'''
SELECT
 MONTH(Sales.unit_creation_date_time) AS unit_creation_month,
 YEAR(Sales.unit_creation_date_time) AS unit_creation_year,
 Sales.analytic_super_category as analytic_super_category,
 Sales.analytic_business_unit as analytic_business_unit,
 map1.owner as owner,
 Sales.listing_id as listing_id,
 count(distinct Sales.order_id) as orders
FROM bigfoot_external_neo.cp_bi_prod_sales__forward_unit_history_fact Sales
left join fdp_uploads.ds_fkint_2gud_partner_sellermapping_1_0 map1 on Sales.seller_id = map1.seller_id
WHERE 
Sales.order_date_key between  lookup_date(trunc(add_months(current_date, -1), 'MM'))
                    AND lookup_date(date_sub(current_date,1))
AND lookup_date(Sales.unit_creation_date_time) BETWEEN lookup_date(trunc(add_months(current_date, -1), 'MM'))
                    AND lookup_date(date_sub(current_date,1))
    AND LOWER(Sales.status) NOT IN ('cancelled', 'null', 'rejected', 'on_hold', 'fse_hold', 'approval_hold', 'created')
    AND Sales.is_shopsy_order = TRUE
    AND UPPER(Sales.marketplace_id) = 'FLIPKART'
    AND LOWER(Sales.type) != 'service'
    AND Sales.category_id NOT IN (21726, 21651)
    AND Sales.is_freebie = FALSE 
    AND (Sales.replacement_for_unit IS NULL OR Sales.replacement_for_unit = 'not_replacement')
    AND (Sales.exchange_for_unit IS NULL OR Sales.exchange_for_unit = 'not_exchange')
GROUP BY 
  MONTH(Sales.unit_creation_date_time),
  YEAR(Sales.unit_creation_date_time),
  Sales.analytic_super_category,
  Sales.analytic_business_unit,
  map1.owner,
  Sales.listing_id
'''
data = pd.read_sql_query(sql1, con=conn)
df=pd.DataFrame(data)
print(df)

# file=r"/home/thotat.vc/analytics_scripts/ed3172207a93914164b2e949d8ed65b8_csv.csv"
# df=pd.read_csv(file)

from datetime import datetime as dt2
today=dt2.now().strftime('%d_%b_%Y')

df.to_csv(f"/home/thotat.vc/Downloads/Pareto_SOS_+_FOS_{today}.csv",index=False)

df['unit_creation_month'] = df['unit_creation_month'].fillna('').astype(str)
df['unit_creation_year'] = df['unit_creation_year'].fillna('').astype(str)
df['owner'] = df['owner'].fillna('UM').astype(str)
df['unit_creation_month_year'] = pd.concat([df['unit_creation_month'], df['unit_creation_year']], axis=1).agg('-'.join, axis=1)
df['unit_creation_month_year'].unique()
import numpy as np
df['flag'] = np.where(df['analytic_business_unit'].isna(), 
                      'FK', 
                      np.where(df['analytic_business_unit'].str.contains('Shopsy', case=False, na=False), 
                               'Shopsy', 
                               'FK'))

# df=df[df['flag']=='Shopsy'] ################################################Change for SOS $ (SOS+FOS)
def pareto_analysis(df, month):
    vertical_summary = df[df['unit_creation_month_year'] == month].groupby(
        ['unit_creation_month_year', 'owner', 'analytic_super_category', 'listing_id', 'analytic_business_unit']
    ).agg(
        total_orders=('orders', 'sum')
    ).reset_index()

    vertical_summary = vertical_summary.sort_values(
        by='total_orders', ascending=False
    ).reset_index(drop=True)

    vertical_summary['cumulative_orders'] = vertical_summary['total_orders'].cumsum()

    total_orders_in_month = vertical_summary['total_orders'].sum()

    vertical_summary['cumulative_percentage'] = (vertical_summary['cumulative_orders'] / total_orders_in_month) * 100

    pareto_thresholds = [30, 50, 60, 80]
    for threshold in pareto_thresholds:
        vertical_summary[f'pareto_{threshold}'] = (vertical_summary['cumulative_percentage'] <= threshold).astype(int)

    return vertical_summary
all_months_summary = []


for month in df['unit_creation_month_year'].unique():
    summary = pareto_analysis(df, month)
    summary['unit_creation_month_year'] = month  
    all_months_summary.append(summary)


final_summary = pd.concat(all_months_summary)


mom_summary = final_summary.groupby(
    ['unit_creation_month_year', 'analytic_super_category', 'analytic_business_unit', 'owner']
).agg(
    total_distinct_listings=('listing_id', 'nunique'),
    pareto_30_count=('pareto_30', 'sum'),
    pareto_50_count=('pareto_50', 'sum'),
    pareto_60_count=('pareto_60', 'sum'),
    pareto_80_count=('pareto_80', 'sum')
).reset_index()

condition = mom_summary['analytic_super_category'].str.contains('Shopsy', case=False, na=False)

# Result: Replace 'Shopsy' with nothing, else use the business_unit column
mom_summary['final_flag'] = np.where(
    condition, 
    mom_summary['analytic_super_category'].str.replace('Shopsy', '', case=False, regex=False), 
    mom_summary['analytic_business_unit']
)


# 1. Create your summary
owner_summary = mom_summary.groupby('owner').agg({'pareto_80_count':'sum'}).reset_index()

# 2. Add the Total row
owner_summary.loc[len(owner_summary)] = ['Total', owner_summary['pareto_80_count'].sum()]

# 1. Create your summary
bu_summary = mom_summary.groupby('final_flag').agg({'pareto_80_count':'sum'}).reset_index()

# 2. Add the Total row
bu_summary.loc[len(bu_summary)] = ['Total', bu_summary['pareto_80_count'].sum()]

# 1. Create your summary
owner_summary_60 = mom_summary.groupby('owner').agg({'pareto_60_count':'sum'}).reset_index()

# 2. Add the Total row
owner_summary_60.loc[len(owner_summary_60)] = ['Total', owner_summary_60['pareto_60_count'].sum()]

# 1. Create your summary
bu_summary_60 = mom_summary.groupby('final_flag').agg({'pareto_60_count':'sum'}).reset_index()

# 2. Add the Total row
bu_summary_60.loc[len(bu_summary_60)] = ['Total', bu_summary_60['pareto_60_count'].sum()]

today = dt.now().strftime('%d_%b_%Y')
file = fr'/home/thotat.vc/Downloads/Pareto_monthly_FOS_+_SOS_{today}.xlsx'

# Specify engine explicitly
with pd.ExcelWriter(file) as writer:
    owner_summary.to_excel(writer, sheet_name='bu_summary_80', index=False)
    bu_summary.to_excel(writer, sheet_name='Owner_Summary_80', index=False)
    owner_summary_60.to_excel(writer, sheet_name='Owner_Summary_60', index=False)
    bu_summary_60.to_excel(writer, sheet_name='bu_Summary_60', index=False)



import os
import numpy as np
import matplotlib.pyplot as plt
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime as dt2
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account

# ---- Date for filename ----
today = dt2.now().strftime('%d_%b_%Y')
# Use SAME path as where you saved the file
file_path = f"/home/thotat.vc/Downloads/Pareto_monthly_FOS_+_SOS_{today}.xlsx"

# ---- Setup Google Drive API ----
SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = '/home/thotat.vc/analytics_scripts/creds.json'  # Replace with your service account file path

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)

drive_service = build('drive', 'v3', credentials=credentials)

# ---- Upload file to folder inside Shared Drive ----
def upload_file_to_shared_drive_folder(file_path, folder_id):
    """Upload a file to a folder inside a shared drive."""
    file_metadata = {
        'name': os.path.basename(file_path),
        'parents': [folder_id]
    }
    media = MediaFileUpload(file_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

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
def upload_to_shared_drive_folder(folder_id, file_path):
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
    folder_id = '14kAPumXT2hvK69Me60R6_T8fz2pZSx4-'  # Replace with actual folder ID
    upload_to_shared_drive_folder(folder_id, file_path)

#>>>>>>>>>>>>>>>>>>>>>---G-SHEET-CODE---<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

# Google Sheets authentication
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_path = r'/home/thotat.vc/analytics_scripts/creds.json'
creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
client = gspread.authorize(creds)

# Open the spreadsheet and worksheet
spreadsheet = client.open("Monthly Business Review-Buy")
worksheet = spreadsheet.worksheet("Pareto_raw")

# --- MULTI-DATAFRAME STACKING LOGIC ---

# Put your dataframes into a list
dataframes = [owner_summary,bu_summary,owner_summary_60,bu_summary_60]  # Replace with your actual DF names

start_row = 5  # Initial starting point (A5)
gap = 3        # Number of empty rows between tables

# Optional: Clear the sheet first if you want a fresh start
# worksheet.clear() 

for i, df in enumerate(dataframes):
    # 1. Upload the dataframe
    # include_column_header=True only for the first one? Or all? 
    # Setting it to True for all so each table has its own header.
    set_with_dataframe(worksheet, df, row=start_row, col=1, include_column_header=True)
    
    # 2. Calculate the next starting row
    # Length of DF + 1 (for header) + the gap
    rows_added = len(df) + 1 
    start_row += rows_added + gap

print(f"Successfully uploaded {len(dataframes)} dataframes with {gap}-row gaps.")

