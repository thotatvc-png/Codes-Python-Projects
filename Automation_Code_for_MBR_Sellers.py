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
WITH FirstOrders AS (
    SELECT 
        a.seller_id,
        a.order_date_key,
        a.analytic_category,
        a.analytic_super_category,
        a.analytic_business_unit,
        COALESCE(map1.owner, 'UM') AS owner,
        ROW_NUMBER() OVER (PARTITION BY a.seller_id ORDER BY a.order_date_key) AS rn
    FROM bigfoot_external_neo.cp_bi_prod_sales__forward_unit_history_fact a
    LEFT JOIN fdp_uploads.ds_fkint_2gud_partner_sellermapping_1_0 map1 
        ON map1.seller_id = a.seller_id
    WHERE a.order_date_key BETWEEN 20210101 AND lookup_date(current_date)
      AND LOWER(a.analytic_business_unit) IN ('shopsy')
      AND LOWER(a.status) NOT IN ('cancelled', 'null', 'rejected', 'on_hold', 'fse_hold', 'approval_hold', 'created')
      AND a.is_shopsy_order = TRUE
      AND UPPER(a.marketplace_id) = 'FLIPKART'
      AND LOWER(a.type) != 'service'
      AND a.category_id NOT IN (21726, 21651)
      AND a.is_freebie = FALSE
      AND (a.replacement_for_unit IS NULL OR a.replacement_for_unit = 'not_replacement')
      AND (a.exchange_for_unit IS NULL OR a.exchange_for_unit = 'not_exchange')
),

FinalTBL AS (
    SELECT 
        seller_id as seller_id, 
        analytic_category as analytic_category,
        analytic_super_category as analytic_super_category,
        owner as owner,
        analytic_business_unit as analytic_business_unit,
        order_date_key as order_date_key
    FROM FirstOrders
    WHERE rn = 1 
      AND order_date_key BETWEEN lookup_date(trunc(add_months(current_date, -1), 'MM'))
                    AND lookup_date(date_sub(current_date,1))
    ORDER BY seller_id
)
SELECT 
seller_id,
analytic_category,
analytic_super_category,
owner,
analytic_business_unit,
order_date_key
from 
FinalTBL
'''
data = pd.read_sql_query(sql1, con=conn)
df_listing=pd.DataFrame(data)
df_listing.to_csv(r'/home/thotat.vc/Downloads/MBR_transacting_sellers_data')
# df_listing=pd.read_csv(r'/home/thotat.vc/Downloads/MBR_transacting_sellers_data')
print(df_listing)

from datetime import datetime as dt2
now = dt2.now()
today=dt2.now().strftime('%d_%b_%Y')
week_number = now.strftime('%U')
current_month = now.strftime('%B')
# previous_month = (now - relativedelta(months=1)).strftime('%B')

filename = f"MBR_transacting_sellers_for_{current_month}_{today}.csv"
file_path = f"/home/thotat.vc/Downloads/{filename}"

df_listing.to_csv(file_path,index=False)




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
    folder_id = '1QvUlecPWAJ8tVCHnDOqEWFH0vZTycwSD'  # Replace with actual folder ID
    # file_path = f"/home/thotat.vc/Downloads/MBR_transacting_sellers_for_previous_month_{today}.csv"
    file_path = f"/home/thotat.vc/Downloads/MBR_transacting_sellers_for_{current_month}_{today}.csv"
  # Replace with actual file path

    upload_to_shared_drive_folder(drive_id, folder_id,file_path) #file_path
    

    df_mbr_owner=df_listing.groupby('owner').agg({'seller_id':'count'}).reset_index()
    df_mbr_supercat=df_listing.groupby('analytic_super_category').agg({'seller_id':'count'}).reset_index()


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
    dataframes = [df_mbr_owner,df_mbr_supercat]

    start_row = 99  # Start pasting at A59
    gap = 3         # Number of empty rows between tables

    # Optional: Clear the sheet first if you want a fresh start
    # worksheet.clear() 

    for i, df in enumerate(dataframes):
        # Upload the dataframe with headers
        set_with_dataframe(worksheet, df, row=start_row, col=1, include_column_header=True)
        
        # Calculate the next starting row
        rows_added = len(df) + 1  # +1 for header
        start_row += rows_added + gap

    print(f"Successfully uploaded {len(dataframes)} dataframes starting at A59 with {gap}-row gaps.")


