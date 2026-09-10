#---------------------MAIN--CODE---------------------------------
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
SELECT 
    lookup_date(a.listing_created_on) AS day,
    map2.mapping AS mapping,
    a.analytic_business_unit AS analytic_business_unit,
    a.analytic_super_category AS analytic_super_category,
    CASE
        WHEN a.analytic_super_category IN ('ShopsyLifeStyle') 
             AND lower(a.analytic_category) IN (
                'na','shopsykidclothing','shopsymensclothingcasualtopwear',
                'shopsymensclothingessentialsandethnic','shopsymensclothingjeans',
                'shopsymensclothingsmartandformals','shopsywomenethniccontemporary',
                'shopsywomenethniccore','shopsywomenwesterncore','shopsywomenwesterngrowth'
             ) THEN 'ShopsyLifeStyle Apparel'
        WHEN a.analytic_super_category IN ('ShopsyLifeStyle') 
             AND lower(a.analytic_category) NOT IN (
                'na','shopsykidclothing','shopsymensclothingcasualtopwear',
                'shopsymensclothingessentialsandethnic','shopsymensclothingjeans',
                'shopsymensclothingsmartandformals','shopsywomenethniccontemporary',
                'shopsywomenethniccore','shopsywomenwesterncore','shopsywomenwesterngrowth'
             ) THEN 'ShopsyLifeStyle Non-Apparel'
        WHEN a.analytic_super_category IN ('ShopsyBGM') 
             AND lower(a.analytic_category) IN ('shopsygrooming','shopsymakeupfragrances') 
             THEN 'ShopsyBGM BPC'
        WHEN a.analytic_super_category IN ('ShopsyBGM') 
             AND lower(a.analytic_category) NOT IN ('shopsygrooming','shopsymakeupfragrances') 
             THEN 'ShopsyBGM GM'
        ELSE a.analytic_super_category 
    END AS analytic_super_category_l2,
    a.analytic_category AS analytic_category,
    a.analytic_vertical AS analytic_vertical,
    b.cms_vertical AS cms_vertical,
    a.seller_id AS seller_id,
    month(a.listing_created_on) AS created_month,
    year(a.listing_created_on) AS created_year,
    weekofyear(a.listing_created_on) AS created_week,
    CASE 
        WHEN a.flipkart_selling_price <= 100 THEN '0-100'
        WHEN a.flipkart_selling_price BETWEEN 100 AND 200 THEN '100-200'
        WHEN a.flipkart_selling_price BETWEEN 200 AND 300 THEN '200-300'
        WHEN a.flipkart_selling_price BETWEEN 300 AND 400 THEN '300-400'
        WHEN a.flipkart_selling_price >= 400 THEN '400+' 
    END AS price_bucket,
    COALESCE(map1.owner, 'UM') AS owner,
    COALESCE(map1.name, 'UM') AS KAM,
    COUNT(DISTINCT a.listing_id) AS total_listings,
    COUNT(DISTINCT CASE WHEN lower(archive.listing_status)='active' THEN a.listing_id END) AS active_listings,
    COUNT(DISTINCT CASE WHEN upper(archive.listing_status)='ACTIVE' AND COALESCE(archive.atp,0) > 0 THEN a.listing_id END) AS active_instock_listings
FROM 
    bigfoot_external_neo.sp_product__listing_hive_dim a 
LEFT JOIN  
    bigfoot_external_neo.sp_product__product_categorization_hive_dim b 
    ON a.product_id = b.product_id
LEFT JOIN 
    fdp_uploads.ds_fkint_2gud_partner_sellermapping_1_0 map1
    ON a.seller_id = map1.seller_id
LEFT JOIN fdp_uploads.ds_fkint_2gud_partner_new_bu_mapping_shopsy_1_0 map2
    ON a.analytic_category = map2.analytic_category
LEFT JOIN (
    SELECT listing_id, 
           date_key,          
           SUM(atp) AS atp,
           LOWER(listing_status) AS listing_status
    FROM bigfoot_external_neo.analytics_cdo__shopsy_listing_history_fact
    WHERE date_key BETWEEN 20251111 AND lookup_date(date_sub(current_date,1))
    GROUP BY listing_id, date_key, listing_status
) archive 
    ON a.listing_id = archive.listing_id 
   AND lookup_date(a.listing_created_on) = archive.date_key
WHERE 
    a.analytic_business_unit IN ('Shopsy')
    AND lookup_date(a.listing_created_on) BETWEEN 20251111 AND lookup_date(date_sub(current_date,1))
GROUP BY 
    a.analytic_business_unit,
    a.analytic_super_category,
    a.analytic_category,
    a.analytic_vertical,
    b.cms_vertical,
    a.seller_id,
    month(a.listing_created_on),
    year(a.listing_created_on),
    weekofyear(a.listing_created_on),
    CASE 
        WHEN a.flipkart_selling_price <= 100 THEN '0-100'
        WHEN a.flipkart_selling_price BETWEEN 100 AND 200 THEN '100-200'
        WHEN a.flipkart_selling_price BETWEEN 200 AND 300 THEN '200-300'
        WHEN a.flipkart_selling_price BETWEEN 300 AND 400 THEN '300-400'
		WHEN a.flipkart_selling_price >= 400 THEN '400+' 
    END,
    COALESCE(map1.owner, 'UM'),
    COALESCE(map1.name, 'UM'),
    lookup_date(a.listing_created_on),
    map2.mapping,
    CASE
        WHEN a.analytic_super_category IN ('ShopsyLifeStyle') 
             AND lower(a.analytic_category) IN (
                'na','shopsykidclothing','shopsymensclothingcasualtopwear',
                'shopsymensclothingessentialsandethnic','shopsymensclothingjeans',
                'shopsymensclothingsmartandformals','shopsywomenethniccontemporary',
                'shopsywomenethniccore','shopsywomenwesterncore','shopsywomenwesterngrowth'
             ) THEN 'ShopsyLifeStyle Apparel'
        WHEN a.analytic_super_category IN ('ShopsyLifeStyle') 
             AND lower(a.analytic_category) NOT IN (
                'na','shopsykidclothing','shopsymensclothingcasualtopwear',
                'shopsymensclothingessentialsandethnic','shopsymensclothingjeans',
                'shopsymensclothingsmartandformals','shopsywomenethniccontemporary',
                'shopsywomenethniccore','shopsywomenwesterncore','shopsywomenwesterngrowth'
             ) THEN 'ShopsyLifeStyle Non-Apparel'
        WHEN a.analytic_super_category IN ('ShopsyBGM') 
             AND lower(a.analytic_category) IN ('shopsygrooming','shopsymakeupfragrances') 
             THEN 'ShopsyBGM BPC'
        WHEN a.analytic_super_category IN ('ShopsyBGM') 
             AND lower(a.analytic_category) NOT IN ('shopsygrooming','shopsymakeupfragrances') 
             THEN 'ShopsyBGM GM'
        ELSE a.analytic_super_category 
    END
'''
data = pd.read_sql_query(sql1, con=conn)
df=pd.DataFrame(data)

import pandas as pd
file=(r'/home/thotat.vc/analytics_scripts/Active_Listings_RE_TL_NAMES.csv')
df_re=pd.read_csv(file)
print(df_re)

merged_df = pd.merge(df,df_re[['seller_id', 'Re_Name', 'TL']], on='seller_id',how='left') 

today = dt.now().strftime('%d_%b_%Y')
merged_df.to_csv(fr'/home/thotat.vc/Downloads/KAM_LISTING_Raw_{today}.csv')
# df=pd.read_csv(r'/home/thotat.vc/Downloads/Winter_PPv_Search_data')
print(merged_df)




grouped_df = (
    merged_df.groupby([
        'day',
        'mapping',
        'analytic_business_unit',
        'analytic_super_category',
        'analytic_super_category_l2',
        'analytic_category',
        'analytic_vertical',
        'cms_vertical',
        'price_bucket',	'owner','kam','Re_Name','TL'

    ])
    .agg({'total_listings':'sum','active_listings':'sum','active_instock_listings':'sum'})   # example aggregation
    .reset_index()
)

#--------------------------G-SHEET-CODE---------------------------------------------------

import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
# Step 3: Google Sheets authentication
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_path = r'/home/thotat.vc/analytics_scripts/creds.json'
creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
client = gspread.authorize(creds)


# Step 4: Open the Google Sheet and target the "MOQ" tab
sheet = client.open("Shopsy Mega Selection Drive - ND'25").worksheet("Raw")
# Step 6: Append df to next empty row in column E
col_e_values = sheet.col_values(1)  # Column E is index 5
next_row = len(col_e_values) + 1 if col_e_values else 1  # Start at row 1 if column is empty
set_with_dataframe(sheet, grouped_df, row=next_row, col=1, include_index=False, include_column_header=False, resize=False)
print(f"df appended successfully starting from cell E{next_row}.")


#-----------Time_Stamp_Code----------------------------------------------------------

from datetime import datetime
import pytz
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- Setup Google Sheets connection ---
# Define the scope
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_path = r'/home/thotat.vc/analytics_scripts/creds.json'
creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
client = gspread.authorize(creds)
spreadsheet = client.open("Shopsy Mega Selection Drive - ND'25")          # Replace with your Google Sheet name
ws = spreadsheet.worksheet("KAM Leaderboard")   

# --- Get current time in Asia/Kolkata timezone ---
tz = pytz.timezone("Asia/Kolkata")
now = datetime.now(tz)

# --- Update cells ---
# A1 → Compact date format (YYYYMMDD)
ws.update_acell("B1", now.strftime("%Y%m%d"))

# N1 → Full timestamp with microseconds

print("Sheet updated successfully ✅")


#--------------DRIVE--CODE-----------------------------------------------------------------

import os
import pandas as pd
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
file_path = f"/home/thotat.vc/Downloads/KAM_LISTING_Raw_{today}.csv"

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
    folder_id = '1d7aNkNqFgqxPcnI-6hjPezcMNhamAmTX'  # Replace with actual folder ID
    upload_to_shared_drive_folder(folder_id, file_path)


# # Step 4: Open the Google Sheet and target the "MOQ" tab
# sheet = client.open("Listings_Sample").worksheet("raw")
# # Step 6: Append df to next empty row in column E
# col_e_values = sheet.col_values(1)  # Column E is index 5
# next_row = len(col_e_values) + 1 if col_e_values else 1  # Start at row 1 if column is empty
# set_with_dataframe(sheet, grouped_df, row=next_row, col=1, include_index=False, include_column_header=False, resize=False)
# print(f"df appended successfully starting from cell E{next_row}.")

