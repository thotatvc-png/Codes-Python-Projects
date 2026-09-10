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
SELECT * FROM (
  WITH seller_first_listing AS (
    SELECT
      curr_listing_fact.listing_created_on AS date_data,
      s.seller_id,
      s.status,
      s.display_name,
      s.business_name,
      s.darwin_tier,
      b.analytic_business_unit,
      b.analytic_super_category,
      b.analytic_category,
      b.analytic_vertical,
      b.cms_vertical,
      COALESCE(map1.owner, 'UM') AS owner,
      ROW_NUMBER() OVER (PARTITION BY s.seller_id ORDER BY curr_listing_fact.listing_created_on ASC) AS rn
    FROM bigfoot_external_neo.sp_product__listing_hive_dim curr_listing_fact
    LEFT JOIN bigfoot_external_neo.sp_product__product_categorization_hive_dim b
      ON curr_listing_fact.product_id = b.product_id
    LEFT JOIN bigfoot_external_neo.sp_seller__seller_hive_dim s
      ON curr_listing_fact.seller_id = s.seller_id
    LEFT JOIN fdp_uploads.ds_fkint_2gud_partner_sellermapping_1_0 map1 
      ON s.seller_id = map1.seller_id
    WHERE 
      LOWER(b.analytic_business_unit) IN ('shopsy')
      AND LOWER(s.status) = 'active'
  )

  SELECT
    COUNT(DISTINCT seller_id) AS overall_new_seller_count,
    COUNT(DISTINCT CASE WHEN (LOWER(owner) IN ('um') OR LOWER(owner) IS NULL) THEN seller_id END) AS um_sellers,
    COUNT(DISTINCT CASE WHEN LOWER(owner) = 're' THEN seller_id END) AS re_sellers,
    COUNT(DISTINCT CASE WHEN LOWER(owner) = 'kam' THEN seller_id END) AS kam_sellers,
    COUNT(DISTINCT CASE WHEN LOWER(analytic_super_category) = 'shopsybgm' THEN seller_id END) AS bgm_sellers,
    COUNT(DISTINCT CASE WHEN LOWER(analytic_super_category) = 'shopsyelectronics' THEN seller_id END) AS electronics_sellers,
    COUNT(DISTINCT CASE WHEN LOWER(analytic_super_category) = 'shopsyhome' THEN seller_id END) AS home_sellers,
    COUNT(DISTINCT CASE WHEN LOWER(analytic_super_category) = 'shopsylifestyle' THEN seller_id END) AS ls_sellers
  FROM seller_first_listing
  WHERE YEAR(date_data) = 2026
    AND date_data between 
  date_sub(current_date, 8)  -- Last week's Monday
      AND 
      date_sub(current_date, 1) -- Last week's Sunday
    AND rn = 1
) qaas_injected_alias
'''
data = pd.read_sql_query(sql1, con=conn)
df_listing=pd.DataFrame(data)
print(df_listing)

# df_listing=pd.read_csv(f'/home/thotat.vc/Downloads/Onboarding_data_for_18_Feb_2026_week_07.csv')

from datetime import datetime as dt2
now = dt2.now()
today=dt2.now().strftime('%d_%b_%Y')
week_number = now.strftime('%U')


filename = f"Onboarding_data_for_{today}_week_{week_number}.csv"
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
    folder_id = '1aMaBTZMx32WtwN7u-RN9X0LrD6EZBd88'  # Replace with actual folder ID
    file_path = f"/home/thotat.vc/Downloads/Onboarding_data_for_{today}_week_{week_number}.csv"
  # Replace with actual file path

    upload_to_shared_drive_folder(drive_id, folder_id,file_path) #file_path

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

# Google Sheets authentication
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_path = r'/home/thotat.vc/analytics_scripts/creds.json'
creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
client = gspread.authorize(creds)

# Open the spreadsheet and worksheet
spreadsheet = client.open("Shopsy Central Supply Dashboard")
worksheet = spreadsheet.worksheet("Onboarding_Sellers")

# --- RANGE OVERWRITE LOGIC ---

# 1. Slice the dataframe: 0:4 gives us 4 rows (1 header + 3 data rows)
# 0:13 gives us 13 columns (A through M)
sliced_df = df_listing.iloc[0:4, 0:13]

# 2. Clear the specific range A1:M4 
# Since batch_clear failed, we use the standard 'clear' on a specific range:
# Note: In older gspread, you specify the range inside the selection
worksheet.format("A1:M4", {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}) # Optional: Reset format
# This is the most compatible way to clear a specific box:
cell_list = worksheet.range('A1:M4')
for cell in cell_list:
    cell.value = ''
worksheet.update_cells(cell_list)

# 3. Upload the sliced data starting at A1
set_with_dataframe(worksheet, sliced_df, row=1, col=1, include_column_header=True)

print("The range A1:M4 has been overwritten successfully.")

