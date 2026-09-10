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

# Format today's date as YYYYMMDD (e.g., 20250908)
today_str = datetime.today().strftime('%Y%m%d')


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
sElEcT * fRoM (with base_lids as (
select distinct analytic_vertical,
product_id,
seller_id,
listing_id,
ai_flag,
flipkart_selling_price,
case when flipkart_selling_price <=100 then '0-100'
     when flipkart_selling_price <=200 then '100-200'
     when flipkart_selling_price <=300 then '200-300'
     when flipkart_selling_price <=400 then '300-400'
     when flipkart_selling_price <=600 then '400-600'
else '600+' end as fsp_bucket,
lid_fsp_rnk
from (
select distinct analytic_vertical,
product_id,
seller_id,
a.listing_id,
case when lower(listing_status) = 'active' and coalesce(a.atp,0)>0 then 1 else 0 end as ai_flag,
flipkart_selling_price,
ROW_NUMBER() Over(partition by product_id order by flipkart_selling_price asc) as lid_fsp_rnk
from bigfoot_external_neo.analytics_cdo__shopsy_listing_history_fact as a 
where lower(analytic_business_unit)='shopsy' 
and flipkart_selling_price is not null 
AND lower(listing_status)='active'
And coalesce(a.atp,0)>0
AND date_key in ({today_str})
AND lower(analytic_vertical) not in  ('shopsymobileprotectiondesignercasecover','shopsybook','shopsybikebodycover','shopsymobileprotectionplaincasecover','shopsywatchcombo','shopsycarbodycover','shopsymobileskin','shopsymobileprotectionmobilescreenguard','shopsycameralensprotector','shopsysmartwatchstraps','shopsysmartwatchscreenguard','shopsyplantsapling','shopsyplantseed')
) adf 
),
image_rnk as (
select distinct product_id
from (
select distinct a.product_id,
flipkart_selling_price,
images.primary_image_md5,
ROW_NUMBER() OVER (PARTITION by images.primary_image_md5 order by flipkart_selling_price asc) as img_rn
from base_lids as a 
join (select distinct product_id, primary_image_md5
from bigfoot_external_neo.analytics_cse__shopsy_productid_primary_imageid_mapping_fact) images on a.product_id=images.product_id
where lid_fsp_rnk=1
) adf 
where img_rn=1
),
    
fk_seller_sqs as (
select seller_id,
MAX(overall_score) as fk_sqs
from bigfoot_external_neo.sp_analytics__seller_score_fact
group by seller_id
),

rating as (
select  seller_id,
num_ratings,
avg_rating as shopsy_seller_rating,
count(1)
from bigfoot_external_neo.analytics_cse__shopsy_seller_id_average_rating_fact
where num_ratings>=50
group by seller_id,
num_ratings,
avg_rating
),
base_final as (
select distinct a.analytic_vertical,
a.listing_id,
case when (shopsy_seller_rating >= 3.5 or (shopsy_seller_rating is null and fk_sqs is null)
or (shopsy_seller_rating is null and fk_sqs >= 3)) and  (b.product_id is not null) then 1 else 0 end as dw_overall_flag, -- DW Overall
case when (shopsy_seller_rating >= 3.5 or (shopsy_seller_rating is null and fk_sqs is null)
or (shopsy_seller_rating is null and fk_sqs >= 3)) then 1 else 0 end as dw_seller_flag_standalone,
case when (b.product_id is not null) then 1 else 0 end as dw_image_flag_standalone,
case when (shopsy_seller_rating < 3.5 or (shopsy_seller_rating is null and fk_sqs < 3)) then 1 else 0 end as dw_inverse_seller_flag_standalone,
map1.owner
from base_lids as a 
left join image_rnk as b on a.product_id=b.product_id
left join rating on a.seller_id=rating.seller_id
left join  fk_seller_sqs on a.seller_id = fk_seller_sqs.seller_id
LEFT JOIN fdp_uploads.ds_fkint_2gud_partner_sellermapping_1_0 map1 ON a.seller_id = map1.seller_id
)
select count(distinct a.listing_id) as total_listings_active_instock,
count(distinct case when (a.owner is not null) then a.listing_id end) as managed_active_instock_listings,
count(distinct case when a.dw_overall_flag=1 then a.listing_id end) as dw_active_instock_listings,
count(distinct case when a.dw_overall_flag=1 and (a.owner is not null) then a.listing_id end) as managed_dw_active_instock_listings,
count(distinct case when a.dw_seller_flag_standalone=1 then a.listing_id end) as seller_dw_active_instock_listings,
count(distinct case when a.dw_seller_flag_standalone=1 and (a.owner is not null) then a.listing_id end) as managed_seller_dw_active_instock_listings,	
count(distinct case when a.dw_image_flag_standalone=1 then a.listing_id end) as image_dw_active_instock_listings,
count(distinct case when a.dw_image_flag_standalone=1 and (a.owner is not null) then a.listing_id end) as managed_image_dw_active_instock_listings,
count(distinct case when a.dw_inverse_seller_flag_standalone=1 then a.listing_id end) as inverse_seller_dw_active_instock_listings,
count(distinct case when a.dw_inverse_seller_flag_standalone=1 and (a.owner is not null) then a.listing_id end) as managed_inverse_seller_dw_active_instock_listings
from base_final a
) qaas_injected_alias
'''
data = pd.read_sql_query(sql1, con=conn)
df_listing=pd.DataFrame(data)
print(df_listing)

# df_listing=pd.read_csv(r'/home/thotat.vc/Downloads/A+I_Discovery_data_19_May_2026_Week_20.csv')

from datetime import datetime as dt2

now = dt2.now()
today = now.strftime('%d_%b_%Y')
week_number = now.strftime('%U')  # Or '%W' if you prefer Monday as the start

filename = f"A+I_Discovery_data_{today}_Week_{week_number}.csv"
file_path = f"/home/thotat.vc/Downloads/{filename}"

# Save the file with the correct extension
df_listing.to_csv(file_path, index=False)


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
    folder_id = '1xICS3F7dtb_t6IbpbiInaBJx1DPVF2sk'  # Replace with actual folder ID
    file_path = f"/home/thotat.vc/Downloads/A+I_Discovery_data_{today}_Week_{week_number}.csv"
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

# --- RANGE OVERWRITE LOGIC (D11:K13) ---

# 1. Slice the dataframe: 3 rows × 7 columns
# D=4th column, K=11th column → total 8 columns
# Rows 11–13 → total 3 rows
sliced_df = df_listing.iloc[0:3, 3:11]

# 2. Clear the specific range D11:K13
cell_list = worksheet.range('D11:K13')
for cell in cell_list:
    cell.value = ''
worksheet.update_cells(cell_list)

# 3. Upload the sliced data starting at D11 (row=11, col=4)
set_with_dataframe(worksheet, sliced_df, row=11, col=4, include_column_header=True)

print("The range D11:K13 has been overwritten successfully.")


 
