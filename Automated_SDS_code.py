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
date_sub(next_day(current_date, 'Sunday'), 14) as `date`, msku_fsp, msku,
bu,
sc,
vertical,
attr_value,
fsp_bucket,
sel_count,
sold_sel_count,
  fsn_count,
  item_count
FROM (
 SELECT    concat(coalesce(bu,""),'|',coalesce(sc,""),'|',coalesce(vertical,""),'|',coalesce(attr_value,""),'|',coalesce(fsp_bucket,"")) as msku_fsp,
 concat(coalesce(bu,""),'|',coalesce(sc,""),'|',coalesce(vertical,""),'|',coalesce(attr_value,"")) as msku,
  bu,
  sc,
  vertical,
  attr_value,
  fsp_bucket,
  fsn_count,
  item_count,
  coalesce(sum(case when lower(bu) = 'lifestyle' then item_count else fsn_count end),0) as sel_count,
  coalesce(sum(case when lower(bu) = 'lifestyle' then sold30_item_count else sold30_fsn_count end),0) as sold_sel_count,
  coalesce(sum(case when lower(bu) = 'lifestyle' then disc30_item_count else disc30_fsn_count end),0) as disc_sel_count,
  coalesce(sum(case when lower(bu) = 'lifestyle' then disc30_500_item_count else disc30_500_fsn_count end),0) as disc_500_sel_count,
  coalesce(sum(l2.units),0) as units ,coalesce(sum(l2.order_items),0) as order_items,
  coalesce(sum(l2.impressions),0) as impressions, coalesce(sum(l2.ppvs),0) as PPVS,
  coalesce(sum(l2.impressions_500),0) as impressions_500, coalesce(sum(l2.ppvs_500),0) as PPVS_500
 FROM 
 (SELECT
bu,
sc,
vertical,
concat(coalesce(msku.attr1_value,""),'|',coalesce(msku.attr2_value,"")) as attr_value,
fsp_bucket,
count(distinct msku.product_id) as fsn_count,
count(distinct msku.itemid) as item_count,
count(distinct case when sales.units > 0 then msku.product_id end) as sold30_fsn_count,
count(distinct case when sales.units > 0 then msku.itemid end) as sold30_item_count,
count(distinct case when traffic.impressions > 0 then msku.product_id end) as disc30_fsn_count,
count(distinct case when traffic.impressions > 0 then msku.itemid end) as disc30_item_count,
count(distinct case when traffic.impressions > 500 then msku.product_id end) as disc30_500_fsn_count,
count(distinct case when traffic.impressions > 500 then msku.itemid end) as disc30_500_item_count,
sum(coalesce(sales.units,0)) as units,
sum(coalesce(sales.order_items,0)) as order_items,
sum(coalesce(traffic.impressions,0)) as impressions,
sum(coalesce(traffic.ppvs,0)) as ppvs,
sum(coalesce(case when traffic.impressions > 500 then traffic.impressions end,0)) as impressions_500,
sum(coalesce(case when traffic.impressions > 500 then traffic.ppvs end,0)) as ppvs_500
FROM
(SELECT
distinct bu,
sc,
vertical,
analytic_vertical,
brand,
itemid,
product_id,
lower(attr1_value) as attr1_value,
lower(attr2_value) as attr2_value,
lower(attr3_value) as attr3_value,
CASE  
WHEN fsp_bucket = '0-100' THEN '000-100'  
ELSE fsp_bucket END AS fsp_bucket,
case when lookup_date(product_creation_date) >= lookup_date(date_sub(current_date(),30)) then 1 else 0 end as fresh_fsn_flag,
row_number() over(partition by product_id order by attr1_value desc, attr2_value desc) as rnk
from bigfoot_external_neo.analytics_cdo__shopsy_csds_msku_fact) msku
inner join 
(select 
product_id 
from bigfoot_external_neo.sp_product__product_categorization_hive_dim 
where lower(cms_vertical) like 'shopsy%'
and lower(analytic_vertical) not in  ('shopsyrakhi','shopsykidcap','shopsymobileprotectiondesignercasecover','shopsybook','shopsybikebodycover','shopsymobileprotectionplaincasecover','shopsywatchcombo','shopsycarbodycover','shopsymobileskin','shopsymobileprotectionmobilescreenguard','shopsycameralensprotector','shopsysmartwatchstraps','shopsysmartwatchscreenguard')
) c on c.product_id = msku.product_id
left join (SELECT product_id,
sum(units) as units,
count(distinct order_item_id) as order_items
FROM bigfoot_external_neo.cp_bi_prod_sales__forward_unit_history_fact fuf
WHERE order_date_key between lookup_date(date_sub(current_date(),32)) and lookup_date(date_sub(current_date(),2))
AND lower(status) in ('in_progress','undelivered','completed','delivered','approved','shipped','ready_to_ship','returned', 'return_requested','activated')
AND type !='service'
AND category_id !=21726
AND category_id !=21651
AND replacement_for_unit IS NULL
AND exchange_for_unit IS NULL
AND is_freebie =FALSE
AND marketplace_id IN ('FLIPKART')
AND is_shopsy_order = True
AND analytic_business_unit = 'Shopsy'
group by product_id)sales on sales.product_id = msku.product_id
left join 
(select
productid,
sum(impressions) as impressions,
sum(primarylistingppvs) as ppvs
from bigfoot_external_neo.cp_cdm_consumer__incr_agg_listing_new_fact cdm
where date_key between lookup_date(date_sub(current_date(),32)) and lookup_date(date_sub(current_date(),2))
and lower(mpid) = 'shopsy'
group by productid) traffic on traffic.productid = msku.product_id
where rnk = 1
group by
bu,
sc,
vertical,
concat(coalesce(msku.attr1_value,""),'|',coalesce(msku.attr2_value,"")),
fsp_bucket) l2 
group by      concat(coalesce(bu,""),'|',coalesce(sc,""),'|',coalesce(vertical,""),'|',coalesce(attr_value,""),'|',coalesce(fsp_bucket,"")),
concat(coalesce(bu,""),'|',coalesce(sc,""),'|',coalesce(vertical,""),'|',coalesce(attr_value,"")),
bu,
sc,
vertical,
attr_value,
  fsn_count,
  item_count,
fsp_bucket
)a
where bu is not null
and lower(bu) <> "null"
'''
data = pd.read_sql_query(sql1, con=conn)
df_listing=pd.DataFrame(data)
print(df_listing)

from datetime import datetime as dt2
today=dt2.now().strftime('%d_%b_%Y')

df_listing.to_csv(f"/home/thotat.vc/Downloads/SDS_DATA_{today}.csv",index=False)


filename = f"SDS_DATA_{today}.csv"
file_path = f"/home/thotat.vc/Downloads/SDS_DATA_{today}.csv"


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
    folder_id = '1-okWdsJw6Xj_zk3rM1zj6_8eVEhAY2Vy'  # Replace with actual folder ID
    file_path = f"/home/thotat.vc/Downloads/SDS_DATA_{today}.csv"
  # Replace with actual file path

    upload_to_shared_drive_folder(drive_id, folder_id,file_path) #file_path

 
