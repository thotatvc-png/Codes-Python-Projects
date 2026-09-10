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
  a.seller_id as seller_id,
  sllr.display_name as display_name,
  b.analytic_business_unit as analytic_business_unit,
  b.analytic_super_category as analytic_super_category,
  b.analytic_category as analytic_category,
  b.analytic_sub_category as analytic_sub_category,
  b.analytic_vertical as analytic_vertical,
  b.cms_vertical as cms_vertical,
  b.title as title,
  prod.type as type,
  a.listing_id as listing_id,
  p.is_eligible as is_eligible,
  a.product_id as product_id,
  CASE
    WHEN LOWER(b.analytic_super_category) = 'shopsylifestyle' 
         AND LOWER(b.analytic_category) IN (
           'na', 'shopsykidclothing', 'shopsymensclothingcasualtopwear',
           'shopsymensclothingessentialsandethnic', 'shopsymensclothingjeans',
           'shopsymensclothingsmartandformals', 'shopsywomenethniccontemporary',
           'shopsywomenethniccore', 'shopsywomenwesterncore'
         ) THEN 'ShopsyLifeStyle Apparel'
    WHEN LOWER(b.analytic_super_category) = 'shopsylifestyle' 
         AND LOWER(b.analytic_category) NOT IN (
           'na', 'shopsykidclothing', 'shopsymensclothingcasualtopwear',
           'shopsymensclothingessentialsandethnic', 'shopsymensclothingjeans',
           'shopsymensclothingsmartandformals', 'shopsywomenethniccontemporary',
           'shopsywomenethniccore', 'shopsywomenwesterncore'
         ) THEN 'ShopsyLifeStyle Non-Apparel'
    ELSE b.analytic_super_category
  END AS anlytic_super_category_l2,
  CASE
    WHEN LOWER(b.analytic_category) IN (
      'shopsymobileprotectiondesignercasecover','shopsybook','shopsybikebodycover',
      'shopsymobileprotectionplaincasecover','shopsywatchcombo','shopsycarbodycover',
      'shopsymobileskin','shopsymobileprotectionmobilescreenguard',
      'shopsycameralensprotector','shopsysmartwatchstraps',
      'shopsysmartwatchscreenguard','shopsyplantsapling','shopsyplantseed'
    ) THEN 1
    ELSE 0
  END AS junk_flag,
  COALESCE(a.minimum_order_quantity, 0) AS minimum_order_quantity,
  a.listing_created_on as listing_created_on,
  a.procurement_sla as procurement_sla,
  a.national_shipping_fee as national_shipping_fee,
  a.zonal_shipping_fee as zonal_shipping_fee,
  a.local_shipping_fee as local_shipping_fee,
  a.listing_status as listing_status,
  b.brand as brand,
  COALESCE(a.flipkart_selling_price, 0) AS flipkart_selling_price,
  c.darwin_tier as darwin_tier,
  c.seller_tier as seller_tier,
  c.darwin_tier_name_v2,
  x.unit_creation_date as unit_creation_date,
  COALESCE(x.units, 0) AS units,
  COALESCE(x.gmv, 0) AS GMV,
  COALESCE(x.orders, 0) AS orders,
  COALESCE(d.current_atp, 0) AS current_atp
FROM bigfoot_external_neo.sp_product__listing_hive_dim a
LEFT JOIN bigfoot_external_neo.sp_product__slm_min_order_quantity_eligibility_fact p
  ON p.listing_id = a.listing_id
LEFT JOIN bigfoot_external_neo.sp_product__product_categorization_hive_dim b
  ON a.product_id = b.product_id
LEFT JOIN bigfoot_external_neo.sp_darwin__sp_final_tiers_fact c
  ON a.seller_id = c.seller_id
LEFT JOIN bigfoot_external_neo.sp_product__product_attribute_hive_dim prod
  ON a.product_id = prod.product_id
LEFT JOIN bigfoot_external_neo.sp_seller__seller_hive_dim sllr
  ON a.seller_id = sllr.seller_id
LEFT JOIN (
  SELECT 
    listing_id,
    lookup_date(unit_creation_date_time) AS unit_creation_date,
    COUNT(DISTINCT order_id) AS orders,
    SUM(units) AS units,
    SUM(gmv) AS GMV
  FROM bigfoot_external_neo.cp_bi_prod_sales__forward_unit_fact Sales
  WHERE lookup_date(unit_creation_date_time) = lookup_date(DATE_SUB(CURRENT_DATE, 1))
    AND LOWER(Sales.status) NOT IN ('cancelled','null','rejected','on_hold', 'fse_hold','approval_hold','created')
    AND Sales.is_shopsy_order = TRUE
    AND UPPER(Sales.marketplace_id) IN ('FLIPKART')
    AND LOWER(Sales.type) != 'service'
    AND Sales.category_id NOT IN (21726, 21651)
    AND Sales.is_freebie = FALSE
    AND (Sales.replacement_for_unit IS NULL OR Sales.replacement_for_unit = 'not_replacement')
    AND (Sales.exchange_for_unit IS NULL OR Sales.exchange_for_unit = 'not_exchange')
  GROUP BY listing_id, lookup_date(unit_creation_date_time)
) x
  ON a.listing_id = x.listing_id
LEFT JOIN (
  SELECT
    listing_id,
    SUM(final_atp) AS current_atp
  FROM bigfoot_external_neo.mp_sp__listing_atp_inventory_fact
  GROUP BY listing_id
) d
  ON a.listing_id = d.listing_id
WHERE LOWER(b.analytic_business_unit) = 'shopsy'
  AND COALESCE(a.minimum_order_quantity, 0) >= 2
'''
data = pd.read_sql_query(sql1, con=conn)
df=pd.DataFrame(data)
print(df)
today = dt.now().strftime('%d_%b_%Y')
df.to_csv(fr'/home/thotat.vc/Downloads/Auto_MOQ_DATA_{today}.csv')
# df=pd.read_csv(r'/home/thotat.vc/analytics_scripts/Auto_MOQ_DATA_19092025.csv')
summary_df = df.groupby('analytic_category').agg({
    'listing_id': 'nunique',
    'product_id': 'nunique'
}).reset_index()

summary_df.rename(columns={
    'listing_id': 'distinct_listing_id',
    'product_id': 'distinct_product_id'
}, inplace=True)

# Step 2: Create grouped_df (append to E–I)
grouped_df = df.groupby(['analytic_category','unit_creation_date']).agg({
    # 'unit_creation_date': 'min',
    # # 'unit_creation_date': lambda x: list(x),
    # 'unit_creation_date': lambda x: ', '.join(sorted(x.astype(str))[:100]), 
    'units': 'sum',
    'gmv': 'sum',
    'orders': 'sum'
}).reset_index()

grouped_df.rename(columns={
    'unit_creation_date': 'earliest_unit_creation_date',
    'units': 'total_units',
    'gmv': 'total_gmv',
    'orders': 'total_orders'
}, inplace=True)

# Step 3: Google Sheets authentication
#------------------------G-sheet-code-overwritting--------------------------
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

# Google Sheets authentication
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_path =r'/home/thotat.vc/analytics_scripts/creds.json'
creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
client = gspread.authorize(creds)

# Upload pivot_table to the correct worksheet
spreadsheet = client.open("Shopsy MOQ Performance")
worksheet = spreadsheet.worksheet("raw-2")
worksheet.clear()
set_with_dataframe(worksheet, summary_df, include_column_header=True)
print("The data is exported Sucessfully")

#------------------------G-sheet-code-Appending--------------------------

import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
# Step 3: Google Sheets authentication
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_path = r'/home/thotat.vc/analytics_scripts/creds.json'
creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
client = gspread.authorize(creds)
#defining gsheet and tab name here 
sheet = client.open("Shopsy MOQ Performance").worksheet("raw")
col_e_values = sheet.col_values(5)  # Column E is index 5
next_row = len(col_e_values) + 1 if col_e_values else 1  # Start at row 1 if column is empty
set_with_dataframe(sheet, grouped_df, row=next_row, col=5, include_index=False, include_column_header=False, resize=False)
print(f"grouped_df appended successfully starting from cell E{next_row}.")




