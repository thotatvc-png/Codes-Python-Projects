import os
import pandas as pd
import jaydebeapi
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from datetime import datetime

# --------------------- CONFIGURATION ---------------------
SERVICE_ACCOUNT_FILE = '/home/thotat.vc/creds.json'
DRIVE_FOLDER_NAME = 'Auto-MOQ'
today_str = datetime.today().strftime('%Y%m%d')
CSV_FILE_PATH = f'MOQ_DATA_RAW_{today_str}.csv'
# --------------------------------------------------------

# Setup CLASSPATH for Hive JDBC
def setup_classpath():
    os.environ['CLASSPATH'] = '/usr/local/fdp-infra-hive/lib/*'

# Connect to Hive using JDBC
def get_hive_connection():
    url = (
        "jdbc:hive2://fkp-fdp-galaxy-sun-zkjn-0001.c.fkp-fdp-galaxy.internal:2181,"
        "fkp-fdp-galaxy-sun-zkjn-0002.c.fkp-fdp-galaxy.internal:2181,"
        "fkp-fdp-galaxy-sun-zkjn-0003.c.fkp-fdp-galaxy.internal:2181/default;"
        "transportMode=http;"
        "httpPath=cliservice;"
        "serviceDiscoveryMode=zooKeeper;"
        "zooKeeperNamespace=fkp-fdp-galaxy-hive3-hs2-agni"
    )
    user = "thotat.vc"
    password = "kiV1thai!@#%"  # Don't hardcode in production
    return jaydebeapi.connect("org.apache.hive.jdbc.HiveDriver", url, {'user': user, 'password': password})

# Load Hive UDFs
def load_hive_functions(cursor):
    cursor.execute("ADD JAR /usr/share/fk-bigfoot-hivejsonserde/json-serde-1.3-SNAPSHOT-jar-with-dependencies.jar")
    cursor.execute("ADD JAR /usr/share/fk-bigfoot-dimlookup/dimlookup-hive-udf-1.0-SNAPSHOT-jar-with-dependencies.jar")
    cursor.execute("ADD JAR gs://fkpdp-mhosy-2nig-1a5d-systemlibs/libraries/hive/jars/hive-udfs-1.0-SNAPSHOT.jar")
    cursor.execute("CREATE TEMPORARY FUNCTION lookup as 'com.flipkart.bigfoot.dimlookup.udf.HiveLookupUDF'")
    cursor.execute("CREATE TEMPORARY FUNCTION lookup_date as 'com.flipkart.bigfoot.dimlookup.udf.DateLookupUDF'")
    cursor.execute("set mapreduce.input.fileinputformat.input.dir.recursive=true")

# Execute query and return DataFrame
def run_query(sql):
    setup_classpath()
    conn = get_hive_connection()
    cursor = conn.cursor()
    load_hive_functions(cursor)
    df = pd.read_sql_query(sql, conn)
    cursor.close()
    conn.close()
    return df

# Upload file to shared Drive folder
def upload_to_drive_shared_folder(file_path, folder_name):
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=['https://www.googleapis.com/auth/drive']
    )
    drive_service = build('drive', 'v3', credentials=credentials)

    try:
        results = drive_service.files().list(
            q="sharedWithMe and mimeType='application/vnd.google-apps.folder'",
            fields="files(id, name)"
        ).execute()
        items = results.get('files', [])
        target_folder_id = None

        for item in items:
            if item['name'] == folder_name:
                target_folder_id = item['id']
                print(f"✅ Found folder '{folder_name}' with ID: {target_folder_id}")
                break

        if not target_folder_id:
            print(f" Folder '{folder_name}' not found in sharedWithMe.")
            return

        file_metadata = {'name': os.path.basename(file_path), 'parents': [target_folder_id]}
        media = MediaFileUpload(file_path, mimetype='text/csv')
        uploaded_file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"✅ File uploaded with ID: {uploaded_file.get('id')}")

    except HttpError as error:
        print(f"❌ Drive API error: {error}")

# ------------------ MAIN EXECUTION ----------------------

if __name__ == "__main__":
    sql = """
WITH yesterday_sales AS (
  SELECT 
    listing_id,
    lookup_date(unit_creation_date_time) AS unit_creation_date,
    COUNT(DISTINCT order_id) AS orders,
    SUM(units) AS units,
    SUM(gmv) AS gmv
  FROM bigfoot_external_neo.cp_bi_prod_sales__forward_unit_fact Sales
  WHERE 
    lookup_date(unit_creation_date_time) = lookup_date(DATE_SUB(CURRENT_DATE, 1))
    AND LOWER(Sales.status) NOT IN ('cancelled', 'null', 'rejected', 'on_hold', 'fse_hold', 'approval_hold', 'created')
    AND Sales.is_shopsy_order = TRUE
    AND UPPER(Sales.marketplace_id) = 'FLIPKART'
    AND LOWER(Sales.type) != 'service'
    AND Sales.category_id NOT IN (21726, 21651)
    AND Sales.is_freebie = FALSE
    AND (Sales.replacement_for_unit IS NULL OR Sales.replacement_for_unit = 'not_replacement')
    AND (Sales.exchange_for_unit IS NULL OR Sales.exchange_for_unit = 'not_exchange')
  GROUP BY listing_id, lookup_date(unit_creation_date_time)
),

listing_atp AS (
  SELECT 
    listing_id,
    SUM(final_atp) AS current_atp
  FROM bigfoot_external_neo.mp_sp__listing_atp_inventory_fact
  GROUP BY listing_id
)

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
  c.darwin_tier_name_v2 as darwin_tier_name_v2,
  x.unit_creation_date as unit_creation_date,
  COALESCE(x.units, 0) AS units,
  COALESCE(x.gmv, 0) AS gmv,
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

LEFT JOIN yesterday_sales x 
  ON a.listing_id = x.listing_id

LEFT JOIN listing_atp d 
  ON a.listing_id = d.listing_id

WHERE 
  LOWER(b.analytic_business_unit) = 'shopsy'
  AND a.minimum_order_quantity >= 2
"""  # Paste full SQL here
    
    df = run_query(sql)
    # After loading SQL results into df
    # df['unit_creation_date'] = df['unit_creation_date'].astype(int).astype(str)
    df.to_csv(CSV_FILE_PATH, index=False)
    print("✅ CSV saved:", CSV_FILE_PATH)
    upload_to_drive_shared_folder(CSV_FILE_PATH, DRIVE_FOLDER_NAME)
