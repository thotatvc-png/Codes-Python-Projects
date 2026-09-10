import pandas as pd
import numpy as np

df_listing=pd.read_csv(r'/home/thotat.vc/analytics_scripts/FOS_SOS_Cofunded_listing_raw.csv') 
print(df_listing)

# df_listing=pd.read_csv(r'/home/thotat.vc/analytics_scripts/listing_raw_new_consolidated_1.csv')

from datetime import datetime as dt2
today=dt2.now().strftime('%d_%b_%Y')
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe


scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('/home/thotat.vc/analytics_scripts/creds.json', scope)
client = gspread.authorize(creds)

spreadsheet = client.open("May Co-Funding Offers")

def get_only_offer_id_column(sheet_name):
    ws = spreadsheet.worksheet(sheet_name)
    
    # Find the "Offer ID" header in the first row
    headers = ws.row_values(1)
    try:
        # get index (1-based for gspread)
        col_index = headers.index("Offer ID") + 1
        
        # Get all values in that specific column (includes header)
        column_data = ws.col_values(col_index)
        
        # Return as DataFrame, skipping the first element (the header)
        return pd.DataFrame(column_data[1:], columns=["Offer ID"])
    
    except ValueError:
        print(f"Error: 'Offer ID' column not found in {sheet_name}")
        return pd.DataFrame(columns=["Offer ID"])

# 2. Fetch only the specific columns
df1 = get_only_offer_id_column("Offer IDs")
df2 = get_only_offer_id_column("50-50 Offers")

# 3. Combine
df_combined = pd.concat([df1, df2], ignore_index=True)

# 4. Clean and Save
# Removes any empty strings/rows
df_combined = df_combined[df_combined["Offer ID"] != ""]

df_combined.to_csv('/home/thotat.vc/analytics_scripts/New_April_cofunded_offers.csv')

df_new_fpo = pd.read_csv(r'/home/thotat.vc/analytics_scripts/New_April_cofunded_offers.csv', encoding='cp1252')
offer_ids_new_fpo = df_new_fpo['Offer ID'].dropna().unique().tolist()
offer_ids_new_fpo_cleaned = [f"'{str(oid)}'" for oid in offer_ids_new_fpo]
offer_ids_new_fpo_clause = ', '.join(offer_ids_new_fpo_cleaned)
print(offer_ids_new_fpo_clause)


#============Optin=========OFFER===CODE===================================
# sql2 = f"""SELECT DISTINCT a.offer_id AS offer_id, 
#                 b.product_id AS product_id, 
#                 a.listing_id AS listing_id, 
#                 b.seller_id AS seller_id, 
#                 c.cms_vertical AS cms_vertical, 
#                 b.listing_status AS listing_status
# FROM (
#     SELECT offer_id, listing_id
#     FROM (
#         SELECT data.listingid AS listing_id,
#                data.offerid AS offer_id,
#                MAX(CASE WHEN data.action = 'Mapped' THEN eventtime END) AS opt_in_time,
#                MAX(CASE WHEN data.action = 'Unmapped' THEN eventtime END) AS opt_out_time
#         FROM bigfoot_journal.dart_fkint_cp_santa_offerlistingmappingchange_5
#         WHERE day >= 20250731
#           AND data.offerid IN ({offer_ids_new_fpo_clause})
#         GROUP BY data.listingid, data.offerid
#     ) a
#     WHERE opt_out_time IS NULL OR opt_in_time > opt_out_time
# ) a
# JOIN bigfoot_external_neo.sp_product__listing_hive_dim b 
#   ON a.listing_id = b.listing_id
# JOIN bigfoot_external_neo.sp_product__product_categorization_hive_dim c
#   ON b.product_id = c.product_id
#   """
# data2 = pd.read_sql_query(sql2, con=conn)
# df_optin=pd.DataFrame(data2)
# df_optin.to_csv(f'/home/thotat.vc/analytics_scripts/FOS_SOS_Cofunded_optin_raw.csv', index=False)

df_optin=pd.read_csv(r'/home/thotat.vc/analytics_scripts/FOS_SOS_Cofunded_optin_raw.csv')

ten_percent_offers = [
'nb:mp:0390e92520','nb:mp:03410ac020','nb:mp:032c154720','nb:mp:03a67a0d20','nb:mp:03e971ae20','nb:mp:03fdb74a20'
]

df_optin=df_optin[df_optin['offer_id'].isin(ten_percent_offers)]
print("10 Percent Offer DF:")
print(df_optin['offer_id'].unique())


if df_optin.empty:
    print("Empty set error – df_optin has no data.")
else:
    # =============================================================================
    # 4. PROCESS BASE FILE & CLEANING
    # =============================================================================
    file_path_1 = r'/home/thotat.vc/analytics_scripts/New_Base_file_for_new_optin.csv'
    df_1 = pd.read_csv(file_path_1, encoding='utf-8')
    df_1['owner'] = df_1['owner'].replace(['', 'null'], np.nan).fillna('UM')
    df_1 = df_1.drop_duplicates()

    df_1_1 = df_1.copy()
    df_1_1['listing_id'] = df_1_1['listing_id'].astype(str).str.strip()
    df_1_1['seller_id'] = df_1_1['seller_id'].astype(str).str.strip()
    df_1_1['cms_vertical'] = df_1_1['cms_vertical'].astype(str).str.strip()
    
    # Exclude venu LIDs
    #=======================================================================
    #---Excluding lid's for venu------------------------------------
    #=======================================================================
    excluded_lids=[
        'LSTXHOH6DT55TZU3WMJ5POALG','LSTXHOH2HVFWYUYNDMGYL8XHB',
        'LSTXHOHM8NZPPGVZMUKX0CFLF','LSTXHOHM8NZFBJQVPSCQJQOYE',
        'LSTXHOHM8NZZFG7PNZZOBGAE2','LSTXHOH9BCTTY5GGXQZZ9AZYK',
        'LSTXHOHABUYZNTA42MSZXJAUN','LSTXHOHB2DR6FZZV8MZOYMR7K',
        'LSTXHOHHFFBCGGU8BUNJLH87H'
    ]
    df_1_1=df_1_1[~df_1_1['listing_id'].isin(excluded_lids)]
    #=======================================================================
    #---Excluding lid's for sanskriti-----END ----HERE----------------------
    #=======================================================================


    # Active Listings Filter & Category Merge
    removal_ids_1 = set(df_listing[df_listing['listing_status'] == 'ACTIVE']['listing_id'].astype(str).str.strip())

    df_listing_subset = df_listing[['listing_id', 'analytic_super_category_l2']].drop_duplicates()
    df_1_1 = df_1_1.drop(columns=['analytic_super_category_l2'], errors='ignore')
    df_1_1 = df_1_1.merge(df_listing_subset, on=['listing_id'], how='left')

    df_1_1['listing_status_current'] = df_1_1['listing_id'].apply(lambda x: 'ACTIVE' if x in removal_ids_1 else 'INACTIVE')
    df_1_1 = df_1_1[df_1_1['analytic_category'] != 'ShopsyMobileProtection']

    # =============================================================================
    # 6. OPT-IN LOGIC & MERGING
    # =============================================================================
    df_optin['listing_id'] = df_optin['listing_id'].astype(str).str.strip()
    df_optin['seller_id'] = df_optin['seller_id'].astype(str).str.strip()
    df_optin['cms_vertical'] = df_optin['cms_vertical'].astype(str).str.strip()

    overall_optin_listings = set(df_optin['listing_id'])

    # Apply Opt-in Flags
    df_1_1['Opted-in listing_id'] = df_1_1['listing_id'].apply(lambda x: 'Opted-in' if x in overall_optin_listings else 'Not opted-in')
    # df_1_1['Opted-in listing_id New FPO'] = df_1_1['listing_id'].apply(lambda x: 'Opted-in' if x in fpo_optin_listings else 'Not opted-in')
    # df_1_1['Opted-in listing_id New Cofunded'] = df_1_1['listing_id'].apply(lambda x: 'Opted-in' if x in cofund_optin_listings else 'Not opted-in')

    # Eligibility (Set to Eligible for calculation purposes)
    # df_1_1['New FPO'] = 'Eligible'
    # df_1_1['New Cofunded'] = 'Eligible'

    #=================OFFER_ID'S==Mapping=========Here=========================================
    # 1. Group df_optin by 'listing_id' and combine 'offer_id's into a list
    offer_mapping = df_optin.groupby('listing_id')['offer_id'].apply(list)

    # 2. Map those lists to df_1_1 based on 'listing_id'
    df_1_1['offer_ids'] = df_1_1['listing_id'].map(offer_mapping)

    df_1_edited = df_1_1.copy()
    print(df_1_edited)


