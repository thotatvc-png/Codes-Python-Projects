import os
import io
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

FOLDER_ID = '1-ZfFo5YuyBwTgvqVL4cvLswf0tUGl-LU'  
CREDS_FILE = '/home/thotat.vc/analytics_scripts/creds.json'
DOWNLOAD_DIR = '/home/thotat.vc/Downloads/'
CONSOLIDATED_FILE = os.path.join(DOWNLOAD_DIR, 'Consolidated_Offers.csv')

SCOPES = ['https://www.googleapis.com/auth/drive']
creds = service_account.Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
service = build('drive', 'v3', credentials=creds)

if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

def download_files_from_folder(folder_id):
    query = f"'{folder_id}' in parents and (mimeType='application/vnd.google-apps.spreadsheet' or mimeType='text/csv')"
    results = service.files().list(q=query, pageSize=1000, fields="files(id, name, mimeType)").execute()
    items = results.get('files', [])
    file_paths = []

    for item in items:
        file_name = item['name'].lower()
        if 'transacting' in file_name or file_name == 'new_mseller_consolidated_base_file_updated.csv':
            print(f"Skipping file: {item['name']}")
            continue

        request = service.files().get_media(fileId=item['id'])
        file_path = os.path.join(DOWNLOAD_DIR, item['name'])

        fh = io.FileIO(file_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        print(f"Downloaded: {item['name']}")
        file_paths.append(file_path)

    return file_paths

def consolidate_and_clean_files(file_paths):
    dataframes = []
    lids_to_exclude = set()

    # Columns we must have in each file
    required_columns = ['Offer ID']
    columns_to_keep_in_output = ['Offer ID']

    for path in file_paths:
        try:
            df = pd.read_csv(path, encoding='cp1252')
        except Exception:
            try:
                df = pd.read_excel(path)
            except Exception:
                print(f"Skipping unreadable file: {path}")
                continue

        df.columns = df.columns.str.strip()


        if not all(col in df.columns for col in required_columns):
            print(f"Skipping file missing required columns: {path}")
            continue

        for col in required_columns:
            df[col] = df[col].astype(str).str.strip()

        manual_mask = df['Offer ID'].str.strip().str.lower().str.contains('manual', na=False)
        lids_to_exclude.update(df.loc[manual_mask, 'Offer ID'].unique().tolist())

        df_clean = df[columns_to_keep_in_output].copy()
        dataframes.append(df_clean)

    if not dataframes:
        print("No valid files to process.")
        return None

    final_df = pd.concat(dataframes, ignore_index=True)

    final_df = final_df[~final_df['Offer ID'].isin(lids_to_exclude)]
    final_df = final_df.drop_duplicates()

    final_df.to_csv(CONSOLIDATED_FILE, index=False, encoding='cp1252')
    print(f"Consolidated DataFrame saved to: {CONSOLIDATED_FILE}")
    print(f"Excluded {len(lids_to_exclude)} LIDs where Offer ID contained 'manual'.")

    return final_df

if __name__ == "__main__":
    files = download_files_from_folder(FOLDER_ID)
    consolidate_and_clean_files(files)

df2=pd.read_csv('/home/thotat.vc/Downloads/Consolidated_Offers.csv', encoding='cp1252')
df2

all_offer_ids=str(tuple(df2['Offer ID']))
print(all_offer_ids)


import os
import io
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

FOLDER_ID = '1-ZfFo5YuyBwTgvqVL4cvLswf0tUGl-LU'  
CREDS_FILE = '/home/thotat.vc/analytics_scripts/creds.json'
DOWNLOAD_DIR = '/home/thotat.vc/Downloads/'
CONSOLIDATED_FILE = os.path.join(DOWNLOAD_DIR, 'Consolidated_Cleaned_Output_Without_Offers.csv')

SCOPES = ['https://www.googleapis.com/auth/drive']
creds = service_account.Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
service = build('drive', 'v3', credentials=creds)

if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

def download_files_from_folder(folder_id):
    query = f"'{folder_id}' in parents and (mimeType='application/vnd.google-apps.spreadsheet' or mimeType='text/csv')"
    results = service.files().list(q=query, pageSize=1000, fields="files(id, name, mimeType)").execute()
    items = results.get('files', [])
    file_paths = []

    for item in items:
        file_name = item['name'].lower()
        if 'transacting' in file_name or file_name == 'updated_m_seller_new_fpo_offer_23-06-2025_base_file.csv'  or file_name == 'm_seller_june_whole_list.csv' or file_name == 'mseller_updated_offers.csv':
            print(f"Skipping file: {item['name']}")
            continue

        request = service.files().get_media(fileId=item['id'])
        file_path = os.path.join(DOWNLOAD_DIR, item['name'])

        fh = io.FileIO(file_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        print(f"Downloaded: {item['name']}")
        file_paths.append(file_path)

    return file_paths

def consolidate_and_clean_files(file_paths):
    dataframes = []
    lids_to_exclude = set()

    # Columns we must have in each file
    required_columns = ['LID', 'Correct cms_vertical', 'Seller Id', 'KAM', 'Offer ID']
    columns_to_keep_in_output = ['LID', 'Correct cms_vertical', 'Seller Id', 'KAM']

    for path in file_paths:
        try:
            df = pd.read_csv(path, encoding='cp1252')
        except Exception:
            try:
                df = pd.read_excel(path)
            except Exception:
                print(f"Skipping unreadable file: {path}")
                continue

        df.columns = df.columns.str.strip()


        if not all(col in df.columns for col in required_columns):
            print(f"Skipping file missing required columns: {path}")
            continue

        for col in required_columns:
            df[col] = df[col].astype(str).str.strip()

        manual_mask = df['Offer ID'].str.strip().str.lower().str.contains('manual', na=False)
        lids_to_exclude.update(df.loc[manual_mask, 'LID'].unique().tolist())
        df_clean = df[columns_to_keep_in_output].copy()
        dataframes.append(df_clean)

    if not dataframes:
        print("No valid files to process.")
        return None

    final_df = pd.concat(dataframes, ignore_index=True)

    # final_df = final_df[~final_df['LID'].isin(lids_to_exclude)]
    # final_df = final_df.drop_duplicates()

    final_df_1=final_df[final_df['Offer ID'].str.strip().str.lower().str.contains('manual')==False]

    # Step 1: Create the mapping dictionary from your data
    kam_mapping = {
        "AAKASH KUMAR MITRA": "AAKASH KUMAR MITRA",
        "ABHISHEK JAWA": "AISHWARYA SAWANT",
        "ADITI": "ADITI",
        "AFFAN": "AFFAN NADEEM",
        "AISHWARYA": "AISHWARYA SAWANT",
        "AISHWARYA ( WOMEN )": "AISHWARYA SAWANT",
        "AISHWARYA S": "AISHWARYA SAWANT",
        "AISHWARYA SAWANT": "AISHWARYA SAWANT",
        "AKASH PATTANAYAK": "AKASH PATTANAYAK",
        "AMIT A": "AMIT AGARWAL",
        "AMIT AGARWAL": "AMIT AGARWAL",
        "AMIT SAHGAL": "ANUSHREE PANT",
        "ANIMESH": "ANIMESH JAIN",
        "ANUKRITI": "ANUKRITI MISHRA",
        "ANUKRITI MISHRA": "ANUKRITI MISHRA",
        "ANUSHREE": "ANUSHREE PANT",
        "ANUSHREE PANT": "ANUSHREE PANT",
        "ARANYA": "ARANYA",
        "ARANYA BANDYOPADHY": "ARANYA",
        "ASHIMA": "ASHIMA MATHUR",
        "ASHIMA MATHUR": "ASHIMA MATHUR",
        "EKTA": "EKTA KHATARKAR",
        "EKTA KHATARKAR": "EKTA KHATARKAR",
        "GEETASHRI": "GEETASHRI",
        "HARSHIKA": "HARSHIKA",
        "HARSHITA PANDEY": "HARSHITA PANDEY",
        "JERIN": "VISHAL SINGHA",
        "KIRTI": "KIRTI DANG",
        "KIRTI DANG": "KIRTI DANG",
        "KUNAL THAKUR": "KUNAL THAKUR",
        "NAMAN": "NAMAN",
        "NIDHI": "NIDHI",
        "NIDHI GUPTA": "NIDHI",
        "NO KAM": "NO KAM",
        "PABITRA": "PABITRA BOWLARY",
        "PABITRA BOWLARY": "PABITRA BOWLARY",
        "PANKAJ": "PANKAJ TAMANG",
        "POOJA": "POOJA RAMESH",
        "POOJA RAMESH": "POOJA RAMESH",
        "PRAKHAR JAIN": "PRAKHAR JAIN",
        "PRATIK": "PRATIK NAG",
        "PRATIK NAG": "PRATIK NAG",
        "PULKIT": "PULKIT",
        "RAJESH": "RAJESH",
        "RE": "RE",
        "RE - ARAFAT": "RE",
        "RISHAV": "RISHAV",
        "RISHAV KUMAR": "RISHAV",
        "RIYA": "RIYA GUPTA",
        "RIYA GUPTA": "RIYA GUPTA",
        "ROHIT": "ROHIT",
        "RUTIKA": "RUTIKA GAHERWAR",
        "RUTIKA GAHERWAR": "RUTIKA GAHERWAR",
        "SAGAR": "SHUBHANKAR",
        "SAGAR BANSAL": "SHUBHANKAR",
        "SAKSHI": "SAKSHI",
        "SAKSHI BAHETI": "SAKSHI",
        "SANSRITI": "SANSRITI BAJORIA",
        "SANSRITI BAJORIA": "SANSRITI BAJORIA",
        "SHIVANI": "SHIVANI AGARWAL",
        "SHIVENDRA": "SHIVENDRA",
        "SHOVAN DAS": "SHOVAN DAS",
        "SHUBHANKAR": "SHUBHANKAR",
        "SWAPNIL": "PULKIT",
        "UM": "UM",
        "VANDANA": "VANDANA RANA",
        "VANDANA RANA": "VANDANA RANA",
        "VENU": "VENU",
        "VISHAL": "VISHAL SINGHA",
        "ANIMESH JAIN": "ANIMESH JAIN",
        "SHIVANI AGARWAL": "SHIVANI AGARWAL"
    }

    # Step 2: Replace values in the 'KAM' column of final_df_1
    final_df_1['KAM'] = final_df_1['KAM'].map(kam_mapping).fillna(final_df_1['KAM'])


    final_df_1.to_csv(CONSOLIDATED_FILE, index=False, encoding='cp1252')
    print(f"Consolidated DataFrame saved to: {CONSOLIDATED_FILE}")
    # print(f"Excluded {len(lids_to_exclude)} LIDs where Offer ID contained 'manual'.")

    return final_df

if __name__ == "__main__":
    files = download_files_from_folder(FOLDER_ID)
    consolidate_and_clean_files(files)









# import os
# import io
# import pandas as pd
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from googleapiclient.http import MediaIoBaseDownload

# FOLDER_ID = '1-ZfFo5YuyBwTgvqVL4cvLswf0tUGl-LU'  
# CREDS_FILE = '/home/thotat.vc/analytics_scripts/creds.json'
# DOWNLOAD_DIR = '/home/thotat.vc/Downloads/'
# # 'C:/Users/h.kishandasmenon/Downloads/'
# CONSOLIDATED_FILE = os.path.join(DOWNLOAD_DIR, 'Consolidated_Cleaned_Output_Without_Offers.csv')

# SCOPES = ['https://www.googleapis.com/auth/drive']
# creds = service_account.Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
# service = build('drive', 'v3', credentials=creds)

# if not os.path.exists(DOWNLOAD_DIR):
#     os.makedirs(DOWNLOAD_DIR)

# def download_files_from_folder(folder_id):
#     query = f"'{folder_id}' in parents and (mimeType='application/vnd.google-apps.spreadsheet' or mimeType='text/csv')"
#     results = service.files().list(q=query, pageSize=1000, fields="files(id, name, mimeType)").execute()
#     items = results.get('files', [])
#     file_paths = []

#     for item in items:
#         file_name = item['name'].lower()
#         if 'transacting' in file_name or file_name == 'updated_m_seller_new_fpo_offer_23-06-2025_base_file.csv'  or file_name == 'm_seller_june_whole_list.csv' or file_name == 'mseller_updated_offers.csv':
#             print(f"Skipping file: {item['name']}")
#             continue

#         request = service.files().get_media(fileId=item['id'])
#         file_path = os.path.join(DOWNLOAD_DIR, item['name'])

#         fh = io.FileIO(file_path, 'wb')
#         downloader = MediaIoBaseDownload(fh, request)
#         done = False
#         while not done:
#             _, done = downloader.next_chunk()

#         print(f"Downloaded: {item['name']}")
#         file_paths.append(file_path)

#     return file_paths

# def consolidate_and_clean_files(file_paths):
#     dataframes = []
#     lids_to_exclude = set()

#     # Columns we must have in each file
#     required_columns = ['LID', 'Correct cms_vertical', 'Seller Id', 'KAM', 'Offer ID']
#     columns_to_keep_in_output = ['LID', 'Correct cms_vertical', 'Seller Id', 'KAM']

#     for path in file_paths:
#         try:
#             df = pd.read_csv(path, encoding='cp1252')
#         except Exception:
#             try:
#                 df = pd.read_excel(path)
#             except Exception:
#                 print(f"Skipping unreadable file: {path}")
#                 continue

#         df.columns = df.columns.str.strip()


#         if not all(col in df.columns for col in required_columns):
#             print(f"Skipping file missing required columns: {path}")
#             continue

#         for col in required_columns:
#             df[col] = df[col].astype(str).str.strip()

#         manual_mask = df['Offer ID'].str.strip().str.lower().str.contains('manual', na=False)
#         lids_to_exclude.update(df.loc[manual_mask, 'LID'].unique().tolist())

#         df_clean = df[columns_to_keep_in_output].copy()
#         dataframes.append(df_clean)

#     if not dataframes:
#         print("No valid files to process.")
#         return None

#     final_df = pd.concat(dataframes, ignore_index=True)

#     final_df = final_df[~final_df['LID'].isin(lids_to_exclude)]
#     final_df = final_df.drop_duplicates()

#     final_df.to_csv(CONSOLIDATED_FILE, index=False, encoding='cp1252')
#     print(f"Consolidated DataFrame saved to: {CONSOLIDATED_FILE}")
#     print(f"Excluded {len(lids_to_exclude)} LIDs where Offer ID contained 'manual'.")

#     return final_df

# if __name__ == "__main__":
#     files = download_files_from_folder(FOLDER_ID)
#     consolidate_and_clean_files(files)

    
# import os
# import io
# import pandas as pd
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from googleapiclient.http import MediaIoBaseDownload

# FOLDER_ID = '1-ZfFo5YuyBwTgvqVL4cvLswf0tUGl-LU'  
# CREDS_FILE = '/home/thotat.vc/analytics_scripts/creds.json'
# # DOWNLOAD_DIR = '/home/thotat.vc/Downloads/'
# # CONSOLIDATED_FILE = os.path.join(DOWNLOAD_DIR, 'Consolidated_Offers.csv')

# SCOPES = ['https://www.googleapis.com/auth/drive']
# creds = service_account.Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
# service = build('drive', 'v3', credentials=creds)

# # if not os.path.exists(DOWNLOAD_DIR):
# #     os.makedirs(DOWNLOAD_DIR)

# def download_files_from_folder(folder_id):
#     query = f"'{folder_id}' in parents and (mimeType='application/vnd.google-apps.spreadsheet' or mimeType='text/csv')"
#     results = service.files().list(q=query, pageSize=1000, fields="files(id, name, mimeType)").execute()
#     items = results.get('files', [])
#     file_paths = []

#     for item in items:
#         file_name = item['name'].lower()
#         if 'transacting' in file_name or file_name == 'new_mseller_consolidated_base_file_updated.csv':
#             print(f"Skipping file: {item['name']}")
#             continue

#         request = service.files().get_media(fileId=item['id'])
#         file_path = os.path.join(DOWNLOAD_DIR, item['name'])

#         fh = io.FileIO(file_path, 'wb')
#         downloader = MediaIoBaseDownload(fh, request)
#         done = False
#         while not done:
#             _, done = downloader.next_chunk()

#         print(f"Downloaded: {item['name']}")
#         file_paths.append(file_path)

#     return file_paths

# def consolidate_and_clean_files(file_paths):
#     dataframes = []
#     lids_to_exclude = set()

#     # Columns we must have in each file
#     required_columns = ['Offer ID']
#     columns_to_keep_in_output = ['Offer ID']

#     for path in file_paths:
#         try:
#             df = pd.read_csv(path, encoding='cp1252')
#         except Exception:
#             try:
#                 df = pd.read_excel(path)
#             except Exception:
#                 print(f"Skipping unreadable file: {path}")
#                 continue

#         df.columns = df.columns.str.strip()


#         if not all(col in df.columns for col in required_columns):
#             print(f"Skipping file missing required columns: {path}")
#             continue

#         for col in required_columns:
#             df[col] = df[col].astype(str).str.strip()

#         manual_mask = df['Offer ID'].str.strip().str.lower().str.contains('manual', na=False)
#         lids_to_exclude.update(df.loc[manual_mask, 'Offer ID'].unique().tolist())

#         df_clean = df[columns_to_keep_in_output].copy()
#         dataframes.append(df_clean)

#     if not dataframes:
#         print("No valid files to process.")
#         return None

#     final_df = pd.concat(dataframes, ignore_index=True)

#     final_df = final_df[~final_df['Offer ID'].isin(lids_to_exclude)]
#     final_df = final_df.drop_duplicates()

#     final_df.to_csv('/home/thotat.vc/Downloads/Consolidated_Offers.csv', index=False, encoding='cp1252')
#     # print(f"Consolidated DataFrame saved to: {CONSOLIDATED_FILE}")
#     print(f"Excluded {len(lids_to_exclude)} LIDs where Offer ID contained 'manual'.")

#     return final_df

# if __name__ == "__main__":
#     files = download_files_from_folder(FOLDER_ID)
#     consolidate_and_clean_files(files)

# df2=pd.read_csv('/home/thotat.vc/Downloads/Consolidated_Offers.csv', encoding='cp1252')
# df2

# all_offer_ids=str(tuple(df2['Offer ID']))
# print(all_offer_ids)

