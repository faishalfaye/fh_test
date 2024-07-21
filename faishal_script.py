from google.cloud import storage
from datetime import datetime, timedelta
import os
import pandas as pd
import subprocess
pd.options.mode.chained_assignment = None # ignoring warnings

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
dataset_path = BASE_DIR + "/DE Dataset"

print("dataset_path", dataset_path)

# ////////////////////////load data\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
lead_log_df = pd.read_csv(f"{dataset_path}/lead_log.csv")
paid_transactions_df = pd.read_csv(f"{dataset_path}/paid_transactions.csv")
referral_rewards_df = pd.read_csv(f"{dataset_path}/referral_rewards.csv")
user_logs_df = pd.read_csv(f"{dataset_path}/user_logs.csv")
user_referral_logs_df = pd.read_csv(f"{dataset_path}/user_referral_logs.csv")
user_referral_statuses_df = pd.read_csv(f"{dataset_path}/user_referral_statuses.csv")
user_referrals_df = pd.read_csv(f"{dataset_path}/user_referrals.csv")

# ////////////////////////data cleaning and processing\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

# CLEANING & PROCESSING LEAD LOG TABLE
lead_log_df = lead_log_df.drop(4) # remove the 4th row because as it is a duplicate record
lead_log_df = lead_log_df.reset_index(drop=True)  # reset the broken indexes

# mapping the correct column data types
lead_log_integer_column = ("id")
lead_log_string_column = ("lead_id", "source_category", "preferred_location", "timezone_location", "current_status")
lead_log_localtimestamp_column = ("created_at")
lead_log_initcap_column = ("preferred_location")

# convert the original column data types into the right ones
for col in lead_log_df.columns:
  if col in lead_log_integer_column:
    lead_log_df[col] = lead_log_df[col].astype(int)
  elif col in lead_log_string_column:
    lead_log_df[col] = lead_log_df[col].astype(str)
    if col in lead_log_initcap_column:
      lead_log_df[col] = lead_log_df[col].apply(lambda text: text.lower().title()) # initcapping
  elif col in lead_log_localtimestamp_column:
    lead_log_df[col] = pd.to_datetime(lead_log_df[col])
    seven_hours = pd.Timedelta(hours=7)
    lead_log_df[col] = (lead_log_df[col] + seven_hours).dt.strftime("%Y-%m-%d %H:%M:%S") # convert to localtime with yyyy-mm-dd hh:mm:ss format


# CLEANING & PROCESSING PAID TRANSACTIONS TABLE

# mapping the correct column data types
paid_transactions_string_column = ("transaction_id", "transaction_status", "transaction_location", "timezone_transaction", "transaction_type")
paid_transactions_localtimestamp_column = ("transaction_at")
paid_transactions_initcap_column = ("transaction_status", "transaction_location", "transaction_type")

# convert the original column data types into the right ones
for col in paid_transactions_df.columns:
  if col in paid_transactions_string_column:
    paid_transactions_df[col] = paid_transactions_df[col].astype(str)
    if col in paid_transactions_initcap_column:
      paid_transactions_df[col] = paid_transactions_df[col].apply(lambda text: text.lower().title()) # initcapping
  elif col in paid_transactions_localtimestamp_column:
      paid_transactions_df[col] = pd.to_datetime(paid_transactions_df[col])
      paid_transactions_df[col] = (paid_transactions_df[col] + seven_hours).dt.strftime("%Y-%m-%d %H:%M:%S") # convert to localtime with yyyy-mm-dd hh:mm:ss format

# CLEANING & PROCESSING REFERRAL REWARDS TABLE

# mapping the correct column data types
referral_rewards_integer_column = ("id", "reward_value", "reward_type")
referral_rewards_localtimestamp_column = ("created_at")

# convert the original column data types into the right ones
for col in referral_rewards_df.columns:
  if col in referral_rewards_integer_column:
    if col == "reward_value":
      referral_rewards_df[col] = pd.to_timedelta(referral_rewards_df[col]).dt.days
    else:
      referral_rewards_df[col] = referral_rewards_df[col].astype(int)
  elif col in referral_rewards_localtimestamp_column:
    referral_rewards_df[col] = pd.to_datetime(referral_rewards_df[col])
    referral_rewards_df[col] = (referral_rewards_df[col] + seven_hours).dt.strftime("%Y-%m-%d %H:%M:%S") # convert to localtime with yyyy-mm-dd hh:mm:ss format

# CLEANING & PROCESSING USER LOGS TABLE

user_logs_df = user_logs_df[~user_logs_df.duplicated(subset=list(user_logs_df.columns)[1:])] # removing the duplicates

# mapping the correct column data types
user_logs_integer_column = ("id")
user_logs_string_column = ("user_id", "name", "phone_number", "homeclub", "timezone_homeclub")
user_logs_date_column = ("membership_expired_date")
user_logs_boolean_column = ("is_deleted")

# convert the original column data types into the right ones
for col in user_logs_df.columns:
  if col in user_logs_integer_column:
    user_logs_df[col] = user_logs_df[col].astype(int)
  elif col in user_logs_string_column:
    user_logs_df[col] = user_logs_df[col].astype(str)
  elif col in user_logs_date_column:
    user_logs_df[col] = pd.to_datetime(user_logs_df[col]).dt.date
  elif col in user_logs_boolean_column:
    user_logs_df[col] = user_logs_df[col].astype(bool)

# CLEANING & PROCESSING USER REFERRAL LOGS TABLE

user_referral_logs_df = user_referral_logs_df.dropna() # removing null values

# mapping the correct column data types
user_referral_logs_integer_column = ("id")
user_referral_logs_string_column = ("user_referral_id", "source_transaction_id")
user_referral_logs_localtimestamp_column = ("created_at")
user_referral_logs_boolean_column = ("is_reward_granted")

# convert the original column data types into the right ones
for col in user_referral_logs_df.columns:
  if col in user_referral_logs_integer_column:
    user_referral_logs_df[col] = user_referral_logs_df[col].astype(int)
  elif col in user_referral_logs_string_column:
    user_referral_logs_df[col] = user_referral_logs_df[col].astype(str)
  elif col in user_referral_logs_localtimestamp_column:
    user_referral_logs_df[col] = pd.to_datetime(user_referral_logs_df[col])
    user_referral_logs_df[col] = (user_referral_logs_df[col] + seven_hours).dt.strftime("%Y-%m-%d %H:%M:%S") # convert to localtime with yyyy-mm-dd hh:mm:ss format

# CLEANING & PROCESSING USER REFERRAL STATUSES TABLE
user_referral_statuses_df["created_at"] = pd.to_datetime(user_referral_statuses_df["created_at"])
user_referral_statuses_df["created_at"] = (user_referral_statuses_df["created_at"] + seven_hours).dt.strftime("%Y-%m-%d %H:%M:%S")

# CLEANING & PROCESSING USER REFERRALS TABLE
user_referrals_df = user_referrals_df.dropna()

# mapping the correct column data types
user_referrals_integer_column = ("referral_reward_id", "user_referral_status_id")
user_referrals_string_column = ("referral_id", "referee_id", "referee_name", "referee_phone", "referral_source", "referrer_id", "transaction_id")
user_referrals_localtimestamp_column = ("referral_at", "updated_at")
user_referrals_boolean_column = ("is_reward_granted")

# convert the original column data types into the right ones
for col in user_referrals_df.columns:
  if col in user_referrals_integer_column:
    user_referrals_df[col] = user_referrals_df[col].astype(int) 
  elif col in user_referrals_string_column:
    user_referrals_df[col] = user_referrals_df[col].astype(str)
  elif col in user_referrals_localtimestamp_column:
    user_referrals_df[col] = pd.to_datetime(user_referrals_df[col])
    user_referrals_df[col] = (user_referrals_df[col] + seven_hours).dt.strftime("%Y-%m-%d %H:%M:%S") # convert to localtime with yyyy-mm-dd hh:mm:ss format
  elif col in user_referrals_boolean_column:
    user_referrals_df[col] = user_referrals_df[col].astype(bool)

# adding prefixes to distinguish columns when merging DataFrames
lead_log_df.columns = 'll_' + lead_log_df.columns.values
paid_transactions_df.columns = 'pt_' + paid_transactions_df.columns.values
referral_rewards_df.columns = 'rr_' + referral_rewards_df.columns.values
user_logs_df.columns = 'ul_' + user_logs_df.columns.values
user_referral_logs_df.columns = 'url_' + user_referral_logs_df.columns.values
user_referral_statuses_df.columns = 'urs_' + user_referral_statuses_df.columns.values
user_referrals_df.columns = 'ur_' + user_referrals_df.columns.values

# LEFT JOIN so the rows won't disappear
combined_df = user_referrals_df.merge(user_logs_df, how="left", left_on=["ur_referrer_id"], right_on=["ul_user_id"])
combined_df = combined_df.merge(referral_rewards_df, how="left", left_on=["ur_referral_reward_id"], right_on=["rr_id"])
combined_df = combined_df.merge(user_referral_logs_df, how="left", left_on=["ur_referral_id"], right_on=["url_user_referral_id"])
combined_df = combined_df.merge(user_referral_statuses_df, how="left", left_on=["ur_user_referral_status_id"], right_on=["urs_id"])
combined_df = combined_df.merge(paid_transactions_df, how="left", left_on=["ur_transaction_id"], right_on=["pt_transaction_id"])
combined_df = combined_df.merge(lead_log_df, how="left", left_on=["ur_referee_id"], right_on=["ll_lead_id"])

# this line is to mimic a join-like operation assuming data only contains 'Lead'
combined_df.loc[combined_df["ur_referral_source"] != "Lead", list(combined_df.columns[37:])] = None 

# function to get_referral_source_category_value
def get_referral_source_category_value(index, row):
  if row['ur_referral_source'] == "User Sign Up":
    return "Online"
  elif row['ur_referral_source'] == "Draft Transaction":
    return 'Offline'
  elif row['ur_referral_source'] == "Lead":
    return lead_log_df.loc[index, "ll_source_category"]

# Create a new column using list comprehension
combined_df['referral_source_category'] = [get_referral_source_category_value(index, row) for index, row in combined_df.iterrows()]

broken_datetime_column_type = ["ul_membership_expired_date", "ur_referral_at", "ur_updated_at", "rr_created_at", "url_created_at", "urs_created_at", "pt_transaction_at", "ll_created_at"]
broken_integer_column_type = ["ul_id", "url_id", "ll_id"]

for col in broken_datetime_column_type:
  combined_df[col] = pd.to_datetime(combined_df[col])

combined_df[broken_integer_column_type] = combined_df[broken_integer_column_type].astype("Int64")

# ////////////////////////basic business logic implementation\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

# function to check if the business logic is valid
def is_business_logic_valid(index, row):
  # VALID CONDITION 1 
  if (row["rr_reward_value"] > 0) and \
     (row["ur_user_referral_status_id"] == 2) and \
     (row["ur_transaction_id"] is not None) and \
     (row["pt_transaction_status"] == "Paid") and \
     (row["pt_transaction_type"] == "New") and \
     (row["ur_referral_at"] > row["pt_transaction_at"]) and \
     (row["pt_transaction_at"].month == row["ur_referral_at"].month) and \
     (row["ur_updated_at"] < row["ul_membership_expired_date"]) and \
     (row["ul_is_deleted"] is not True) and \
     (row["url_is_reward_granted"] is True):
     return True
  # VALID CONDITION 2
  elif ((row["urs_description"] == "Menunggu") or (row["urs_description"] == "Tidak Berhasil")) and \
       row["rr_reward_value"] is None:
       return True
  # INVALID CONDITION 1
  elif row["rr_reward_value"] > 0 and \
       row["urs_description"] != "Berhasil":
       return False
  # INVALID CONDITION 2
  elif row["rr_reward_value"] > 0 and \
       row["ur_transaction_id"] is None:
       return False
  # INVALID CONDIITON 3
  elif row["rr_reward_value"] is None and \
       row["ur_transaction_id"] is not None and \
       row["pt_transaction_status"] == "Paid" and \
       row["ur_referral_at"] < row["pt_transaction_at"]:
       return False
  # INVALID CONDITION 4
  elif row["urs_description"] == "Berhasil" and \
       (row["rr_reward_value"] is None) or (row["rr_reward_value"] == 0):
       return False
  # INVALID CONDITION 5
  elif row["ur_referral_at"] > row["pt_transaction_at"]:
       return False

# creating the is_business_logic_valid column
combined_df['is_business_logic_valid'] = [is_business_logic_valid(index, row) for index, row in combined_df.iterrows()]

correct_column_order = [
    'url_id', 'ur_referral_id', 'ur_referral_source', 'referral_source_category',
    'ur_referral_at', 'ur_referrer_id', 'ul_name', 'ul_phone_number',
    'ul_homeclub', 'ur_referee_id', 'ur_referee_name', 'ur_referee_phone',
    'urs_description', 'rr_reward_value', 'ur_transaction_id', 'pt_transaction_status',
    'pt_transaction_at', 'pt_transaction_location', 'pt_transaction_type', 'ur_updated_at',
    'url_created_at', 'is_business_logic_valid'
]

correct_column_name = [
    'referral_details_id', 'referral_id', 'referral_source', 'referral_source_category',
    'referral_at', 'referrer_id', 'referrer_name', 'referrer_phone_number',
    'referrer_homeclub', 'referee_id', 'referee_name', 'referee_phone',
    'referral_status', 'num_reward_days', 'transaction_id', 'transaction_status',
    'transaction_at', 'transaction_location', 'transaction_type', 'updated_at',
    'reward_granted_at', 'is_business_logic_valid'
]

 # correcting the columns order
combined_df = combined_df[correct_column_order]
combined_df["url_id"] = list(combined_df.index.values) 
combined_df.columns = correct_column_name

# uploading the csv output to Google Cloud Storage
current_wib_time = (datetime.utcnow() + timedelta(hours=7)).strftime("%H:%M:%S")
OUTPUT_FILE_PATH = BASE_DIR + f"/output_{current_wib_time}.csv"
combined_df.to_csv(OUTPUT_FILE_PATH, index=False)

project_id = "data-rider-367401"
set_gcp_project_id = f"gcloud config set project {project_id}"
subprocess.run([set_gcp_project_id], shell=True)

GS_BUCKET_PATH = f"gs://faishal_bucket"
gsutil_move = f"gsutil mv {OUTPUT_FILE_PATH} {GS_BUCKET_PATH}"
subprocess.run([gsutil_move], shell=True)