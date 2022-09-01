#-------------------------------------------------------------------
# Script to backup hosted feature services that specified in a CSV
# file.
# Hosted feature services must have sync enabled but do not have to
# be owned by script user.
# Parameters in lines 279-294 can be changed to enable download of
# attachments.
# See: https://developers.arcgis.com/python/api-reference/arcgis.features.managers.html?#syncmanager
# See: https://developers.arcgis.com/rest/services-reference/enterprise/create-replica.htm
#
# **WARNING** If run more than once on the same day, the previously 
# downloaded files will be overwritten (unless you re-named or 
# moved them before re-running the script)
#
# Andy Fairbairn, 2020. https://github.com/ShedAndy/AGOLBackup
# Zach Cravens, Khem So, Elyse Sachs, 2022.
#-------------------------------------------------------------------

#-------------------------------------------------------------------
# PARAMETERS
#-------------------------------------------------------------------
# Portal Credentials
portal_url = "https://fws.maps.arcgis.com"

# Full Backup - if true, attempts to backup all listed hosted feature services (HFS)
# if false, only backs up those that have been edited since the last successful
# backup of that HFS
full_backup = False

# CSV location - the CSV should be formatted with a column called "item_id" for ArcGIS Online
# item identifier. You can get the item_id from the URL of the item details page.
# e.g., https://fws.maps.arcgis.com/home/item.html?id=bfd80655aa3345e68c44ccdc551753c1. The item_id is: bfd80655aa3345e68c44ccdc551753c1
# Other columns such as "item_name" might be useful for context but will not be used by this script
csv_location = r"C:\Users\kso\Documents\DOI\Region 1 Inventory and Monitoring Program - demo\backup-items.csv"

# Download location - the zipped file geodatabases will be put into a folder named by date. 
# Make sure this location is writable. No trailing slash on end of folder path!!
download_location = r"C:\Users\kso\Documents\DOI\Region 1 Inventory and Monitoring Program - demo"

# File name for the record of successful backups - saved in download location
successful_backups = 'Last_Successful_Backup.csv'

#-------------------------------------------------------------------

#--------------------------------------------------------------------
# PRINT MESSAGE TO USERS WHO MIGHT BE CONFUSED BY SCHEDULED TASK!
#---------------------------------------------------------------------
print("\n\n*****ArcGIS Online BACKUP SCRIPT*****")
print("*****DO NOT CLOSE THIS WINDOW WHILST RUNNING!!!*****\n")
print("You can minimise this window and work as normal.")
print("When the script has completed it will say BACKUP COMPLETED, at which")
print("point you can close this window if it doesn't do so automatically.\n")

#-------------------------------------------------------------------
# MODULES
#-------------------------------------------------------------------
from arcgis.gis import GIS
from IPython.display import display
from datetime import datetime, date
from itertools import chain
from re import split
import time
import os
import zipfile
import pandas as pd
import arcgis.features
import shutil

#-------------------------------------------------------------------

# You must be logged into your ArcGIS Online account through ArcGIS Pro for this script to work.
gis = GIS("pro")

# Today's date, used in file path of downloaded backups
date_today_obj = datetime.now()
date_today = date_today_obj.strftime('%Y%m%d_%H%M%S')
year = date_today_obj.strftime('%Y')
month = date_today_obj.strftime('%m')
ts_today = int(round(datetime.now().timestamp()*1000,0))

# Log for this backup run
run_log_folder = download_location + '\\logs'

# Where the log last successful backup log will be kept
success_log_csv_path = download_location + '\\' + successful_backups

#-------------------------------------------------------------------
# GENERAL FUNCTIONS
#-------------------------------------------------------------------

def stamp_to_text(ts):
    return datetime.utcfromtimestamp(ts/1e3).strftime('%d/%m/%Y %H:%M:%S')

def check_create_folder(path):
    if not os.path.isdir(path):
        print(path + " doesn't exist - attempting to create it")
        try:
            os.makedirs(path)
        except:
            print("Unable to create folder, check permissions")

def set_indexes(df_list,index):
    for df in df_list:
        if df.index.name != index:
            df.set_index(index, inplace=True)

def reset_indexes(df_list):
    for df in df_list:
        if df.index.name is not None:
            df.reset_index(inplace=True)

def update_df(df,updating_df):
    # Set the dataframe indexes to a matching field
    set_indexes([df,updating_df],'item_id')
    
    # Update exising rows in the df with values from updating_df
    df.update(updating_df)
    
    # Append rows from updating_df that are not in df
    index_diff = updating_df.index.difference(df.index)
    updated_df = df.append(updating_df.loc[index_diff.values.tolist(),:], sort=False)
    
    # Reset dataframe indexes to the default
    reset_indexes([df,updating_df,updated_df])
    return updated_df

def export_df(df,path):
    try:
        # Write updated values out to last successful backup csv
        df.to_csv(path, index = False)
        return True
    except:
        print("Unable to write dataframe to {} - Open in Excel? Check permissions for the folder.".format(path))
        return False

def split_string(string, delimiters):
    """Splits a string by a list of delimiters.
    Args:
        string (str): string to be split
        delimiters (list): list of delimiters
    Returns:
        list: list of split strings
    """
    pattern = r'|'.join(delimiters)
    return split(pattern, string)
 
#-------------------------------------------------------------------
# SEARCH FOR HOSTED FEATURE SERVICES TO BACKUP
#-------------------------------------------------------------------

item_csv = pd.read_csv(csv_location)
if len(item_csv) < 1:
    exit()

item_list = []

for i in range(len(item_csv)):
    item_id = item_csv.iloc[i,0]
    item = gis.content.search(query="id:"+item_id, item_type = "Feature Layer")
    item_list.append(item)

item_list = list(chain(*item_list))
print("\nList of ArcGIS Online items to backup:")
print(item_list)

#-------------------------------------------------------------------
# UPDATE INFO ON HOSTED FEATURE SERVICES
#-------------------------------------------------------------------

def item_info(item):
    # Function to take an item id and return last update date of the
    # of the item, and the last edit date of feature class or tables
    
    updated_ts = item.modified
    
    # Get the last_edit_date for the HFS item - believe the only way
    # to get this is to check each layer and table in feature service
    # last edit date across all item layers and tables
    last_edit_date_ts = 0 
    
    if item._has_layers() == True:
        for flyr in item.layers:
            #print(flyr.properties.name)
            if flyr.properties.editingInfo.lastEditDate >= last_edit_date_ts:
                last_edit_date_ts = flyr.properties.editingInfo.lastEditDate

        for tbl in item.tables:
            #print(tbl.properties.name)
            if tbl.properties.editingInfo.lastEditDate >= last_edit_date_ts:
                last_edit_date_ts = tbl.properties.editingInfo.lastEditDate
    
    last_edit_date = stamp_to_text(last_edit_date_ts)
    
    return {'item_id':item.id,
            'item_name':item.name,
            'item_title':item.title,
            'url':item.url,
            'updated_ts':updated_ts,
            'last_edit_date':last_edit_date,
            'last_edit_date_ts':last_edit_date_ts}

# Open or create log/list of last successful backups
success_log_exists = False

# Only backing up HFS that have been edited since last successful backup
# Try to open log used to check this, if unsuccessful just backup everthing
try:
    success_log_df = pd.read_csv(success_log_csv_path)
    success_log_exists = True
except:
    # Unsuccessful, so backup up all HFS
    full_backup = True
    print("Couldn't open log of successful backups, backing up all services")
    print(success_log_csv_path)
    success_log_exists = False
    print("------------------")

# Gather info about items in the csv
item_info_list = []

for item in item_list:
    # item_info() returns a dictionary with layer/table last edit date and item update date
    item_info_list.append(item_info(item))

items_df = pd.DataFrame(item_info_list)
#success_log_df = pd.read_csv(success_log_csv_path)

if success_log_exists:
    # Update the last edited dates of items in the last good backup list
    # and add any not new items to be backed up that are not already in there
    success_log_df = update_df(success_log_df,items_df)
else:
    # Create a last good backup list from items_df
    success_log_df = items_df.copy()
    success_log_df.insert(5,"backup_date","Not yet backed up")
    success_log_df.insert(6,"backup_ts",0)
    success_log_df.insert(7,"zip_path","Not yet backed up")

# Export updated/newly created success_log_df
export_df(success_log_df, success_log_csv_path)

#if not a full backup, list of items that have a stale backup
if full_backup == False:
    query =  ((success_log_df['backup_ts'] < success_log_df['last_edit_date_ts'])
              | (success_log_df['backup_ts']!=success_log_df['backup_ts'])) 
    stale_list = success_log_df[query]['item_id'].values.tolist()
    print(stale_list)

#-------------------------------------------------------------------
# EXPORT TO FGDB
#-------------------------------------------------------------------
def create_replica(item):
    # Skip if we're not doing a full backup, and the id is not
    # on the list of items needing a fresh backup
    
    if (full_backup == True) or (item.id in stale_list):
        print("------------------")        
        print("Exporting {} to fgdb".format(item.title))
        # generate url for the Feature Layer Collection from the list of items to update
        url = item.url

        # get Feature Layers
        data = arcgis.features.FeatureLayerCollection(url, gis)

        # get number of layers
        dataLyr = len(data.layers)

        # generate string of number of layers
        dataLyr_list = data.layers
        dataTbl_list = data.tables
        dataNum_list = dataLyr_list + dataTbl_list
        
        dataNumSeq_list = []
        for x in dataNum_list:
            data_list_str = str(x)
            split_results = split_string(data_list_str, ['FeatureServer/','">'])
            dataNumSeq_list.append(split_results[1])
            lyrSeq = ", ".join(dataNumSeq_list)
       
        download_path = download_location + '\\backups\\' + item.title + '\\' + date_today
        # If download_path folder doesn't exist, create it
        check_create_folder(download_path)

        # download the replica
        try:
            replica = data.replicas.create(replica_name = item.title + "_" + date_today,
                                       layers = lyrSeq,
                                       layer_queries=None,
                                       geometry_filter=None,
                                       replica_sr=None,
                                       transport_type='esriTransportTypeUrl',
                                       return_attachments=False,
                                       return_attachments_databy_url=False,
                                       asynchronous=False,
                                       attachments_sync_direction='none',
                                       sync_model='none',
                                       data_format='filegdb',
                                       replica_options=None,
                                       wait=False,
                                       out_path= download_path,
                                       sync_direction=None)
        except:
            print("***Error exporting {}***".format(item.title))
            os.rmdir(download_path)
            check_create_folder(run_log_folder)
            error_log = run_log_folder + '\\error_' + date_today + '.log'
            import logging
            logging.basicConfig(filename = error_log, level = logging.ERROR)
            logging.exception(str(Exception))
            print("***Error logged in" + error_log + "***")
            pass

for item in item_list:
    fgdb = create_replica(item)

#-------------------------------------------------------------------
# MOVE FILES
#-------------------------------------------------------------------
def move_items(item):
    for item in item_list:
        try:
            if (full_backup == True) or (item.id in stale_list):
                # rename the zipfile
                zipList = os.listdir(download_location + '\\backups\\' + item.title + '\\' + date_today)
                zipName = ''.join(str(e) for e in zipList)
                oldName = r"{0}\backups\{1}\{2}\{3}".format(download_location,item.title,date_today,zipName)
                newName = r"{0}\backups\{1}\{2}\{1}_{2}.zip".format(download_location,item.title,date_today)
                os.rename(oldName, newName)
            
                # move the renamed zipfile
                src_path = r"{0}\backups\{1}\{2}\{1}_{2}.zip".format(download_location,item.title,date_today)    
                dst_path = r"{0}\backups\{1}\{1}_{2}.zip".format(download_location,item.title,date_today)
                shutil.move(src_path, dst_path)
            
                # remove the old directory
                dirPath = r"{0}\backups\{1}\{2}".format(download_location,item.title,date_today)
                
                print(item.title + " BACKUP COMPLETED")
                               
                try:
                    os.rmdir(dirPath)
                except OSError as e:
                    print("Error: %s : %s" % (dirPath, e.strerror))
        except:
            pass
        
run_move = move_items(item)

#-------------------------------------------------------------------
# LOGS
#-------------------------------------------------------------------
# Check success of downloads and update log files accordingly
def zip_path(item):
    return r"{0}\backups\{1}\{1}_{2}.zip".format(download_location,item.title,date_today)

def check_zip(item):
        
    try:
        with zipfile.ZipFile(zip_path(item)) as test_result:
            #print('{} backup is OK'.format(item.title))
            test_result.close
            return 'success'
    except:
        print(item.title + " backup is invalid or doesn't exist")
        return 'fail'

def create_run_log():
    log_list = []
    for item in item_list:
        log_row = {'item_id':item.id,
                   'item_name':item.name,
                   'item_title':item.title,
                   'zip_path':zip_path(item),
                   'status': "Backup still fresh"}

        if (full_backup == True) or (item.id in stale_list): 
            log_row['status'] = check_zip(item)
        log_list.append(log_row)
    return log_list

def export_run_log():
    df = pd.DataFrame(create_run_log())
    check_create_folder(run_log_folder)
    run_log_path = r"{}\{}_backup_run_log.csv".format(run_log_folder,date_today)
    export_df(df,run_log_path)
    return df

def update_logs():
    # Export run log to csv and get a DataFrame
    run_df = export_run_log()
    # Use the run log to update last good backup list
    run_df.insert(len(run_df.columns),"backup_date",stamp_to_text(ts_today))
    run_df.insert(len(run_df.columns),"backup_ts",ts_today)
    update_df(success_log_df,run_df[run_df['status']=='success'])
    export_df(success_log_df,success_log_csv_path)
    return run_df

run_df = update_logs()
print("Logs updated")