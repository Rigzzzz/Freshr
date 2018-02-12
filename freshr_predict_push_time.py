
# coding: utf-8

# In[1]:

# Since the goal of the push is not specified in the test,
# I assume that Freshr push notifications aim at engaging
# users who are not currently active in the app
# maybe through recommendations

# Here I am getting for each user the day(s)
# of the week when they are not active in the app
# I also get for each of them the time slots when they are the most active 
# because push notifications must be sent at appropriate time,
# when they are less likely to interrupt the user

# In[2]:

import pandas as pd
import numpy as np
from datetime import datetime
import time
import sys

# In[3]:

# Hourly time slots in a day
time_slots = [(i,i+1) for i in range(24)]
weekdays = {0:'Monday', 1:'Tuesday', 2:'Wednesday', 3:'Thursday', 4:'Friday', 5:'Saturday', 6:'Sunday'}

# Convert milliseconds timestamp to string formatted datetime
def ms_to_datetime(timestamp):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(timestamp/1000.0))

# Convert string formatted datetime to datetime object
def str_to_datetime(string):
    return datetime.strptime(string, '%Y-%m-%d %H:%M:%S')

# Get the time slot from a string formatted datetime
def get_time_slot(string):
    hour = str_to_datetime(string).hour
    return time_slots[hour]

# Get the weekday from a string formatted datetime
def get_weekday(string):
    dt_obj = str_to_datetime(string)
    return dt_obj.weekday()

# Read CSV dataset into a dataframe, clean NaN and duplicates
# and add columns that convert milliseconds timestamps to string formatted datetime
def csv_to_clean_df(dataset_path):
    df = pd.read_csv(dataset_path)
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    ms_cols = ['watermark', 'timestamp']
    df[ms_cols] = df[ms_cols].applymap(ms_to_datetime)
    df['time_slot'] = df['timestamp'].apply(get_time_slot)
    df['weekday'] = df['timestamp'].apply(get_weekday)
    #df.head()
    return df

# For each user get the days when he is inactive in the app
def get_inactive_weekdays(per_user_active_weekdays):
    weekdays_index = [key for key in weekdays]
    per_user_active_weekdays['inactive_weekdays'] = per_user_active_weekdays['active_weekdays']
    for i, user_id in enumerate(per_user_active_weekdays['user_id']):
        per_user_active_weekdays.set_value(i, 'inactive_weekdays',
                                           np.setdiff1d(weekdays_index,
                                                        per_user_active_weekdays.ix[i, 'active_weekdays']).tolist())
    return per_user_active_weekdays

# In[4]:

def main(argv):
    start = datetime.now()
    try:
        df = csv_to_clean_df(argv[0])
    except:
        print('File path argument error')
        sys.exit(1)
    # Get number of times each user opened the conversation for each 1h-window/slot in a day
    per_user_time_slot_count = df.groupby(['user_id', 'time_slot']).time_slot.count().reset_index(name="count")
    # Get number of times each user opened the conversation for each weekday
    per_user_weekday_count = df.groupby(['user_id', 'weekday']).weekday.count().reset_index(name="count")
    # Get the weekdays when the user is active in the app
    per_user_active_weekdays = per_user_weekday_count.groupby(['user_id'])['weekday']\
                                                     .apply(lambda x: list(x))\
                                                     .reset_index(name='active_weekdays')
    # Get also the weekdays when the user is not active in the app
    per_user_active_weekdays = get_inactive_weekdays(per_user_active_weekdays)
    #per_user_active_weekdays.head()

    # Get for each user the time slots in a day when he uses the most the application
    per_user_time_slots_max_count = per_user_time_slot_count.groupby(['user_id'])['count'].max().reset_index()
    per_user_most_active_time_slots = pd.merge(per_user_time_slot_count,
                                               per_user_time_slots_max_count,
                                               on=['user_id', 'count'])
    per_user_most_active_time_slots = per_user_most_active_time_slots.groupby(['user_id'])['time_slot']\
                                                                     .apply(lambda x: list(x)) \
                                                                     .reset_index(name="time_slots")

    # Merge the results, make them readable and write the final dataframe to a CSV file
    per_user_best_time = pd.merge(per_user_active_weekdays,
                                  per_user_most_active_time_slots,
                                  on='user_id').drop('active_weekdays', axis=1)
    per_user_best_time['inactive_weekdays'] = per_user_best_time['inactive_weekdays'].apply(lambda x: [weekdays[idx] for idx in x])
    per_user_best_time['time_slots'] = per_user_best_time['time_slots'].apply(lambda slots: ["between {0}h and {1}h".format(slot[0], slot[1]) for slot in slots])
    per_user_best_time.to_csv(argv[1])
    print(datetime.now() - start)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print('Too few arguments')
        sys.exit(1)
    elif len(sys.argv) > 3:
        print('Too many arguments')
        sys.exit(1)
    main(sys.argv[1:])


