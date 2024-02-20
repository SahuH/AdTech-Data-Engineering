import json
import pandas as pd


def standardize_ad_impressions(ad_impressions):
    # data validation
    valid_impressions = [impression for impression in ad_impressions if 'ad_id' in impression and 'user_id' in impression]
    
    # Convert timestamp to datetime format
    for impression in valid_impressions:
        impression['timestamp'] = pd.to_datetime(impression['timestamp'])

    return valid_impressions

def standardize_clicks_conversions(clicks_conversions):
    # data validation
    valid_events = [event for event in clicks_conversions if 'user_id' in event and 'event_type' in event]
    
    # Convert timestamp to datetime format
    for event in valid_events:
        event['timestamp'] = pd.to_datetime(event['timestamp'])

    return valid_events

def correlate_impressions_with_events(ad_impressions, clicks_conversions):
    correlated_data = []
    for impression in ad_impressions:
        user_id = impression['user_id']
        related_events = [event for event in clicks_conversions if event['user_id'] == user_id]
        if related_events:
            correlated_data.append({
                'user_id': user_id,
                'timestamp': impression['timestamp'],
                'ad_id': impression['ad_id'],
                'related_events': related_events
            })
    return correlated_data

def filter_and_deduplicate(data):
    deduplicated_data = pd.DataFrame(data).drop_duplicates(subset=['user_id', 'timestamp']).to_dict('records')
    return deduplicated_data


with open('ad_impressions.json', 'r') as file:
    ad_impressions = json.load(file)

clicks_conversions = pd.read_csv('clicks_conversions.csv').to_dict('records')


standardized_impressions = standardize_ad_impressions(ad_impressions)

standardized_events = standardize_clicks_conversions(clicks_conversions)

correlated_data = correlate_impressions_with_events(standardized_impressions, standardized_events)

filtered_data = filter_and_deduplicate(correlated_data)

for item in filtered_data:
    print(item)
