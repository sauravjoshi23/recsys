import pandas as pd
from faker import Faker
import random

fake = Faker()
num_users = 100
num_ads = 200
num_transactions = 300

users = []
for _ in range(num_users):
    user_id = fake.uuid4()
    age = fake.random_int(min=18, max=70)
    gender = fake.random_element(elements=('Male', 'Female', 'Other'))
    location = fake.country()
    
    if 18 <= age <= 30:
        preferences = ['Fashion', 'Tech']
    elif 50 <= age <= 70:
        preferences = ['Travel', 'Food']
    else:
        preferences = fake.random_elements(elements=('Fashion', 'Tech', 'Food', 'Travel', 'Sports'), unique=True, length=2)
    
    users.append([user_id, age, gender, location, ','.join(preferences)])
user_profiles = pd.DataFrame(users, columns=['userId', 'age', 'gender', 'location', 'preferences'])

ads = []
categories = ['Fashion', 'Tech', 'Food', 'Travel', 'Sports']
for _ in range(num_ads):
    ad_id = fake.uuid4()
    category = fake.random_element(elements=categories)
    title = fake.catch_phrase()
    description = fake.text()
    tags = ','.join(fake.words(nb=3))
    ads.append([ad_id, title, description, category, tags])
ads_content = pd.DataFrame(ads, columns=['adId', 'title', 'description', 'category', 'tags'])

interactions = []
for user_id in user_profiles['userId']:
    user_preferences = user_profiles.loc[user_profiles['userId'] == user_id, 'preferences'].iloc[0].split(',')
    
    for category in user_preferences:
        relevant_ads = ads_content.loc[ads_content['category'] == category, 'adId'].tolist()
        
        if not relevant_ads:
            continue  
        
        ad_id = random.choice(relevant_ads)
        action = 'click' if fake.random_int(min=1, max=100) <= 70 else 'view'
        interactions.append([user_id, ad_id, action])

    non_preferred_categories = set(categories) - set(user_preferences)
    for category in non_preferred_categories:
        if fake.random_int(min=1, max=100) <= 20: 
            non_preferred_ads = ads_content.loc[ads_content['category'] == category, 'adId'].tolist()
            
            if not non_preferred_ads:
                continue  
            
            ad_id = random.choice(non_preferred_ads)
            action = 'view'
            interactions.append([user_id, ad_id, action])

user_ad_interactions = pd.DataFrame(interactions, columns=['userId', 'adId', 'action'])

item_interactions = []
for ad_id1 in ads_content['adId']:
    category1 = ads_content.loc[ads_content['adId'] == ad_id1, 'category'].iloc[0]
    other_ads = ads_content[ads_content['category'].isin([category1, 'Travel']) & (ads_content['adId'] != ad_id1)]
    
    if other_ads.empty:
        continue 
    
    ad_id2 = random.choice(other_ads['adId'].tolist())
    frequency = random.randint(1, 100)
    item_interactions.append([ad_id1, ad_id2, frequency])

item_item_interaction = pd.DataFrame(item_interactions, columns=['adId1', 'adId2', 'frequency'])

transactions = []
for user_id in user_profiles['userId']:
    user_preferences = user_profiles.loc[user_profiles['userId'] == user_id, 'preferences'].iloc[0].split(',')
    ads_for_user = ads_content[ads_content['category'].isin(user_preferences)]['adId'].tolist()
    
    if len(ads_for_user) < 2:  
        continue
    
    ads_in_transaction = random.sample(ads_for_user, random.randint(1, 2))
    transaction_id = fake.uuid4()
    transactions.append([transaction_id, user_id, ','.join(ads_in_transaction)])

transactions_data = pd.DataFrame(transactions, columns=['transactionId', 'userId', 'adIds'])

user_profiles.to_csv('./data/user_profiles.csv', index=False)
ads_content.to_csv('./data/ads_content.csv', index=False)
user_ad_interactions.to_csv('./data/user_ad_interactions.csv', index=False)
item_item_interaction.to_csv('./data/item_item_interaction.csv', index=False)
transactions_data.to_csv('./data/transactions.csv', index=False)
