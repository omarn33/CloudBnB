import sys, csv, datetime, json
import pymongo
from pymongo.errors import ConnectionFailure

# List of available cities and datasets:
cities = ['los-angeles', 'portland', 'salem', 'san-diego']
datasets = ['calendar', 'listings', 'neighbourhoods', 'reviews']

if len(sys.argv) != 5:
    raise Exception('Exactly 4 arguments are required: <HOST> <CITY> <DATASET> <LIMIT>')

city = sys.argv[2].lower() # City to process data for
dataset = sys.argv[3].lower() # Specific csv file to process
filename = '../AirBnB-Datasets/' + city + '/' + dataset + '.csv' # Change if path is different.

LIMIT = int(sys.argv[4]) # Maximum number of documents that could be inserted
count = 0 # Number of attempted insertions/updates

# Check if city is in dataset:
if city not in cities:
    raise Exception('City not in dataset.')

# Check if dataset is available:
if dataset not in datasets:
    raise Exception('Dataset not available.')

# Connect to MongoDB database:
host = sys.argv[1].lower() # Host of MongoDB server
client = pymongo.MongoClient('mongodb://' + host)

try:
    client.admin.command('ping')
except ConnectionFailure:
    print("Server not available")
    exit()

db = client['airbnb_listings']
collection = db[dataset]

def loadCalendar():
    global count

    with open(filename, 'r', encoding='utf8') as f:
        reader = csv.DictReader(f)

        currPeriod = None # The current availability period being processed
        prevRow = None # Stores the previous row that was proccessed

        for row in reader:
            if count >= LIMIT:
                break

            year = int(row['date'][0:4])
            month = int(row['date'][5:7])
            day = int(row['date'][8:])
            date = datetime.datetime(year, month, day)

            if row['available'] == 't':
                isAvailable = True
            else:
                isAvailable = False
            
            if prevRow == None: # If this is the very first document
                currPeriod = {
                    'start_date': date,
                    'end_date': date,
                    'available': isAvailable,
                    'total_nights': 1
                }
                prevRow = row
                continue

            if (row['listing_id'] == prevRow['listing_id']) & (row['available'] == prevRow['available']):
                # Continue editing current availability period:
                currPeriod['end_date'] = date
                currPeriod['total_nights'] += 1
                prevRow = row
            else:
                # Insert availability period and move on to next one:
                try:
                    collection.update_one({'_id': prevRow['listing_id']}, {
                        '$set': {
                            'min_nights': int(prevRow['minimum_nights']),
                            'max_nights': int(prevRow['maximum_nights']),
                        },

                        '$addToSet': {
                            'availability_periods': currPeriod
                        }
                    })
                except:
                    #print('Failed to insert availability period.')
                    pass
                
                currPeriod = {
                    'start_date': date,
                    'end_date': date,
                    'available': isAvailable,
                    'total_nights': 1
                }
                
                prevRow = row
                count += 1

def loadListings():
    global count

    with open(filename, 'r', encoding='utf8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            if count >= LIMIT:
                break

            try:
                avg_rating = float(row['review_scores_rating'])
            except:
                avg_rating = 0

            listing = {
                '_id': row['id'],
                'listing_url': row['listing_url'],
                'listing_name': row['name'],
                'listing_image': row['picture_url'],
                'price': row['price'],
                'description': row['description'],
                'neighborhood': row['neighbourhood_cleansed'],
                'city': city,
                'property_type': row['property_type'],
                'room_type': row['room_type'],
                'accommodates': int(row['accommodates']),
                'amenities': json.loads(row['amenities']),
                'avg_rating': avg_rating,
                'total_reviews': int(row['number_of_reviews']),
                
                'host_info': {
                    'host_id': row['host_id'],
                    'host_url': row['host_url'],
                    'host_name': row['host_name'],
                    'host_image': row['host_picture_url']
                }
            }

            calendar = {
                '_id': row['id'],
                'min_nights': 0,
                'max_nights': 0,
                'availability_periods': []
            }

            reviews = {
                '_id': row['id'],
                'comments': []
            }
            
            try:
                # Insert Listing + Associated Documents:
                collection.update_one({'_id': row['id']}, {'$setOnInsert': listing}, upsert=True)
                db['calendar'].update_one({'_id': row['id']}, {'$setOnInsert': calendar}, upsert=True)
                db['reviews'].update_one({'_id': row['id']}, {'$setOnInsert': reviews}, upsert=True)
            except:
                #print('Could not insert docuemnts.')
                pass

            count += 3

def loadNeighbourhoods():
    global count

    with open(filename, 'r', encoding='utf8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            if count >= LIMIT:
                break

            neighbourhood = {
                'city': city,
                'neighbourhood_group': row['neighbourhood_group'],
                'neighbourhood': row['neighbourhood']
            }

            try:
                # Insert neighbourhood:
                collection.insert_one(neighbourhood)
            except:
                #print('Could not insert neighbourhood.')
                pass

            count += 1

def loadReviews():
    global count

    with open(filename, 'r', encoding='utf8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            if count >= LIMIT:
                break

            year = int(row['date'][0:4])
            month = int(row['date'][5:7])
            day = int(row['date'][8:])
            date = datetime.datetime(year, month, day)

            comment = {
                'reviewer_id': row['reviewer_id'],
                'reviewer_name': row['reviewer_name'],
                'date': date,
                'comments': row['comments']
            }

            try:
                # Insert Review:
                collection.update_one({'_id': row['listing_id']}, {
                    '$addToSet': {
                        'comments': comment
                    }
                })
            except:
                #print('Could not insert review.')
                pass

            count += 1

if __name__ == '__main__':
    if dataset == 'calendar':
        loadCalendar()
    elif dataset == 'listings':
        loadListings()
    elif dataset == 'neighbourhoods':
        loadNeighbourhoods()
    elif dataset == 'reviews':
        loadReviews()
    
    print('Number of attempted insertions/updates: ' + str(count))