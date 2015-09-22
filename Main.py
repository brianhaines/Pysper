#!/usr/bin/env python3
import requests
import sqlite3
from datetime import datetime
import pytz
from time import sleep
from time import time


def getUSRPW(pwPath):
	with open(pwPath, 'r') as myfile:
		data=myfile.readlines() #.replace('\n', '')
	return data


def main():
	#Get username and PW from file
	pw = getUSRPW("API_usrname_PW.txt")
	usrName = pw[0].replace('\n', '')
	PW = pw[1]

	#Build the API url using username and PW
	urlString = ("https://", usrName, ":",PW, "@", "api.prosper.com/api/Listings/") #?$top=75")
	urlString = ''.join(urlString)

	
	#create or open existing db
	db = sqlite3.connect('Listings_elpsedTracker.db')

	# Get a cursor object
	cursor = db.cursor()
	#This is an SQL string to create a table in the database.
	cursor.execute('''CREATE TABLE IF NOT EXISTS currentListings(listingNumber INTEGER unique PRIMARY KEY, 
				MemberKey TEXT, ListingCreationDate TEXT, ListingStartDate TEXT, ListingRequestAmount REAL,
				ListingAmountFunded REAL, AmountRemaining REAL, PercentFunded REAL, listingElapsedSeconds REAL,
				ListingStatus INTEGER, ListingStatusDescription TEXT
				)''')

	#This is an SQL string to create a table in the database.
	cursor.execute('''CREATE TABLE IF NOT EXISTS historicalListings(listingNumber INTEGER unique PRIMARY KEY, 
				MemberKey TEXT, ListingCreationDate TEXT, ListingStartDate TEXT, ListingRequestAmount REAL,
				ListingAmountFunded REAL, AmountRemaining REAL, PercentFunded REAL, listingElapsedSeconds REAL,
				ListingStatus INTEGER, ListingStatusDescription TEXT, lastSeen TEXT
				)''')

	#Create the table for tracking each time we query the listings
	cursor.execute('''CREATE TABLE IF NOT EXISTS queryTracker(qNum INTEGER PRIMARY KEY ASC, 
			queryDate TEXT, queryTime TEXT, listingCount INTEGER,qElapsed REAL)''')

	db.commit()

	# Format response as JSON
	headers = {'Content-Type': 'application/json'}
	
	#Define the timezone
	pacific = pytz.timezone('US/Pacific')

	# Long running loop
	while True:
		#Current Time
		start_time = time()
		t = datetime.now(pacific)
		queryDateTime = [t.date().isoformat(),t.time().isoformat()]

		# Send request listing to Prosper
		try:
			r = requests.get(urlString, headers=headers)
			# Convert JSON response to a python dict
			listings = r.json()
		except requests.exceptions.RequestException as e:
			print('Request Error: ', e)
			sleep(2)
			

		# Create temporary table for listings just returned
		cursor.execute('''CREATE TABLE IF NOT EXISTS tempListings(listingNumber INTEGER unique PRIMARY KEY, 
				MemberKey TEXT, ListingCreationDate TEXT, ListingStartDate TEXT, ListingRequestAmount REAL,
				ListingAmountFunded REAL, AmountRemaining REAL, PercentFunded REAL, listingElapsedSeconds REAL,
				ListingStatus INTEGER, ListingStatusDescription TEXT
				)''')
		# Loop over results
		i=1
		for listing in listings:
			#Make a dict to hold these changes
			#'OR IGNORE' allows the INSERT command to ignore any rows where the 'unique' constraint is violated.
			
			# Calc the listing elapsed time.
			t2 = listing['ListingStartDate']
			t2 = datetime.strptime(t2, "%Y-%m-%dT%H:%M:%S.%f")
			t2 = t2.replace(tzinfo = pacific)
			list_elapse = t2 - t

			try:
				cursor.execute('''INSERT INTO tempListings(listingNumber, MemberKey, ListingCreationDate, ListingStartDate, 
					ListingRequestAmount, ListingAmountFunded, AmountRemaining, PercentFunded, listingElapsedSeconds, 
					ListingStatus, ListingStatusDescription) VALUES(?,?,?,?,?,?,?,?,?,?,?)''',
				 (listing['ListingNumber'],
					listing['MemberKey'],
					listing['ListingCreationDate'],
					listing['ListingStartDate'],
					listing['ListingRequestAmount'],
					listing['ListingAmountFunded'],
					listing['AmountRemaining'],
					listing['PercentFunded'],
					round(list_elapse.total_seconds(),2),
					#listing['AmountRemaining'],
					listing['ListingStatus'],
					listing['ListingStatusDescription']))
				
			except Exception as e:
				print("Intsert Temp Error: ", e)

			i += 1
		
		db.commit()

		# The temp Listing table is now populated. Perform a join on temp and current to get
		# those listing that are no longer on the API. Move them to the historical table.
		hist=[]
		for row in cursor.execute('''SELECT * FROM currentListings LEFT OUTER JOIN tempListings 
				ON currentListings.listingNumber = tempListings.listingNumber 
				WHERE tempListings.ListingNumber IS NULL'''):
			h = row[:11] + (t.isoformat(),)
			
			hist.append(h[:12]) # Why does join give me N * 2 columns?

		# Put the listings no longer in current into historical
		try:
			cursor.executemany('INSERT OR IGNORE INTO historicalListings VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', hist)
			# Replace Current with Temp
			cursor.execute('DROP TABLE currentListings')
			cursor.execute('ALTER TABLE tempListings RENAME TO currentListings')

			# Delete the temporary table
			cursor.execute('DROP TABLE IF EXISTS tempListings')
			db.commit()
		except Exception as e:
			print("Intsert Many Error: ", e)		

		# Put the query stats into the Query Tracker
		try:
			# Put query record into the queryTracker table
			q_elapsed = round(time() - start_time,2)
			cursor.execute('''INSERT INTO queryTracker(queryDate, queryTime, listingCount, qElapsed) VALUES(?,?,?,?)''',
				(queryDateTime[0],queryDateTime[1],(i-1),q_elapsed))
			db.commit()
		except Exception as e:
			print("Intsert Tracker Error: ", e)		
		

		print('Query Time: ', queryDateTime[1])
		print('Listings: ', i-1)
		print('Elapsed Time: ',q_elapsed)

		# Wait for N seconds between queries
		sleep(2)

	# Tidy up the database
	cursor.close()
	db.close()
		

		
if __name__ == '__main__':
	main()