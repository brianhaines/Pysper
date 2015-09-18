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
	urlString = ("https://", usrName, ":",PW, "@", "api.prosper.com/api/Listings/") #?$top=20")
	urlString = ''.join(urlString)

	
	#create or open existing db
	db = sqlite3.connect('Listings.db')

	# Get a cursor object
	cursor = db.cursor()
	#This is an SQL string to create a table in the database.
	cursor.execute('''CREATE TABLE IF NOT EXISTS currentListings(listingNumber INTEGER unique PRIMARY KEY, 
				VerificationStage INTEGER, ListingStatus INTEGER, MemberKey TEXT, ListingStartDate TEXT)''')

	db.commit()

	#Create the table for tracking each time we query the listings
	cursor.execute('''CREATE TABLE IF NOT EXISTS queryTracker(qNum INTEGER PRIMARY KEY ASC, 
			queryDate TEXT, queryTime TEXT, listingCount INTEGER,qElapsed REAL)''')

	db.commit()

	# Format response as JSON
	headers = {'Content-Type': 'application/json'}
	
	#Define the timezone
	eastern = pytz.timezone('US/Eastern')


	while True:
		#Current Time
		start_time = time()
		t = datetime.now(eastern)
		queryDateTime = [t.date().isoformat(),t.time().isoformat()]

		# Send request listing to Prosper
		
		try:
			r = requests.get(urlString, headers=headers)
		except requests.exceptions.RequestException as e:
			print('Request Error: ', e)
			sleep(2)
			r = requests.get(urlString, headers=headers)

		# Convert JSON response to a python dict
		listings = r.json()

		# Loop over results
		i=1
		for listing in listings:

			#'OR IGNORE' allows the INSERT command to ignore any rows where the 'unique' constraint is violated.
			cursor.execute('''INSERT OR IGNORE INTO currentListings(listingNumber, VerificationStage, ListingStatus, MemberKey, ListingStartDate) VALUES(?,?,?,?,?)''',
			 (listing['ListingNumber'],
			 	listing['VerificationStage'],
			 	listing['ListingStatus'],
			 	listing['MemberKey'],
			 	listing['ListingStartDate']))
			
			db.commit()
			i += 1

		
		q_elapsed = round(time() - start_time,4)

		# Put query record into the queryTracer table
		cursor.execute('''INSERT INTO queryTracker(queryDate, queryTime, listingCount, qElapsed) VALUES(?,?,?,?)''',
			(queryDateTime[0],queryDateTime[1],(i-1),q_elapsed))

		db.commit()

		print('Query Time: ', queryDateTime[1])
		print('Listings: ', i-1)
		print('Total Time: ',q_elapsed)

		# Wait for N seconds between queries
		sleep(30)


	cursor.close()
	db.close()
		

if __name__ == '__main__':
	main()