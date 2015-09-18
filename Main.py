#!/usr/bin/env python3
import requests
import sqlite3
from datetime import datetime
import pytz


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

	# Send request for JSON listing to Prosper
	headers = {'Content-Type': 'application/json'}
	r = requests.get(urlString, headers=headers)
	j = r.json()

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
			queryDate TEXT, queryTime TEXT, listingCount INTEGER)''')

	db.commit()


	#Current Time
	eastern = pytz.timezone('US/Eastern')
	t = datetime.now(eastern)
	queryDateTime = [t.date().isoformat(),t.time().isoformat()]


	i=1
	for listing in j:

		#'OR IGNORE' allows the INSERT command to ignore any rows where the 'unique' constraint is violated.
		cursor.execute('''INSERT OR IGNORE INTO currentListings(listingNumber, VerificationStage, ListingStatus, MemberKey, ListingStartDate) VALUES(?,?,?,?,?)''',
		 (listing['ListingNumber'],
		 	listing['VerificationStage'],
		 	listing['ListingStatus'],
		 	listing['MemberKey'],
		 	listing['ListingStartDate']))
		
		db.commit()
			

		print('____________________')
		print('Listing: ', i)
		print('ListingNumber: ', listing['ListingNumber'])
		print('VerificationStage: ', listing['VerificationStage'])
		print('ListingStatus: ', listing['ListingStatus'])
		print('MemberKey: ', listing['MemberKey'])
		print('ListingStartDate:', listing['ListingStartDate'])
		print('____________________')
		i+=1

	# Put query record into the queryTracer table
	cursor.execute('''INSERT INTO queryTracker(queryDate, queryTime, listingCount) VALUES(?,?,?)''',(queryDateTime[0],queryDateTime[1],(i-1)))

	db.commit()

	# When done looping over JSON listings, close the DB connection
	db.close

if __name__ == '__main__':
	main()