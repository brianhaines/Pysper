#!/usr/bin/env python3
import requests
import sqlite3


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

	# When done looping over JSON listings, close the DB connection
	db.close

if __name__ == '__main__':
	main()