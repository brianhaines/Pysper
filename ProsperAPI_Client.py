#!/usr/bin/env python3
import requests
import asyncio
import json
import sqlite3
import pytz
from datetime import datetime
from time import time
from time import sleep
from ast import literal_eval


def getUSRPW(pwPath):
	with open(pwPath, 'r') as myfile:
		data=myfile.readlines() #.replace('\n', '')
	return data

def apiURL(username, password, limit=False):
	if limit == False:
		s = ("https://", username, ":",password, "@", "api.prosper.com/api/Listings/")
		return ''.join(s)
	else:
		s = ("https://", username, ":",password, "@", "api.prosper.com/api/Listings/?$top=",str(limit))
		s = ''.join(s)
		return s

def pingAPI(url_str, headers):
	try:
		res = requests.get(url_str, headers=headers)
		# Convert JSON response to a python dict
		listings = res.json()
		return listings
	except requests.exceptions.RequestException as e:
		print('Request Error: ', e)
		sleep(2)
	except Exception as e:
		print('Reques Error: ', e)

def ListingStartDate(lst_strt):
	l_year = lst_strt[:4]
	l_month = lst_strt[5:7]
	l_day = lst_strt[8:10]
	l_hour = lst_strt[11:13]
	l_minute = lst_strt[14:16]
	l_second = lst_strt[17:19]
	return datetime(int(l_year),int(l_month),int(l_day),int(l_hour),int(l_minute),int(l_second))

def elapsedBucket(el_secs):
	if el_secs <= 60:
		return ('percent_1_minute')
	elif el_secs > 60 and el_secs <= 120:
		return ('percent_2_minute')
	elif el_secs > 120 and el_secs <= 180:
		return ('percent_3_minute')
	elif el_secs > 180 and el_secs <= 300:
		return ('percent_5_minute')
	elif el_secs > 300 and el_secs <= 600:
		return ('percent_10_minute')
	elif el_secs > 600 and el_secs <= 900:
		return ('percent_15_minute')
	elif el_secs > 900 and el_secs <= 1800:
		return ('percent_30_minute')
	elif el_secs > 1800 and el_secs <= 3600:
		return ('percent_60_minute')
	else:
		return	'LongTime'


def defineTables(dtbs,crsr):
	#This is an SQL string to create a table in the database.
	crsr.execute('''CREATE TABLE IF NOT EXISTS elapsedListings(listingNumber INTEGER unique PRIMARY KEY, 
				MemberKey TEXT, ListingCreationDate TEXT, ListingStartDate TEXT, ListingRequestAmount REAL,
				ListingAmountFunded REAL, AmountRemaining REAL, PercentFunded REAL, listingElapsedSeconds REAL,
				ListingStatus INTEGER, ListingStatusDescription TEXT, percent_1_minute REAL, percent_2_minute REAL,
				percent_3_minute REAL, percent_5_minute REAL, percent_10_minute REAL, percent_15_minute REAL,
				percent_30_minute REAL, percent_60_minute REAL
				)''')


	#This is an SQL string to create a table in the database.
	crsr.execute('''CREATE TABLE IF NOT EXISTS currentListings(listingNumber INTEGER unique PRIMARY KEY, 
				MemberKey TEXT, ListingCreationDate TEXT, ListingStartDate TEXT, ListingRequestAmount REAL,
				ListingAmountFunded REAL, AmountRemaining REAL, PercentFunded REAL, listingElapsedSeconds REAL,
				ListingStatus INTEGER, ListingStatusDescription TEXT, percent_1_minute REAL, percent_2_minute REAL,
				percent_3_minute REAL, percent_5_minute REAL, percent_10_minute REAL, percent_15_minute REAL,
				percent_30_minute REAL, percent_60_minute REAL
				)''')

	#This is an SQL string to create a table in the database.
	crsr.execute('''CREATE TABLE IF NOT EXISTS historicalListings(listingNumber INTEGER unique PRIMARY KEY, 
				MemberKey TEXT, ListingCreationDate TEXT, ListingStartDate TEXT, ListingRequestAmount REAL,
				ListingAmountFunded REAL, AmountRemaining REAL, PercentFunded REAL, listingElapsedSeconds REAL,
				ListingStatus INTEGER, ListingStatusDescription TEXT, percent_1_minute REAL, percent_2_minute REAL,
				percent_3_minute REAL, percent_5_minute REAL, percent_10_minute REAL, percent_15_minute REAL,
				percent_30_minute REAL, percent_60_minute REAL
				)''')

	#Create the table for tracking each time we query the listings
	crsr.execute('''CREATE TABLE IF NOT EXISTS queryTracker(qNum INTEGER PRIMARY KEY ASC, 
			queryDateTime TEXT, listingCount INTEGER,qElapsed REAL)''')

	dtbs.commit()



	

def main():
	#Get username and PW from file
	pw = getUSRPW("API_usrname_PW.txt")
	usrName = pw[0].replace('\n', '')
	PW = pw[1]
	
	#create or open existing db
	db = sqlite3.connect('Listings_MetaData.db')

	# Get a cursor object
	cursor = db.cursor()

	# Set journal mode to WAL (Write Ahead Log)
	cursor.execute("PRAGMA journal_mode = %s" %("WAL"))

	# Create Tables if not already
	defineTables(db,cursor)

	# Format response as JSON
	headers = {'Content-Type': 'application/json'}
	
	#Define the timezone America/Los_Angeles  US/Pacific
	pacific = pytz.timezone('America/Los_Angeles')

	# Close the cursor before opening it again 
	cursor.close

	# Get the API url with usrname and PW
	urlString = apiURL(usrName, PW,10)

	# Long running loop
	while True:
		# Start performance timer
		start_time = time()
		# Get the current Datetime object. Microseconds set to 0 because Prosper sometimes
		# truncates their datetime strings
		t = datetime.now(pacific).replace(microsecond=0)
		queryDateTime = [t.date().isoformat(),t.time().isoformat()]
		
		# Time in seconds from the epoc with no microseconds
		t_sec = int(t.strftime("%s"))

		# Send request listing to Prosper				
		listings = pingAPI(urlString, headers)
		
		# API response time
		API_time = time()

		# Get a new cursor		
		cursor = db.cursor()

		# Loop over the results from API Query
		i = 1
		for listing in listings:
			# Datetime object from the listings response json text
			t_start = ListingStartDate(listing['ListingStartDate'])

			# Listings start datetime in seconds
			t_start_sec = int(t_start.strftime("%s"))

			# Number of seconds since this listing started
			elapsed_listing_sec = t_sec - t_start_sec
			# elapsed_listing_sec = 15
			# Given the number of elapsed seconds in the listings,
			# return the appropriate elapsed bucket / column name		
			el_col = elapsedBucket(elapsed_listing_sec)
			
			# Now we can put each listing into the elapsedListings table
			# First buil
			if el_col == "LongTime":
				update_Str = "UPDATE OR IGNORE elapsedListings  SET ListingAmountFunded = ?, AmountRemaining = ?, PercentFunded = ?,listingElapsedSeconds = ?, ListingStatus = ?, ListingStatusDescription = ? WHERE listingNumber = ?"
				update_Vals = (listing['ListingAmountFunded'],listing['AmountRemaining'],listing['PercentFunded'],elapsed_listing_sec, listing['ListingStatus'],listing['ListingStatusDescription'],listing['ListingNumber'])
				# print(update_Vals)
				cursor.execute(update_Str,update_Vals)
			else:
				update_Str = ("UPDATE OR IGNORE elapsedListings SET ListingAmountFunded = ?, AmountRemaining = ?, PercentFunded = ?, listingElapsedSeconds = ?, ListingStatus = ?, ListingStatusDescription = ?,"," {0} = ? WHERE listingNumber = ?")
				update_Str = ''.join(update_Str)
				update_Str = update_Str.format(el_col)

				update_Vals = (listing['ListingAmountFunded'],listing['AmountRemaining'],listing['PercentFunded'],elapsed_listing_sec, listing['ListingStatus'],listing['ListingStatusDescription'],elapsed_listing_sec,listing['ListingNumber'])
				# print(update_Vals)
				cursor.execute(update_Str,update_Vals)

			if cursor.rowcount <1:
				# Insert
				if elapsed_listing_sec > 3600:
					insert_Str = "INSERT OR IGNORE INTO allListings(listingNumber, MemberKey, ListingCreationDate, ListingStartDate, listingRequestAmount, ListingAmountFunded, AmountRemaining, PercentFunded, listingElapsedSeconds, ListingStatus, ListingStatusDescription), VALUES(?,?,?,?,?,?,?,?,?,?,?)"
					update_Vals = (listing['ListingNumber'],listing['MemberKey'],listing['ListingCreationDate'],listing['ListingStartDate'],listing['ListingRequestAmount'],listing['ListingAmountFunded'],listing['AmountRemaining'],listing['PercentFunded'],list_elapse_sec,listing['ListingStatus'],listing['ListingStatusDescription'])

					print(insert_Str)

				else:
					insert_Str = "INSERT OR IGNORE INTO allListings(listingNumber, MemberKey, ListingCreationDate, ListingStartDate, listingRequestAmount, ListingAmountFunded, AmountRemaining, PercentFunded, listingElapsedSeconds, ListingStatus, ListingStatusDescription,{0}, VALUES(?,?,?,?,?,?,?,?,?,?,?)"
					insert_Str = insert_Str.format(el_col)
					print(insert_Str)


			listing['ListingNumber'],
					listing['MemberKey'],
					listing['ListingCreationDate'],
					listing['ListingStartDate'],
					listing['ListingRequestAmount'],
					listing['ListingAmountFunded'],listing['AmountRemaining'],listing['PercentFunded'],list_elapse_sec,
					listing['ListingStatus'],listing['ListingStatusDescription']
					#round(list_elapse.total_seconds(),2),#listing['AmountRemaining'],listing['ListingStatus'],listing['ListingStatusDescription'],cursor.execute(update_Str,update_Vals)
			# listing['ListingNumber'],listing['MemberKey'],listing['ListingCreationDate'],listing['ListingStartDate'],listing['ListingRequestAmount'],
			# listingNumber, MemberKey, ListingCreationDate, ListingStartDate, ListingRequestAmount,
			# ListingAmountFunded, AmountRemaining, PercentFunded, listingElapsedSeconds,
			# ListingStatus, ListingStatusDescription, percent_1_minute, percent_2_minute,
			# percent_3_minute, percent_5_minute, percent_10_minute, percent_15_minute,
			# percent_30_minute, percent_60_minute



		# Put query record into the queryTracker table
		q_elapsed = round(time() - start_time,2)

		# Print Query Info
		print('___________________________')
		print('Query Time: ', queryDateTime[1])
		print('Total Query Time: ', q_elapsed)

		# Wait for N seconds between queries
		sleep(3)




	cursor.close()
	db.close()










if __name__ == '__main__':
	main()
