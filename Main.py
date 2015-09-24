#!/usr/bin/env python3
import requests
import sqlite3
from datetime import datetime
import pytz
from time import sleep
from time import time
from ast import literal_eval


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
	#urlString = ("https://", usrName, ":",PW, "@", "api.prosper.com/api/Listings/?$top=169")
	urlString = ''.join(urlString)

	
	#create or open existing db
	db = sqlite3.connect('Listings_elpsedTracker.db')

	# Get a cursor object
	cursor = db.cursor()

	#This is an SQL string to create a table in the database.
	cursor.execute('''CREATE TABLE IF NOT EXISTS currentListings(listingNumber INTEGER unique PRIMARY KEY, 
				MemberKey TEXT, ListingCreationDate TEXT, ListingStartDate TEXT, ListingRequestAmount REAL,
				ListingAmountFunded REAL, AmountRemaining REAL, PercentFunded REAL, listingElapsedSeconds REAL,
				ListingStatus INTEGER, ListingStatusDescription TEXT, elapsedFunding TEXT
				)''')

	#This is an SQL string to create a table in the database.
	cursor.execute('''CREATE TABLE IF NOT EXISTS historicalListings(listingNumber INTEGER unique PRIMARY KEY, 
				MemberKey TEXT, ListingCreationDate TEXT, ListingStartDate TEXT, ListingRequestAmount REAL,
				ListingAmountFunded REAL, AmountRemaining REAL, PercentFunded REAL, listingElapsedSeconds REAL,
				ListingStatus INTEGER, ListingStatusDescription TEXT, elapsedFunding TEXT, lastSeen TEXT
				)''')

	#Create the table for tracking each time we query the listings
	cursor.execute('''CREATE TABLE IF NOT EXISTS queryTracker(qNum INTEGER PRIMARY KEY ASC, 
			queryDate TEXT, queryTime TEXT, listingCount INTEGER,qElapsed REAL)''')

	db.commit()

	# Format response as JSON
	headers = {'Content-Type': 'application/json'}
	
	#Define the timezone America/Los_Angeles  US/Pacific
	pacific = pytz.timezone('America/Los_Angeles')

	# Long running loop
	while True:
		#Current Time
		start_time = time()
		t = datetime.now(pacific).replace(microsecond=0)
		queryDateTime = [t.date().isoformat(),t.time().isoformat()]
		t_sec = int(t.strftime("%s"))

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
				ListingStatus INTEGER, ListingStatusDescription TEXT, elapsedFunding TEXT, lastSeen TEXT
				)''')

		# Loop over results
		i=1
		elapse_dict = {}
		for listing in listings:
			# Calc the listing elapsed time.
			#t2 = listing['ListingStartDate']
			l_year = listing['ListingStartDate'][:4]
			l_month = listing['ListingStartDate'][5:7]
			l_day = listing['ListingStartDate'][8:10]
			l_hour = listing['ListingStartDate'][11:13]
			l_minute = listing['ListingStartDate'][14:16]
			l_second = listing['ListingStartDate'][17:19]


			t2 = datetime(int(l_year),int(l_month),int(l_day),int(l_hour),int(l_minute),int(l_second))
			#t2 = datetime.strptime(listing['ListingStartDate'], "%Y-%m-%dT%H:%M:%S.%f")
			t2_sec = int(t2.strftime("%s"))
			t2 = t2.replace(tzinfo = pacific)
			list_elapse = t - t2
			list_elapse_sec = t_sec - t2_sec

			# Make elapsed funding tuple
			#elFund = (round(list_elapse.total_seconds(),2), listing['PercentFunded'])
			elFund = [(list_elapse_sec, listing['PercentFunded'])]
			elFund = str(elFund)
			elapse_dict[listing['ListingNumber']] = elFund

			try:
				cursor.execute('''INSERT INTO tempListings(listingNumber, MemberKey, ListingCreationDate, ListingStartDate, 
					ListingRequestAmount, ListingAmountFunded, AmountRemaining, PercentFunded, listingElapsedSeconds, 
					ListingStatus, ListingStatusDescription,elapsedFunding) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',
				 (listing['ListingNumber'],
					listing['MemberKey'],
					listing['ListingCreationDate'],
					listing['ListingStartDate'],
					listing['ListingRequestAmount'],
					listing['ListingAmountFunded'],
					listing['AmountRemaining'],
					listing['PercentFunded'],
					list_elapse_sec,
					#round(list_elapse.total_seconds(),2),
					#listing['AmountRemaining'],
					listing['ListingStatus'],
					listing['ListingStatusDescription'],
					elFund))
				
			except Exception as e:
				print("Intsert Temp Error: ", e)

			i += 1
		
		# First commit after creating tempListings table
		db.commit()

		# The temp Listing table is now populated. Perform a join on temp and current to get
		# those listing that are no longer on the API. Move them to the historical table.
		hist=[]
		for row in cursor.execute('''SELECT * FROM currentListings LEFT OUTER JOIN tempListings 
				ON currentListings.listingNumber = tempListings.listingNumber 
				WHERE tempListings.ListingNumber IS NULL'''):

			h = row[:12] + (t.isoformat(),)

			hist.append(h[:13]) # Why does join give me N * 2 columns?

		# Put the listings no longer in current into historical
		try:
			cursor.executemany('INSERT OR IGNORE INTO historicalListings VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)', hist)
			
			db.commit()
		except Exception as e:
			print("Intsert Many Error: ", e)		

		# Loop over the current listings and pull out the elapsedFunding values
		
		for curListing in cursor.execute('''SELECT cl.listingNumber, cl.elapsedFunding FROM currentListings cl'''):
			#elapse_dict[curListing[0]] = curListing[1].append(elapse_dict[curListing[0]])
			try:
				el_cur_list = literal_eval(curListing[1]) # Gives us a list
				tmp_cur_list = literal_eval(elapse_dict[curListing[0]])
				combined_list = el_cur_list + tmp_cur_list
				elapse_dict[curListing[0]]=str(combined_list)
			except Exception as e:
				#when the key is missting, skip it
				print("Elapsed Key missing: ", e)
			

		# For each key in elapse_dict, update that tempListing field
		for key, value in elapse_dict.items():
			cursor.execute('''UPDATE tempListings SET elapsedFunding = ? WHERE ListingNumber = ?''', (value, key))

		# Replace Current with Temp
		cursor.execute('DROP TABLE currentListings')
		cursor.execute('ALTER TABLE tempListings RENAME TO currentListings')

		# Delete the temporary table
		cursor.execute('DROP TABLE IF EXISTS tempListings')

		db.commit()

	# Select the listingElapsedSeconds field from temporary
		#for each_hist in hist:
		#	list_num = (each_hist[0],)
		#	cursor.execute('''SELECT currentListings.listingElapsedSeconds 
		##		WHERE currentListings.ListingNumber = ?''', list_num)
		#	list_elaps = cursor.fetchone()
		
			# 
		


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
		sleep(.1)

	# Tidy up the database
	cursor.close()
	db.close()
		

		
if __name__ == '__main__':
	main()