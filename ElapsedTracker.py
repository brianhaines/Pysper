import sqlite3

def makeElTable(dtbs, crsr):
	# Create the Elapsed Funding Tracking tables
	# Average and Std Dev for each combo for each bucket

	create_str = '''CREATE TABLE IF NOT EXISTS ElapsedStatsTracker_{0}(timeBucket TEXT PRIMARY KEY,
				AA_1 REAL, AA_2 REAL, AA_3 REAL, AA_4 REAL, AA_5 REAL, AA_6 REAL, AA_7 REAL, AA_8 REAL, AA_9 REAL, AA_10 REAL, AA_11 REAL, 
				A_1 REAL, A_2 REAL, A_3 REAL, A_4 REAL, A_5 REAL, A_6 REAL, A_7 REAL, A_8 REAL, A_9 REAL, A_10 REAL, A_11 REAL, 
				B_1 REAL, B_2 REAL, B_3 REAL, B_4 REAL, B_5 REAL, B_6 REAL, B_7 REAL, B_8 REAL, B_9 REAL, B_10 REAL, B_11 REAL, 
				C_1 REAL, C_2 REAL, C_3 REAL, C_4 REAL, C_5 REAL, C_6 REAL, C_7 REAL, C_8 REAL, C_9 REAL, C_10 REAL, C_11 REAL, 
				D_1 REAL, D_2 REAL, D_3 REAL, D_4 REAL, D_5 REAL, D_6 REAL, D_7 REAL, D_8 REAL, D_9 REAL, D_10 REAL, D_11 REAL, 
				E_1 REAL, E_2 REAL, E_3 REAL, E_4 REAL, E_5 REAL, E_6 REAL, E_7 REAL, E_8 REAL, E_9 REAL, E_10 REAL, E_11 REAL, 
				HR_1 REAL, HR_2 REAL, HR_3 REAL, HR_4 REAL, HR_5 REAL, HR_6 REAL, HR_7 REAL, HR_8 REAL, HR_9 REAL, HR_10 REAL, HR_11 REAL,
				TERM INTEGER)'''
	
	create_str_36 = create_str.format("36")
	create_str_60 = create_str.format("60")
	
	# Create the table in the database
	try:
		crsr.execute(create_str_36)
		crsr.execute(create_str_60)
		dtbs.commit()
	except Exception as e:
		print('El Tracker Creation Errror: ', e)

def updateStats(dtbs, crsr):
	# 1. Query rawListings for each category (77 times)
	# 2. If there are more than 2 listings for the queried category, 
	# 	pull each listing's elapsed funding curve from elapsed table
	# 3. Do Math on the elapsed curves to get averages and standard deviations
	# 4. Puth the results into ElapsedTracker tables
	# 5. Do some interpolation to fill any unfilled categories

	categoryList = ['AA_1', 'AA_2', 'AA_3', 'AA_4', 'AA_5', 'AA_6', 'AA_7', 'AA_8', 'AA_9', 'AA_10', 'AA_11', 
					'A_1', 'A_2', 'A_3', 'A_4', 'A_5', 'A_6', 'A_7', 'A_8', 'A_9', 'A_10', 'A_11', 
					'B_1', 'B_2', 'B_3', 'B_4', 'B_5', 'B_6', 'B_7', 'B_8', 'B_9', 'B_10', 'B_11', 
					'C_1', 'C_2', 'C_3', 'C_4', 'C_5', 'C_6', 'C_7', 'C_8', 'C_9', 'C_10', 'C_11', 
					'D_1', 'D_2', 'D_3', 'D_4', 'D_5', 'D_6', 'D_7', 'D_8', 'D_9', 'D_10', 'D_11', 
					'E_1', 'E_2', 'E_3', 'E_4', 'E_5', 'E_6', 'E_7', 'E_8', 'E_9', 'E_10', 'E_11', 
					'HR_1', 'HR_2', 'HR_3', 'HR_4', 'HR_5', 'HR_6', 'HR_7', 'HR_8', 'HR_9', 'HR_10', 'HR_11']

	rateScoreCounter = []
	for i, category in enumerate(categoryList):
		rateScore = category.split('_')
		
		queryStr = "SELECT ListingNumber FROM rawListings WHERE ProsperRating = '{0}' AND ProsperScore = '{1}'"
		queryStr = queryStr.format(rateScore[0],rateScore[1])
		
		counter = 0
		ListingNumber_list = []
		for n in crsr.execute(queryStr):
			counter += 1 # Count the occurances of each rateScore
			# For each listingNumber returned, build a tuple
			ListingNumber_list.append(str(n[0]))

		# Done with the previous cursor, Build a list of ListingNumbers for the next SQL string
		if counter >0:
			ListingNumber_str = ','.join(ListingNumber_list)
			ListingNumber_str = "("+ListingNumber_str+")"
			
			# Build the SQL SELECT string
			## Bifurcate here to account for 36 and 60 month terms
			queryStr = "SELECT avg(percent_30_seconds), avg(percent_1_minute), avg(percent_2_minute), avg(percent_3_minute), avg(percent_5_minute), avg(percent_10_minute), avg(percent_15_minute), avg(percent_30_minute), avg(percent_60_minute) FROM elapsedListings WHERE ListingNumber IN {0}"
			queryStr = queryStr.format(ListingNumber_str)
			
			for m in crsr.execute(queryStr):
				print(category, m)

			# Here is where we insert the averages for 36 and 60 months into their tables.




		rateScoreCounter.append((rateScore,counter)) # Accumulate all of the rateScore counts

	# Print the rateScore counts
	# for each in rateScoreCounter:
	# 	print(each)



def main():
	#create or open existing db
	db = sqlite3.connect('Listings_MetaData.db')

	# Get a cursor object
	cursor = db.cursor()

	# Set journal mode to WAL (Write Ahead Log)
	# cursor.execute("PRAGMA journal_mode = %s" %("WAL"))
	# db.commit

	# Pull stats of elapsed funding curves
	updateStats(db, cursor)

	# makeElTable(db, cursor)
	

	cursor.close()
	db.close()



if __name__ == '__main__':
	main()
