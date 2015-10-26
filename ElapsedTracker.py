import sqlite3

def makeElTable(dtbs, crsr):
	# Create the Elapsed Funding Tracking tables
	# Average and Std Dev for each combo for each bucket

	create_str = '''CREATE TABLE IF NOT EXISTS ElapsedStatsTracker_{0}(sizeRange TEXT PRIMARY KEY, TERM INTEGER, Counter INTEGER,
				AA_1 TEXT, AA_2 TEXT, AA_3 TEXT, AA_4 TEXT, AA_5 TEXT, AA_6 TEXT, AA_7 TEXT, AA_8 TEXT, AA_9 TEXT, AA_10 TEXT, AA_11 TEXT, 
				A_1 TEXT, A_2 TEXT, A_3 TEXT, A_4 TEXT, A_5 TEXT, A_6 TEXT, A_7 TEXT, A_8 TEXT, A_9 TEXT, A_10 TEXT, A_11 TEXT, 
				B_1 TEXT, B_2 TEXT, B_3 TEXT, B_4 TEXT, B_5 TEXT, B_6 TEXT, B_7 TEXT, B_8 TEXT, B_9 TEXT, B_10 TEXT, B_11 TEXT, 
				C_1 TEXT, C_2 TEXT, C_3 TEXT, C_4 TEXT, C_5 TEXT, C_6 TEXT, C_7 TEXT, C_8 TEXT, C_9 TEXT, C_10 TEXT, C_11 TEXT, 
				D_1 TEXT, D_2 TEXT, D_3 TEXT, D_4 TEXT, D_5 TEXT, D_6 TEXT, D_7 TEXT, D_8 TEXT, D_9 TEXT, D_10 TEXT, D_11 TEXT, 
				E_1 TEXT, E_2 TEXT, E_3 TEXT, E_4 TEXT, E_5 TEXT, E_6 TEXT, E_7 TEXT, E_8 TEXT, E_9 TEXT, E_10 TEXT, E_11 TEXT, 
				HR_1 TEXT, HR_2 TEXT, HR_3 TEXT, HR_4 TEXT, HR_5 TEXT, HR_6 TEXT, HR_7 TEXT, HR_8 TEXT, HR_9 TEXT, HR_10 TEXT, HR_11 TEXT
				)'''
	
	create_str_36 = create_str.format("36")
	create_str_60 = create_str.format("60")
	
	# Create the table in the database
	try:
		crsr.execute(create_str_36)
		crsr.execute(create_str_60)
		dtbs.commit()
	except Exception as e:
		print('El Tracker Creation Errror: ', e)

def updateStats(dtbs, crsr, term_months):
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

	sizeList = [(1999, 4000), (4001, 6000), (6001, 8000), (8001, 10000), (10001, 12000), (12001, 15000), (15001,20000), (20001, 25000), (25001, 30000), (30001, 35000)]

	rateScoreCounter = []
	for i, category in enumerate(categoryList):
		
		counter = 0
		for j, size in enumerate(sizeList):
			rateScore = category.split('_')

			queryStr = "SELECT ListingNumber FROM rawListings WHERE ProsperRating = '{0}' AND ProsperScore = '{1}' AND ListingRequestAmount BETWEEN {2} AND {3}"
			queryStr = queryStr.format(rateScore[0],rateScore[1], size[0], size[1])
			

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
				# Bifurcate here to account for 36 and 60 month terms
				queryStr = "SELECT avg(el.percent_30_seconds), avg(el.percent_1_minute), avg(el.percent_2_minute), avg(el.percent_3_minute), avg(el.percent_5_minute), avg(el.percent_10_minute), avg(el.percent_15_minute), avg(el.percent_30_minute), avg(el.percent_60_minute)  FROM elapsedListings AS el  INNER JOIN rawListings AS raw ON el.listingNumber=raw.listingNumber WHERE raw.ListingTerm = {0} AND el.ListingNumber IN {1}"
				# queryStr_60 = "SELECT avg(el.percent_30_seconds), avg(el.percent_1_minute), avg(el.percent_2_minute), avg(el.percent_3_minute), avg(el.percent_5_minute), avg(el.percent_10_minute), avg(el.percent_15_minute), avg(el.percent_30_minute), avg(el.percent_60_minute)  FROM elapsedListings AS el  INNER JOIN rawListings AS raw ON el.listingNumber=raw.listingNumber WHERE raw.ListingTerm = 60 AND el.ListingNumber IN {0}"
				queryStr = queryStr.format(term_months, ListingNumber_str)
				# queryStr_60 = queryStr_60.format(ListingNumber_str)
				# print(queryStr)


				# # Do the the query for specified term
				prim_key = category + '_' + str(size[0]) + '_' + str(size[1])
				update_Vals = []
				insert_Vals =  []
				for m in crsr.execute(queryStr):
					elapsed_vals = m

				update_Str = "UPDATE OR IGNORE ElapsedStatsTracker_{0} SET {1} = ?, Counter = ?, Term = ? WHERE sizeRange  = ?"
				update_Str = update_Str.format(term_months, category)
				update_Vals.append(str(elapsed_vals))
				update_Vals.append(counter)
				update_Vals.append(term_months)
				update_Vals.append(prim_key)
				# print(size, update_Vals)
				crsr.execute(update_Str,update_Vals)
				dtbs.commit()
				# print(prim_key, counter, m)
				if crsr.rowcount <1:
					# Nothing updated, so insert.
					insert_Str = "INSERT OR IGNORE INTO ElapsedStatsTracker_{0}(sizeRange, Counter, Term, {1}) VALUES(?,?,?,?)"
					insert_Str = insert_Str.format(term_months, category)
					
					insert_Vals.append(prim_key)
					insert_Vals.append(counter)
					insert_Vals.append(term_months)
					insert_Vals.append(str(elapsed_vals))
					crsr.execute(insert_Str,insert_Vals)
					dtbs.commit()

		rateScoreCounter.append((category, counter))	
		
	for each in rateScoreCounter:
		print(each)





def main():
	#create or open existing db
	db = sqlite3.connect('Listings_MetaData.db')

	# Get a cursor object
	cursor = db.cursor()

	# Set journal mode to WAL (Write Ahead Log)
	# cursor.execute("PRAGMA journal_mode = %s" %("WAL"))
	# db.commit

	# Make the tables
	makeElTable(db, cursor)
	

	# Pull stats of elapsed funding curves
	updateStats(db, cursor,36)

	

	cursor.close()
	db.close()



if __name__ == '__main__':
	main()
