
def listingSaver(listings,dtbs, crsr):
	# Loop through listings JSON dict
	insert_vals = []
	for listing in listings:
		insert_vals.append((listing['ListingNumber'],
						listing['MemberKey'],
						listing['LoanNumber'],
						listing['CreditPullDate'],
						listing['ListingStartDate'],
						listing['ListingEndDate'],
						listing['ListingCreationDate'],
						listing['ListingStatus'],
						listing['ListingStatusDescription'],
						listing['VerificationStage'],
						listing['ListingRequestAmount'],
						listing['ListingAmountFunded'],
						listing['AmountRemaining'],
						listing['PercentFunded'],
						listing['PartialFundingIndicator'],
						listing['FundingThreshold'],
						listing['ProsperRating'],
						listing['EstimatedReturn'],
						listing['EstimatedLossRate'],
						listing['LenderYield'],
						listing['EffectiveYield'],
						listing['BorrowerRate'],
						listing['BorrowerAPR'],
						listing['ListingTerm'],
						listing['ListingMonthlyPayment'],
						listing['ScoreX'],
						listing['ScoreXChange'],
						listing['FICOScore'],
						listing['ProsperScore'],
						listing['ListingCategory'],
						listing['ListingTitle'],
						listing['IncomeRange'],
						listing['IncomeRangeDescription'],
						listing['StatedMonthlyIncome'],
						listing['IncomeVerifiable'],
						listing['DTIwProsperLoan'],
						listing['EmploymentStatusDescription'],
						listing['Occupation'],
						listing['MonthsEmployed'],
						listing['BorrowerState'],
						listing['BorrowerCity'],
						listing['BorrowerMetropolitanArea'],
						listing['FIL001'],
						listing['FIL023'],
						listing['FIN001'],
						listing['FIN026'],
						listing['FIN801'],
						listing['PriorProsperLoansActive'],
						listing['PriorProsperLoans'],
						listing['PriorProsperLoansPrincipalBorrowed'],
						listing['PriorProsperLoansPrincipalOutstanding'],
						listing['PriorProsperLoansBalanceOutstanding'],
						listing['PriorProsperLoansCyclesBilled'],
						listing['PriorProsperLoansOnTimePayments'],
						listing['PriorProsperLoansLateCycles'],
						listing['PriorProsperLoansLatePaymentsOneMonthPlus'],
						listing['MaxPriorProsperLoan'],
						listing['MinPriorProsperLoan'],
						listing['PriorProsperLoanEarliestPayOff'],
						listing['PriorProsperLoans31DPD'],
						listing['PriorProsperLoans61DPD'],
						listing['LenderIndicator'],
						listing['GroupIndicator'],
						listing['GroupName'],
						listing['ChannelCode'],
						listing['AmountParticipation'],
						listing['MonthlyDebt'],
						listing['CurrentDelinquencies'],
						listing['DelinquenciesLast7Years'],
						listing['PublicRecordsLast10Years'],
						listing['PublicRecordsLast12Months'],
						listing['FirstRecordedCreditLine'],
						listing['CreditLinesLast7Years'],
						listing['InquiriesLast6Months'],
						listing['AmountDelinquent'],
						listing['CurrentCreditLines'],
						listing['OpenCreditLines'],
						listing['BankcardUtilization'],
						listing['TotalOpenRevolvingAccounts'],
						listing['InstallmentBalance'],
						listing['RealEstateBalance'],
						listing['RevolvingBalance'],
						listing['RealEstatePayment'],
						listing['RevolvingAvailablePercent'],
						listing['TotalInquiries'],
						listing['TotalTradeItems'],
						listing['SatisfactoryAccounts'],
						listing['NowDelinquentDerog'],
						listing['WasDelinquentDerog'],
						listing['OldestTradeOpenDate'],
						listing['DelinquenciesOver30Days'],
						listing['DelinquenciesOver60Days'],
						listing['DelinquenciesOver90Days'],
						listing['IsHomeowner'],
						listing['InvestmentTypeID'],
						listing['InvestmentTypeDescription'],
						listing['WholeLoanStartDate'],
						listing['WholeLoanEndDate'],
						listing['LastUpdatedDate']
						))

	# Loop is finished, now create the insert string
	insert_Str = '''INSERT OR IGNORE INTO rawListings(ListingNumber,
					MemberKey,
					LoanNumber,
					CreditPullDate,
					ListingStartDate,
					ListingEndDate,
					ListingCreationDate,
					ListingStatus,
					ListingStatusDescription,
					VerificationStage,
					ListingRequestAmount,
					ListingAmountFunded,
					AmountRemaining,
					PercentFunded,
					PartialFundingIndicator,
					FundingThreshold,
					ProsperRating,
					EstimatedReturn,
					EstimatedLossRate,
					LenderYield,
					EffectiveYield,
					BorrowerRate,
					BorrowerAPR,
					ListingTerm,
					ListingMonthlyPayment,
					ScoreX,
					ScoreXChange,
					FICOScore,
					ProsperScore,
					ListingCategory,
					ListingTitle,
					IncomeRange,
					IncomeRangeDescription,
					StatedMonthlyIncome,
					IncomeVerifiable,
					DTIwProsperLoan,
					EmploymentStatusDescription,
					Occupation,
					MonthsEmployed,
					BorrowerState,
					BorrowerCity,
					BorrowerMetropolitanArea,
					FIL001,
					FIL023,
					FIN001,
					FIN026,
					FIN801,
					PriorProsperLoansActive,
					PriorProsperLoans,
					PriorProsperLoansPrincipalBorrowed,
					PriorProsperLoansPrincipalOutstanding,
					PriorProsperLoansBalanceOutstanding,
					PriorProsperLoansCyclesBilled,
					PriorProsperLoansOnTimePayments,
					PriorProsperLoansLateCycles,
					PriorProsperLoansLatePaymentsOneMonthPlus,
					MaxPriorProsperLoan,
					MinPriorProsperLoan,
					PriorProsperLoanEarliestPayOff,
					PriorProsperLoans31DPD,
					PriorProsperLoans61DPD,
					LenderIndicator,
					GroupIndicator,
					GroupName,
					ChannelCode,
					AmountParticipation,
					MonthlyDebt,
					CurrentDelinquencies,
					DelinquenciesLast7Years,
					PublicRecordsLast10Years,
					PublicRecordsLast12Months,
					FirstRecordedCreditLine,
					CreditLinesLast7Years,
					InquiriesLast6Months,
					AmountDelinquent,
					CurrentCreditLines,
					OpenCreditLines,
					BankcardUtilization,
					TotalOpenRevolvingAccounts,
					InstallmentBalance,
					RealEstateBalance,
					RevolvingBalance,
					RealEstatePayment,
					RevolvingAvailablePercent,
					TotalInquiries,
					TotalTradeItems,
					SatisfactoryAccounts,
					NowDelinquentDerog,
					WasDelinquentDerog,
					OldestTradeOpenDate,
					DelinquenciesOver30Days,
					DelinquenciesOver60Days,
					DelinquenciesOver90Days,
					IsHomeowner,
					InvestmentTypeID,
					InvestmentTypeDescription,
					WholeLoanStartDate,
					WholeLoanEndDate,
					LastUpdatedDate
					) VALUES(?,?,?,?,?,?,?,?,?,?,
						?,?,?,?,?,?,?,?,?,?,
						?,?,?,?,?,?,?,?,?,?,
						?,?,?,?,?,?,?,?,?,?,
						?,?,?,?,?,?,?,?,?,?,
						?,?,?,?,?,?,?,?,?,?,
						?,?,?,?,?,?,?,?,?,?,
						?,?,?,?,?,?,?,?,?,?,
						?,?,?,?,?,?,?,?,?,?,
						?,?,?,?,?,?,?,?,?
						)'''
	# Use execute many to put the list of listings into the rawListings table
	try:
		crsr.executemany(insert_Str, insert_vals)
		dtbs.commit()
	except Exception as e:
		print('Insert Raw Error: ', e) 
	finally:
		listingUpdater(listings, dtbs, crsr)
	

def makeRawTable(dtbs, crsr):
	# With a database and a cursor, create the rawListings table if it doesn't
	# already exist

	create_str = '''CREATE TABLE IF NOT EXISTS rawListings(ListingNumber INTEGER UNIQUE PRIMARY KEY,
					MemberKey TEXT,
					LoanNumber INTEGER,
					CreditPullDate TEXT,
					ListingStartDate TEXT,
					ListingEndDate TEXT,
					ListingCreationDate TEXT,
					ListingStatus INTEGER,
					ListingStatusDescription TEXT,
					VerificationStage INTEGER,
					ListingRequestAmount REAL,
					ListingAmountFunded REAL,
					AmountRemaining REAL,
					PercentFunded REAL,
					PartialFundingIndicator TEXT,
					FundingThreshold REAL,
					ProsperRating INTEGER,
					EstimatedReturn REAL,
					EstimatedLossRate REAL,
					LenderYield REAL,
					EffectiveYield REAL,
					BorrowerRate REAL,
					BorrowerAPR REAL,
					ListingTerm INTEGER,
					ListingMonthlyPayment REAL,
					ScoreX TEXT,
					ScoreXChange TEXT,
					FICOScore TEXT,
					ProsperScore INTEGER,
					ListingCategory INTEGER,
					ListingTitle TEXT,
					IncomeRange INTEGER,
					IncomeRangeDescription TEXT,
					StatedMonthlyIncome REAL,
					IncomeVerifiable TEXT,
					DTIwProsperLoan REAL,
					EmploymentStatusDescription TEXT,
					Occupation TEXT,
					MonthsEmployed INTEGER,
					BorrowerState TEXT,
					BorrowerCity TEXT,
					BorrowerMetropolitanArea TEXT,
					FIL001 INTEGER,
					FIL023 INTEGER,
					FIN001 INTEGER,
					FIN026 INTEGER,
					FIN801 INTEGER,
					PriorProsperLoansActive INTEGER,
					PriorProsperLoans INTEGER,
					PriorProsperLoansPrincipalBorrowed REAL,
					PriorProsperLoansPrincipalOutstanding REAL,
					PriorProsperLoansBalanceOutstanding REAL,
					PriorProsperLoansCyclesBilled INTEGER,
					PriorProsperLoansOnTimePayments INTEGER,
					PriorProsperLoansLateCycles INTEGER,
					PriorProsperLoansLatePaymentsOneMonthPlus INTEGER,
					MaxPriorProsperLoan REAL,
					MinPriorProsperLoan REAL,
					PriorProsperLoanEarliestPayOff INTEGER,
					PriorProsperLoans31DPD INTEGER,
					PriorProsperLoans61DPD INTEGER,
					LenderIndicator INTEGER,
					GroupIndicator TEXT,
					GroupName TEXT,
					ChannelCode INTEGER,
					AmountParticipation REAL,
					MonthlyDebt REAL,
					CurrentDelinquencies INTEGER,
					DelinquenciesLast7Years INTEGER,
					PublicRecordsLast10Years INTEGER,
					PublicRecordsLast12Months INTEGER,
					FirstRecordedCreditLine TEXT,
					CreditLinesLast7Years INTEGER,
					InquiriesLast6Months INTEGER,
					AmountDelinquent REAL,
					CurrentCreditLines INTEGER,
					OpenCreditLines INTEGER,
					BankcardUtilization REAL,
					TotalOpenRevolvingAccounts INTEGER,
					InstallmentBalance REAL,
					RealEstateBalance REAL,
					RevolvingBalance REAL,
					RealEstatePayment REAL,
					RevolvingAvailablePercent REAL,
					TotalInquiries INTEGER,
					TotalTradeItems INTEGER,
					SatisfactoryAccounts INTEGER,
					NowDelinquentDerog INTEGER,
					WasDelinquentDerog INTEGER,
					OldestTradeOpenDate INTEGER,
					DelinquenciesOver30Days INTEGER,
					DelinquenciesOver60Days INTEGER,
					DelinquenciesOver90Days INTEGER,
					IsHomeowner TEXT,
					InvestmentTypeID INTEGER,
					InvestmentTypeDescription TEXT,
					WholeLoanStartDate TEXT,
					WholeLoanEndDate TEXT,
					LastUpdatedDate	 TEXT
		)'''
	try:
		crsr.execute(create_str)
		dtbs.commit()
	except Exception as e:
		print('Raw Creation Errror: ', e)
	

def listingUpdater(listings,dtbs, crsr):
	# Loop through listings JSON dict
	update_vals = []
	for listing in listings:
		update_vals.append((listing['ListingEndDate'],
						listing['ListingStatus'],
						listing['ListingStatusDescription'],
						listing['VerificationStage'],
						listing['ListingAmountFunded'],
						listing['AmountRemaining'],
						listing['PercentFunded'],
						listing['PartialFundingIndicator'],
						listing['ProsperRating'],
						listing['IncomeRange'],
						listing['IncomeRangeDescription'],
						listing['StatedMonthlyIncome'],
						listing['IncomeVerifiable'],
						listing['EmploymentStatusDescription'],
						listing['Occupation'],
						listing['MonthsEmployed'],
						listing['RevolvingAvailablePercent'],
						listing['IsHomeowner'],
						listing['LastUpdatedDate'],
						listing['ListingNumber']
						))


	# Loop is finished, now create the insert string
	update_Str = '''UPDATE OR IGNORE rawListings SET
					ListingEndDate = ?,
					ListingStatus = ?,
					ListingStatusDescription = ?,
					VerificationStage = ?,
					ListingAmountFunded = ?,
					AmountRemaining = ?,
					PercentFunded = ?,
					PartialFundingIndicator = ?,
					ProsperRating = ?,
					IncomeRange = ?,
					IncomeRangeDescription = ?,
					StatedMonthlyIncome = ?,
					IncomeVerifiable = ?,
					EmploymentStatusDescription = ?,
					Occupation = ?,
					MonthsEmployed = ?,
					RevolvingAvailablePercent = ?,
					IsHomeowner = ?,
					LastUpdatedDate = ?
					WHERE listingNumber = ?
					'''
	# Use execute many to update the raw listings
	try:
		crsr.executemany(update_Str, update_vals)
		dtbs.commit()
	except Exception as e:
		print('Update Raw Error: ', e) 