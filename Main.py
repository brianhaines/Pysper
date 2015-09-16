#!/usr/bin/env python3
import requests


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
	urlString = ("https://", usrName, ":",PW, "@", "api.prosper.com/api/Listings/?$top=20")
	urlString = ''.join(urlString)
	
	headers = {'Content-Type': 'application/json'}
	r = requests.get(urlString, headers=headers)
	j = r.json()

	for listing in j:
		print('____________________')
		print('MemberKey: ', listing['MemberKey'])
		print('ListingNumber: ', listing['ListingNumber'])
		print('ListingStartDate:', listing['ListingStartDate'])
		print('____________________')
	


if __name__ == '__main__':
	main()