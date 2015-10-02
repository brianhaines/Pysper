#!/usr/bin/env python3
import requests
import asyncio
import json
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







if __name__ == '__main__':
	#q = asyncio.Queue()
	main()
