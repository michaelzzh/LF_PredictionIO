import json
import csv
import urllib.request, urllib.parse
import http.server
import socketserver
from time import sleep



entityId = 0

def getData(filename):
	data = []
	with open(filename) as f:
		for line in f:
			data.append(json.loads(line))
	return data

def sendSpamTraining(filename, accessKey, server):
	print("importing data from" + filename)
	global entityId
	trainingData = getData(filename)
	#to extract later for testing the algo
	currentId = 0.0
	overallLength = float(len(trainingData))
	for row in trainingData:
		entityType = row['entityType']
		eventTime = row['eventTime']
		properties = row['properties']
		event = row['event']
		
		if filename == 'sentimentanalysis.json':
			sentiment = ''
			if properties['sentiment'] > 2:
			  	sentiment = "positive"
			else:
				sentiment = "negative"

			properties = {"text" : properties['phrase'], "label" : sentiment}
		data = {
			"event" : event,
			"entityType" : entityType,
			"entityId" : entityId,
			"properties" : properties,
			"eventTime" : eventTime
		}
		entityId += 1
		url = 'http://'+server+':7070/events.json?accessKey='+accessKey
		encodedData = json.dumps(data).encode('utf-8')
		header = {"Content-Type" : "application/json"}
		req = urllib.request.Request(url, encodedData, header)
		f = urllib.request.urlopen(req)
		progress = int(currentId / overallLength * 100)
		print(str(progress)+"%", end="\r")
		currentId += 1
		#totalPOSTs += currentPOST + "\n\n"
	print('Done, total data imported: ' + str(currentId))

def testSingleSpam(engineId, server, text, port, clientId):
	data = {
			"clientId": clientId,
			"engineId": engineId,
			"properties": [{
							"queryId":"fjdsaklfj",
							"queryString": "{\"text\":\""+ text + "\"}"
							}]
		}
	url = 'http://'+server+':'+str(port)+'/queries.json'
	encodedData = json.dumps(data).encode('utf-8')
	header = {"Content-Type" : "application/json"}
	req = urllib.request.Request(url, encodedData, header)
	f = urllib.request.urlopen(req)
	fetchedData = f.read().decode('utf-8')
	jsResult = json.loads(fetchedData)
	print(jsResult)


#=====================================================================================================
#////////////////////////////////////////////我是分界线\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
#=====================================================================================================

dataParsing = __import__("Data Parsing script")

'''
Reads from the file 'filename', sends first half of data to pio data preparator to train engine.

Input: 
filename - a .csv file in the same directory 

Output:
<no return value>

'''
def sendGoodTrainingData(filename, accessKey, server):
	allData = dataParsing.cleanParseData(filename)
	trainingData = allData[1:int(len(allData)/2)]
	#to extract later for testing the algo
	currentId = 1
	for row in trainingData:
		eId = "u"+ str(currentId)
		midterm1 = 0
		midterm2 = 0
		final = 0
		if row[32] >= 10:
			final = 1
		if row[31] >= 10:
			midterm2 = 1
		if row[30] >= 10:
			midterm1 = 1
		row[30] = midterm1
		row[31] = midterm2
		data = {
			"event" : "$set",
			"entityType" : "user",
			"entityId" : eId,
			"properties" : {
				"features" : row[2:32],
				"label" : final
			}
		}
		url = 'http://'+server+':7070/events.json?accessKey='+accessKey
		encodedData = json.dumps(data).encode('utf-8')
		header = {"Content-Type" : "application/json"}
		req = urllib.request.Request(url, encodedData, header)
		f = urllib.request.urlopen(req)
		#print(f.read())
		currentId += 1
		print("data entry sent: " + str(currentId), end="\r")
	print("done, total dataset imported: " + str(currentId))

def addOne(x):
	return x+1
def sendALotOfGoodTrainingData(filename, accessKey, server):
	allData = dataParsing.cleanParseData(filename)
	trainingData = allData[1:int(len(allData)/2)]
	#to extract later for testing the algo
	currentId = 1
	for i in range(1,500):
		for row in trainingData:
			eId = "u"+ str(currentId)
			midterm1 = 0
			midterm2 = 0
			final = 0
			if row[32] >= 10:
				final = 1
			if row[31] >= 10:
				midterm2 = 1
			if row[30] >= 10:
				midterm1 = 1
			row[30] = midterm1
			row[31] = midterm2
			dataList = list(map(addOne, row[2:32]))
			data = {
				"event" : "$set",
				"entityType" : "user",
				"entityId" : eId,
				"properties" : {
					"features" : dataList,
					"label" : final
				}
			}
			url = 'http://'+server+':7070/events.json?accessKey='+accessKey
			encodedData = json.dumps(data).encode('utf-8')
			header = {"Content-Type" : "application/json"}
			req = urllib.request.Request(url, encodedData, header)
			f = urllib.request.urlopen(req)
			#print(f.read())
			currentId += 1
			print("data entry sent: " + str(currentId), end="\r")
	print("done, total dataset imported: " + str(currentId))


'''
Tests the accuracy of the Prediction engine.

Input: 
filename - a .csv file in the same directory.

Output:
<no return value>
'''
def testGoodAccuracy(filename, port, server, engineId, num):
	allData = dataParsing.cleanParseData(filename)
	testingData = allData[int(len(allData)/2):len(allData)]
	hit = 0.0
	miss = 0.0
	start = len(testingData)-num
	for row in testingData[start:start+1]:
		midterm1 = 0
		midterm2 = 0
		final = 0
		if row[32] >= 10:
			final = 1
		if row[31] >= 10:
			midterm2 = 1
		if row[30] >= 10:
			midterm1 = 1
		row[31] = midterm2
		row[30] = midterm1
		expectedOutput = float(final)
		expectedRawOutput = row[32]
		data = {
			"engineId": engineId,
			"groupId": "",
			"properties": [{
							"queryId": "",
							"queryString": "{\"features\":"+ '[' + ','.join(map(str, row[2:32])) + "]}"
			}]
		}
		print("{\"features\":"+ '[' + ','.join(map(str, row[2:32])) + "]}")
		#print("{\"features\":"+ '[' + ','.join(map(str, row[2:32])) + "]}")
		url = 'http://'+server+':'+str(port)+'/queries.json'
		encodedData = json.dumps(data).encode('utf-8')
		header = {"Content-Type" : "application/json"}
		req = urllib.request.Request(url, encodedData, header)
		f = urllib.request.urlopen(req)
		fetchedData = f.read().decode('utf-8')
		jsResult = json.loads(json.loads(fetchedData))

		groupId = (jsResult['groupId'])

		while(jsResult['status'] != 'COMPLETED'):
			jsResult = doLastQuery(groupId, server, port, engineId)
			print("progress: " + str(int(jsResult['progress']*100)) + "%", end="\r")
			sleep(0.1)
		print("progress: 100%")
		print(jsResult['predictions'])

		#print(jsResult)
		resultString = jsResult['predictions'][0]['resultString']
		actualResult = json.loads(resultString)['label']
		if float(actualResult) == expectedOutput:
			print("hit")
		else:
			print("miss")
		print("actual score: " + str(expectedRawOutput))
		#print("hit: " + str(hit) + " miss: " + str(miss), end="\r")
	#print("hit: " + str(hit) + " miss: " + str(miss))
	#print("accuracy: " + str(int(hit / (hit + miss) * 100)) + "%")

def testGoodAccuracyBatch(filename, port, server, engineId):
	allData = dataParsing.cleanParseData(filename)
	testingData = allData[int(len(allData)/2):len(allData)]
	hit = 0.0
	miss = 0.0
	properties = []
	queryId = 200
	for i in range(0,1):
		for row in testingData:
			midterm1 = 0
			midterm2 = 0
			if row[31] >= 10:
				midterm2 = 1
			if row[30] >= 10:
				midterm1 = 1
			#row[31] = midterm2
			#row[30] = midterm1
			properties.append({"queryId":"","queryString":"{\"features\":"+ '[' + ','.join(map(str, row[2:30])) + ',' + str(midterm1) + ',' + str(midterm2) + "]}"})
			queryId += 1
	data = {
		"engineId": engineId,
		"groupId": "",
		"properties": properties
	}
	url = 'http://'+server+':'+str(port)+'/queries.json'
	encodedData = json.dumps(data).encode('utf-8')
	header = {"Content-Type" : "application/json"}
	req = urllib.request.Request(url, encodedData, header)
	f = urllib.request.urlopen(req)
	fetchedData = f.read().decode('utf-8')
	jsResult = json.loads(json.loads(fetchedData))
	groupId = (jsResult['groupId'])

	while(jsResult['status'] != 'COMPLETED'):
		jsResult = doLastQuery(groupId, server, port, engineId)
		print("progress: " + str(int(jsResult['progress']*100)) + "%", end="\r")
		sleep(0.1)
	print("progress: 100%")
	#print(jsResult['predictions'])
	index = 0
	hit = 0
	miss = 0
	misses = []
	for prediction in jsResult['predictions']:
		final = 0
		if testingData[index][32] >= 10:
			final = 1
		expectedOutput = float(final)
		actualResult = float(json.loads(prediction['resultString'])['label'])
		if actualResult == expectedOutput:
	 		hit += 1
		else:
	 		miss += 1
	 		misses.append((prediction,testingData[index][30],testingData[index][31],testingData[index][32]))
		index += 1
	 	#print("hit: " + str(hit) + " miss: " + str(miss), end="\r")
	print("hit: " + str(hit) + " miss: " + str(miss))
	print("accuracy: " + str(int(hit / (hit + miss) * 100)) + "%")
	for (m,a,b,c) in misses:
		print(m['resultString'] + "actual score" + str(a) + "," + str(b) + "," + str(c))



def doLastQuery(groupId, server, port, engineId):
	data = {
		"engineId": engineId,
		"groupId": groupId,
		"properties": []
	}
	url = 'http://'+server+':'+str(port)+'/queries.json'
	encodedData = json.dumps(data).encode('utf-8')
	header = {"Content-Type" : "application/json"}
	req = urllib.request.Request(url, encodedData, header)
	f = urllib.request.urlopen(req)
	fetchedData = f.read().decode('utf-8')
	jsResult = json.loads(json.loads(fetchedData))
	return jsResult

def singleQuery(filename, engineId, server, num, port):
	allData = dataParsing.cleanParseData(filename)
	testingData = allData[len(allData)-num]
	midterm1 = 0
	midterm2 = 0
	final = 0
	if testingData[32] >= 10:
		final = 1
	if testingData[31] >= 10:
		midterm2 = 1
	if testingData[30] >= 10:
		midterm1 = 1
	data = {
			"engineId": engineId,
			"groupId": "",
			"properties": [{
							"queryId": "",
							"queryString": "{\"features\":"+ '[' + ','.join(map(str, testingData[2:32])) + "]}"
			}]
		}
	print("{\"features\":"+ '[' + ','.join(map(str, testingData[2:32])) + "]}")
	url = 'http://'+server+':'+str(port)+'/queries.json'
	encodedData = json.dumps(data).encode('utf-8')
	header = {"Content-Type" : "application/json"}
	req = urllib.request.Request(url, encodedData, header)
	try:
		f = urllib.request.urlopen(req)
		fetchedData = f.read().decode('utf-8')
		print(fetchedData)
	except urllib.error.HTTPError as err:
		if err.code == 500:
			print("engine not trained yet\n")
		else:
			raise	

def sendBadTrainingData(filename, accessKey, server):
	allData = dataParsing.cleanParseData(filename)
	trainingData = allData[1:int(len(allData)/2)]
	#to extract later for testing the algo
	currentId = 1
	for row in trainingData:
		eId = "u"+ str(currentId)
		midterm1 = 0
		midterm2 = 0
		final = 0
		if row[32] >= 10:
			final = 1
		if row[31] >= 10:
			midterm2 = 1
		if row[30] >= 10:
			midterm1 = 1
		data = {
			"event" : "$set",
			"entityType" : "user",
			"entityId" : eId,
			"properties" : {
				"features" : row[0:20],
				"label" : final
			}
		}
		url = 'http://'+server+':7070/events.json?accessKey='+accessKey
		encodedData = json.dumps(data).encode('utf-8')
		header = {"Content-Type" : "application/json"}
		req = urllib.request.Request(url, encodedData, header)
		f = urllib.request.urlopen(req)
		#print(f.read())
		currentId += 1
		print("data entry sent: " + str(currentId), end="\r")
	print("done, total dataset imported: " + str(currentId))


'''
Tests the accuracy of the Prediction engine.

Input: 
filename - a .csv file in the same directory.

Output:
<no return value>
'''
def testBadAccuracy(filename, port, server, engineId, num):
	allData = dataParsing.cleanParseData(filename)
	testingData = allData[int(len(allData)/2):len(allData)]
	hit = 0.0
	miss = 0.0
	for row in testingData[len(testingData)-num:len(testingData)-num+1]:
		midterm1 = 0
		midterm2 = 0
		final = 0
		if row[32] >= 10:
			final = 1
		if row[31] >= 10:
			midterm2 = 1
		if row[30] >= 10:
			midterm1 = 1
		expectedOutput = float(final)
		data = {
			"groupId": "",
			"engineId": engineId,
			"properties": [{
							"queryId": "",
							"queryString": "{\"features\":"+ '[' + ','.join(map(str, row[0:20])) + "]}"
						  }]
		}
		print("{\"features\":"+ '[' + ','.join(map(str, row[0:20])) + "]}")
		url = 'http://'+server+':'+str(port)+'/queries.json'
		encodedData = json.dumps(data).encode('utf-8')
		header = {"Content-Type" : "application/json"}
		req = urllib.request.Request(url, encodedData, header)
		f = urllib.request.urlopen(req)
		fetchedData = f.read()
		jsResult = json.loads(json.loads(fetchedData.decode('utf-8')))
		groupId = (jsResult['groupId'])
		while(jsResult['status'] != 'COMPLETED'):
			jsResult = doLastQuery(groupId, server, port, engineId)
			print("progress: " + str(int(jsResult['progress']*100)) + "%", end="\r")
			sleep(0.1)
		print("progress: 100%")
		print(jsResult['predictions'])

		#print(jsResult)
		resultString = jsResult['predictions'][0]['resultString']
		actualResult = json.loads(resultString)['label']
		if float(actualResult) == expectedOutput:
			print("hit")
		else:
			print("miss")
	# 	actualResult = float(jsResult[9:12])
	# 	#print(actualResult)
	# 	if float(actualResult) == expectedOutput:
	# 		hit += 1
	# 	else:
	# 		miss += 1
	# 	print("hit: " + str(hit) + " miss: " + str(miss), end="\r")
	# print("hit: " + str(hit) + " miss: " + str(miss))
	# print("accuracy: " + str(int(hit / (hit + miss) * 100)) + "%")
def testBadAccuracyBatch(filename, port, server, engineId):
	allData = dataParsing.cleanParseData(filename)
	testingData = allData[int(len(allData)/2):len(allData)]
	hit = 0.0
	miss = 0.0
	properties = []
	for row in testingData:
		midterm1 = 0
		midterm2 = 0
		final = 0
		if row[32] >= 10:
			final = 1
		if row[31] >= 10:
			midterm2 = 1
		if row[30] >= 10:
			midterm1 = 1
		properties.append({"queryId":"","queryString":"{\"features\":"+ '[' + ','.join(map(str, row[0:20])) + "]}"})
	data = {
		"groupId": "",
		"engineId": engineId,
		"properties": properties
	}
	url = 'http://'+server+':'+str(port)+'/queries.json'
	encodedData = json.dumps(data).encode('utf-8')
	header = {"Content-Type" : "application/json"}
	req = urllib.request.Request(url, encodedData, header)
	f = urllib.request.urlopen(req)
	fetchedData = f.read()
	jsResult = json.loads(json.loads(fetchedData.decode('utf-8')))
	groupId = (jsResult['groupId'])
	while(jsResult['status'] != 'COMPLETED'):
		jsResult = doLastQuery(groupId, server, port, engineId)
		print("progress: " + str(int(jsResult['progress']*100)) + "%", end="\r")
		sleep(0.1)
	print("progress: 100%")
	print(jsResult['predictions'])
	index = 0
	hit = 0
	miss = 0
	for prediction in jsResult['predictions']:
		final = 0
		if testingData[index][32] >= 10:
			final = 1
		expectedOutput = float(final)
		actualResult = float(json.loads(prediction['resultString'])['label'])
		if actualResult == expectedOutput:
	 		hit += 1
		else:
	 		miss += 1
	 	#print("hit: " + str(hit) + " miss: " + str(miss), end="\r")
	print("hit: " + str(hit) + " miss: " + str(miss))
	print("accuracy: " + str(int(hit / (hit + miss) * 100)) + "%")

def trainEngine(userName, server, baseEngine):
	data = {
			"engineId" : userName,
			"accessKey" : "",
			"baseEngine" : baseEngine
		}
	url = 'http://'+server+':7070/engine/train'
	encodedData = json.dumps(data).encode('utf-8')
	header = {"Content-Type" : "application/json"}
	req = urllib.request.Request(url, encodedData, header)
	f = urllib.request.urlopen(req)
	fetchedData = f.read().decode('utf-8')
	print(fetchedData)
	status = ""

	textDisplay = ["training.  ", "training.. ", "training..."]
	index = 0
	while status != "COMPLETED":
		status = getTrainingStatus(userName, server)
		print(textDisplay[index % 3], end="\r")
		index += 1
		sleep(0.5)
	print("training completed")


def getTrainingStatus(engineId, server):
	data = {
			"engineId" : engineId,
			"baseEngine" : ""
		}
	url = 'http://'+server+':7070/engine/status'
	encodedData = json.dumps(data).encode('utf-8')
	header = {"Content-Type" : "application/json"}
	req = urllib.request.Request(url, encodedData, header)
	f = urllib.request.urlopen(req)
	fetchedData = f.read().decode('utf-8')
	jsResult = json.loads(json.loads(fetchedData))
	return jsResult['status']
	#return fetchedData

def deployEngine(userName, portNum, server):
	data = {
			"userName" : userName,
			"accessKey" : "",
			"port" : portNum
		}
	url = 'http://'+server+':7070/engine/deploy'
	encodedData = json.dumps(data).encode('utf-8')
	header = {"Content-Type" : "application/json"}
	req = urllib.request.Request(url, encodedData, header)
	f = urllib.request.urlopen(req)
	fetchedData = f.read().decode('utf-8')
	print(fetchedData)


def registerApp(name, accessKey, server, baseEngine):
	data = data = {
			"engineId" : name,
			"accessKey" : accessKey,
			"baseEngine" : baseEngine
		}
	url = 'http://'+server+':7070/engine/register'
	encodedData = json.dumps(data).encode('utf-8')
	header = {"Content-Type" : "application/json"}
	req = urllib.request.Request(url, encodedData, header)
	f = urllib.request.urlopen(req)
	fetchedData = f.read()
	jsResult = json.loads(json.loads(fetchedData))
	print(jsResult['engineId'])
	f = open('projects.txt', 'w')
	f.write(name + '\n')
	f.write(jsResult['engineId'] + '\n')
	f.write(baseEngine + '\n')
	f.close()
	return jsResult['engineId']


def getProjectData():
	lines = []
	with open("projects.txt") as f:
		lines = f.readlines()
	return lines


def userInput():
	engineIndex = -1
	baseEngines = ["baseClassification", "baseTextClassification"]
	ports = [8080, 9000]

	server1 = "v-dev-ubudsk-2"
	server2 = "v-dev-ubudsk-3"

	currentServer = server1
	projects = getProjectData()
	num = 1
	for i in range(0,len(projects)-2, 3):
		print(str(num) + "." + projects[i].replace('\n', '') + ": " + projects[i+1].replace('\n', '') + "baseEngine: " + projects[i+2].replace('\n', '') + '\n')

	projectchoice = int(input("select project or input 0 to create a new one\n"))
	userAppName = ""
	baseEngine = ""
	if(projectchoice == 0):
		userAppName = input("enter project name: \n")
		index = -1
		while index <= 0 or index > 2:
			index = int(input("select a base engine: 1. classification, 2. spamIdentifier\n"))
			engineIndex = index - 1
		baseEngine = baseEngines[engineIndex]
	else:
		userAppName = projects[(projectchoice*3-2)].replace('\n', '')
		baseEngine = projects[(projectchoice*3-1)].replace('\n', '')
		if(baseEngine == 'baseClassification'):
			engineIndex = 0
		else:
			engineIndex = 1
		print("project id is: " + userAppName)

	
	
	accessKey = userAppName
	serverNum = int(input("enter server number"))
	if serverNum == 2:
		currentServer = server2
	print("current server is " + currentServer)
	running = True
	clientId = "client1"
	while running:

		response = input("enter changeServer, register, query, train, testGood, testBad, testBadBatch, testGoodBatch, sendInsaneData, sendGoodTraining, sendBadTraining, sendSpamTraining, testSpam or quit: \n")
		if response == 'register':
			print("the base engine is: " + baseEngine + "\n")
			userAppName = registerApp(userAppName, accessKey, currentServer, baseEngine)
			accessKey = userAppName
			print("project id is: " + userAppName)
		elif response == 'changeServer':
			serverNum = int(input("enter server number\n"))
			if serverNum == 1:
				currentServer = server1
			else:
				currentServer = server2
			print("current server is " + currentServer + "\n")
		elif response == 'train':
			trainEngine(userAppName, currentServer, baseEngine)
		elif response == 'testGood':
			num = int(input("enter which query to perform"))
			testGoodAccuracy('student-mat.csv', ports[engineIndex], currentServer, userAppName, num)
		elif response == 'testBad':
			num = int(input("enter which query to perform"))
			testBadAccuracy('student-mat.csv', ports[engineIndex], currentServer, userAppName, num)
		elif response == 'sendGoodTraining':
			sendGoodTrainingData('student-mat.csv', accessKey, currentServer)
		elif response == 'sendInsaneData':
			sendALotOfGoodTrainingData('student-mat.csv', accessKey, currentServer)
		elif response == 'sendBadTraining':
			sendBadTrainingData('student-mat.csv', accessKey, currentServer)
		elif response == 'testBadBatch':
			testBadAccuracyBatch('student-mat.csv', ports[engineIndex], currentServer, userAppName)
		elif response == 'testGoodBatch':
			testGoodAccuracyBatch('student-mat.csv', ports[engineIndex], currentServer, userAppName)
		elif response == 'sendSpamTraining':
			sendSpamTraining('emails.json', userAppName, currentServer)
			sendSpamTraining('stopwords.json', userAppName, currentServer)
		elif response == 'testSpam':
			text = input("enter text\n")
			testSingleSpam(userAppName, currentServer, text, ports[engineIndex], clientId)
		elif response == 'quit':
			running = False
		else:
			print("unknown command\n")
	


#testAccuracy('student-mat.csv')
#sendTrainingData('student-mat.csv')
userInput()