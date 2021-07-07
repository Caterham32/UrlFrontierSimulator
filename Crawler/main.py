import yaml
import random
import queue

# load config
config = yaml.safe_load(open("config.yml"))
InitialUrlCount = config["numberUrl"]
InitialServerCount = config["numberServer"]
intervall = config["intervall"]
numberOfFrontQueues = config["numberOfFrontQueues"]

urlNumber = 0
latencyMin = 10
latencyMax = 1000
updateTimeMin = 10
updateTimeMax = 1000

updateTimeIntervall = (updateTimeMax-updateTimeMin)/numberOfFrontQueues

class UrlModel:
    def __init__(self, identifier, server, updateTime : int):
        self.Identifier = identifier
        self.Server = server
        self.UpdateTime = updateTime

    def print(self):
        print("URL number: " + str(self.Identifier))
        self.Server.print()
        print("Url UpdateTime: " + str(self.UpdateTime))

class Server:
    def __init__(self, identifier, latency : int):
        self.Identifier = identifier
        self.Latency = latency

    def print(self):
        print("Server Identifier: " + str(self.Identifier))
        print("Server Latency: " + str(self.Latency))

def generateServer(count : int):
    global latencyMin, latencyMax
    list = []
    for i in range(count):
        latency = random.randint(latencyMin, latencyMax)
        list.append(Server(i,latency))
    return list

def generateUrls(queue, count : int, serverList):
    global urlNumber, updateTimeMin, updateTimeMax
    maxIndex = len(serverList) - 1
    for i in range(count):
        # used to choose server
        index = random.randint(0, maxIndex)
        updatetime  = random.randint(updateTimeMin, updateTimeMax)
        queue.put(UrlModel(urlNumber, serverList[index],updatetime))
        urlNumber += 1
    return queue

def prioritization(urlQueue, frontQueues):
    global updateTimeIntervall
    while not urlQueue.empty():
        item = urlQueue.get()
        index = int((item.UpdateTime - updateTimeMin)//updateTimeIntervall)
        frontQueues[index].put(item)
    return frontQueues



# list of timestamps    
times = [1]

# initial queue
urlQueue = queue.Queue()

# initial frontQueue
frontQueues = []
for i in range(numberOfFrontQueues):
    frontQueues.append(queue.Queue())

# generate initial data
serverlist = generateServer(InitialServerCount)
generateUrls(urlQueue, InitialUrlCount, serverlist)

for timestamp in times:

    # generate new data at timestamp
    urlCount = random.randint(10, 100)
    urlQueue = generateUrls(urlQueue, urlCount, serverlist)
    frontQueues = prioritization(urlQueue,frontQueues)

i = 0

for frontQueue in frontQueues:
    print("frontQueue " + str(i) + " :" )
    while not frontQueue.empty():
        item = frontQueue.get()
        item.print()
    print ("-----------------------------------------------------")
    i += 1



   