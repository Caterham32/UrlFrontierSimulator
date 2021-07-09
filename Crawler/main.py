import yaml
import random
import queue
import heapq

# load config
config = yaml.safe_load(open("config.yml"))
_initialUrlCount = config["numberUrl"]
_initialServerCount = config["numberServer"]
_numberOfFrontQueues = config["numberOfFrontQueues"]

_urlNumber = 0
_latencyMin = 10
_latencyMax = 1000
_updateTimeMin = 24
_updateTimeMax = 2400

# for back selctor
_queueWeights = list(reversed([ x + 1  for x in range(_numberOfFrontQueues)]))

# table for mapping host to bach queue number
_hostsToBackQueue = {}

_heap = []

_updateTimeIntervall = (_updateTimeMax - _updateTimeMin)/_numberOfFrontQueues + 1

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
    def __init__(self, identifier, latency : int, lastVisit : int):
        self.Identifier = identifier
        self.Latency = latency
        self.LastVisit = lastVisit

    def print(self):
        print("Server Identifier: " + str(self.Identifier))
        print("Server Latency: " + str(self.Latency))
        print("Server Latency: " + str(self.LastVisit))

def generateServer(count : int):
    global _latencyMin, _latencyMax
    list = []
    for i in range(count):
        latency = random.randint(_latencyMin, _latencyMax)
        list.append(Server(i,latency,0))
    return list

def generateUrls(queue, count : int, serverList):
    global _urlNumber, _updateTimeMin, _updateTimeMax
    maxIndex = len(serverList) - 1
    for i in range(count):
        # used to choose server
        index = random.randint(0, maxIndex)
        updatetime  = random.randint(_updateTimeMin, _updateTimeMax)
        queue.put(UrlModel(_urlNumber, serverList[index], updatetime))
        _urlNumber += 1
    return queue

def prioritization(urlQueue, frontQueues):
    global _updateTimeIntervall, _updateTimeMin
    while not urlQueue.empty():
        item = urlQueue.get()
        index = int((item.UpdateTime - _updateTimeMin)//_updateTimeIntervall)
        frontQueues[index].put(item)

def pullUrlFromFront(frontQueues):
    global _queueWeights
    while True:
        # chooses url from a front queue, distribution is set by queueWeights
        q = random.choices(population = frontQueues, weights = _queueWeights)[0]

        # if queue is empty choose a new one
        if(q.empty()):
            continue
        else:
            return q.get()


def fillBackQueue(count, frontQueues, backQueues, serverlist, _hostsToBackQueue):
    global _heap
    for i in range(count):
        newUrl = pullUrlFromFront(frontQueues)
        if newUrl.Server.Identifier in _hostsToBackQueue:
            index = _hostsToBackQueue[newUrl.Server.Identifier]
            backQueues[index].put(newUrl)
        else:
            item = queue.Queue()
            item.put(newUrl)
            backQueues.append(item)
            _hostsToBackQueue[newUrl.Server.Identifier] = len(backQueues) - 1

            # idear is to visit server with low latency with higher priority
            # => could be done better
            #
            # heap contains (metric ,ServerIdentifier)
            metric = calculateHeapValue(newUrl.Server.Latency, newUrl.Server.LastVisit)
            heapq.heappush(_heap, (metric, newUrl.Server.Identifier))


def calculateHeapValue(Latency, lastVisit):
    global _latencyMax, _latencyMin
    # metric to calculate heap weigth:
    # latency + penalty term, if server was alredy visited
    return Latency + lastVisit*(_latencyMax - _latencyMin)

def removeItemAndUpdateTable(_hostsToBackQueue, itemKey):
    oldvalue = _hostsToBackQueue[itemKey]
    del _hostsToBackQueue[itemKey]

    dictCache = _hostsToBackQueue

    for key,value in _hostsToBackQueue.items():
        if(value > oldvalue):
            _hostsToBackQueue.update( {key : (value - 1) })

def prettyPrintQueue(q):
    queueAsList = list(q.queue)
    listLength = len(queueAsList)

    if(listLength < 3):
        for i in range(listLength):
            queueAsList[i].print()
    else:
        for i in range(3):
            queueAsList[i].print()
    print("--------")

# list of timestamps    
times = [1,2,3,4,5]

# initial queue
urlQueue = queue.Queue()

# initial front queues
frontQueues = []
for i in range(_numberOfFrontQueues):
    frontQueues.append(queue.Queue())

# initial back queues
backQueues = []

# final queue which can be used by crawler thread
finalQueue = queue.Queue()

# 

# generate initial data
serverlist = generateServer(_initialServerCount)
generateUrls(urlQueue, _initialUrlCount, serverlist)

prioritization(urlQueue, frontQueues)

for timestamp in times:

    print(str(timestamp) + " run start") 
    # generate new data at timestamp
    urlCount = random.randint(10, 100)

    # generate new url's for timestamp
    generateUrls(urlQueue, urlCount, serverlist)

    # add urls to front queues
    prioritization(urlQueue, frontQueues)

    # same amount of url's are added to back queue as were added to the front queue
    fillBackQueue(urlCount, frontQueues, backQueues, serverlist, _hostsToBackQueue)

    for q in backQueues:
        print("back queue " + str(backQueues.index(q)) + " with count " + str(q.qsize()) + ":" )
        prettyPrintQueue(q)

    # choose back queue with serverId with help of heap
    serverIdentifier = heapq.heappop(_heap)[1]
    backQueueIndex = _hostsToBackQueue[serverIdentifier]
    backQueue = backQueues[backQueueIndex]

    serverlist[serverIdentifier].LastVisit = timestamp

    print("choosen back queue:")
    prettyPrintQueue(backQueue)

    # consume one back queue
    while not backQueue.empty():
        item = backQueue.get()
        finalQueue.put(item)

    print("Final queue")
    prettyPrintQueue(finalQueue)

    # empty final Queue for better visulization
    while not finalQueue.empty():
        finalQueue.get()
    
    # remove used back queue from back queue list and mapping
    backQueues.pop(backQueueIndex)
    removeItemAndUpdateTable(_hostsToBackQueue, serverIdentifier)

    print(str(timestamp) + " run complete -----------------------") 
