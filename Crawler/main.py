import yaml
import random
import queue

# load config
config = yaml.safe_load(open("config.yml"))
InitialUrlCount = config["numberUrl"]
InitialServerCount = config["numberServer"]
intervall = config["intervall"]

class UrlModel:
    def __init__(self, identifier, server, updateTime : int):
        self.Identifier = identifier
        self.Server = server
        self.UpdateTime = updateTime

    def print(self):
        print("URL: " + str(self.Identifier))
        self.Server.print()
        print("UpdateTime: " + str(self.UpdateTime))

class Server:
    def __init__(self, identifier, latency : int):
        self.Identifier = identifier
        self.Latency = latency

    def print(self):
        print("Identifier: " + str(self.Identifier))
        print("Latency: " + str(self.Latency))

def generateServer(count : int):
    list = []
    for i in range(count):
        latency = random.randint(10, 1000)
        list.append(Server(i,latency))
    return list

def generateUrls(queue, count : int, serverList):
    maxIndex = len(serverList)-1
    for i in range(count):
        index = random.randint(0, maxIndex)
        updatetime  = random.randint(10, 1000)
        queue.put(UrlModel(i,serverList[index],updatetime))
    return queue

def generateData(queue, serverCount: int, urlCount : int):
    serverlist = generateServer(serverCount)
    return generateUrls(queue, urlCount,serverlist)
    
times = [1]

urlList = queue.Queue()

urlList = generateData(urlList, InitialServerCount,InitialUrlCount)

#for timestamp in times:
    # generateData()


while not urlList.empty():
    item = urlList.get()
    item.print()


   