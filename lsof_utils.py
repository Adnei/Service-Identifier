import pandas as pd
import re
import collections
import operator

def lsofMapping(file):
    data_frame = pd.read_csv(file, header=None)
    file_map = [line.split() for line in data_frame[0]]
    # file_map = map(lambda line: line.split(), data_frame[0]) # --> then parse list(file_map)

    return file_map

def getPort(str):
    port_re = re.compile(r'\d{5}')
    if len(port_re.findall(str)) > 0 :
        return port_re.findall(str)[0]
    return

def getPortsList(file_map):
    #aux func for lambda
    def lineHasPort(line):
        if len(line) > 8 and getPort(line[8]):
            return int(getPort(line[8]))
        return 0
                                                     #list of ports and their lines [{port: 1234, line: 0}, {port: 5678, line: 1}]
    ports = list(filter( lambda el : el['port'] > 0, [{'port':lineHasPort(line), 'line':idx} for idx,line in enumerate(file_map)]))
    return ports


def getRepeatedList(portList):
    repeated = [item for item, count in collections.Counter(map(operator.itemgetter('port'), portList)).items() if count > 1 ]
    repeatedPorts = list(filter(lambda el : el['port'] in repeated, portList ))
    return sorted(repeatedPorts, key=lambda portEl: portEl['port'])
