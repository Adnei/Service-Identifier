import lsof_utils as l_utils

def checkRepeatedPorts(formattedRepeatedPorts, fileMap):
    isConflicting = False
    conflictingArr = []
    for portEl in formattedRepeatedPorts:
        services = [ fileMap[line][2] for line in portEl['lines'] ]
                #rabbitmq is not a problem, so we filter it    #remove repeated service names
        filteredServices = filter(lambda el: el != 'rabbitmq', list(dict.fromkeys(services)))
        if len(list(filteredServices)) > 1:
            isConflicting = True
        conflictingArr.append((portEl['port'],isConflicting))
    return conflictingArr


def printRepeated(fileMap, repeatedPorts):
    print('======================================================================================')
    print(fileMap[0][2], '||', fileMap[0][8])

    for idx, portEl in enumerate(repeatedPorts):
        if idx < 1 or portEl['port'] != repeatedPorts[idx-1]['port']:
            print('\n======================================================================================')
            print('PORT', portEl['port'],'\n')
        print(fileMap[portEl['line']][2], '||',fileMap[portEl['line']][8] )

#path to a lsof default outoput file
lsofFile = '../RabbitMQ/lsof_5672_v2'
fileMap = l_utils.lsofMapping(lsofFile)
portsList = l_utils.getPortsList(fileMap)
repeatedPorts = l_utils.getRepeatedList(portsList)

#ugly code
#list comprehension (?), map (?), filter (?), reduce (?)
formattedRepeatedPorts = []
ports=[]
formattedIdx = 0
for idx, portEl in enumerate(repeatedPorts):
    if repeatedPorts[idx-1]['port'] != portEl['port'] or idx == 0:
        formattedRepeatedPorts.append({'port':portEl['port'], 'lines':[portEl['line']]})
        formattedIdx += 1
    else:
        formattedRepeatedPorts[formattedIdx-1]['lines'].append(portEl['line'])


# printRepeated(fileMap, repeatedPorts)
conflictingPorts = list(filter(lambda el:  el[1] == True, checkRepeatedPorts(formattedRepeatedPorts, fileMap)))
if len(conflictingPorts):
    print('Conflicting ports:\n',conflictingPorts)
else:
    print('There are no conflicting ports')
