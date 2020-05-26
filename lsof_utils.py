import pandas as pd
import re
import collections
import operator

def lsof_mapping(file):
    data_frame = pd.read_csv(file, header=None)
    file_map = [line.split() for line in data_frame[0]]
    # file_map = map(lambda line: line.split(), data_frame[0]) # --> then parse list(file_map)

    return file_map

def get_port(str):
    port_re = re.compile(r'\d{5}')
    if len(port_re.findall(str)) > 0 :
        return port_re.findall(str)[0]
    return

def get_ports_list(file_map):
    #aux func for lambda
    def line_has_port(line):
        if len(line) > 8 and get_port(line[8]):
            return int(get_port(line[8]))
        return 0
                                                     #list of ports and their lines [{port: 1234, line: 0}, {port: 5678, line: 1}]
    ports = list(filter( lambda el : el['port'] > 0, [{'port':line_has_port(line), 'line':idx} for idx,line in enumerate(file_map)]))
    return ports


def get_repeated_list(ports_list):
    repeated = [item for item, count in collections.Counter(map(operator.itemgetter('port'), ports_list)).items() if count > 1 ]
    repeated_ports = list(filter(lambda el : el['port'] in repeated, ports_list ))
    sorted_repeated_ports = sorted(repeated_ports, key=lambda port_el: port_el['port'])
    formatted_repeated_ports = []
    formatted_idx = 0
    for idx, port_el in enumerate(sorted_repeated_ports):
        if sorted_repeated_ports[idx-1]['port'] != port_el['port'] or idx == 0:
            formatted_repeated_ports.append({'port':port_el['port'], 'lines':[port_el['line']]})
            formatted_idx += 1
        else:
            formatted_repeated_ports[formatted_idx-1]['lines'].append(port_el['line'])
    return formatted_repeated_ports

def check_repeated_ports(repeated_ports, file_map):
    is_conflicting = False
    conflicting_arr = []
    for port_el in repeated_ports:
        services = [ file_map[line][2] for line in port_el['lines'] ]
                #rabbitmq is not a problem, so we filter it    #remove repeated service names
        filtered_services = filter(lambda el: el != 'rabbitmq', list(dict.fromkeys(services)))
        if len(list(filtered_services)) > 1:
            is_conflicting = True
        conflicting_arr.append((port_el['port'],is_conflicting))
    return conflicting_arr

def validate_file(lsof_file=None, file_map=None, ports_list=None, repeated_ports=None):
    if(lsof_file is not None):
        file_map = None
        ports_list = None
        repeated_ports = None
    else:
        if(file_map is not None):
            if(ports_list is None):
                ports_list = get_ports_list(file_map)
                #repeated_ports depends on ports_list. Thus, if ports_list is None, repeated_ports HAS to be None too
                repeated_ports = get_repeated_list(ports_list)
            elif( (repeated_ports is None)):
                repeated_ports = get_repeated_list(ports_list)
        else:
            return -1 #ERROR. Missing parameters

    conflicting_ports = list(filter(lambda el:  el[1] == True, check_repeated_ports(repeated_ports, file_map)))
    if len(conflicting_ports):
        return 0 #Not valid
    return 1 #Valid


def print_repeated(file_map, repeated_ports):
    print('======================================================================================')
    print(file_map[0][2], '||', file_map[0][8])

    for idx, port_el in enumerate(repeated_ports):
        if idx < 1 or port_el['port'] != repeated_ports[idx-1]['port']:
            print('\n======================================================================================')
            print('PORT', port_el['port'],'\n')
        print(file_map[port_el['line']][2], '||',file_map[port_el['line']][8] )
