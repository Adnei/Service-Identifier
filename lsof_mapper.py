import pandas as pd
import re
import collections
import operator


class Lsof_Mapper:
    """
        Lsof_Mapper creates a map of a lsof file.
        This is used specifically for the output of lsof with "-i :5672" flag.
        This class intents to map all the ports communication to RabbitMQ while an OpenStack cloud is being used

        Attributes:
                    file_path (string): path to the lsof output file
                    file_map (list): list of lines and columns of the file
                    ports_list (list): a list of ports found in this file. It also holds the line where the port was found
                    repeated_ports (list): a list of repeated ports in the file. Each port has a list of lines where it was found
                    is_valid_file (boolean): a flag that validates if this file can be used or not. Check __validate_file for more information
    """
    def __init__(self, file_path):
        self.file_path = file_path
        self.file_map = None
        self.ports_list = None
        self.repeated_ports = None
        self.service_port_mapper = None
        self.is_valid_file = self.__validate_file()
        if(self.is_valid_file):
            self.service_port_mapper = self.__build_service_port_mapper()

    def print_repeated(self):
        """
            @DEPRECATED
            prints out all the repeated ports
        """
        print('======================================================================================')
        print(self.file_map[0][2], '||', self.file_map[0][8])

        for idx, port_el in enumerate(self.repeated_ports):
            if idx < 1 or port_el['port'] != self.repeated_ports[idx-1]['port']:
                print('\n======================================================================================')
                print('PORT', port_el['port'],'\n')
            print(self.file_map[port_el['line']][2], '||',self.file_map[port_el['line']][8] )

    def __validate_file(self):
        """
            Validates if 'self.file_path' is a valid file.

            Basically, if two different services (except RabbitMQ) are using the same port, then this is an invalid file.
            Otherwise, the file is marked down as valid.
            A valid file may be used to characterize network traffic.

            Returns True, in case file is valid and False, in case not
        """
        if(self.file_path is not None):
            self.file_map = self.__build_file_map(self.file_path)
            self.ports_list = self.__build_ports_list(self.file_map)
            self.repeated_ports = self.__build_repeated_list(self.ports_list)

        conflicting_ports = list(filter(lambda el:  el[1] == True, self.__check_repeated_ports()))
        if len(conflicting_ports):
            return False
        return True

    def __check_repeated_ports(self):
        """
            Checks out for conflicting ports among repeated ones.

            A port is said conflicting whenever more than one service is using it too (except RabbitMQ).

            Returns a list of tuples as follows: (<port>, <is_conflicting>). Ex.: [(1234, True), (5678, False)]
        """
        is_conflicting = False
        conflicting_arr = []
        for port_el in self.repeated_ports:
            services = [ self.file_map[line][2] for line in port_el['lines'] ]
                    #rabbitmq is not a problem, so we filter it    #remove repeated service names
            filtered_services = filter(lambda el: el != 'rabbitmq', list(dict.fromkeys(services)))
            if len(list(filtered_services)) > 1:
                is_conflicting = True
            conflicting_arr.append((port_el['port'],is_conflicting))
        return conflicting_arr

    def __build_repeated_list(self, ports_list):
        """
            Builds a list of repeated ports.
            The lines where the same port was found are grouped by the port.

            Returns a list as follows: [{'port':1234, 'lines':[10,11]}]
        """
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

    def __build_file_map(self, file_path):
        """
            Creates a map of the lsof file.

            It used pandas to interpret the file.

            Returns a list of lines. Each line has N columns
            Columns from 0 to 8 indicate: "COMMAND", "PID", "USER", "FD", "TYPE", "DEVICE", "SIZE/OFF", "NODE", "NAME".
            Ex.: file_map[0][2] --> gets the line 0 column 2 from file_map. It will get the USER from line 0.

        """
        data_frame = pd.read_csv(file_path, header=None)
        file_map = [line.split() for line in data_frame[0]]
        # file_map = map(lambda line: line.split(), data_frame[0]) # --> then parse list(file_map)

        return file_map

    def __extract_port_from_column(self, str):
        """
            Applies a simple regex agaisnt a string and tries to find a port numberself.
            Used in column "NAME" (index 8) from any line. See __build_ports_list implementation
        """
        port_re = re.compile(r'\d{5}')
        if len(port_re.findall(str)) > 0 :
            return port_re.findall(str)[0]
        return None

    def __build_ports_list(self, file_map):
        """
            Creates a list holding all the ports and its lines

            Returns: a list as follows: [{'port':1234, 'line':1}]
        """
        #aux func for lambda
        def line_has_port(line):
            if len(line) > 8 and self.__extract_port_from_column(line[8]):
                return int(self.__extract_port_from_column(line[8]))
            return 0
                                                         #list of ports and their lines [{port: 1234, line: 0}, {port: 5678, line: 1}]
        ports = list(filter( lambda el : el['port'] > 0, [{'port':line_has_port(line), 'line':idx} for idx,line in enumerate(file_map)]))
        return ports

    def __build_service_port_mapper(self):
        """
            Creates a map with services and their ports

            Returns a dict as follows: {<service>:<set of ports>}. Ex.: {'glance':{1234,5678}, 'neutron':{1111,2222}}
        """
        def port_append(service, port_obj, service_port_mapper):
            """
                Aux function. Either appends a port to a server or creates the server entry in the map
            """
            if(service in service_port_mapper):
                service_port_mapper[service].add(port_obj['port'])
            else:
                service_port_mapper[service] = {port_obj['port']}

        def fix_service_name(service):
            return service.split('-')[0].split()[0]

        service_port_mapper = {}
        repeated = [port_obj['port'] for port_obj in self.repeated_ports]
        unique_ports = list(filter( lambda el: el['port'] not in repeated, self.ports_list))

        for repeated_obj in self.repeated_ports:
            for line in repeated_obj['lines']:
                service = self.file_map[line][2]
                if service != 'rabbitmq':
                    break
            port_append(fix_service_name(service), repeated_obj, service_port_mapper)

        for port_obj in unique_ports:
            service = self.file_map[line][2]
            port_append(fix_service_name(service), port_obj, service_port_mapper)

        return service_port_mapper
