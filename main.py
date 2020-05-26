import lsof_utils as l_utils

#path to a lsof default outoput file
lsofFile = 'lsof_5672'
file_map = l_utils.lsof_mapping(lsofFile)
ports_list = l_utils.get_ports_list(file_map)
repeated_ports = l_utils.get_repeated_list(ports_list)

file_validation = l_utils.validate_file(file_map=file_map, ports_list=ports_list, repeated_ports=repeated_ports)

if(file_validation < 0):
    print('ERROR!!!! Missing params')
elif(file_validation == 1):
    print('VALID FILE!!!')
else:
    print('NOT VALID FILE!!! ')
