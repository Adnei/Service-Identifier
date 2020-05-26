from lsof_mapper import Lsof_Mapper

#path to a lsof default outoput file
lsof_file = 'lsof_5672'
lsof_mapper = Lsof_Mapper(lsof_file)

if(lsof_mapper.is_valid_file):
    print(lsof_mapper.service_port_mapper)
