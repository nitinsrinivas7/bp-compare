import csv,glob
import os
import json

#Cluster Name
cluster_name="CLUSTER_NAME"

#Ambari Username
ambari_username="AMBARI_USER"

#Ambari Password
ambari_password="AMBARI_PASSWORD"

#Directory where the configuration compare scripts are located
directory="/config-compare"

#Path where Blueprint Json of the current cluster is located
blueprint_1="/config-compare/blueprint_1.json"

#Path of the Blueprint Json from which the configurations need to be imported
blueprint_2="/config-compare/blueprint_2.json"

#Tab seperated File with the difference between two blueprints
diff_file = "/config-compare/output.tsv"

config_type=[]
value=[]
b1_json_value=[]
b2_json_value=[]
complete_value_if_truncated=[]
conf=[]
property=[]

#Blueprint comparison [ Output - Tab seperated file with the differences]
def blueprint_comparison():
    #Create new file if not exists
    if not os.path.exists(diff_file):
        os.system("touch"+diff_file)
    compare_cmd = 'python '+directory+'/compare-configs.py '+blueprint_1+' '+blueprint_2+' > '+diff_file
    os.system(compare_cmd)

blueprint_comparison()


#Reading the TSV file
with open(diff_file) as csvfile:
  readCSV = csv.reader(csvfile, delimiter='\t')
  #Ignoring header from the file
  readCSV.next()
  for row in readCSV:
    config_type.append(row[0])
    value.append(row[1])
    b1_json_value.append(row[2])
    b2_json_value.append(row[3])
    print(len(row))
    if len(row)<5:
        complete_value_if_truncated.append("NULL")
    else:
        complete_value_if_truncated.append(row[4])


#Get configurations using configs.py script
def get_configs(filename,configuration_type):
    get_cmd = '/var/lib/ambari-server/resources/scripts/configs.py --user='+ambari_username+' --password='+ambari_password+' --port=8080 --action=get --host=localhost --cluster='+cluster_name+' --config-type='+configuration_type+' --file='+filename
    os.system(get_cmd)


#Set configurations using configs.py script
def set_configs(filename,configuration_type):
    set_cmd = '/var/lib/ambari-server/resources/scripts/configs.py --user='+ambari_username+' --password='+ambari_password+' --port=8080 --action=set --host=localhost --cluster='+cluster_name+' --config-type='+configuration_type+' --file='+filename
    os.system(set_cmd)


for i in range(0,len(config_type)):
    if len(config_type[i].split(' : '))>4:
        print len(config_type[i].split(' : '))
        conf.insert(i,config_type[i].split(' : ')[2])
        property.insert(i,config_type[i].split(' : ')[4])

        #Define Filename
        filename='/tmp/'+conf[i]+'_payload.json'

        #GET values
        get_configs(filename,conf[i])

        #Value comparision
        if b2_json_value[i] == ' X ' and b1_json_value[i] == ' - ':
            if "..." in value[i]:
                new_value=complete_value_if_truncated[i]
            else:
                new_value=value[i]

            #Open Json file to read the existing configs
            with open(filename,'r') as f:
                json_data = json.load(f)

            #Create a new property & assign value / Modify existing value
            json_data['properties'][property[i]]=new_value

            #Open Json file to write configs
            with open(filename, "w") as jsonFile:
                json.dump(json_data, jsonFile)

            #SET value
            set_configs(filename,conf[i])

        #Deleting values in current cluster if not there in the required bp
        if b2_json_value[i] == ' - ' and b1_json_value[i] == ' X ':
            if b2_json_value[i-1] == ' - ' and b1_json_value[i-1] == ' X ' and config_type[i] == config_type[i-1] and i>0:
                with open(filename,'r') as f:
                    json_data = json.load(f)

                del json_data['properties'][property[i]]
                with open(filename, "w") as jsonFile:
                    json.dump(json_data, jsonFile)

                #SET value
                set_configs(filename,conf[i])
                print (json_data['properties'])
    else:
        print("This config does not contain the property ")
        print len(config_type[i].split(' : '))

#Delete intermediate Json files created
for files in glob.glob('doSet_version*.json') :
    os.remove( files )
