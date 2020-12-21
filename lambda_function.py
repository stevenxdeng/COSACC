import base64
import json
import boto3
import time
import math
from decimal import *
from threading import Thread

sqs = boto3.resource('sqs')
dynamodb = boto3.resource('dynamodb')
client = boto3.client('lambda')

TLS_position_x = 745.45
TLS_position_y = 1118.61

Distance_Table = {}
Speed_Table = {}
Gap_Table = {}

input_table = dynamodb.Table('Speed_file')
output_table = dynamodb.Table('Speed_advisory')
Historical_Table = dynamodb.Table('Historical_State_Table_113850262')
queue = sqs.get_queue_by_name(QueueName='TLS_ACK_113850262')
input = ''

def get_Distance(table,ID):
    try:
        response = table.get_item(Key={'vehicle': ID})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return float(response['Item']['distance'])

def Historical_State_Update(Table, ID, distance):
    Table.put_item(
        Item={
            'vehicle':ID,
            'distance':Decimal(str(distance))
             }
      )
      
def Assign_Vehicle(pos_x,pos_y,phase,remain,ID,speed,gap):
    X = TLS_position_x - float(pos_x)
    Y = TLS_position_y - float(pos_y)
    distance = round(math.sqrt(X*X+Y*Y),2)
            
    if phase == 0:
        if distance <= get_Distance(Historical_Table,ID) and distance > remain * 16.0:
            Distance_Table[ID] = distance
            Speed_Table[ID] = speed
            Gap_Table[ID] = gap
    else:
        #Speed Advisories only give to vehicles approaching TLS
        if distance <= get_Distance(Historical_Table,ID):
            Distance_Table[ID] = distance
            Speed_Table[ID] = speed
            Gap_Table[ID] = gap
    Historical_State_Update(Historical_Table,ID,distance)
    
        
def compute_advisory(platoon,Gap_Table,Speed_Table,delay_time):
    
    Distance_Switch = False
    
    if len(platoon) < 2:
        Single_Distance = Distance_Table[platoon[0]]
        adv = round(Single_Distance / delay_time,1)
        if adv > 16.0:
            adv = 16.0
    else:
        Platoon_Gap = []
        Platoon_Speed = []

        for vehicle in platoon:
            if not Distance_Switch:
                Single_Distance = Distance_Table[vehicle]
                Platoon_Gap.append(str(Gap_Table[vehicle]))
                Platoon_Speed.append(str(Speed_Table[vehicle]))
                Distance_Switch = True
            else:
                Platoon_Gap.append(str(Gap_Table[vehicle]))
                Platoon_Speed.append(str(Speed_Table[vehicle]))

        adv = round(Single_Distance / delay_time,1)
        if adv > 16.0:
            adv = 16.0
            
        input_message_main = 'Vehicle Number of this platoon: ' + str(len(platoon))
        input_message = { 'VehicleNum': len(platoon),
                          'PlatoonID': platoon,
                          'PlatoonGap': Platoon_Gap,
                          'PlatoonSpeed': Platoon_Speed
                        }
        resp = client.invoke(FunctionName='arn:aws:lambda:us-east-1:233952390740:function:ALG_Invocation',
                             InvocationType='RequestResponse',
                             Payload=json.dumps(input_message)
                            )
        resjson = json.load(resp['Payload'])
        print('Resonse JSON: ', resjson)
    
    output_table.put_item(
        Item={
            'vehicle': platoon[0],
            'advisory':Decimal(str(adv))
             }
    )
    
def scan_and_process_input_table(current_phase,remain):
    thread_list = []
    total_segments = 8 # number of parallel scans
    for i in range(total_segments):
        # Instantiate and store the thread
        thread = Thread(target=parallel_scan_and_process_input_table, args=(i,total_segments,current_phase,remain))
        thread_list.append(thread)
    # Start threads
    for thread in thread_list:
        thread.start()
    # Block main thread until all threads are finished
    for thread in thread_list:
        thread.join()
        
def parallel_scan_and_process_input_table(segment, total_segments, current_phase, remain):
    threads = []
    thread_number = 0
    #print("Total segments = "+str(total_segments)+" segment "+str(segment))
    vehicles = input_table.scan(
        Segment=segment, 
        TotalSegments=total_segments,
        ConsistentRead=True
        )
    print('Looking at segment ' + str(segment) + ' of '+ str(total_segments) + " "+str(len(vehicles['Items']))+" vehicles\n")
    
    for i in vehicles['Items']:
        thread = Thread(target=Assign_Vehicle, args=(i['pos_x'],i['pos_y'],current_phase,remain,i['vehicle'],i['speed'],i['gap']))
        threads.append(thread)
        thread_number += 1
        if thread_number > 8:
            for thread in threads:
                thread.start()
            thread_number = 0
            threads.clear()
        
    for thread in threads:
        thread.start()
    


def lambda_handler(event, context):
    payload = ''
    current_phase = 0
    
    Platoons = []
    Platoon_index = -1
    
    threads = []
    thread_number = 0
    
    test_records =["Test"]
    
    # TODO implement
    #for record in event['Records']:
    for record in test_records:
        #input = str(base64.b64decode(record['kinesis']['data']))[2:-1]
        input = '2,10'

        if ',' == input[1]:
            decoded_message = input.split(',')
            current_phase = int(decoded_message[0])
            remain = int(decoded_message[1])
        else:
            payload = input
            
        if current_phase == 0:
            delay_time = remain + 48
        elif current_phase == 1:
            delay_time = remain + 45
        elif current_phase == 2:
            delay_time = remain + 3
        else:
            delay_time = remain

        start_time = int(round(time.time() * 1000))
        
        vehicles = input_table.scan()
        for i in vehicles['Items']:
            if int(i['vehicle']) < 50:
                threads.append(Thread(target=Assign_Vehicle, args=(i['pos_x'],i['pos_y'],current_phase,remain,i['vehicle'],i['speed'],i['gap'])))
                thread_number += 1
                if thread_number > 12:
                    for thread in threads:
                        thread.start()
                    thread_number = 0
                    threads.clear()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        
        #scan_and_process_input_table(current_phase,remain);
        
        end_time = int(round(time.time() * 1000))
        print('Speed file processing time is: ', end_time-start_time)
         
        start_time = int(round(time.time() * 1000))   
        if Distance_Table:
            Sorted_Distance = sorted(Distance_Table.items(), key=lambda x: x[1])
            
            for i in range(len(Sorted_Distance)):
                if float(Gap_Table[Sorted_Distance[i][0]]) > 50.0 or i == 0:
                    Platoon_index += 1
                    Platoons.append([])
                    Platoons[Platoon_index].append(Sorted_Distance[i][0])
                else:
                    Platoons[Platoon_index].append(Sorted_Distance[i][0])
            
            threads.clear()
            thread_number = 0
            for platoon in Platoons:
                threads.append(Thread(target=compute_advisory,args=(platoon,Gap_Table,Speed_Table,delay_time,)))
                thread_number += 1
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
                
        end_time = int(round(time.time() * 1000))
        payload += str(end_time-start_time)
        response = queue.send_message(MessageBody=payload)

        print(payload)
        print('Processing time is: ', end_time-start_time)

    return 'successfully processed {} records.'.format(len(test_records))
    #return 'successfully processed {} records.'.format(len(event['Records']))