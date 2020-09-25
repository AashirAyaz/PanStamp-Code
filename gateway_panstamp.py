pr__author__ = 'Martin Götz, Aashir Ayaz'

##Libraries
import serial
import pymysql
import socket
import time
import codecs
import re ## For regular expressions
import struct
from datetime import datetime
import sys

## Parameters for Serial Conneciton
serialHandler = serial.Serial(
                      port=sys.argv[1],
                      baudrate=38400,
                      parity=serial.PARITY_NONE,
                      stopbits=serial.STOPBITS_ONE,
                      bytesize=serial.EIGHTBITS,
                      timeout=0
                   )

label_Sent = 0
tcp_server_connection = 0
connection = None

##To make a connection object, to perform operation on the database tables.
def connectToDatabase():
    conn = pymysql.connect(host='mysql.hrz.tu-chemnitz.de', port=3306, user='gab_database_rw', passwd='HeiL3eeNgah', db='gab_database')
    return conn

## To initialize a cursor
def getAcursor(conn):
    cur = conn.cursor()
    return cur
class Node:
    nodeId = 4
    long_address = ""
    measurability = ""
    nodeTitle = ""
    endpt=""

    def __init__(self,nodeId,long_address,measurability,nodeTitle,endpt):
        self.nodeTitle = nodeTitle
        self.long_address = long_address
        self.measurability = measurability
        self.nodeId = nodeId
        self.endpt = endpt


class Property:
    nodeId = 4
    propertyId = 4
    propertyName = ""
    propertyXLabel = ""
    propertyYLabel = ""
    propertyXUnit = ""
    propertyYUnit = ""
    propertyFactor = 0.0

    def __init__(self,nodeId,propertyId,propertyName,propertyXLabel,propertyYLabel,propertyXUnit,propertyYUnit,propertyFactor):
        self.nodeId = nodeId
        self.propertyId = propertyId
        self.propertyName = propertyName
        self.propertyXLabel = propertyXLabel
        self.propertyYLabel = propertyYLabel
        self.propertyXUnit = propertyXUnit
        self.propertyYUnit = propertyYUnit
        self.propertyFactor = propertyFactor

## To insert the PanStamp information into the database in Panstamp master table
## cur - Cursor over the database to point out the actual register
## conn - Opened connection to the database.

def insertIntoNodeTable(node,databaseHandler,databaseCursor):
    try:
        if_Exists = ""
        args = (node.panstamp,node.endpt,node.measurability,node.nodeId,if_Exists)
        databaseCursor.callproc("SP_INSERT_INTO_node_table",(args))
        result = databaseCursor.fetchall()
        if_Exists = result[0][0]
        if(if_Exists== 1):
            print("Node already registered, Error code- 0x01 transmitted with Pre-registered node id ",result[0][1])
            

def insertIntoPropertyTable(propertyObj,databaseHandler,databaseCursor):
    try:
        print("Debug: ",propertyObj.nodeId,"|",propertyObj.propertyId,"|", propertyObj.propertyName,"|", propertyObj.propertyXLabel,"|", propertyObj.propertyYLabel,"|", propertyObj.propertyXUnit,"|", propertyObj.propertyYUnit,"|",propertyObj.propertyFactor)
        args = (propertyObj.nodeId, propertyObj.propertyId, propertyObj.propertyName, propertyObj.propertyXLabel, propertyObj.propertyYLabel, propertyObj.propertyXUnit, propertyObj.propertyYUnit,propertyObj.propertyFactor)
        databaseCursor.callproc("SP_INSERT_INTO_property_table",(args))
        databaseHandler.commit()
    except pymysql.Error() as e:
        databaseHandler.rollback()
        raise

def insertIntoValueTable(node_id,property_id,property_val,time,datex,databaseHandler,databaseCursor):
    try:
        args = (node_id,property_id,property_val,time,datex)
        databaseCursor.callproc("SP_INSERT_INTO_value_table",(args))
        databaseHandler.commit()
    except pymysql.Error() as e:
        databaseHandler.rollback()
        raise


def sendToDevice(code,nodeid,address,endpt):
    lowbyte = address and 0xff
    highbyte = (address and 0xff00) >> 8
    newline = '\n'
    transmitbytes = [code,nodeid, lowbyte, highbyte,int(endpt,16)]
    serialHandler.write(transmitbytes)
    serialHandler.write(newline.encode('utf=8'))
    print(transmitbytes)
## To retrieve details about a node and its p roperty from the Panstamp property table
## If node exists, the details are passed on to Matlab for plotting
def getPropertyFromTable(cur,conn,property_Obj,status, long_Address):
    try:
        ## Preparing the statement to selecta regsister from property table.
        exists_in_master = ""
        args = (property_Obj.nodeId, property_Obj.propertyId,property_Obj.propertyName,property_Obj.propertyXLabel,property_Obj.propertyYLabel,property_Obj.propertyXUnit,property_Obj.propertyYUnit,property_Obj.propertyFactor)
        cur.callproc("SP_SELECT_FROM_property_table",(args))
        result = cur.fetchone()
        ## If there is not register (was not found) with the given PK try to insert it asnew one.
        if(result != None):
            print("Send : Status : ",status)
            ## Prepare the time to save into the new register.
            str_now = datetime.now().strftime('%Y-%m-%d')
            registerintoval(property_Obj.nodeId,property_Obj.propertyId,status,time.strftime('%H:%M:%S'),str_now,0)
        else:
            ## If finds register with such PK, try to locate it in the Master.
            args = (property_Obj.nodeId,exists_in_master)
            cur.callproc("SP_SELECT_FROM_node_table",(args))
            result = cur.fetchone()
            exists_in_master = result[0]
            ## If the register eve doesn't exist in Master:
            if(exists_in_master == 1):
                print("Error : No entry in property table.")
                sendErrorToPanstamp(property_Obj.nodeId,errorCode=2,long_Address=long_Address)
            else:
                print("Error : No entry in both tables.")
                sendErrorToPanstamp(0,errorCode=3, long_Address=long_Address)

    except pymysql.Error() as e:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

## To send error codes to Panstamp
def sendErrorToPanstamp(nodeId,errorCode, long_Address):
    try:
       ## MG: The destination address should be taken from the incoming packets.
        ## DEST_ADDR_LONG = bytes(long_Address, 'utf-8')
        print(long_Address)
        ## print(DEST_ADDR_LONG)
        error = b''
        send_nodeId = bytes([nodeId])
        send_errorCode = bytes([errorCode])
        error += send_nodeId[0:]
        error+= send_errorCode[0:]
        print("Send : Error : ",errorCode,", Node Id : ",nodeId)
        panStampHandler.send("tx",data=error,dest_addr_long=long_Address,dest_addr=b'\xff\xfd')
    except KeyboardInterrupt:
        print("Exception caught.")

def sendNodeIdToPanstamp(nodeId, long_Address):
    try:
        ## MG: The destination address should be taken from the incoming packets.
        ## DEST_ADDR_LONG = bytes(long_Address)
        DEST_ADDR = b'\xff\xfd'
        node_Identifier = bytes([nodeId])
        print ("Send : Node Id ",nodeId)
        panStampHandler.send("tx",data=node_Identifier,dest_addr_long=long_Address,dest_addr=DEST_ADDR)
        time.sleep(30)
    except KeyboardInterrupt:
        print("Exception caught.")

## To send data to Matlab

## To parse the input data and decide what kind it is
def parseInputData(rf_Data, long_Address):
    node_Id = rf_Data[0]
    # add_L = codecs.getencoder('hex_codec')(long_Address)[0]
    # print(add_L)
    # add_Long = add_L.decode("utf-8")
    # print(add_Long)
    print(long_Address)
    if node_Id == 0:
        # For try over Master table
        print("New node.")
        parsePacketOne(rf_Data,long_Address)
    else:
        # For try over detailed tables.
        print("Registered node.")
        parsePacketN(rf_Data,long_Address)

## To parse an introductory packet
## rf_Data radio_requence data stream
## Put into variables the receuved values and sent those to the DB.
def parsePacketOne(rf_Data,long_Address):
    print(long_Address)
    ## Collect from specific positions the required info
    packet_No = rf_Data[1]
    measure_No = rf_Data[6] ## Value of certain performed measure
    node_Title = str(rf_Data[8:8+rf_Data[7]],encoding='utf-8')
    if(packet_No == 1):
        print ("Introductory packet received.")
        full_Version_Num =struct.unpack('f',rf_Data[2:6])[0]
        version_Num = round(full_Version_Num,2)
        master_Entry = PanstampMaster(0,long_Address,measure_No,node_Title)
        print("Protocol Version : ",version_Num)
        print("Long Address : ",master_Entry.longAddress)
        print("Title : ",master_Entry.nodeTitle)
        print("Number of properties : ",master_Entry.measurability)
        conn = connectToDatabase()
        cur = getAcursor(conn)
        insertIntoPanstampMaster(cur,conn,master_Entry)
    else:
        parsePacketN(rf_Data,long_Address)

## To parse a packet other than the first one (information packet, final packet, status packet)
def parsePacketN(rf_Data,long_Address):
    if(len(rf_Data) > 3):
        node_Id = rf_Data[0]
        property_Id = rf_Data[2]
        property_Name = str(rf_Data[4:4+rf_Data[3]],encoding='utf-8')
        x_Label = str(rf_Data[5+rf_Data[3]:5+rf_Data[3]+rf_Data[4+rf_Data[3]]],encoding='utf-8')
        y_Label = str(rf_Data[6+rf_Data[3]+rf_Data[4+rf_Data[3]]:6+rf_Data[3]+rf_Data[4+rf_Data[3]]+rf_Data[5+rf_Data[3]+rf_Data[4+rf_Data[3]]]],encoding='utf-8')
        x_Unit = str(rf_Data[7+rf_Data[3]+rf_Data[4+rf_Data[3]]+rf_Data[5+rf_Data[3]+rf_Data[4+rf_Data[3]]]
                    :7+rf_Data[3]+rf_Data[4+rf_Data[3]]+rf_Data[5+rf_Data[3]+rf_Data[4+rf_Data[3]]]+rf_Data[6+rf_Data[3]+rf_Data[4+rf_Data[3]]+rf_Data[5+rf_Data[3]+rf_Data[4+rf_Data[3]]]]],
                    encoding='utf-8')
        y_Unit = str(rf_Data[8+rf_Data[3]+rf_Data[4+rf_Data[3]]+rf_Data[5+rf_Data[3]+rf_Data[4+rf_Data[3]]]+rf_Data[6+rf_Data[3]+rf_Data[4+rf_Data[3]]+rf_Data[5+rf_Data[3]+rf_Data[4+rf_Data[3]]]]
                    :8+rf_Data[3]+rf_Data[4+rf_Data[3]]+rf_Data[5+rf_Data[3]+rf_Data[4+rf_Data[3]]]+rf_Data[6+rf_Data[3]+rf_Data[4+rf_Data[3]]+rf_Data[5+rf_Data[3]+rf_Data[4+rf_Data[3]]]]
                    +rf_Data[7+rf_Data[3]+rf_Data[4+rf_Data[3]]+rf_Data[5+rf_Data[3]+rf_Data[4+rf_Data[3]]]+rf_Data[6+rf_Data[3]+rf_Data[4+rf_Data[3]]+rf_Data[5+rf_Data[3]+rf_Data[4+rf_Data[3]]]]]],
                    encoding='utf-8')
        full_factor =struct.unpack('f',rf_Data[8+rf_Data[3]+rf_Data[4+rf_Data[3]]+rf_Data[5+rf_Data[3]+rf_Data[4+rf_Data[3]]]+rf_Data[6+rf_Data[3]+rf_Data[4+rf_Data[3]]+rf_Data[5+rf_Data[3]+rf_Data[4+rf_Data[3]]]]
                    +rf_Data[7+rf_Data[3]+rf_Data[4+rf_Data[3]]+rf_Data[5+rf_Data[3]+rf_Data[4+rf_Data[3]]]+rf_Data[6+rf_Data[3]+rf_Data[4+rf_Data[3]]+rf_Data[5+rf_Data[3]+rf_Data[4+rf_Data[3]]]]]
                    :])[0]
        factor = round(full_factor,2)
        print("Information packet received.")
        print("Node Id : ",node_Id)
        print("Property Id : ",property_Id)
        print("Property Name : ",property_Name)
        print("X label : ",x_Label)
        print("Y label : ",y_Label)
        print("X unit : ",x_Unit)
        print("Y unit : ",y_Unit)
        print("Factor : ",factor)
        property_Object = PanstampProperty(node_Id, property_Id, property_Name, x_Label, y_Label, x_Unit, y_Unit,factor)
        conn = connectToDatabase()
        cur = getAcursor(conn)
        insertIntoPanstampProperty(cur,conn,property_Object)
    elif(len(rf_Data) == 2):
        print("Final packet received.")
    else:
       ## If there is a previuos register, try to consult and return it.
        node_Id = rf_Data[0]
        property_Id = rf_Data[1]
        status_Value = rf_Data[2]
        property_Object = PanstampProperty(node_Id, property_Id,"","","","","","")
        print("Status packet received.")
        print("Node Id : ",node_Id)
        print("Status value : ",status_Value)
        print("Property Id : ",property_Id)
        conn = connectToDatabase()
        cur = getAcursor(conn)
        getPropertyFromTable(cur,conn,property_Object,status_Value, long_Address)

def main():
    while True:
        try:
            print('Started Coordinator Gateway application.... Waiting for Serial Data')
            print ('Selected COM port ------>',sys.argv[1])
            i=0
            while True:
                if (serialHandler.inWaiting()>0):
                    #databaseHandler.ping(True)
                    #databaseCursor=databaseHandler.cursor()
                    incomingPanStampData = serialHandler.readline().decode("ISO-8859-1")
                    #handlerfn(incomingPanstampPacketString,databaseHandler,databaseCursor)
                    #print("incomingPanStampData ",incomingPanStampData)
                """ ##here update Panstamp Data from Panstamp Coordinator """
                    if(i<1):
                        i+=1
                    elif(len):
                        print("incomingPanStampData ",incomingPanStampData)
                        y=str(incomingPanStampData)
                        #print("incomingPanStampDataYY",y)
                        data=y.split("-")
                        if(len(data)>1):
                            #print("incomingPanStampDataYYdd",data)
                            rssi= data[0]
                           # print("incomingPanStampDataYYdd",rssi)
                            lqi= data[1]
                           # print("incomingPanStampDataYYdd",lqi)
                            str_now = datetime.now().strftime('%Y-%m-%d')
                            conn = connectToDatabase()
                            cur = getAcursor(conn)
                            #if a:
                            #insertIntoValueTable(4,1,k,time.strftime('%H:%M:%S'),str_now,conn,cur)
                            if(lqi!="" or rssi!=""):
                             print("RSSI",rssi)
                             print("LQI",lqi)
                             insertIntoValueTable(4,2,rssi,time.strftime('%H:%M:%S'),str_now,conn,cur) 
                             insertIntoValueTable(4,1,lqi,time.strftime('%H:%M:%S'),str_now,conn,cur)
                    #else: 
                     #insertIntoValueTable(4,1,time.strftime('%H:%M:%S'),str_now,conn,cur) 
            #rf_Data = incomingPanStampData['rf_data']
            # Test input data for debugging
            #rf_Data = b'\x08\x01\x0b'
            #rf_Data = b'\x00\x01\x01\x03\x07Room001'
            #long_Address = incomingPanStampData['source_addr_long']
            # Define test long_address for debugging
            #long_Address = b'\x00\x13\xa2\x00@\xac\xbd\xcf'
            #parseInputData(rf_Data,long_Address)
            
            # Send data to Panstamp for debugging
            #sendErrorToPanstamp("8","1")
            #sendNodeIdToPanstamp("4")
            
            
            
            
            time.sleep(0.5)

        except KeyboardInterrupt as e:
            ## Securely close the serial connection upon keyboard interrupt so that it is not blocked on next call.
            print("Exception caught as keyboard interrupt")
            ser.close()
            print("<----------------Program terminated followed by closing the active serial port------------->")
            exit


if __name__=="__main__" : main()

int('hello world')
