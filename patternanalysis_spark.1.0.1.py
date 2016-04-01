#Load packages
import time
import csv
import pymongo
import pandas
from pandas import DataFrame
from pymongo import MongoClient
from pyspark import SparkContext


#define the function to get info for connection swithcing jobs & promotion
def getswitchjobtime(user):
    #get bountyme id for this user
    bountymeId = user.get('identity',{}).get('bountyUserId','')

    #get all connections for each user
    connections = user.get('connections',[])

    #get number of connections for each user
    n = len(connections)
    if n == 0:
        n = 1

    #get current date
    currentDate = time.strftime("%d/%m/%Y")
    currentDay = int(currentDate.split('/')[0])
    currentMonth = int(currentDate.split('/')[1])
    currentYear = int(currentDate.split('/')[2])

    #define a dictionary for converting months to numbers
    month_conv = {}
    month_list = ['january','february','march','april','may','june','july','august','september','october','november','december']
    for i, month in enumerate(month_list,1):
        month_conv[month] = i

    #create basic metrics
    connectionList = []
    percentageSwitchedOneMonth = 0
    percentageSwitchedSixMonths = 0
    percentageSwitchedOneYear = 0
    percentagePromotedOneMonth = 0
    percentagePromotedSixMonths = 0
    percentagePromotedOneYear = 0

    for connection in connections:
        #get headline
        connectionHeadline = connection.get('details',{}).get('headline','')

        #get job info for three last positions for each connection and convert to string
        connectionCompany1 = connection.get('details',{}).get('threeCurrentPositions',{}).get('firstPos',{}).get('company','').lower().encode('ascii','ignore')
        connectionCompany2 = connection.get('details',{}).get('threeCurrentPositions',{}).get('secondPos',{}).get('company','').lower().encode('ascii','ignore')
        connectionCompany3 = connection.get('details',{}).get('threeCurrentPositions',{}).get('thirdPos',{}).get('company','').lower().encode('ascii','ignore')
        connectionDate1 = connection.get('details',{}).get('threeCurrentPositions',{}).get('firstPos',{}).get('dates','').lower().encode('ascii','ignore')
        connectionDate2 = connection.get('details',{}).get('threeCurrentPositions',{}).get('secondPos',{}).get('dates','').lower().encode('ascii','ignore')
        connectionDate3 = connection.get('details',{}).get('threeCurrentPositions',{}).get('thirdPos',{}).get('dates','').lower().encode('ascii','ignore')

        #set default values
        newPositionTime = 'NA'
        switched = False
        promoted = False
        student = False

        #get info for switching jobs/promotion and time
        if connectionCompany1 != connectionCompany2:
            if 'present' in connectionDate1:
                if len(connectionDate1.split()) >= 3:
                    jobYear = int(connectionDate1.split()[1])
                    jobMonth = month_conv.get(connectionDate1.split()[0])
                    newPositionTime = (currentYear-jobYear)*12 + (currentMonth-jobMonth)
                elif len(connectionDate1.split()) == 2:
                    jobYear = int(connectionDate1.split()[0])
                    jobMonth = 1
                    newPositionTime = (currentYear-jobYear)*12 + (currentMonth-jobMonth)
            else:
                if len(connectionDate1.split()) == 4:
                    jobYear = int(connectionDate1.split()[3])
                    jobMonth = month_conv.get(connectionDate1.split()[2])
                    newPositionTime = (currentYear-jobYear)*12 + (currentMonth-jobMonth)
            switched = True
        else:
            if 'present' in connectionDate1:
                if len(connectionDate1.split()) >= 3:
                    jobYear = int(connectionDate1.split()[1])
                    jobMonth = month_conv.get(connectionDate1.split()[0])
                    newPositionTime = (currentYear-jobYear)*12 + (currentMonth-jobMonth)
                elif len(connectionDate1.split()) == 2:
                    jobYear = int(connectionDate1.split()[0])
                    jobMonth = 1
                    newPositionTime = (currentYear-jobYear)*12 + (currentMonth-jobMonth)
                promoted = True
            else:
                if len(connectionDate1.split()) == 4:
                    jobYear = int(connectionDate1.split()[3])
                    jobMonth = month_conv.get(connectionDate1.split()[2])
                    newPositionTime = (currentYear-jobYear)*12 + (currentMonth-jobMonth)
                switched = True

        #filter by whether the connection is a current student
        if ('student' in connectionHeadline.lower() or
            'intern' in connectionHeadline.lower() or
            'candidate' in connectionHeadline.lower() or
            'fellow' in connectionHeadline.lower()):
            student = True

        #see whether this connection switched/promoted since the last year
        if newPositionTime == 'NA':
            n = n - 1
            #print connection.get("profile",{}).get('firstName','')
        else:
            if switched == True:
                if newPositionTime <= 1:
                    percentageSwitchedOneMonth = percentageSwitchedOneMonth + 1
                    percentageSwitchedSixMonths = percentageSwitchedSixMonths + 1
                    percentageSwitchedOneYear = percentageSwitchedOneYear + 1
                elif newPositionTime <= 6:
                    percentageSwitchedSixMonths = percentageSwitchedSixMonths + 1
                    percentageSwitchedOneYear = percentageSwitchedOneYear + 1
                elif newPositionTime <= 12:
                    percentageSwitchedOneYear = percentageSwitchedOneYear + 1
            elif promoted == True:
                if newPositionTime <= 1:
                    percentagePromotedOneMonth = percentagePromotedOneMonth + 1
                    percentagePromotedSixMonths = percentagePromotedSixMonths + 1
                    percentagePromotedOneYear = percentagePromotedOneYear + 1
                elif newPositionTime <= 6:
                    percentagePromotedSixMonths = percentagePromotedSixMonths + 1
                    percentagePromotedOneYear = percentagePromotedOneYear + 1
                elif newPositionTime <= 12:
                    percentagePromotedOneYear = percentagePromotedOneYear + 1

        #add some basic profile info for each recommendation
        connection_dict = {}
        identity_dict = {}
        profile_dict = {}
        #add some basic profile info for each recommendation
        profile_dict["pictureUrl"] = connection.get("profile",{}).get('pictureUrl','')
        profile_dict["firstName"] = connection.get("profile",{}).get('firstName','')
        profile_dict["lastName"] = connection.get("profile",{}).get('lastName','')
        #add some basic id info for each recommendation
        identity_dict["email"] = connection.get("identity",{}).get('emailAddress')
        identity_dict["phoneNumber"] = connection.get("identity",{}).get('phoneNumber')
        identity_dict["bountUserId"] = connection.get("identity",{}).get('bountyUserId')

        connection_dict["linkedInExternalId"] = connection.get('linkedin',{}).get('externalId','')
        connection_dict["profile"] = profile_dict
        connection_dict["identity"] = identity_dict
        connection_dict["headline"] = connectionHeadline
        connection_dict["currentCompany"] = connectionCompany1
        connection_dict["student"] = student
        connection_dict["promoted"] = promoted
        connection_dict["switched"] = switched
        connection_dict["newPositionTime"] = newPositionTime
        connectionList.append(connection_dict)

        #print promoted
        #print switched
        #print newPositionTime
        #print connection.get("profile",{}).get('firstName','')

    percentageSwitchedOneMonth = float(percentageSwitchedOneMonth)/n
    percentageSwitchedSixMonths = float(percentageSwitchedSixMonths)/n
    percentageSwitchedOneYear = float(percentageSwitchedOneYear)/n
    percentagePromotedOneMonth = float(percentagePromotedOneMonth)/n
    percentagePromotedSixMonths = float(percentagePromotedSixMonths)/n
    percentagePromotedOneYear = float(percentagePromotedOneYear)/n
    percentageList = [percentageSwitchedOneMonth,percentageSwitchedSixMonths,percentageSwitchedOneYear,percentagePromotedOneMonth,percentagePromotedSixMonths,percentagePromotedOneYear]

    userinfo = {}
    userinfo["bountymeId"] = bountymeId
    userinfo["connections"] = connectionList
    userinfo["summary"] = {"all": percentageList}

    return(userinfo)

#define the function for filtering basic metrics for non-student
def filterstudent(x):
    #get number of connections for each user
    connections = x.get("connections",[])
    n = len(connections)
    if n == 0:
        n = 1

    #create basic metrics
    percentageSwitchedOneMonth = 0
    percentageSwitchedSixMonths = 0
    percentageSwitchedOneYear = 0
    percentagePromotedOneMonth = 0
    percentagePromotedSixMonths = 0
    percentagePromotedOneYear = 0

    #get job switching data for each connection
    for connection in connections:
        newPositionTime = connection.get("newPositionTime")
        student = connection.get("student")
        promoted = connection.get("promoted")
        switched = connection.get("switched")

        #calculate statistics based on filters
        if newPositionTime == 'NA':
            n = n - 1
        else:
            if student == False:
                if switched == True:
                    if newPositionTime <= 1:
                        percentageSwitchedOneMonth = percentageSwitchedOneMonth + 1
                        percentageSwitchedSixMonths = percentageSwitchedSixMonths + 1
                        percentageSwitchedOneYear = percentageSwitchedOneYear + 1
                    elif newPositionTime <= 6:
                        percentageSwitchedSixMonths = percentageSwitchedSixMonths + 1
                        percentageSwitchedOneYear = percentageSwitchedOneYear + 1
                    elif newPositionTime <= 12:
                        percentageSwitchedOneYear = percentageSwitchedOneYear + 1
                elif promoted == True:
                    if newPositionTime <= 1:
                        percentagePromotedOneMonth = percentagePromotedOneMonth + 1
                        percentagePromotedSixMonths = percentagePromotedSixMonths + 1
                        percentagePromotedOneYear = percentagePromotedOneYear + 1
                    elif newPositionTime <= 6:
                        percentagePromotedSixMonths = percentagePromotedSixMonths + 1
                        percentagePromotedOneYear = percentagePromotedOneYear + 1
                    elif newPositionTime <= 12:
                        percentagePromotedOneYear = percentagePromotedOneYear + 1

    percentageSwitchedOneMonth = float(percentageSwitchedOneMonth)/n
    percentageSwitchedSixMonths = float(percentageSwitchedSixMonths)/n
    percentageSwitchedOneYear = float(percentageSwitchedOneYear)/n
    percentagePromotedOneMonth = float(percentagePromotedOneMonth)/n
    percentagePromotedSixMonths = float(percentagePromotedSixMonths)/n
    percentagePromotedOneYear = float(percentagePromotedOneYear)/n
    percentageList = [percentageSwitchedOneMonth,percentageSwitchedSixMonths,percentageSwitchedOneYear,percentagePromotedOneMonth,percentagePromotedSixMonths,percentagePromotedOneYear]

    #append statistics into x
    x["summary"]["winthoutStudent"] = percentageList

    return(x)

#define the function for filtering basic metrics for company info
def filtercompany(x,companylist):
    #get number of connections for each user
    connections = x.get("connections",[])
    n = len(connections)
    if n == 0:
        n = 1

    #create basic metrics
    percentageSwitchedOneMonth = 0
    percentageSwitchedSixMonths = 0
    percentageSwitchedOneYear = 0
    percentagePromotedOneMonth = 0
    percentagePromotedSixMonths = 0
    percentagePromotedOneYear = 0

    #get job switching data for each connection
    for connection in connections:
        newPositionTime = connection.get("newPositionTime")
        student = connection.get("student")
        promoted = connection.get("promoted")
        switched = connection.get("switched")

        #calculate statistics based on filters
        if newPositionTime == 'NA':
            n = n - 1
        else:
            if connection.get("currentCompany").lower() in companyList:
                if switched == True:
                    if newPositionTime <= 1:
                        percentageSwitchedOneMonth = percentageSwitchedOneMonth + 1
                        percentageSwitchedSixMonths = percentageSwitchedSixMonths + 1
                        percentageSwitchedOneYear = percentageSwitchedOneYear + 1
                    elif newPositionTime <= 6:
                        percentageSwitchedSixMonths = percentageSwitchedSixMonths + 1
                        percentageSwitchedOneYear = percentageSwitchedOneYear + 1
                    elif newPositionTime <= 12:
                        percentageSwitchedOneYear = percentageSwitchedOneYear + 1
                elif promoted == True:
                    if newPositionTime <= 1:
                        percentagePromotedOneMonth = percentagePromotedOneMonth + 1
                        percentagePromotedSixMonths = percentagePromotedSixMonths + 1
                        percentagePromotedOneYear = percentagePromotedOneYear + 1
                    elif newPositionTime <= 6:
                        percentagePromotedSixMonths = percentagePromotedSixMonths + 1
                        percentagePromotedOneYear = percentagePromotedOneYear + 1
                    elif newPositionTime <= 12:
                        percentagePromotedOneYear = percentagePromotedOneYear + 1

    percentageSwitchedOneMonth = float(percentageSwitchedOneMonth)/n
    percentageSwitchedSixMonths = float(percentageSwitchedSixMonths)/n
    percentageSwitchedOneYear = float(percentageSwitchedOneYear)/n
    percentagePromotedOneMonth = float(percentagePromotedOneMonth)/n
    percentagePromotedSixMonths = float(percentagePromotedSixMonths)/n
    percentagePromotedOneYear = float(percentagePromotedOneYear)/n
    percentageList = [percentageSwitchedOneMonth,percentageSwitchedSixMonths,percentageSwitchedOneYear,percentagePromotedOneMonth,percentagePromotedSixMonths,percentagePromotedOneYear]

    #append statistics into x
    x["summary"]["targetCompany"] = percentageList

    return(x)


#connect to the Mongodb
client = MongoClient('localhost', 3001)
db = client.meteor

#######################
#the following session will be deleted
#######################
#import company list & school list locally
with open('Fortune500.csv', 'rb') as inputCompanyList:
    reader = csv.reader(inputCompanyList)
    companyList = [x.lower() for x in list(reader)[0]]
#######################

#retrieve data for all bountyme users
users = []
userIds = []
linkedinUsers = []
for user in db.users.find():
    users.append(user)
    userIds.append(user.get('_id',''))

#retrieve data for all connections for every bountyme user
for linkedinUser in db.LinkedInCollectionTest.find({"identity.bountyUserId":{"$in":userIds}}):
    linkedinUsers.append(linkedinUser)

linkedinUsers.append({})

#conf spark
sc = SparkContext("local","spark test")

output = sc.parallelize(linkedinUsers).map(getswitchjobtime).map(filterstudent).collect()

print output

#output = filtercompany(filterstudent(getswitchjobtime(linkedinUsers[0])),companyList)
