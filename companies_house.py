# code to extract companies address details from company house api for list of companies

__author__ = 'steve morris'


import requests

import pymssql

import datetime

authcode = "enter auth code here"

a=requests.auth.HTTPBasicAuth(authcode, '')

server = "server_name"
user = "user_name"
password = "password"

conn = pymssql.connect(server, user, password, "ew_dw")
cursor = conn.cursor()



# select multi academy trusts

cursor.execute("select distinct trust from dim_company where trust not in (select distinct company from dim_company where phase like 'Academy%') and source_of_data = 'Department of Education' and active_status = 'Active' and trust in (select trust from dim_company group by trust having count(company_id) > 1)")
rows = cursor.fetchall()
for row in rows:

    company_name = row[0]
    print company_name.replace(', The','').replace(' ','+')
    str='https://api.companieshouse.gov.uk/search/companies/?q='+company_name.replace(', The','').replace(' ','+')
    r= requests.get(url=str,auth=a,timeout = 200)



    r_json= r.json()
  ## print row
   ## print r_json





    if  len(r_json["items"]) >0:
        print r_json["items"][0]
        item_data=[]
        company =""
        company_number=""
        company_status=""
        address1=""
        address2=""
        city=""
        region=""
        postcode=""

        company= r_json["items"][0]["title"]
        company_number =  r_json["items"][0]["company_number"]
        company_status =  r_json["items"][0]["company_status"]

        for address in r_json["items"][0]["address"]:
            if address == "address_line_1":
                address1 = r_json["items"][0]["address"]["address_line_1"][:50]
            if address == "address_line_2":
                address2 = r_json["items"][0]["address"]["address_line_2"][:50]
            if address == "locality":
                city = r_json["items"][0]["address"]["locality"]

            if address == "region":
                region = r_json["items"][0]["address"]["region"]

            if address == "postal_code":
                postcode = r_json["items"][0]["address"]["postal_code"]

        item_data =[company,company_number,company_status,address1,address2,city,region,postcode]
        print item_data

        cursor.executemany(
        "INSERT INTO temp_trusts ([companies_house_number],[company],[address1],[address2],[city],[county],[postcode],[country],[phase],[company_type],[active_status],[date_created],[forecasting_group]) VALUES (%s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s,%s,%s)",
        [(company_number,company_name,address1,address2,city,region,postcode,'United Kingdom','Academy Chain Central Offices','ACADEMIC','Active',datetime.datetime.now(),'Other')
        ])
# you must call commit() to persist your data if you don't set autocommit to True
        conn.commit()


        company =""
        company_number=""
        company_status=""
        address1=""
        address2=""
        city=""
        region=""
        postcode=""




conn.close()




