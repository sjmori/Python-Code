__author__ = 'user'

import urllib2
# If you are using Python 3+, import urllib instead of urllib2

import json

def percentage_format(num, x):
     return str(int(round(num,x)*100))+"%"


def azure_results(Age, Marital_Status, Gender, Weight_Category, Cholesterol, Stress_Management, Trait_Anxiety):
    data =  {

            "Inputs": {

                    "input1":
                    {
                        "ColumnNames": ["Age", "Marital_Status", "Gender", "Weight_Category", "Cholesterol", "Stress_Management", "Trait_Anxiety", "2nd_Heart_Attack", "id"],
                        "Values": [ [ Age, Marital_Status, Gender, Weight_Category, Cholesterol, Stress_Management, Trait_Anxiety, "value", "0" ] ]
                    },        },
                "GlobalParameters": {
    }
        }

    body = str.encode(json.dumps(data))

    url = 'https://ussouthcentral.services.azureml.net/workspaces/552eecd8e1b14c3bb6be0d818f90aee1/services/68cbe18482a64e2c8a24ed685b0762e4/execute?api-version=2.0&details=true'
    api_key = 'O4RjTMX5/4usdscitlL4tlEs2FuN4M52cc2iFonBpkQJUyQgkSatah5MTtSX7Yc7rHOLXXxPWDHNcSxLX5lWjQ==' # Replace this with the API key for the web service
    headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}

    req = urllib2.Request(url, body, headers)

    try:
        response = urllib2.urlopen(req)

        # If you are using Python 3+, replace urllib2 with urllib.request in the above code:
        # req = urllib.request.Request(url, body, headers)
        # response = urllib.request.urlopen(req)

        result = response.read()


        result_data = json.loads(result)

        result_data = json.loads(result)

        percentage = float(result_data['Results']['output1']['value']['Values'][0][9])

        percentage_output = percentage_format(percentage,2)
        return percentage_output
    except urllib2.HTTPError, error:
        print("The request failed with status code: " + str(error.code))

        # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
        print(error.info())

        print(json.loads(error.read()))
