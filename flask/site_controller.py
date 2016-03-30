__author__ = 'user'

import flask


from logistic_regression import azure_results

app = flask.Flask(__name__)

@app.route('/search/')

def index(myvalue=None):
    """ Displays the index page accessible at '/'
    """

    #?Age=75&Marital_Status=2&Gender=1&Weight_Category=3&Cholesterol=180&Stress_Management=1&Trait_Anxiety=65
    Age = flask.request.args.get('Age')
    Marital_Status = flask.request.args.get('Marital_Status')

    if Marital_Status == "Single":
        Marital_Status=0
    elif Marital_Status =="Married":
        Marital_Status=1
    elif Marital_Status == "Divorced":
        Marital_Status=2
    else:
        Marital_Status=3



    Gender = flask.request.args.get('Gender')

    if Gender == "Female":
        Gender=0

    else:
        Gender=1


    Weight_Category = flask.request.args.get('Weight_Category')

    if Weight_Category == "Normal":
        Weight_Category=0
    elif Weight_Category =="Overweight":
        Weight_Category=1

    else:
        Weight_Category=2
    Cholesterol = flask.request.args.get('Cholesterol')
    Stress_Management = flask.request.args.get('Stress_Management')

    if Stress_Management == "No":
        Stress_Management=0

    else:
        Stress_Management=1



    Trait_Anxiety = flask.request.args.get('Trait_Anxiety')

    


    ml = azure_results(Age, Marital_Status, Gender, Weight_Category, Cholesterol, Stress_Management, Trait_Anxiety)

    return flask.render_template('index.html',myvalue=ml)


#def return_result():
 #   x=azure_results()
  #  return x




if __name__ == '__main__':
    app.debug=True
    app.run()
