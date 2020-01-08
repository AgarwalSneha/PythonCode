import firebase_admin as fb
from firebase_admin import firestore
import pandas as pd
import os

cred = fb.credentials.Certificate('./ServiceAccountKey.json')
default_app = fb.initialize_app(cred)
db = firestore.client()


def readFiles():
    with os.scandir("./csv/") as entries :    #reading all files in the folder 'csv'
        for entry in entries:
            path='./csv/'+entry.name
            dataframe = pd.read_csv(path)
            rec = dataframe.to_dict(orient='records')
            write(dataframe, rec)

def write(df, dict):
    datalist = 0
    my_doc_ref = db.collection('ObservationSheets').document()   #storing data in the collection 'observationSheets'
    for i in df['ObId']:
        i = 1
        datalist = datalist + i
        j = datalist - 1
        my_doc_ref.set(
            {
                u'TACID': dict[0]['TACID'],
                u'Version': dict[0]['Version'],
                u'Student Code': dict[0]['Student Code'],
                u'Section': dict[0]['Section'],
                u'School': dict[0]['School'],
                u'AugmentedData': {dict[j]['ObId']: {
                    u'Filled Value': dict[j]['Filled Value'],
                    u'Complete': dict[j]['Complete'],
                    u'Correct': dict[j]['Correct']

                }}
            }, merge=True
        )


readFiles()
