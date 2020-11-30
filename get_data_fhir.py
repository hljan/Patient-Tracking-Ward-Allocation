import json
import requests
from sqlalchemy import create_engine
import pandas as pd

api_base = 'https://r4.smarthealthit.org'

loinc_codes = {
    'ward allocation': '91891-2',
    'COVID-19 test result': '95424-8',
    'patient has disease': '92256-7',
    'leukocytes': '33256-9',
    'platelets': '777-3',
    'platelets mean volume': '32623-1',
    'eosinophils': '711-2',
    'monocytes': '742-7'
}


def search_patient_data(patient_id):
    patient_data = dict()

    url = api_base + '/Patient?_id=' + patient_id
    headers = {'Content-Type': 'application/json'}
    res = requests.get(url=url, headers=headers).text
    res = json.loads(res)

    # get the patient's personal data
    if res['total'] > 0:
        patient_data['birth date'] = res['entry'][0]['resource']['birthDate']
        patient_data['full name'] = res['entry'][0]['resource']['name'][0]['given'][0] + ' ' \
                                    + res['entry'][0]['resource']['name'][0]['family']

    for i, (key, values) in enumerate(loinc_codes.items()):
        url = api_base + '/Observation?_sort=-date' \
              + '&subject=Patient/' + patient_id \
              + '&code=http://loinc.org|' + loinc_codes[key]
        headers = {'Content-Type': 'application/json'}
        res = requests.get(url=url, headers=headers).text
        res = json.loads(res)

        # get the patient's vital sign, ward allocation and test result
        if res['total'] > 0:
            if key == 'ward allocation':
                patient_data[key] = \
                    res['entry'][0]['resource']['valueCodeableConcept']['coding'][0]['code']
            elif key in ['COVID-19 test result', 'patient has disease']:
                patient_data[key] = \
                    res['entry'][0]['resource']['valueBoolean']
            else:
                patient_data[key] = \
                    res['entry'][0]['resource']['valueQuantity']['value']
                unit_key = 'UoM ' + key
                patient_data[unit_key] = \
                    res['entry'][0]['resource']['valueQuantity']['unit']

    return patient_data


def search_all_patient_data():
    patient_id_list = get_database_patients()
    patient_records = list()

    for patient_id in patient_id_list:
        patient_record = search_patient_data(patient_id[0])
        patient_record['patient id'] = patient_id[0]
        patient_records.append(patient_record)

    return patient_records


def get_database_patients(path='sqlite:///project_database.db'):
    disk_engine = create_engine(path)
    df_id = pd.read_sql('SELECT * FROM app_patient_list', disk_engine)

    # command = 'SELECT * FROM patient_source WHERE Patient ID in ' + df_id
    # df_records = pd.read_sql(command, disk_engine)

    return df_id.values.tolist()
