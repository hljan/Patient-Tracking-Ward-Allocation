import json
import requests
from sqlalchemy import create_engine
import pandas as pd

api_base = 'https://r4.smarthealthit.org'
path = 'sqlite:///project_database.db'
disk_engine = create_engine(path)

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

patients_records_with_calculation = dict()


def search_patient_data(patient_id):
    # search in local database, if not found search on FHIR server and update local database
    patient_data = search_local_database(patient_id)

    if not patient_data:

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
                elif key == 'COVID-19 test result' or key == 'patient has disease':
                    patient_data[key] = \
                        res['entry'][0]['resource']['valueBoolean']
                else:
                    patient_data[key] = \
                        res['entry'][0]['resource']['valueQuantity']['value']
                    unit_key = 'UoM ' + key
                    patient_data[unit_key] = \
                        res['entry'][0]['resource']['valueQuantity']['unit']

        patient_data['patient id'] = patient_id

        # save query data into database for better performance
        save_to_local_database(patient_data)

    return patient_data


def search_all_patient_data():
    patient_id_list = get_database_patients()
    patient_records = list()

    for patient_id in patient_id_list:
        patient_record = search_patient_data(patient_id[0])
        patient_records.append(patient_record)

    return patient_records


def get_database_patients():
    df_id = pd.read_sql('SELECT * FROM app_patient_list', disk_engine)

    return df_id.values.tolist()


def save_to_local_database(patient_data):
    df = pd.DataFrame([patient_data])
    df.to_sql('app_query_list', disk_engine, if_exists='append', index=False)

    return


def search_local_database(patient_id):
    try:
        command = 'SELECT * FROM app_query_list WHERE "patient id" = "' + patient_id + '"'
        df_data = pd.read_sql(command, disk_engine)
        if df_data.empty:
            return dict()
        else:
            patient_data = df_data.to_dict('r')
            patient_data = patient_data[0]
    except Exception:
        return dict()

    return patient_data


def clear_local_database():
    disk_engine.execute('DELETE FROM app_query_list')

    return


def count_ward_allocation(list_patients):
    ward_allocation = dict()
    # mock up the hospital ward capacity
    ward_allocation['regular ward total'] = 50
    ward_allocation['semi-intensive unit total'] = 20
    ward_allocation['intensive care unit total'] = 10
    ward_allocation['total'] = 80
    ward_allocation['current'] = 0
    ward_allocation['no allocation'] = 0
    ward_allocation['regular ward'] = 0
    ward_allocation['semi-intensive unit'] = 0
    ward_allocation['intensive care unit'] = 0

    for patient in list_patients:
        if patient['ward allocation'] == 'no allocation':
            ward_allocation['no allocation'] += 1
            ward_allocation['current'] += 1
        elif patient['ward allocation'] == 'regular ward':
            ward_allocation['regular ward'] += 1
            ward_allocation['current'] += 1
        elif patient['ward allocation'] == 'semi-intensive unit':
            ward_allocation['semi-intensive unit'] += 1
            ward_allocation['current'] += 1
        elif patient['ward allocation'] == 'intensive care unit':
            ward_allocation['intensive care unit'] += 1
            ward_allocation['current'] += 1

    return ward_allocation


def calculate_health_status(list_patients, ward_allocation):
    global patients_records_with_calculation

    patients_records_with_calculation = dict()

    sorted_list = sorted(list_patients, key=lambda i: i['patient has disease'], reverse=False)
    sorted_list = sorted(sorted_list, key=lambda i: i['leukocytes'], reverse=False)
    sorted_list = sorted(sorted_list, key=lambda i: i['platelets'], reverse=False)
    sorted_list = sorted(sorted_list, key=lambda i: i['platelets mean volume'], reverse=False)
    sorted_list = sorted(sorted_list, key=lambda i: i['eosinophils'], reverse=False)
    sorted_list = sorted(sorted_list, key=lambda i: i['monocytes'], reverse=True)
    sorted_list = sorted(sorted_list, key=lambda i: i['COVID-19 test result'], reverse=True)

    for i in range(len(sorted_list)):
        if i / len(sorted_list) < 0.1:
            sorted_list[i]['health status'] = 'Emergent'
            if ward_allocation['intensive care unit'] < ward_allocation['intensive care unit total']:
                sorted_list[i]['suggest ward'] = 'intensive care unit'
            elif ward_allocation['semi-intensive unit'] < ward_allocation['semi-intensive unit total']:
                sorted_list[i]['suggest ward'] = 'semi-intensive unit'
            elif ward_allocation['regular ward'] < ward_allocation['regular ward total']:
                sorted_list[i]['suggest ward'] = 'regular ward'
            else:
                sorted_list[i]['suggest ward'] = 'no allocation'
        elif i / len(sorted_list) < 0.4:
            sorted_list[i]['health status'] = 'Semi-urgent'
            if ward_allocation['semi-intensive unit'] < ward_allocation['semi-intensive unit total']:
                sorted_list[i]['suggest ward'] = 'semi-intensive unit'
            elif ward_allocation['regular ward'] < ward_allocation['regular ward total']:
                sorted_list[i]['suggest ward'] = 'regular ward'
            else:
                sorted_list[i]['suggest ward'] = 'no allocation'
        elif i / len(sorted_list) < 0.8:
            sorted_list[i]['health status'] = 'Warning'
            if ward_allocation['regular ward'] < ward_allocation['regular ward total']:
                sorted_list[i]['suggest ward'] = 'regular ward'
            else:
                sorted_list[i]['suggest ward'] = 'no allocation'
        else:
            sorted_list[i]['health status'] = 'Good'
            sorted_list[i]['suggest ward'] = 'no allocation'

    patients_records_with_calculation = sorted_list

    return sorted_list

def get_health_status(patient_id):

    global patients_records_with_calculation

    for patient in patients_records_with_calculation:
        if patient_id == patient['patient id']:
            return patient
