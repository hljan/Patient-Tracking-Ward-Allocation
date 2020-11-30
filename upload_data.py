from datetime import datetime, timezone
from sqlalchemy import create_engine
import pandas as pd
import names
import uuid
import json
import requests

settings = {
    'api_base': 'https://r4.smarthealthit.org'
}

LOINC_CODES = ['91891-2', '95424-8', '92256-7', '33256-9', '777-3', '32623-1', '711-2', '742-7']
LOINC_TEXTS = ['Facility Bed type', 'SARS-CoV-2 (COVID-19) RNA in Respiratory specimen by Sequencing',
               'Has infectious disease or illness',
               'Leukocytes [#/volume] corrected for nucleated erythrocytes in Blood by Automated count',
               'Platelets [#/volume] in Blood by Automated count',
               'Platelet mean volume [Entitic volume] in Blood by Automated count',
               'Eosinophils [#/volume] in Blood by Automated count',
               'Monocytes [#/volume] in Blood by Automated count']


def createPatient(patient_data):
    resource = dict()

    # map patient data from database
    resource['resourceType'] = 'Patient'
    resource['id'] = patient_data['Patient ID']
    resource['active'] = True
    resource['gender'] = patient_data['gender']
    resource['birthDate'] = patient_data['dob']

    name = list()
    name_detail = dict()
    name_detail['use'] = 'official'
    name_detail['family'] = patient_data['family name']
    name_detail['given'] = list()
    name_detail['given'].append(patient_data['given name'])
    name_detail['prefix'] = list()
    if patient_data['gender'] == 'male':
        name_detail['prefix'].append('Mr.')
    elif patient_data['gender'] == 'female':
        name_detail['prefix'].append('Ms.')
    name.append(name_detail)
    resource['name'] = name

    return resource


def createObservationForPatient(patient_data, index):
    resource = dict()

    # map observation data from database
    resource['resourceType'] = 'Observation'
    resource['id'] = str(uuid.uuid4())
    resource['status'] = 'final'
    resource['subject'] = dict()
    resource['subject']['reference'] = 'Patient/' + patient_data['Patient ID']
    resource['effectiveDateTime'] = datetime.now(timezone.utc).isoformat()
    resource['issued'] = resource['effectiveDateTime']

    if patient_data.index[index] == 'ward allocation':
        # category
        category_type = 'survey'
        category_display = 'Survey'

        # valueCodeableConcept
        valueCodeableConcept = dict()
        coding_concept = list()
        coding_concept_detail = dict()
        coding_concept_detail['system'] = 'http://loinc.org'
        coding_concept_detail['code'] = patient_data[index]
        coding_concept_detail['display'] = patient_data[index]
        coding_concept.append(coding_concept_detail)
        valueCodeableConcept['coding'] = coding_concept
        valueCodeableConcept['text'] = coding_concept_detail['display']
        resource['valueCodeableConcept'] = valueCodeableConcept

    elif patient_data.index[index] in ['SARS-Cov-2 exam result', 'has_disease']:
        # category
        category_type = 'exam'
        category_display = 'Exam'

        # valueBoolean
        if patient_data[index] == 1:
            resource['valueBoolean'] = True
        elif patient_data[index] == 0:
            resource['valueBoolean'] = False

    else:
        # category
        category_type = 'laboratory'
        category_display = 'Laboratory'

        # valueQuantity
        valueQuantity = dict()
        valueQuantity['value'] = patient_data[index]
        if patient_data.index[index] == 'Mean platelet volume':
            valueQuantity['unit'] = 'fL'
        else:
            valueQuantity['unit'] = '10*3/uL'
        valueQuantity['system'] = 'http://unitsofmeasure.org'
        valueQuantity['code'] = valueQuantity['unit']
        resource['valueQuantity'] = valueQuantity

    # category
    category = list()
    category_detail = dict()
    category_detail['coding'] = list()
    coding_cat = dict()
    coding_cat['system'] = 'http://terminology.hl7.org/CodeSystem/observation-category'
    coding_cat['code'] = category_type
    coding_cat['display'] = category_display
    category_detail['coding'].append(coding_cat)
    category.append(category_detail)
    resource['category'] = category

    # code
    code = dict()
    code_detail = list()
    coding_code = dict()
    coding_code['system'] = 'http://loinc.org'
    coding_code['code'] = LOINC_CODES[index - 5]
    coding_code['display'] = LOINC_TEXTS[index - 5]
    code_detail.append(coding_code)
    code['coding'] = code_detail
    code['text'] = coding_code['display']
    resource['code'] = code

    return resource


def uploadPatient(patient_data):
    patient_json = json.dumps(patient_data)
    headers = {'Content-Type': 'application/json'}
    url = settings['api_base'] + '/Patient/' + patient_data['id']

    res = requests.put(url=url, headers=headers, data=patient_json).text
    res = json.loads(res)

    # print('Patient ID ' + res['id'] + ' successfully created on FHIR server:', url)

    return res


def uploadObservation(observation_data):
    observation_json = json.dumps(observation_data)
    headers = {'Content-Type': 'application/json'}
    url = settings['api_base'] + '/Observation/' + observation_data['id']

    res = requests.put(url=url, headers=headers, data=observation_json).text
    res = json.loads(res)

    # print('Observation ID ' + res['id'] + ' successfully created on FHIR server:', url)

    return res


def get_database_data(path='sqlite:///project_database.db'):
    disk_engine = create_engine(path)
    df_data = pd.read_sql('SELECT * FROM patient_source', disk_engine)

    return df_data


def export_patient_list(patient_source_list, path='sqlite:///project_database.db'):
    disk_engine = create_engine(path)
    df_patient_list = pd.DataFrame(patient_source_list, columns=['Patient ID'])
    df_patient_list.to_sql('app_patient_list', disk_engine, if_exists='replace', index=False)

    uploaded_patients_message = str(
        len(patient_source_list)) + ' sample patient records have been uploaded to FHIR server.'
    print(uploaded_patients_message)
    print('Useful patient list has been export to', path)

    return uploaded_patients_message


def create_sample_users(patient_list, path='sqlite:///project_database.db'):
    # generate 3 test users from the patient_list (2 patients + 1 clinician)
    disk_engine = create_engine(path)

    sample_users = dict()
    sample_users['userid'] = list()
    sample_users['username'] = list()
    sample_users['password'] = list()
    sample_users['usertype'] = list()
    sample_users['fullname'] = list()
    # create patient user
    for i in range(0, 2):
        sample_users['userid'].append(patient_list.at[i, 'Patient ID'])
        sample_users['username'].append('patient' + str(i + 1))
        sample_users['password'].append('patient' + str(i + 1))
        sample_users['usertype'].append('patient')
        if patient_list.at[i, 'gender'] == 'male':
            sample_users['fullname'].append('Mr. ' + patient_list.at[i, 'given name'] + ' '
                                            + patient_list.at[i, 'family name'])
        else:
            sample_users['fullname'].append('Ms. ' + patient_list.at[i, 'given name'] + ' '
                                            + patient_list.at[i, 'family name'])

    # create clinician user
    sample_users['userid'].append(str(uuid.uuid4()))
    sample_users['username'].append('clinician1')
    sample_users['password'].append('clinician1')
    sample_users['usertype'].append('clinician')
    sample_users['fullname'].append('Dr. ' + names.get_last_name() + ' ' + names.get_first_name())

    df = pd.DataFrame(sample_users, columns=['userid', 'username', 'password', 'usertype', 'fullname'])

    df.to_sql('user_info', disk_engine, if_exists='replace', index=False)

    return


def main(sample_size=5):
    df_patient_source = get_database_data()
    # reduce the size of sample data for demonstration
    df_patient_source = df_patient_source[:sample_size]
    patient_list = list()
    for index, row in df_patient_source.iterrows():
        # print(row)
        patient = createPatient(row)
        res = uploadPatient(patient)
        # when put patient data successfully
        if 'id' in res:
            for i in range(5, df_patient_source.shape[1]):
                observation = createObservationForPatient(row, i)
                res = uploadObservation(observation)
            # when put observation data successfully
            if 'id' in res:
                print('Processing ', index + 1, '/', df_patient_source.shape[0])
                patient_list.append(row['Patient ID'])
            else:
                print('Error on ', index + 1, '/', df_patient_source.shape[0])
        else:
            print('Error on ', index + 1, '/', df_patient_source.shape[0])

    # create sample users
    create_sample_users(df_patient_source)

    # export a database for the app
    msg_sample_patients = export_patient_list(patient_list)

    return msg_sample_patients

## working query
# https://r4.smarthealthit.org/Patient?_id=168f9f9a-20a6-4ff9-b2f2-445dd527a15b
# https://r4.smarthealthit.org/Observation?subject=Patient/168f9f9a-20a6-4ff9-b2f2-445dd527a15b&code=http://loinc.org|95424-8&_sort=-date
