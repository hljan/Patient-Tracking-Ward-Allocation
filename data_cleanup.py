from sqlalchemy import create_engine
import random
import names
import uuid
import pandas as pd
import numpy as np


def import_data(path='dataset.xlsx'):
    df = pd.read_excel(path)

    original_size_message = 'Original data size: ' + str(df.shape[0]) \
                            + ' entries and ' + str(df.shape[1]) + ' parameters.'
    print(original_size_message)

    # Standardize the test result as 1 and 0
    df['SARS-Cov-2 exam result'] = df['SARS-Cov-2 exam result'].map({'positive': 1, 'negative': 0})
    # Map detected/not_detected and positive/negative to 1 and 0
    df = df.replace({'positive': 1, 'negative': 0, 'detected': 1, 'not_detected': 0})

    # Map null percentage by column
    df_null_pct = df.isna().mean().round(4) * 100
    df_null_pct.sort_values(ascending=False)
    # Remove null columns > 90% (should remain 39 features)
    nulls = df_null_pct[df_null_pct > 90]
    df = df[[col for col in df.columns if col not in nulls]]

    # Drop this feature due to 0 variance
    df.drop('Parainfluenza 2', axis=1, inplace=True)

    # Summarize features as presence of antigens
    df['has_disease'] = df[df.columns[20:]].sum(axis=1)
    df.loc[df['has_disease'] > 1, 'has_disease'] = 1
    df['has_disease'] = df['has_disease'].astype(int)

    # Extract the require columns (according to the research)
    extract_features = ['Patient ID',
                        'Patient addmited to regular ward (1=yes, 0=no)',
                        'Patient addmited to semi-intensive unit (1=yes, 0=no)',
                        'Patient addmited to intensive care unit (1=yes, 0=no)',
                        'SARS-Cov-2 exam result',
                        'Patient age quantile',
                        'Leukocytes',
                        'Platelets',
                        'has_disease',
                        'Eosinophils',
                        'Mean platelet volume ',
                        'Monocytes']
    df_extracted = df[extract_features]

    # Remove entries with missing values
    df_extracted = df_extracted.dropna(axis=0)

    print('Data size after clean-up: ', df_extracted.shape)

    return df_extracted, original_size_message


def export_to_database(df_data, path='sqlite:///project_database.db'):
    disk_engine = create_engine(path)
    df_data.to_sql('patient_source', disk_engine, if_exists='replace', index=False)

    print('Source data has been exported to', path)

    return


def generate_patient_data(df_patient):
    gender = ['female', 'male']
    age_range = [10, 80]  # fake data
    age_qua_range = [min(df_patient['Patient age quantile']), max(df_patient['Patient age quantile'])]

    # gender
    df_patient['gender'] = np.random.randint(0, 2, df_patient.shape[0])
    df_patient['gender'] = df_patient['gender'].map({0: gender[0], 1: gender[1]})

    # age (for getting dob)
    ages = (((age_range[1] - age_range[0]) / (age_qua_range[1] - age_qua_range[0]) * df_patient[
        'Patient age quantile']) + age_range[0]).astype(int)

    for index, row in df_patient.iterrows():
        # ward allocation
        if row['Patient addmited to regular ward (1=yes, 0=no)'] == 1:
            df_patient.at[index, 'ward allocation'] = 'regular ward'
        elif row['Patient addmited to semi-intensive unit (1=yes, 0=no)'] == 1:
            df_patient.at[index, 'ward allocation'] = 'semi-intensive unit'
        elif row['Patient addmited to intensive care unit (1=yes, 0=no)'] == 1:
            df_patient.at[index, 'ward allocation'] = 'intensive care unit'
        else:
            df_patient.at[index, 'ward allocation'] = 'no allocation'

        # dob
        dob = pd.Timestamp('now') - pd.DateOffset(years=ages[index],
                                                  months=random.randint(0, 12),
                                                  days=random.randint(0, 31))
        df_patient.at[index, 'dob'] = dob.date().isoformat()

        # last name
        df_patient.at[index, 'family name'] = names.get_last_name()
        # first name
        df_patient.at[index, 'given name'] = names.get_first_name(row['gender'])
        # patient id
        df_patient.at[index, 'Patient ID'] = str(uuid.uuid4())

    df_patient['Mean platelet volume'] = df_patient['Mean platelet volume ']
    # clean redundant and rearrange df columns
    df_patient = df_patient[['Patient ID',
                             'family name',
                             'given name',
                             'gender',
                             'dob',
                             'ward allocation',
                             'SARS-Cov-2 exam result',
                             'has_disease',
                             'Leukocytes',
                             'Platelets',
                             'Mean platelet volume',
                             'Eosinophils',
                             'Monocytes']]

    cleanup_size_message = 'Data size after clean-up: ' + str(df_patient.shape[0]) \
                           + ' entries and ' + str(df_patient.shape[1]) + ' parameters.'
    print(cleanup_size_message)

    return df_patient, cleanup_size_message


def main():
    data, msg_original_size = import_data()
    generate_data, msg_cleanup_size = generate_patient_data(data)
    export_to_database(generate_data)

    return msg_original_size, msg_cleanup_size
