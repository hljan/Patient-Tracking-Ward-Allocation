from flask import Blueprint, render_template
from flask_login import login_required, current_user
from get_data_fhir import search_patient_data, search_all_patient_data, count_ward_allocation, calculate_health_status, \
    get_health_status
import data_cleanup
import upload_data

main = Blueprint('main', __name__)


@main.route('/initiate_patient_data/<status>')
def initiate_patient_data(status):
    if status == 'data_cleanup':
        msg_origin, msg_cleanup = data_cleanup.main()
        return render_template('progress.html', status=status, msg_origin=msg_origin, msg_cleanup=msg_cleanup)
    elif status == 'data_upload':
        msg_sample_patients = upload_data.main()
        return render_template('progress.html', status=status, msg_sample_patients=msg_sample_patients)

    return render_template('progress.html', status=status)


@main.route('/patient/<patient_id>')
@login_required
def patient(patient_id):
    patient_record = search_patient_data(patient_id)
    return render_template('patient.html', name=current_user.fullname, patient_record=patient_record)


@main.route('/clinician/<page>/<user_id>')
@login_required
def clinician(page, user_id):
    if page == 'Overview':
        patient_records = search_all_patient_data()
        ward_allocation = count_ward_allocation(patient_records)
        patient_records_with_status = calculate_health_status(patient_records, ward_allocation)
        return render_template('clinician.html', page=page, name=current_user.fullname,
                               patient_records=patient_records_with_status, ward_allocation=ward_allocation)
    if page == 'Details':
        # patient_record = search_patient_data(user_id)
        patient_record = get_health_status(user_id)
        return render_template('clinician_details.html', c_name=current_user.fullname, p_name=patient_record['full name'], patient_record=patient_record)
