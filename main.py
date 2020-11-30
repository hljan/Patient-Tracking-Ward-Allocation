from flask import Blueprint, render_template
from flask_login import login_required, current_user
from get_data_fhir import search_patient_data, search_all_patient_data
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


@main.route('/clinician/<page>')
@login_required
def clinician(page):
    if page == 'Overview':
        patient_records = search_all_patient_data()
        return render_template('clinician.html', page=page, name=current_user.fullname, patient_records=patient_records)


@main.route('/profile')
@login_required
def profile():
    return render_template('profile.html', name=current_user.fullname, id=current_user.userid)
