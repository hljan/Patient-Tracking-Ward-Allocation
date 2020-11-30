from flask import Blueprint, render_template, redirect, url_for, request, flash
from flask_login import login_user, login_required, logout_user
from datetime import timedelta
from model import UserInfo
from get_data_fhir import clear_local_database

auth = Blueprint('auth', __name__)


@auth.route('/')
def login():
    return render_template('login.html')


@auth.route('/login', methods=['POST'])
def login_post():
    username = request.form.get('username')
    password = request.form.get('password')
    remember = True if request.form.get('remember') else False

    user = UserInfo.query.filter_by(username=username).first()

    # check the credential
    if not user or password != user.password:
        flash('Login credentials incorrect, please try again.')
        return redirect(url_for('auth.login'))

    # when the credential is valid, proceed to patient/clinician page
    login_user(user, remember=remember, duration=timedelta(minutes=10))
    if user.usertype == 'patient':
        return redirect(url_for('main.patient', patient_id=user.userid))
    elif user.usertype == 'clinician':
        return redirect(url_for('main.clinician', page='Overview', user_id=user.userid))


@auth.route('/logout')
@login_required
def logout():
    logout_user()
    clear_local_database()
    return redirect(url_for('auth.login'))
