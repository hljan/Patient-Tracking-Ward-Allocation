from app import db
from flask_login import UserMixin


class UserInfo(UserMixin, db.Model):
    userid = db.Column(db.String(50), primary_key=True)  # primary key
    username = db.Column(db.String(10), unique=True)
    password = db.Column(db.String(10))
    usertype = db.Column(db.String(10))
    fullname = db.Column(db.String(10))

    # overwrite get_id function
    def get_id(self):
        return self.userid
