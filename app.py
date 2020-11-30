# Reference: https://www.digitalocean.com/community/tutorials/how-to-add-authentication-to-your-app-with-flask-login

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager

# Initial App
db = SQLAlchemy()


def create_app():
    app = Flask(__name__)

    # app config
    app.config['SECRET_KEY'] = 'secretkey'
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///project_database.db'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    # db config
    db.init_app(app)

    login_manager = LoginManager()
    login_manager.login_view = 'auth.login'
    login_manager.init_app(app)
    from model import UserInfo
    @login_manager.user_loader
    def load_user(user_id):
        #  use userid to query user
        return UserInfo.query.get(user_id)

    # route config
    from auth import auth as auth_blueprint
    app.register_blueprint(auth_blueprint)
    from main import main as main_blueprint
    app.register_blueprint(main_blueprint)

    return app


# run flask app
if __name__ == "__main__":
    app = create_app()
    db.create_all(app=app)
    app.run(host="localhost", port=5000, debug=True)
