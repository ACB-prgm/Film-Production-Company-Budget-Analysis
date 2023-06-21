from flask import Flask, redirect, request, render_template
from modules import DBXReader
import requests
import os

application = Flask(__name__)

# Dropbox application credentials
CLIENT_ID = os.environ["APP_KEY"]
CLIENT_SECRET = os.environ["APP_SECRET"]

# Redirect URI
REDIRECT_URI = 'https://ProductionBudgetAnalyzer-env.eba-8us8qt3u.us-east-1.elasticbeanstalk.com/auth/callback'

# OAuth endpoints
AUTHORIZE_URL = 'https://www.dropbox.com/oauth2/authorize'
TOKEN_URL = 'https://api.dropboxapi.com/oauth2/token'

@application.route('/')
def index():
    return render_template("templates/home.html")

@application.route("/login", methods=["GET"])
def login():
    # Redirect the user to the Dropbox authorization URL
    auth_url = f"{AUTHORIZE_URL}?response_type=code&client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
    return redirect(auth_url)

@application.route('/auth/callback')
def auth_callback():
    # Retrieve the authorization code from the query parameters
    auth_code = request.args.get('code')
    print(auth_code)

    # Exchange the authorization code for an access token
    payload = {
        'code': auth_code,
        'grant_type': 'authorization_code',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'redirect_uri': REDIRECT_URI
    }
    response = requests.post(TOKEN_URL, data=payload)

    if response.status_code == 200:
        access_token = response.json()['access_token']
        # Use the access token to make API requests on behalf of the user
        # Add your code here to perform actions with the access token
        return f"Access Token: {access_token}"
    else:
        return 'Error retrieving access token'


if __name__ == '__main__':
    application.run()
