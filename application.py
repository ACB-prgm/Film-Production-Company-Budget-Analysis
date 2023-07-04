from flask import Flask, redirect, url_for, request, render_template
from urllib.parse import urlencode
from modules import DBXReader
import requests
import base64
import boto3
import os


application = Flask(__name__)

# Dropbox OAuth endpoints
AUTHORIZE_URL = 'https://www.dropbox.com/oauth2/authorize'
TOKEN_URL = 'https://api.dropboxapi.com/oauth2/token'
CHECK_TOKEN_URL = 'https://api.dropboxapi.com/2/check/user'

# Redirect URI
REDIRECT_URI = 'https://productionbudgetanalyzer.xyz/auth/callback'
if os.path.isdir("test"): # for debugging locally
    import json
    with open("test/dbx_secrets.json") as f:
        secrets = json.load(f)
        for key in secrets:
            os.environ[key] = secrets[key]
    
    REDIRECT_URI = "http://127.0.0.1:5000/auth/callback"

BUCKET = "626-api-info"
GOOGLE_OAUTH_SECRETS = "google_oauth_client_secrets.json"
DBX_TOKENS = "dbx_tokens.json"

# Dropbox application credentials
DBX_CLIENT_ID = os.environ["APP_KEY"]
DBX_CLIENT_SECRET = os.environ["APP_SECRET"]

s3 = boto3.client("s3")


# PAGES ————————————————————————————————————————————————————————————————————————————————————————————————————————
@application.route('/')
def index():
    return render_template("home.html")


@application.route("/auth/login", methods=["GET"])
def login():
    valid_token = dbx_token_valid()

    if os.environ.get("access_token") and valid_token:
        print("logged in")
        return redirect(url_for('index'))
    elif os.environ.get("refresh_token") and not valid_token:
        print("refresh")
        success = refresh_dbx_token()
        if success:
            return redirect(url_for('index'))
    
    print("login again")
    return redirect(dbx_signin_url())


@application.route('/auth/callback')
def auth_callback():
    # Retrieve the authorization code from the query parameters
    auth_code = request.args.get('code')

    # Exchange the authorization code for an access token
    payload = {
        'code': auth_code,
        'grant_type': 'authorization_code',
        'client_id': DBX_CLIENT_ID,
        'client_secret': DBX_CLIENT_SECRET,
        'redirect_uri': REDIRECT_URI
    }
    response = requests.post(TOKEN_URL, data=payload)

    if response.status_code == 200:
        update_dbx_s3_tokens(response.json())
        return f"Access Token: {os.environ['access_token']}"
    else:
        return 'Error retrieving access token'


# HELPERS ————————————————————————————————————————————————————————————————————————————————————————————————————————
def dbx_signin_url() -> str:
    # Redirect the user to the Dropbox authorization URL
    params = {
        'response_type': 'code',
        'client_id': DBX_CLIENT_ID,
        'redirect_uri': REDIRECT_URI,
        'force_reapprove': 'true',
        'token_access_type' : 'offline'
    }
    return f"{AUTHORIZE_URL}?{urlencode(params)}"


def dbx_token_valid():
    headers = {
        'Authorization': f'Bearer {os.environ.get("access_token")}',
        "Content-Type": "application/json",
    }
    data = {'query':'user'}
    response = requests.post(CHECK_TOKEN_URL, headers=headers, data=data)

    if response.status_code != 401:
        # Token is valid
        return True
    else:
        # Token is invalid or expired
        return False

def refresh_dbx_token():
    if not os.environ.get("refresh_token"):
        return False
    
    data = {
        'grant_type' : 'refresh_token',
        'refresh_token' : os.environ["refresh_token"],
    }
    # Prepare the headers
    auth = base64.b64encode(f"{DBX_CLIENT_ID}:{DBX_CLIENT_SECRET}".encode()).decode()
    headers = {'Authorization': f"Basic {auth}",}

    # # Make the POST request
    response = requests.post(TOKEN_URL, headers=headers, data=data)

    # Check the response
    if response.status_code == 200:
        update_dbx_s3_tokens(response.json())
        return True
    else:
        print(f"Request failed with status {response.status_code}")
        return False

def get_dbx_s3_tokens():
    dbx_tokens = json.loads(s3.get_object(Bucket=BUCKET, Key=DBX_TOKENS)["Body"].read())
    os.environ["access_token"] = dbx_tokens.get("access_token", "")
    os.environ["refresh_token"] = dbx_tokens.get("refresh_token", "")

    return dbx_tokens

def update_dbx_s3_tokens(response):
    dbx_tokens = get_dbx_s3_tokens()

    for token in ["access_token", "refresh_token"]:
        if response.get(token):
            os.environ[token] = response[token]
            dbx_tokens[token] = response[token]
    
    s3.put_object(Bucket=BUCKET, Key=DBX_TOKENS, Body=json.dumps(dbx_tokens))



if __name__ == '__main__':
    get_dbx_s3_tokens()

    application.run()
