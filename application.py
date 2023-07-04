from flask import Flask, redirect, url_for, request, render_template
from google_auth_oauthlib.flow import Flow
from urllib.parse import urlencode
from modules import DBXReader
import requests
import base64
import boto3
import json
import os


application = Flask(__name__)


DBX_CHECK_TOKEN_URL = "https://api.dropboxapi.com/2/check/user"
GOOGLE_CHECK_TOKEN_URL = "https://www.googleapis.com/oauth2/v1/tokeninfo"

local = os.path.isdir("test") # For debugging

# S3 PATHS
BUCKET = "626-api-info"
GOOGLE_OAUTH_SECRETS = "google_oauth_client_secrets.json"
GOOGLE_TOKENS = "google_tokens.json"
DBX_OAUTH_SECRETS = "dbx_secrets.json"
DBX_TOKENS = "dbx_tokens.json"

GOOGLE_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

s3 = boto3.client("s3")


# PAGES ————————————————————————————————————————————————————————————————————————————————————————————————————————
@application.route('/')
def index():
    return render_template("home.html")


@application.route("/auth/login", methods=["GET"])
def login():
    print("LOGIN")
    # DBX FLOW
    token_valid = dbx_token_valid()
    if os.environ.get("dbx_access_token") and token_valid:
        print("dbx logged in")
    elif os.environ.get("dbx_refresh_token") and not token_valid:
        print("dbx refresh")
        success = refresh_dbx_token()
        if not success:
            print("dbx login again")
            return redirect(dbx_signin_url())
    else:
        return redirect(dbx_signin_url())
        
    # GOOGLE FLOW
    token_valid = google_token_valid()
    if os.environ.get("google_access_token") and token_valid:
        print("google logged in")
    elif os.environ.get("google_refresh_token") and not token_valid:
        print("google refresh")
        success = refresh_google_token()
        if not success:
            print("google login again")
            return redirect(google_auth_url())
    else:
        redirect(google_auth_url())
    
    return "LOGIN SUCESSFULL"

@application.route('/auth/callback/<service>')
def auth_callback(service):
    # Retrieve the authorization code from the query parameters
    auth_code = request.args.get('code')
    
    if service == "dbx":
        secrets = get_dbx_secrets()
    elif service == "google":
        secrets = get_google_secrets()
    
    payload = {
        'code': auth_code,
        'grant_type': 'authorization_code',
        'client_id': secrets["client_id"],
        'client_secret': secrets["client_secret"],
        'redirect_uri': secrets["redirect_uri"]
    }
    response = requests.post(secrets["token_uri"], data=payload)

    if response.status_code == 200:
        update_s3_tokens(service, response.json())
        return redirect(url_for("login"))
    else:
        return 'Error retrieving access token'



# HELPERS ————————————————————————————————————————————————————————————————————————————————————————————————————————
def dbx_signin_url() -> str:
    # Redirect the user to the Dropbox authorization URL
    secrets = get_dbx_secrets()
    params = {
        'response_type': 'code',
        'client_id': secrets["client_id"],
        'redirect_uri': secrets["redirect_uri"],
        'force_reapprove': 'true',
        'token_access_type' : 'offline'
    }
    return f"{secrets['auth_uri']}?{urlencode(params)}"

def google_auth_url() -> str:
    gsecrets = get_google_secrets()
    params = {
        "client_id" : gsecrets["client_id"],
        "redirect_uri" : gsecrets["redirect_uri"],
        "response_type" : "code",
        "scope" : " ".join(GOOGLE_SCOPES),
        "access_type" : "offline"
    }
    print(params)
    return f"{gsecrets['auth_uri']}?{urlencode(params)}"

def dbx_token_valid() -> bool:
    headers = {
        'Authorization': f'Bearer {os.environ.get("dbx_access_token")}',
        "Content-Type": "application/json",
    }
    data = {'query':'user'}
    response = requests.post(DBX_CHECK_TOKEN_URL, headers=headers, data=data)

    if response.status_code != 401:
        # Token is valid
        return True
    else:
        # Token is invalid or expired
        return False

def refresh_dbx_token() -> bool:
    if not os.environ.get("refresh_token"):
        return False
    secrets = get_dbx_secrets()
    data = {
        'grant_type' : 'refresh_token',
        'refresh_token' : os.environ["refresh_token"],
    }
    # Prepare the headers
    auth = base64.b64encode(f"{secrets['client_id']}:{secrets['client_secret']}".encode()).decode()
    headers = {'Authorization': f"Basic {auth}",}

    # # Make the POST request
    response = requests.post(secrets["token_uri"], headers=headers, data=data)

    # Check the response
    if response.status_code == 200:
        update_s3_tokens("dbx", response.json())
        return True
    else:
        print(f"Request failed with status {response.status_code}")
        return False

def google_token_valid() -> bool:
    token = os.environ.get("google_access_token")
    response = requests.get('{}?access_token={}'.format(GOOGLE_CHECK_TOKEN_URL, token))

    if response.status_code == 200:
        return True
    else:
        return False

def refresh_google_token() -> bool:
    secrets = get_google_secrets()

    data = {
    'client_id': secrets["client_id"],
    'client_secret': secrets["client_secret"],
    'refresh_token': os.environ["google_refresh_token"],
    'grant_type': 'refresh_token',
    }

    response = requests.post('https://oauth2.googleapis.com/token', data=data)
    if response.status_code == 200:
        update_s3_tokens("google", response.json())
        return True
    else:
        return False

def get_google_secrets() -> dict:
    secrets = json.loads(s3.get_object(Bucket=BUCKET, Key=GOOGLE_OAUTH_SECRETS)["Body"].read())["web"]
    secrets["redirect_uri"] = secrets["redirect_uris"][int(local)]
    return secrets

def get_dbx_secrets() -> dict:
    secrets = json.loads(s3.get_object(Bucket=BUCKET, Key=DBX_OAUTH_SECRETS)["Body"].read())
    secrets["redirect_uri"] = secrets["redirect_uris"][int(local)]
    return secrets

def get_s3_tokens(service):
    if service == "dbx":
        key = DBX_TOKENS
    elif service == "google":
        key = GOOGLE_TOKENS

    try:
        tokens = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except:
        tokens = {"access_token":"", "refresh_token":""}

    return tokens

def update_s3_tokens(service, response):
    s3_tokens = get_s3_tokens(service)

    for token in ["access_token", "refresh_token"]:
        if response.get(token):
            os.environ["%s_%s" % (service, token)] = response[token]
            s3_tokens[token] = response[token]
    
    if service == "dbx":
        key = DBX_TOKENS
    elif service == "google":
        key = GOOGLE_TOKENS
    
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(s3_tokens))

def populate_environ_tokens() -> None:
    for service in ["dbx", "google"]:
        tokens = get_s3_tokens(service)
        for token in ["access_token", "refresh_token"]:
            os.environ["%s_%s" % (service, token)] = tokens[token]


if __name__ == '__main__':
    populate_environ_tokens()
    application.run()
