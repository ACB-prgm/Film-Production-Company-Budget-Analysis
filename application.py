from flask import Flask, redirect, url_for, request, render_template, make_response, jsonify
from urllib.parse import urlencode
from modules import DBXReader
import multiprocessing
import requests
import dropbox
import gspread
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
DBX_LINK = "dbx_link.txt"

GOOGLE_SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
processing_data = None

s3 = boto3.client("s3")


# PAGES ————————————————————————————————————————————————————————————————————————————————————————————————————————
@application.route('/')
def index():
    return render_template("home.html")

@application.route("/auth/login", methods=["GET"])
def login():
    populate_environ_tokens()

    # DBX FLOW
    token_valid = dbx_token_valid()
    if os.environ.get("dbx_access_token") and token_valid:
        print("dbx logged in")
    elif os.environ.get("dbx_refresh_token") and not token_valid:
        print("dbx refresh")
        success = refresh_dbx_token()
        if not success:
            print("dbx login again")
            return redirect(dbx_auth_url())
    else:
        return redirect(dbx_auth_url())
        
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
        return redirect(google_auth_url())
    
    return render_template("link_submission.html")

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

@application.route('/submit', methods=['POST'])
def submit():
    update_dbx_link(request.form['link'])
    start_processing(force_restart=True)
    return render_template("processing.html", url=url_for("processing"), message="Your data is being processed. This may take some time.")

@application.route('/dbx_webhook', methods=["POST"])
def dbx_webhook():
    if link_exists():
        start_processing()
        return "Success"
    else:
        return error(400, "No Link Found")
    

@application.route('/processing/datasets', methods=['GET'])
def processing():
    _processing = processing_data and processing_data.is_alive()
    if _processing:
        return render_template("processing.html", url=url_for("processing"), delay=3000, message="Your data is still processing...")
    
    return "Sucess! Your data has been processed"


def start_processing(force_restart=False):
    global processing_data
    _processing = processing_data and processing_data.is_alive()

    if not _processing: # Check if not currently processing
        processing_data = multiprocessing.Process(target=process_data)
        processing_data.start()
    elif force_restart and _processing: # if already processing and need to override
        processing_data.terminate()
        processing_data = multiprocessing.Process(target=process_data)
        processing_data.start()


def process_data():
    populate_environ_tokens()
    dbx = dropbox.Dropbox(os.environ["dbx_access_token"])
    dbx_reader = DBXReader.DbxDataRetriever(os.environ["dbx_link"], dbx)
    dbx_reader.create_datasets()
    upload_dfs_to_google_sheet(dbx_reader.datasets, "626_budget_analysis")

# HELPERS ————————————————————————————————————————————————————————————————————————————————————————————————————————
def dbx_auth_url() -> str:
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
    return f"{gsecrets['auth_uri']}?{urlencode(params)}"

def dbx_token_valid() -> bool:
    headers = {
        'Authorization': f'Bearer {os.environ.get("dbx_access_token")}',
        "Content-Type": "application/json",
    }
    data = {'query':'user'}
    response = requests.post(DBX_CHECK_TOKEN_URL, headers=headers, data=data)

    if response.status_code != 401: # 401 is unauthorized
        # Token is valid
        return True
    else:
        # Token is invalid or expired
        return False

def refresh_dbx_token() -> bool:
    if not os.environ.get("dbx_refresh_token"):
        return False
    secrets = get_dbx_secrets()
    data = {
        'grant_type' : 'refresh_token',
        'refresh_token' : os.environ["dbx_refresh_token"],
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

def upload_dfs_to_google_sheet(dfs:dict, sheet_name:str):
    gc = create_gspread_client()

    try:
        # Try to open the Google Sheet if it exists
        sheet = gc.open(sheet_name)
    except gspread.exceptions.SpreadsheetNotFound:
        # Create a new Google Sheet if it doesn't exist
        sheet = gc.create(sheet_name)


    for idx, df_name in enumerate(dfs):
        df = dfs.get(df_name)
        try:
            worksheet = sheet.get_worksheet(idx)
            worksheet.clear()
            worksheet.update_title(df_name)
        except gspread.WorksheetNotFound:
            worksheet = sheet.add_worksheet(df_name, len(df), len(df.columns))
        
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())

    # sheet.share(share_email, "user", "writer", notify=False)

def create_gspread_client():
    secrets = get_google_secrets()
    auth_user = {
        "refresh_token": os.environ.get("google_refresh_token"),
        "token_uri": secrets["token_uri"],
        "client_id": secrets["client_id"],
        "client_secret": secrets["client_secret"],
    }

    gc, _ = gspread.oauth_from_dict(authorized_user_info=auth_user)

    return gc

def link_exists() -> bool:
    if os.environ.get("dbx_link"):
        return True

    try:
        link = s3.get_object(Bucket=BUCKET, Key=DBX_LINK)["Body"].read().decode('utf-8')
        os.environ["dbx_link"] = link
        return True
    except:
        return False

def update_dbx_link(link) -> None:
    os.environ["dbx_link"] = link
    s3.put_object(Bucket=BUCKET, Key=DBX_LINK, Body=link)

def error(num, message):
    status_code = num
    message = message
    response = make_response(jsonify({"error": message}), status_code)
    return response


if __name__ == '__main__':
    application.run()
