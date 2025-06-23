import os
import time
import pickle

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


class GoogleDriveHandler:
    def __init__(self, credentials_file, token_file, scopes=None):
        if scopes is None:
            scopes = ["https://www.googleapis.com/auth/drive.file"]
        self.credentials_file = credentials_file
        self.token_file = token_file
        self.scopes = scopes
        self.creds = None
        self.service = None
        self.authenticate()

    def authenticate(self):
        # Load credentials from token file if it exists
        if os.path.exists(self.token_file):
            with open(self.token_file, "rb") as token:
                self.creds = pickle.load(token)

        # If there are no valid credentials, prompt the user to log in
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.credentials_file, self.scopes)
                self.creds = flow.run_local_server(port=6060)

            # Save the credentials for the next run
            with open(self.token_file, "wb") as token:
                pickle.dump(self.creds, token)

        self.service = build("drive", "v3", credentials=self.creds)

    def upload_file(self, file_path, file_name, folder_id=None):
        file_metadata = {"name": file_name}
        if folder_id:
            file_metadata["parents"] = [folder_id]

        media = MediaFileUpload(file_path, mimetype="text/csv")

        for attempt in range(3):
            try:
                file = self.service.files().create(body=file_metadata, media_body=media, fields="id").execute()
                return file.get("id")
            except Exception as e:
                if attempt == 2:
                    raise e
                time.sleep(10)
