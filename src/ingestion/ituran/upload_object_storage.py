import requests

# Pre-signed POST URL and fields
url = "https://s3.fr-par.scw.cloud/bib-platform-prod-data"
fields = {
    "key": "response/ituran/SoH-1.csv",
    "acl": "private",
    "policy": "eyJleHBpcmF0aW9uIjogIjIwMjQtMTEtMjlUMDk6MjA6MDNaIiwgImNvbmRpdGlvbnMiOiBbeyJidWNrZXQiOiAiYmliLXBsYXRmb3JtLXByb2QtZGF0YSJ9LCBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAicmVzcG9uc2UvaXR1cmFuLyJdLCB7IngtYW16LWFsZ29yaXRobSI6ICJBV1M0LUhNQUMtU0hBMjU2In0sIHsieC1hbXotY3JlZGVudGlhbCI6ICJTQ1c5UDZRMVQyNkYySkdTQzFBUy8yMDI0MTEyOS9mci1wYXIvczMvYXdzNF9yZXF1ZXN0In0sIHsieC1hbXotZGF0ZSI6ICIyMDI0MTEyOVQwODIwMDNaIn1dfQ==",
    "x-amz-signature": "828663af0307e1d848be94b9e4ddf856f46ebf8a8026ee5d093557be91f06151",
    "x-amz-algorithm": "AWS4-HMAC-SHA256",
    "x-amz-credential": "SCW9P6Q1T26F2JGSC1AS/20241129/fr-par/s3/aws4_request",
    "x-amz-date": "20241129T082003Z",
}

# File to upload
files = {'file': open('src/ingestion/ituran/SoH-1.csv', 'rb')}

# Upload the file
response = requests.post(url, data=fields, files=files)

if response.status_code == 204:
    print("Upload successful!")
else:
    print("Upload failed:", response.text)
