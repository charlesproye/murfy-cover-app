import os
import base64
import subprocess
import json
import time
import requests
from typing import Dict, Optional
from pathlib import Path

class OktaTokenGenerator:
    def __init__(self, kid: str, aud: str, client: str, scope: str, pwd: str):
        self.kid = kid
        self.aud = aud
        self.client = client
        self.scope = scope
        self.pwd = pwd
        self.temp_files = []
        self.pkcs12_file = "src/pkcs12.txt"
        
        # Verify pkcs12.txt exists
        if not os.path.exists(self.pkcs12_file):
            raise FileNotFoundError(f"Required file {self.pkcs12_file} not found in current directory")

    def __del__(self):
        """Clean up temporary files"""
        for file in self.temp_files:
            if os.path.exists(file):
                os.remove(file)

    def _run_openssl_command(self, command: str) -> None:
        """Execute an OpenSSL command"""
        try:
            subprocess.run(command, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            raise Exception(f"OpenSSL command failed: {e}")

    def _create_temp_file(self, content: str, suffix: str) -> str:
        """Create a temporary file with the given content"""
        filename = f"temp_{suffix}"
        with open(filename, 'w') as f:
            f.write(content)
        self.temp_files.append(filename)
        return filename

    def _read_file(self, filename: str) -> str:
        """Read content from a file"""
        with open(filename, 'r') as f:
            return f.read().strip()

    def _base64_encode(self, data: str) -> str:
        """Base64 encode a string"""
        return base64.b64encode(data.encode()).decode().rstrip('=')

    def _url_safe_base64(self, data: str) -> str:
        """Convert base64 to URL-safe base64"""
        return data.replace('+', '-').replace('/', '_')

    def generate_token(self) -> Dict:
        """Generate Okta token using PKCS12 certificate"""
        try:
            # Step 1: Convert PKCS12 to private key
            print(f"Processing PKCS12 file: {self.pkcs12_file}")
            self._run_openssl_command(f"openssl base64 -d -a -in {self.pkcs12_file} -out keystore.p12")
            self._run_openssl_command(f"openssl pkcs12 -in keystore.p12 -clcerts -nodes -nocerts -passin pass:{self.pwd} -out private.key")
            self.temp_files.extend(['keystore.p12', 'private.key'])

            # Step 2: Create JWT header
            header = json.dumps({"alg": "RS256", "kid": self.kid})
            header_b64 = self._base64_encode(header)
            header_file = self._create_temp_file(header_b64, "header.b64")

            # Step 3: Create JWT payload
            current_time = int(time.time())
            payload = json.dumps({
                "aud": self.aud,
                "exp": current_time + 300,
                "iss": self.client,
                "sub": self.client,
                "iat": current_time,
                "jti": current_time + 3600
            })
            payload_b64 = self._base64_encode(payload)
            payload_file = self._create_temp_file(payload_b64, "payload.b64")

            # Step 4: Create unsigned JWT
            unsigned_jwt = f"{header_b64}.{payload_b64}"
            unsigned_file = self._create_temp_file(unsigned_jwt, "unsigned.b64")

            # Step 5: Sign the JWT
            self._run_openssl_command(f"openssl dgst -sha256 -sign private.key -out jwt_signature.bin {unsigned_file}")
            self.temp_files.append("jwt_signature.bin")

            # Step 6: Base64 encode the signature
            with open("jwt_signature.bin", "rb") as f:
                signature_b64 = base64.b64encode(f.read()).decode().rstrip('=')
            signature_url = self._url_safe_base64(signature_b64)

            # Step 7: Create final JWT
            final_jwt = f"{header_b64}.{payload_b64}.{signature_url}"

            # Step 8: Make the token request
            print("Requesting token from Okta...")
            response = requests.post(
                self.aud,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client,
                    "client_assertion": final_jwt,
                    "scope": self.scope,
                    "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )

            if response.status_code != 200:
                raise Exception(f"Token request failed: {response.text}")

            return response.json()

        except Exception as e:
            raise Exception(f"Token generation failed: {str(e)}")

def main():
    # Configuration
    config = {
        "kid": "2026-12-17-3279460",
        "aud": "https://sso.renault.com/oauth2/aus133y6mks4ptDss417/v1/token",
        "client": "irn-79947_ope_pkjwt_wgm4dlo1vhng",
        "scope": "apis.default",
        "pwd": "WJ1SXNpp3Cgz"
    }

    try:
        print("Initializing Okta token generator...")
        generator = OktaTokenGenerator(**config)
        token_data = generator.generate_token()
        print("\nToken generated successfully:")
        print(json.dumps(token_data, indent=2))
    except Exception as e:
        print(f"\nError: {str(e)}")

if __name__ == "__main__":
    main() 
