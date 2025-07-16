import os
import base64
import subprocess
import json
import time
import aiohttp
import asyncio
import logging
from typing import Dict, Optional, Tuple
from pathlib import Path
from enum import Enum
from core.env_utils import get_env_var

class RenaultAPIError(Exception):
    """Base exception for Renault API errors"""
    pass

class RenaultAPIErrorCode(Enum):
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    TOO_MANY_REQUESTS = 429
    INTERNAL_SERVER_ERROR = 500
    SERVICE_UNAVAILABLE = 503

    @classmethod
    def get_error_message(cls, code: int) -> str:
        messages = {
            cls.BAD_REQUEST.value: "Bad request - The request was malformed or invalid",
            cls.UNAUTHORIZED.value: "Invalid or missing authorization in header",
            cls.FORBIDDEN.value: "Forbidden - Access to the resource is denied",
            cls.NOT_FOUND.value: "The requested resource was not found",
            cls.TOO_MANY_REQUESTS.value: "Too many requests - Rate limit exceeded",
            cls.INTERNAL_SERVER_ERROR.value: "Internal server error - An error occurred on the server side",
            cls.SERVICE_UNAVAILABLE.value: "Service unavailable - The server is temporarily unable to service the request"
        }
        return messages.get(code, "Unknown error occurred")

class RenaultApi:
    def __init__(self, kid: str, aud: str, client: str, scope: str, pwd: str):
        self.kid = kid
        self.aud = aud
        self.client = client
        self.scope = scope
        self.pwd = pwd
        self.temp_files = []
        self.pkcs12_file = "src/activation/api/pkcs12.txt"
        self._token_data: Optional[Dict] = None
        self._token_expiry: float = 0
        
        # Model mapping dictionary
        self.model_mapping = {
            "megane e-tech": "megane e-tech",
            "zoe": "zoe"
        }
        
        # Type mapping dictionary
        self.type_mapping = {
            "r110": "r110",
            "r135": "r135"
        }
        
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

    async def _generate_token(self) -> Dict:
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

            # Step 3: Create JWT payload with unique jti (JWT ID)
            current_time = int(time.time())
            unique_jti = f"{current_time}_{int(time.time() * 1000) % 1000000}"
            payload = json.dumps({
                "aud": self.aud,
                "exp": current_time + 300,  # 5 minutes expiration
                "iss": self.client,
                "sub": self.client,
                "iat": current_time,
                "jti": unique_jti
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

            # Step 8: Make the token request asynchronously
            print("Requesting token from Okta...")
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.aud,
                    data={
                        "grant_type": "client_credentials",
                        "client_id": self.client,
                        "client_assertion": final_jwt,
                        "scope": self.scope,
                        "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
                    },
                    headers={"Content-Type": "application/x-www-form-urlencoded"}
                ) as response:
                    if response.status != 200:
                        text = await response.text()
                        raise Exception(f"Token request failed: {text}")
                    
                    return await response.json()

        except Exception as e:
            raise Exception(f"Token generation failed: {str(e)}")

    async def _get_token(self) -> str:
        """Get a valid access token, renewing if necessary"""
        current_time = time.time()
        
        if not self._token_data or current_time >= self._token_expiry - 60:
            self._token_data = await self._generate_token()
            self._token_expiry = current_time + self._token_data.get('expires_in', 3600)
        
        return self._token_data['access_token']

    async def get_vehicle_info(self, session: aiohttp.ClientSession, vin: str) -> Tuple[str, str, str, Optional[str]]:
        """
        Get vehicle information from Renault API using the provided VIN.
        
        Args:
            session (aiohttp.ClientSession): The aiohttp session to use for the request
            vin (str): The Vehicle Identification Number
            
        Returns:
            tuple: (model, version, type, start_date)
            Returns default values ("renault model unknown", "renault version unknown", "renault type unknown", None) if API call fails
        """
        try:
            url = f"{get_env_var('RENAULT_API_URL')}/vehicle-information/v1/vehicles/{vin}"
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    access_token = await self._get_token()
                    headers = {
                        "Authorization": f"Bearer {access_token}",
                        "accept": "application/json",
                        "apiKey": f"{get_env_var('RENAULT_API_KEY')}"
                    }
                    
                    async with session.get(url, headers=headers) as response:
                        if response.status == 200:
                            data = await response.json()
                            print(data)
                            model = data.get("model", "renault model unknown").lower()
                            type_and_version = data.get("version").lower()
                            
                            # Split type_and_version into version (first word) and type (remaining words)
                            parts = type_and_version.split(maxsplit=1)
                            version = parts[0] if parts else "renault version unknown"
                            type_ = parts[1] if len(parts) > 1 else "renault type unknown"
                            
                            # Check if type contains any of the mapped values
                            for key in self.type_mapping:
                                if key in type_:
                                    type_ = self.type_mapping[key]
                                    break
                            
                            # Map the model using the mapping dictionary
                            for key, value in self.model_mapping.items():
                                if key in model:
                                    model = value
                                    break
                                    
                            return (
                                model,
                                type_,
                                version,
                                data.get("firstRegistrationDate")
                            )
                            
                        error_code = response.status
                        error_message = RenaultAPIErrorCode.get_error_message(error_code)
                        
                        if error_code in [401, 429, 500, 503]:
                            if error_code == 401:
                                self._token_data = None
                            
                            if error_code == 429:
                                retry_after = int(response.headers.get('Retry-After', 60))
                                print(f"Rate limit exceeded. Waiting {retry_after} seconds before retry...")
                                await asyncio.sleep(retry_after)
                            else:
                                wait_time = min(2 ** retry_count, 60)
                                print(f"Retryable error occurred. Waiting {wait_time} seconds before retry...")
                                await asyncio.sleep(wait_time)
                            
                            retry_count += 1
                            continue
                        
                        if error_code in [400, 403, 404]:
                            raise RenaultAPIError(f"{error_message} (HTTP {error_code})")
                        
                        raise RenaultAPIError(f"Unexpected error: {error_message} (HTTP {error_code})")
                        
                except aiohttp.ClientError as e:
                    if retry_count < max_retries - 1:
                        retry_count += 1
                        wait_time = min(2 ** retry_count, 60)
                        print(f"Network error occurred. Waiting {wait_time} seconds before retry...")
                        await asyncio.sleep(wait_time)
                        continue
                    raise RenaultAPIError(f"Network error after {max_retries} retries: {str(e)}")
            
            raise RenaultAPIError(f"Request failed after {max_retries} retries")
            
        except Exception as e:
            print(f"Error getting vehicle info for VIN {vin}: {str(e)}")
            return (
                "renault model unknown",
                "renault version unknown",
                "renault type unknown",
                None
            )
    
    async def get_vehicle_wltp_range(self, session: aiohttp.ClientSession, vin: str) -> int:
        """
        Get the WLTP combined range for a vehicle using the provided VIN.
        
        Args:
            session (aiohttp.ClientSession): The aiohttp session to use for the request
            vin (str): The Vehicle Identification Number
            
        Returns:
            int: The WLTP combined range for the vehicle
            Returns 0 if API call fails
        """
        try:
            url = f"{get_env_var('RENAULT_API_URL')}/import-vehicle-info/v1/wltp/vin?vin={vin}"
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    access_token = await self._get_token()
                    headers = {
                        "Authorization": f"Bearer {access_token}",
                        "accept": "application/json",
                        "apiKey": get_env_var("RENAULT_API_KEY")
                    }
                    
                    async with session.get(url, headers=headers) as response:
                        if response.status == 200:
                            data = await response.json()
                            return data.get("wltpElecRangeCombined", 0)
                        else:
                            error_code = response.status
                            error_message = RenaultAPIErrorCode.get_error_message(error_code)
                            raise RenaultAPIError(f"Unexpected error: {error_message} (HTTP {error_code})")
                            
                except aiohttp.ClientError as e:
                    if retry_count < max_retries - 1:
                        retry_count += 1
                        wait_time = min(2 ** retry_count, 60)
                        print(f"Network error occurred. Waiting {wait_time} seconds before retry...")
                        await asyncio.sleep(wait_time)
                        continue
                    raise RenaultAPIError(f"Network error after {max_retries} retries: {str(e)}")
            
            raise RenaultAPIError(f"Request failed after {max_retries} retries")
            
        except Exception as e:
            print(f"Error getting vehicle WLTP range for VIN {vin}: {str(e)}")
            return None
        

