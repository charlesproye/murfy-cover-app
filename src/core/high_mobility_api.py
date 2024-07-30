import requests
import json
from urllib.parse import urlencode, quote
from datetime import datetime

class HMApi():
    """
    Represents an instance of the HM API with a base URL (different in sandbox
    and prod), client id and client secret.
    """

    base_url = ""
    client_id = ""
    client_secret = ""

    __token = None
    __token_exp = float('inf')

    def __init__(self, base_url, client_id, client_secret):
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.__fetch_token()

    def __fetch_token(self):
        r = requests.post(
            f"{self.base_url}/v1/access_tokens",
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
        ).json()
        self.__token = r.get("access_token")
        timestamp = (datetime.utcnow() - datetime(1970, 1, 1)).total_seconds()
        expires_in = int(r.get("expires_in"))
        self.__token_exp = timestamp + expires_in

    def __get_token(self):
        timestamp = (datetime.utcnow() - datetime(1970, 1, 1)).total_seconds()
        if timestamp > self.__token_exp:
            self.__fetch_token()
        return self.__token

    def create_clearance(self, vehicles):
        """Creates a clearance with the HM API
    
        Arguments:
        vehicles -- the vehicle list to activate (taken from `parse_vins`)
        """
        token = self.__get_token()
        result = requests.post(
            f"{self.base_url}/v1/fleets/vehicles", 
            json={"vehicles":vehicles},
            headers={"Authorization": f"Bearer {token}"}
        )
        if result.status_code == requests.codes.ok:
            return 0, result.json()
        else:
            return 1, result.json()

    def list_clearances(self, status=None, brand=None):
        """Lists clearances of vehicles activated through the HM API
    
        Arguments:
        status -- the status filter
        brand -- the brand filter
        """
        filter = {}
        if status: 
            filter["status"] = {"operator": "eq", "value": status}
        if brand:
            filter["brand"] = {"operator": "eq", "value": brand}
        if filter:
            encoded_filter = urlencode({"filter":filter}, quote_via=quote).replace('%27', '%22')
            token = self.__get_token()
            result = requests.get(
                f"{self.base_url}/v1/fleets/vehicles?{encoded_filter}",
                headers={"Authorization": f"Bearer {token}"}
            )
            if result.status_code == requests.codes.ok:
                return 0, [{"vin": vehicle["vin"], "brand": vehicle["brand"], "status": vehicle["status"]} for vehicle in result.json()]
            else:
                return 1, result.json()
        token = self.__get_token()
        result = requests.get(
            f"{self.base_url}/v1/fleets/vehicles",
            headers={"Authorization": f"Bearer {token}"}
        )
        if result.status_code == requests.codes.ok:
            return 0, [{"vin": vehicle["vin"], "brand": vehicle["brand"], "status": vehicle["status"]} for vehicle in result.json()]
        else:
            return 1, result.json()

    def get_clearance(self, vin):
        """Get the clearance for a single vehicle
    
        Arguments:
        vin --- The VIN of the vehicle
        """
        token = self.__get_token()
        result = requests.get(
            f"{self.base_url}/v1/fleets/vehicles/{vin}",
            headers={"Authorization": f"Bearer {token}"}
        )
        if result.status_code == requests.codes.ok:
            res = result.json()
            return 0, {"vin": res["vin"], "brand": res["brand"], "status": res["status"]}
        else:
            return 1, result.json()
            
    def delete_clearance(self, vin):
        """Delete the clearance for a single vehicle
    
        Arguments:
        vin --- The VIN of the vehicle
        """
        token = self.__get_token()
        result = requests.delete(
            f"{self.base_url}/v1/fleets/vehicles/{vin}",
            headers={"Authorization": f"Bearer {token}"}
        )
        if result.status_code == requests.codes.ok:
            res = result.json()
            return 0, {"vin": res["vin"], "brand": res["brand"], "status": res["status"]}
        else:
            return 1, result.json()

    def get_vehicle_info(self, vin):
        """Get a vehicle's info

        Arguments:
        vin --- The VIN of the vehicle
        """
        token = self.__get_token()
        result = requests.get(
            f"{self.base_url}/v1/vehicle-data/autoapi-13/{vin}",
            headers={"Authorization": f"Bearer {token}"}
        )
        return (result.status_code != requests.codes.ok), result.json()

    def save_vehicle_info(self, vin):
        """Save a vechicle's info in the correct s3 bucket

        Arguments:
        vin --- The VIN of the vehicle
        """
        error, info = self.get_vehicle_info(vin)
        if error:
            return error
        else:
            load_dotenv()
            brand = obj["brand"]
            key = obj["vin"]
            s3 = boto3.client("s3",
                              region_name="fr-par",
                              endpoint_url=f"https://{os.getenv('S3_BUCKET') or ''}.s3.fr-par.scw.cloud",
                              aws_access_key_id=os.getenv("S3_KEY") or "",
                              aws_secret_access_key=os.getenv("S3_SECRET") or "")
            s3.put_object(
                Body=bytes(json.dumps(obj).encode("UTF-8")),
                Bucket=brand,
                Key=f"response/{key}-{time.time()}.json"
            )
            return 0

