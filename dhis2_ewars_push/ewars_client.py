import requests
import json
import pandas as pd
import re
from openhexa.sdk import current_run


class EWARSClient:
    def __init__(self, user: str, client: str, password: str, aid: int, base_url: str = "https://dse.ewars.ws/api/tp"):
        self.base_url = base_url
        self.user = user
        self.client = client
        self.password = password
        self.aid = aid
        self.token = None
        self.headers = {}

        self.authenticate()

    def authenticate(self):
        """Authenticate and retrieve the token."""
        end_point = "login"
        payload = {"username": self.user, "client": self.client, "password": self.password}

        response = requests.request("POST", url=f"{self.base_url}/{end_point}", data=json.dumps(payload))
        if response.status_code == 200:
            data = response.json().get("data", None)
            if data:
                self.token = data.get("access_token")
                self.headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
                print(f"Connected to {self.base_url}")
        else:
            raise Exception(f"Authentication failed: {response.text}")

    def _get(self, endpoint: str, params: dict):
        """Perform a GET request with authentication."""
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.get(url=url, headers=self.headers, params=params)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            raise Exception(f"GET request '/{endpoint}' failed: {e}")

    def _post(self, endpoint: str, payload: dict):
        """Perform a POST request with authentication."""
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.post(url=url, data=json.dumps(payload), headers=self.headers)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            raise Exception(f"POST request '/{endpoint}' failed: {e}")

    def clean_strings(self, input_list):
        """Remove 'Title_Table.' and '.' from each string in the list."""
        return [re.sub(r"\s+", "_", x.replace("Title_Table.", "").replace(".", "_")).lower() for x in input_list]

    def get_locations(self):
        """Retrieve all locations from EWARS"""
        if not self.token:
            raise Exception("Cannot make request: Authentication required.")

        payload = {"aid": self.aid}
        try:
            response = self._post(endpoint="get_locations", payload=payload)
            resp_results = response.json().get("results", [])
            if len(resp_results) == 0:
                return []
            result_df = pd.json_normalize(resp_results)
            result_df.columns = self.clean_strings(input_list=result_df.columns)
            return result_df
        except Exception as e:
            print(f"Error while retrieving locations: {e}")
            return []

    def get_locations_metadata(self):
        """Retrieve all locations metadata from EWARS"""
        if not self.token:
            raise Exception("Cannot make request: Authentication required.")

        payload = {"aid": self.aid}
        try:
            response = self._post(endpoint="get_location_metadata", payload=payload)
            resp_results = response.json().get("results", [])
            if len(resp_results) == 0:
                return []
            result_df = pd.json_normalize(resp_results)
            result_df.columns = self.clean_strings(input_list=result_df.columns)
            return result_df
        except Exception as e:
            print(f"Error while retrieving locations metadata: {e}")
            return []

    def get_forms(self):
        """Retrieve all locations metadata from EWARS"""
        if not self.token:
            raise Exception("Cannot make request: Authentication required.")

        payload = {"aid": self.aid}
        try:
            response = self._post(endpoint="get_forms_id_name", payload=payload)
            resp_results = response.json().get("results", [])
            if len(resp_results) == 0:
                return []
            results_df = pd.json_normalize(resp_results)
            results_df.columns = self.clean_strings(input_list=results_df.columns)
            return results_df
        except Exception as e:
            print(f"Error while retrieving forms: {e}")
            return []

    def get_form_config(self, form_id: int):
        """Retrieve all locations metadata from EWARS"""
        if not self.token:
            raise Exception("Cannot make request: Authentication required.")
        if not form_id:
            raise Exception("The form_id is required!.")

        payload = {
            "aid": self.aid,
            "form_id": form_id,
        }
        try:
            response = self.form_config = self._post(endpoint="get_form_config", payload=payload)
            resp_results = response.json().get("results", [])
            if len(resp_results) == 0:
                return []
            results_df = pd.DataFrame.from_dict(resp_results, orient="index").reset_index()
            results_df.columns = ["indicator_key", "label", "type"]
            results_df["indicator_key"] = self.clean_strings(input_list=results_df["indicator_key"])
            return results_df
        except Exception as e:
            print(f"Error while retrieving forms: {e}")
            return []

    def get_reports_for_date(self, form_id: int, date: str):
        """Retrieve all locations metadata from EWARS"""
        if not self.token:
            raise Exception("Cannot make request: Authentication required.")
        if not form_id:
            raise Exception("The form_id is required!.")
        if not date:
            raise Exception("The date is required!.")

        params = {
            "aid": self.aid,
            "username": self.user,
            "password": self.password,
            "from_date": date,  # Only one date (same) to avoid time outs..
            "to_date": date,
            "form_id": form_id,
            "get_data_with_labels": False,
            "is_sub_form": False,
        }
        try:
            response = self._get(endpoint="reports", params=params)
            resp_value = response.json().get("value", None)
            if not resp_value:
                return pd.DataFrame()
            reports_df = pd.json_normalize(resp_value)
            "data"
            reports_df.columns = self.clean_strings(input_list=reports_df.columns)
            reports_df.columns = reports_df.columns.str.replace("^data_", "", regex=True)  # remove extra "data_"
            return reports_df
        except Exception as e:
            current_run.log_info(f"Error while retrieving forms: {e}")
            return pd.DataFrame()
