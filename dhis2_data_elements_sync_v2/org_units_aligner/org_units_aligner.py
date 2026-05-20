import json
import logging

import pandas as pd
import polars as pl
import requests
from openhexa.toolbox.dhis2 import DHIS2
from packaging.version import Version
from pydantic.alias_generators import to_camel

from .data_models import OrgUnit
from .exceptions import AlignerError
from .utils import log_message


class DHIS2PyramidAligner:
    """Align organisation units between two DHIS2 instances.

    This class is stateless and provides methods to synchronize organisation units
    from a source DHIS2 instance to a target DHIS2 instance. The alignment process
    compares the pyramids of both instances and performs the necessary operations
    to keep the target up to date with the source.

    Supported operations include:
    - Creating organisation units that exist in the source but not in the target.
    - Updating organisation units that exist in both but differ in their attributes.

    This class does not store any state between calls; all data must be provided
    as method parameters.
    """

    def __init__(
        self,
        logger: logging.Logger | None = None,
        required_columns: set[str] | None = None,
        logging_interval: int = 2000,
    ):
        self.required_columns = (
            required_columns if required_columns is not None else {"id", "name", "shortName", "openingDate", "parent"}
        )
        self.logger = logger or logging.getLogger(__name__)
        self.logging_interval = logging_interval
        self._reset_summary()
        self.log_function = log_message

    def _reset_summary(self) -> None:
        """Reset the summary of operations performed during the alignment process."""
        self.summary = {
            "created": 0,
            "updated": 0,
            "errors": {
                "create": [],
                "update": [],
            },
        }

    def _log_message(self, message: str, level: str = "info", log_current_run: bool = True, error_details: str = ""):
        """Log a message using the configured logging function."""
        self.log_function(
            logger=self.logger,
            message=message,
            error_details=error_details,
            level=level,
            log_current_run=log_current_run,
            exception_class=AlignerError,
        )

    def _normalize_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize DataFrame column names to camelCase.

        Parameters
        ----------
        df : pl.DataFrame
            The DataFrame to normalize.

        Returns
        -------
        pl.DataFrame
            The DataFrame with column names normalized to camelCase.
        """
        return df.rename({col: to_camel(col) for col in df.columns})

    def _validate_columns(self, source_pyramid: pl.DataFrame) -> None:
        """Validate that the source pyramid contains all required columns."""
        missing_columns = self.required_columns - set(source_pyramid.columns)
        if missing_columns:
            raise AlignerError(
                f"Source pyramid is missing required columns: {missing_columns}"
                "Columns can be in camelCase (e.g. 'shortName') or snake_case (e.g. 'short_name')."
            )

    def align_to(
        self,
        target_dhis2: DHIS2,
        source_pyramid: pl.DataFrame | pd.DataFrame = None,
    ):
        """Syncs the extracted pyramid data with the target DHIS2 instance."""
        # Load the target pyramid
        if source_pyramid is None or source_pyramid.shape[0] == 0:
            self._log_message("Source pyramid is empty. Organisation units alignment skipped.", level="warning")
            return

        if isinstance(source_pyramid, pd.DataFrame):
            source_pyramid = pl.from_pandas(source_pyramid)

        source_pyramid = self._normalize_columns(source_pyramid)
        self._validate_columns(source_pyramid)
        self._reset_summary()

        self._log_message(f"Source org units df: {source_pyramid.shape}", level="info", log_current_run=False)
        self._log_message(f"Retrieving organisation units from target DHIS2: {target_dhis2.api.url}")

        # Retrieve full pyramid from target DHIS2
        target_pyramid = target_dhis2.meta.organisation_units(
            fields="id,name,shortName,openingDate,closedDate,parent,level,path,geometry"
        )
        target_pyramid = pl.DataFrame(target_pyramid)
        self._log_message(f"Target org units df: {target_pyramid.shape}", level="info", log_current_run=False)

        # NOTE: Geometry is valid for versions > 2.32
        if Version(target_dhis2.version) <= Version("2.32"):
            source_pyramid = source_pyramid.with_columns(pl.lit(None).alias("geometry"))
            target_pyramid = target_pyramid.with_columns(pl.lit(None).alias("geometry"))
            self._log_message("DHIS2 version not compatible with geometry. Geometry set to None.", level="warning")

        # Create new OU in target DHIS2
        pyramid_ou_new = source_pyramid.filter(~pl.col("id").is_in(target_pyramid["id"]))
        self._push_org_units_create(
            ou_to_create=pyramid_ou_new,
            target_dhis2=target_dhis2,
        )

        # Select OU uid that match between DHIS2 source and target (to check for updates)
        matching_ou_ids = source_pyramid.filter(pl.col("id").is_in(target_pyramid["id"]))["id"].to_list()
        self._push_org_units_update(
            org_unit_source=source_pyramid,
            org_unit_target=target_pyramid,
            ou_to_check=matching_ou_ids,
            target_dhis2=target_dhis2,
        )
        self._log_message("Organisation units alignment finished.")

    def _push_org_units_create(
        self,
        ou_to_create: pl.DataFrame,
        target_dhis2: DHIS2,
    ) -> None:
        """Create organisation units in the target DHIS2 instance.

        Parameters
        ----------
        ou_to_create : pl.DataFrame
            DataFrame containing organisation unit data to be created.
        target_dhis2 : DHIS2
            DHIS2 client for the target instance.

        This function iterates over the organisation units, validates them, and
        attempts to create them in the target DHIS2.
        Logs errors and information about the creation process.
        """
        if ou_to_create.shape[0] == 0:
            self._log_message("No new organisation units to create.")
            return

        self._log_message(f"Creating {ou_to_create.shape[0]} organisation units.")
        for row_tuple in ou_to_create.iter_rows(named=True):
            try:
                ou = OrgUnit(**row_tuple)
                if ou.is_valid():
                    response = self._push_org_unit(
                        dhis2_client=target_dhis2,
                        org_unit=ou,
                        strategy="CREATE",
                    )
                    self._handle_response(response=response, ou=ou, error_type="create")
                else:
                    self._log_and_append_error(
                        error_type="create",
                        ou=ou,
                        error_msg="Organisation unit is not valid and cannot be created.",
                        level="error",
                        log_current_run=False,
                    )

            except Exception as e:
                raise AlignerError(f"Unexpected error occurred while creating organisation units. Error: {e}") from e

        errors_count = len(self.summary["errors"]["create"])
        if errors_count > 0:
            self._log_message(f"{errors_count} errors occurred during creation. Please check execution logs.")

    def _handle_response(self, response: requests.Response, ou: OrgUnit, error_type: str = "create") -> None:
        """Handle the response from the DHIS2 API after attempting to create or update an organisation unit."""
        if error_type not in ["create", "update"]:
            raise ValueError(f"Invalid error_type: {error_type}. Must be 'create' or 'update'.")

        if not response.ok:
            self._log_and_append_error(
                error_type=error_type,
                ou=ou,
                error_msg=(
                    f"HTTP {response.status_code} error while trying to {error_type} "
                    f"organisation unit {ou.id}: {response.text}"
                ),
                level="error",
                log_current_run=False,
            )
            return

        parsed_response = self._try_parse_json(response)
        if parsed_response is None:
            self._log_and_append_error(
                error_type=error_type,
                ou=ou,
                error_msg=(
                    f"Failed to parse response while trying to {error_type} "
                    f"organisation unit {ou.id}: {response.content}"
                ),
                level="error",
                log_current_run=False,
            )
            return

        status = parsed_response.get("status", "").upper()
        if status not in ["OK", "SUCCESS"]:
            self._log_and_append_error(
                error_type=error_type,
                ou=ou,
                error_msg=f"Failed to {error_type} organisation unit {ou.id}. Response: {parsed_response}",
                level="error",
                log_current_run=False,
            )
        else:
            self.summary[error_type] += 1
            self._log_message(f"Organisation unit {ou.name} ({ou.id}) {error_type}d successfully.")

    def _push_org_units_update(
        self,
        org_unit_source: pl.DataFrame,
        org_unit_target: pl.DataFrame,
        ou_to_check: list[str],
        target_dhis2: DHIS2,
    ):
        """Update org units based on matching id list."""
        if not ou_to_check:
            self._log_message("No organisation units to update.")
            return

        self._log_message(f"Checking for updates in {len(ou_to_check)} organisation units.")

        source_dict = {row["id"]: row for row in org_unit_source.to_dicts()}
        target_dict = {row["id"]: row for row in org_unit_target.to_dicts()}
        total = len(ou_to_check)

        for index, ou_id in enumerate(ou_to_check):
            try:
                ou_source = OrgUnit(**source_dict[ou_id])
                ou_target = OrgUnit(**target_dict[ou_id])

                if ou_source != ou_target:
                    response = self._push_org_unit(
                        dhis2_client=target_dhis2,
                        org_unit=ou_source,
                        strategy="UPDATE",
                    )
                    self._handle_response(response=response, ou=ou_source, error_type="update")
            except Exception as e:
                raise AlignerError(
                    f"Unexpected error occurred while updating organisation unit {ou_id}. Error: {e}"
                ) from e

            if index % self.logging_interval == 0 or index == total - 1:
                self._log_message(f"Organisation units checked: {index + 1}/{total}")

        self._log_message(f"Organisation units updated: {self.summary['updated']}")
        errors_count = len(self.summary["errors"]["update"])
        if errors_count > 0:
            self._log_message(f"{errors_count} errors occurred during update. Please check the execution logs.")

    def _push_org_unit(
        self,
        dhis2_client: DHIS2,
        org_unit: OrgUnit,
        strategy: str = "CREATE",
    ) -> requests.Response:
        """Pushes an organisation unit to the DHIS2 instance using the specified strategy.

        Parameters
        ----------
        dhis2_client : DHIS2
            DHIS2 client for the target instance.
        org_unit : OrgUnitObj
            Organisation unit object to be pushed.
        strategy : str, optional
            Strategy for pushing ('CREATE' or 'UPDATE'), by default "CREATE".

        Returns
        -------
        requests.Response
            Request response from the DHIS2 API.
        """
        if strategy == "CREATE":
            return dhis2_client.api.session.post(
                f"{dhis2_client.api.url}/organisationUnits",
                json=org_unit.to_json(),
            )
        if strategy == "UPDATE":
            return dhis2_client.api.session.put(
                f"{dhis2_client.api.url}/organisationUnits/{org_unit.id}",
                json=org_unit.to_json(),
            )
        raise ValueError(f"Unsupported strategy: {strategy}")

    def _try_parse_json(self, r: requests.Response) -> dict | None:
        """Safely parse the JSON response from a requests.Response object.

        Returns:
            dict: The parsed JSON response if successful, or None if parsing fails or if the response is None.
        """
        if r is None:
            return None

        try:
            return r.json()
        except (ValueError, json.JSONDecodeError):
            return None

    def _log_and_append_error(
        self,
        error_type: str,
        ou: OrgUnit,
        error_msg: str,
        level: str = "error",
        log_current_run: bool = False,
    ):
        """Helper function to log an error message and append it to the import summary."""
        error_dict = {"ou": str(ou), "error": error_msg}
        self.summary["errors"][error_type].append(error_dict)
        self._log_message(f"[{error_type}] {error_msg} ou={ou}", level=level, log_current_run=log_current_run)
