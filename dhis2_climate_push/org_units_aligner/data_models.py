import json
from dataclasses import dataclass

import pandas as pd
import polars as pl
from pydantic import BaseModel, ConfigDict, field_validator, model_validator
from pydantic.alias_generators import to_camel


@dataclass
class DataPointModel:
    """Data model representing a DHIS2 data point.

    Attributes
    ----------
    dataElement : str
        The unique identifier for the data element.
    period : str
        The reporting period for the data point.
    orgUnit : str
        The organizational unit associated with the data point.
    categoryOptionCombo : str
        The category option combination identifier.
    attributeOptionCombo : str
        The attribute option combination identifier.
    value : float
        The value of the data point.
    """

    dataElement: str  # noqa: N815
    period: str
    orgUnit: str  # noqa: N815
    categoryOptionCombo: str  # noqa: N815
    attributeOptionCombo: str  # noqa: N815
    value: str

    def to_json(self) -> dict:
        """Return a dictionary representation of the data point suitable for DHIS2 JSON format.

        Returns
        -------
        dict
            A dictionary with keys corresponding to DHIS2 data value fields.
        """
        if self.value is None or (isinstance(self.value, str) and not self.value.strip()):
            return {
                "dataElement": self.dataElement,
                "period": self.period,
                "orgUnit": self.orgUnit,
                "categoryOptionCombo": self.categoryOptionCombo,
                "attributeOptionCombo": self.attributeOptionCombo,
                "value": "",
                "comment": "deleted value",
            }

        return {
            "dataElement": self.dataElement,
            "period": self.period,
            "orgUnit": self.orgUnit,
            "categoryOptionCombo": self.categoryOptionCombo,
            "attributeOptionCombo": self.attributeOptionCombo,
            "value": self.value,
        }

    def __str__(self) -> str:
        return (
            f"DataPointModel("
            f"dataElement={self.dataElement}, "
            f"period={self.period}, "
            f"orgUnit={self.orgUnit}, "
            f"categoryOptionCombo={self.categoryOptionCombo}, "
            f"attributeOptionCombo={self.attributeOptionCombo}, "
            f"value={self.value})"
        )


class OrgUnit(BaseModel):
    """Represents an Organisation Unit (OrgUnit) in DHIS2."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        alias_generator=to_camel,
        populate_by_name=True,
    )

    id: str | None = None
    name: str | None = None
    short_name: str | None = None
    opening_date: str | None = None
    closed_date: str | None = None
    parent: dict | None = None
    level: int | None = None
    path: str | None = None
    geometry: dict | None = None

    @field_validator("geometry", mode="before")
    @classmethod
    def parse_geometry(cls, v: str | dict | None) -> dict | None:
        """Parses the geometry field, which can be a JSON string or a dictionary.

        Returns:
            dict | None: A dictionary representing the geometry, or None if not provided.
        """
        if isinstance(v, str):
            return json.loads(v)
        return v

    @model_validator(mode="before")
    @classmethod
    def from_series_or_tuple(cls, v: pd.Series | pl.Series | tuple | dict) -> dict:
        """Converts input Polars Series or a named tuple to a dictionary before validation.

        Returns:
            Any: A dictionary representation of the input, or the input itself if it's not a Series or named tuple.
        """
        if isinstance(v, pd.Series):
            return v.to_dict()
        if isinstance(v, tuple) and hasattr(v, "_fields"):
            return v._asdict()
        return v

    def is_valid(self) -> bool:
        """Checks if the essential fields for an OrgUnit are present.

        Returns:
            bool: True if the OrgUnit has all required fields, False otherwise.
        """
        return all([self.id, self.name, self.short_name, self.opening_date, self.parent])

    def to_json(self) -> dict:
        """Converts the OrgUnit instance to a JSON-serializable dictionary (DHIS2).

        Returns:
            dict: A DHIS2 representation of the OrgUnit instance.
        """
        json_dict = {
            "id": self.id,
            "name": self.name,
            "shortName": self.short_name,
            "openingDate": self.opening_date,
            "closedDate": self.closed_date,
            "parent": {"id": self.parent.get("id")} if self.parent else None,
        }
        if self.geometry:
            json_dict["geometry"] = {
                "type": self.geometry["type"],
                "coordinates": self.geometry["coordinates"],
            }
        return {k: v for k, v in json_dict.items() if v is not None}

    def __str__(self) -> str:
        return (
            f"OrgUnit(id={self.id}, name={self.name}, short_name={self.short_name}, "
            f"opening_date={self.opening_date}, closed_date={self.closed_date}, "
            f"parent={self.parent}, level={self.level}, path={self.path}, "
            f"geometry={'<set>' if self.geometry else None})"
        )
