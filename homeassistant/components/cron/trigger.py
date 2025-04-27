"""Offer cron automation rules."""

from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from typing import TYPE_CHECKING, Any

import croniter
import voluptuous as vol

from homeassistant.const import CONF_PLATFORM
from homeassistant.core import (
    CALLBACK_TYPE,
    HassJob,
    HassJobType,
    HomeAssistant,
    callback,
)
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.event import async_track_point_in_utc_time
from homeassistant.helpers.trigger import TriggerActionType, TriggerInfo
from homeassistant.helpers.typing import ConfigType
from homeassistant.util import dt as dt_util

_LOGGER = logging.getLogger(__name__)

TRIGGER_SCHEMA = vol.All(
    cv.TRIGGER_BASE_SCHEMA.extend(
        {
            vol.Required(CONF_PLATFORM): "cron",
            vol.Required("pattern"): str,
        }
    ),
)


async def async_validate_trigger_config(
    hass: HomeAssistant, config: ConfigType
) -> ConfigType:
    """Validate config."""
    validated = TRIGGER_SCHEMA(config)
    pattern = validated["pattern"]
    if not croniter.croniter.is_valid(pattern):
        raise HomeAssistantError(f"Failed to validate {pattern} as cron pattern")
    return validated


@dataclass(slots=True)
class _TrackCronIterator:
    hass: HomeAssistant
    job: HassJob[[datetime], Coroutine[Any, Any, None] | None]
    listener_job_name: str
    iterator: croniter.croniter
    _pattern_time_change_listener_job: HassJob[[datetime], None] | None = None
    _cancel_callback: CALLBACK_TYPE | None = None

    def async_attach(self) -> None:
        """Initialize track job."""
        self._pattern_time_change_listener_job = HassJob(
            self._pattern_time_change_listener,
            self.listener_job_name,
            job_type=HassJobType.Callback,
        )
        self._cancel_callback = async_track_point_in_utc_time(
            self.hass,
            self._pattern_time_change_listener_job,
            self._calculate_next(dt_util.utcnow()),
        )

    def _calculate_next(self, start: datetime) -> datetime:
        return self.iterator.get_next(datetime, start)  # type: ignore[return-value]

    @callback
    def _pattern_time_change_listener(self, _: datetime) -> None:
        """Listen for matching time_changed events."""
        hass = self.hass
        # Fetch time again because we want the actual time, not the
        # time when the timer was scheduled
        utc_now = dt_util.utcnow()
        localized_now = dt_util.as_local(utc_now)
        if TYPE_CHECKING:
            assert self._pattern_time_change_listener_job is not None
        self._cancel_callback = async_track_point_in_utc_time(
            hass,
            self._pattern_time_change_listener_job,
            self._calculate_next(utc_now + timedelta(seconds=1)),
        )
        hass.async_run_hass_job(self.job, localized_now, background=True)

    @callback
    def async_cancel(self) -> None:
        """Cancel the call_at."""
        if TYPE_CHECKING:
            assert self._cancel_callback is not None
        self._cancel_callback()


async def async_attach_trigger(
    hass: HomeAssistant,
    config: ConfigType,
    action: TriggerActionType,
    trigger_info: TriggerInfo,
) -> CALLBACK_TYPE:
    """Attach trigger of specified platform."""
    trigger_data = trigger_info["trigger_data"]
    job = HassJob(action, f"cron trigger {trigger_info}")

    @callback
    def cron_automation_listener(now: datetime) -> None:
        """Listen for time changes and calls action."""
        hass.async_run_hass_job(
            job,
            {
                "trigger": {
                    **trigger_data,
                    "platform": "cron",
                    "now": now,
                    "description": "cron",
                }
            },
        )

    pattern = config["pattern"]
    pattern_iter = croniter.croniter(pattern)
    track = _TrackCronIterator(
        hass,
        HassJob(cron_automation_listener, f"Cron job for {pattern}"),
        f"Cron trigger job for '{pattern}'",
        pattern_iter,
    )
    track.async_attach()
    return track.async_cancel
