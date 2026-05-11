"""Config flow for Wyoming Satellite Plus integration."""

import logging
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.const import CONF_HOST, CONF_PORT
from homeassistant.helpers.service_info.zeroconf import ZeroconfServiceInfo

from .const import DOMAIN
from .data import WyomingService

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_HOST): str,
        vol.Required(CONF_PORT): int,
    }
)


class WyomingSatellitePlusConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Wyoming Satellite Plus integration."""

    VERSION = 1

    _service: WyomingService | None = None
    _name: str | None = None

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle the initial step."""
        if user_input is None:
            return self.async_show_form(
                step_id="user", data_schema=STEP_USER_DATA_SCHEMA
            )

        service = await WyomingService.create(
            user_input[CONF_HOST],
            user_input[CONF_PORT],
        )

        if service is None:
            return self.async_show_form(
                step_id="user",
                data_schema=STEP_USER_DATA_SCHEMA,
                errors={"base": "cannot_connect"},
            )

        if name := service.get_name():
            return self.async_create_entry(title=name, data=user_input)

        return self.async_abort(reason="no_services")

    async def async_step_zeroconf(
        self, discovery_info: ZeroconfServiceInfo
    ) -> ConfigFlowResult:
        """Handle zeroconf discovery."""
        _LOGGER.debug("Zeroconf discovery info: %s", discovery_info)
        if discovery_info.port is None:
            return self.async_abort(reason="no_port")

        service = await WyomingService.create(discovery_info.host, discovery_info.port)
        if (service is None) or (not (name := service.get_name())):
            return self.async_abort(reason="no_services")

        self._name = name

        unique_id = f"plus_{discovery_info.name}_{self._name}"
        await self.async_set_unique_id(unique_id)
        self._abort_if_unique_id_configured()

        self.context["title_placeholders"] = {"name": self._name}

        self._service = service
        return await self.async_step_zeroconf_confirm()

    async def async_step_zeroconf_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle a flow initiated by zeroconf."""
        assert self._service is not None
        assert self._name is not None

        if user_input is None:
            return self.async_show_form(
                step_id="zeroconf_confirm",
                description_placeholders={"name": self._name},
                errors={},
            )

        return self.async_create_entry(
            title=self._name,
            data={
                CONF_HOST: self._service.host,
                CONF_PORT: self._service.port,
            },
        )

    def _iter_entries(self, host: str, port: int):
        """Yield entries with matching host/port."""
        for entry in self._async_current_entries(include_ignore=True):
            if entry.data.get(CONF_HOST) == host and entry.data.get(CONF_PORT) == port:
                yield entry
