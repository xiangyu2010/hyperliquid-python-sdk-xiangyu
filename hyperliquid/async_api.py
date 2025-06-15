import json
import logging
from json import JSONDecodeError

import httpx

from hyperliquid.utils.constants import MAINNET_API_URL
from hyperliquid.utils.error import ClientError, ServerError
from hyperliquid.utils.types import Any


class AsyncAPI:
    def __init__(self, base_url=None):
        self.base_url = base_url or MAINNET_API_URL
        self._logger = logging.getLogger(__name__)
        self._client = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(headers={"Content-Type": "application/json"})
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def post(self, url_path: str, payload: Any = None) -> Any:
        payload = payload or {}
        url = self.base_url + url_path

        if not self._client:
            async with httpx.AsyncClient(headers={"Content-Type": "application/json"}) as client:
                response = await client.post(url, json=payload)
                return await self._process_response(response)

        response = await self._client.post(url, json=payload)
        return await self._process_response(response)

    async def _process_response(self, response):
        self._handle_exception(response)
        try:
            return response.json()
        except ValueError:
            return {"error": f"Could not parse JSON: {response.text}"}

    def _handle_exception(self, response):
        status_code = response.status_code
        if status_code < 400:
            return
        if 400 <= status_code < 500:
            try:
                err = json.loads(response.text)
            except JSONDecodeError:
                raise ClientError(status_code, None, response.text, None, response.headers)
            if err is None:
                raise ClientError(status_code, None, response.text, None, response.headers)
            error_data = err.get("data")
            raise ClientError(status_code, err["code"], err["msg"], response.headers, error_data)
        raise ServerError(status_code, response.text)


if __name__ == '__main__':
    import asyncio
    async def main():
        async with AsyncAPI() as api:
            payload={"type":"referral","user":"0x02e59af6d9fbf6bf0490dd9de78909027d7878ec"}
            print(await api.post('/info', payload=payload))

    asyncio.run(main())