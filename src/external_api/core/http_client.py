import aiohttp


class HttpClient:
    session: aiohttp.ClientSession | None = None

    def start(self):
        self.session = aiohttp.ClientSession()

    async def stop(self):
        if self.session:
            await self.session.close()
        self.session = None

    def __call__(self) -> aiohttp.ClientSession:
        if self.session is None:
            self.start()

        return self.session  # type: ignore[invalid-return-type]


HTTP_CLIENT = HttpClient()
