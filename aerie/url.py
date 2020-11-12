import typing as t
import urllib.parse


class URL:
    def __init__(self, url: str):
        self.url = url
        self._components: urllib.parse.ParseResult = urllib.parse.urlparse(url)

    @property
    def scheme(self) -> str:
        return self._components.scheme

    @property
    def driver(self) -> str:
        return self.scheme.split('+')[0]

    @property
    def library(self) -> t.Optional[str]:
        if '+' in self.scheme:
            return self.scheme.split('+')[1]
        return None

    @property
    def hostname(self) -> str:
        return self._components.hostname

    @property
    def port(self) -> int:
        return int(self._components.port)

    @property
    def username(self) -> t.Optional[str]:
        return self._components.username

    @property
    def password(self) -> t.Optional[str]:
        return self._components.password

    @property
    def db_name(self) -> str:
        if self.driver == 'sqlite':
            return self._components.path
        return self._components.path[1:]

    @property
    def options(self) -> dict:
        return dict(urllib.parse.parse_qsl(self._components.query))

    def __str__(self):
        credentials = ''
        if self.username:
            credentials = f'{self.username}:******@'

        components = self._components._replace(
            netloc=f'{credentials}{self.hostname}:{self.port}'
        )
        return urllib.parse.urlunparse(components)
