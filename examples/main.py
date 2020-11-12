#!/usr/bin/env python
import asyncio

from aerie import Database


async def main():
    db = Database('sqlite:///tmp/example.db')

    async with db:
        await db.execute(
            'create table if not exists users ('
            'id integer primary key, '
            'username varchar(256), '
            'email varchar(256) )'
        )
        result = await db.execute(
            'insert into users (username, email) values ("alex", "alex@")'
        )
        print(result)


asyncio.run(main())
