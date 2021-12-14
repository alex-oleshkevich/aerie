import asyncio
import os
import sqlalchemy as sa

from aerie import Aerie, Model

PAGE = int(os.environ.get('PAGE', 1))
PAGE_SIZE = int(os.environ.get('PAGE_SIZE', 20))
DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite+aiosqlite:///:memory:')


class User(Model):
    __tablename__ = 'users'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)

    def __str__(self) -> str:
        return self.name or 'n/a'


async def main() -> None:
    db = Aerie(DATABASE_URL)

    # create tables
    await db.schema.drop_tables()
    await db.schema.create_tables()

    # create some users
    async with db.session() as session:
        session.add_all([User(id=x + 1, name='User %s' % (x + 1)) for x in range(100)])
        await session.flush()

        stmt = session.select(User)
        page = await session.query(stmt).paginate(PAGE, PAGE_SIZE)

        print('Current page: %s' % page.page)
        print('Current page size: %s' % page.page_size)
        print('Total pages: %s' % page.total_pages)
        print('Total rows: %s' % page.total_rows)
        print('Has prev page: %s' % page.has_previous)
        print('Prev page number: %s' % page.previous_page)
        print('Has next page: %s' % page.has_next)
        print('Next page number: %s' % page.next_page)
        print(
            'Displaying rows: %s - %s'
            % (
                page.start_index,
                page.end_index,
            )
        )
        print('Rows %s:' % [r.name for r in page])


if __name__ == '__main__':
    asyncio.run(main())
