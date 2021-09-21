# import asyncio
# import os
#
# from aerie.database import Aerie
# from aerie.fields import BigIntegerField
# from aerie.models import Model
# from aerie.session import DbSession
#
# DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite:///database.sqlite')
#
#
# # @entity(pk=['id'], table_name='users', indexes=[
# #     Index(columns=['first_name', 'last_name'], unique=True),
# # ])
# # class User(Model):
# #     id = BigIntegerField()
# #     first_name = first_name
#
# # async def main():
# #     db = Aerie(DATABASE_URL)
# #     async with db.session() as session:
# #
#
#
# if __name__ == 'main':
#     asyncio.run(main())
