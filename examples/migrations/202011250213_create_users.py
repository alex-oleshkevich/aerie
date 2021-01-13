from aerie.schema.builder import SchemaBuilder

migration_id = '202011250213'


def upgrade(builder: SchemaBuilder):
    with builder.create_table('users') as users:
        users.increments()
        users.string('email', index=True, primary_key=True)
        users.string('first_name', null=True)
        users.string('last_name', null=True)
        users.string('bio', default='cool boy')
        users.timestamps()
        users.add_index(['first_name', 'last_name'], sort={
            'first_name': 'desc nulls last',
            'last_name': 'asc nulls last',
        })


def downgrade(builder: SchemaBuilder):
    builder.drop_table('users')
