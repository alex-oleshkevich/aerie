migration_id = '202011250213'


def upgrade(builder):
    with builder.create_table('profiles') as table:
        table.string('github', null=True, default='')
        table.index('github', name='github_idx', unique=False)
        # table.unique_index('github')
        table.foreign_key('users', on='user_id', references='users.id')
        table.reference('users')  # add user_id and add foreign key
        table.increments()
        table.timestamps()
        table.rename('user_profiles')

    with builder.alter_table('users') as table:
        table.change('username', length=512)
        table.drop('')

    builder.sql('select 1 as one')


def downgrade(builder):
    pass
