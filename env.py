from sqlalchemy import *

engine = create_engine('postgresql://postgres:th32s7@localhost/dvdrental2', echo=True)
metadata = MetaData(bind=engine)

def upgrade():
    pass

def downgrade():
    pass