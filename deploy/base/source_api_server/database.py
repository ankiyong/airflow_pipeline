from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

# load .env
load_dotenv()

username = os.environ.get('USERNAME')
password = os.environ.get('PASSWORD')
host = os.environ.get('HOST')
port = os.environ.get('PORT')
db_name = os.environ.get('DB_NAME')


engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db_name}')
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
