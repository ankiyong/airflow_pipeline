

from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from schemas import ResponseModel,OrderSchema
from crud import get_orders,get_order_after_last_value
import database


app = FastAPI()

def get_db():
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/orders/id/{last_value}", response_model=list[OrderSchema])
def read_orders_last_value(last_value: int,db: Session=Depends(get_db)):
    return get_order_after_last_value(db,last_value)