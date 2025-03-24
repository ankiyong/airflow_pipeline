from sqlalchemy.orm import Session
from models import Order
from schemas import OrderSchema
from fastapi import HTTPException
from datetime import datetime
from sqlalchemy import and_,desc
import time

def get_order_after_last_value(db: Session, last_value: int):
    orders = db.query(Order).filter(and_(Order.log_time <= datetime.now(), Order.log_time > time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_value)))).all()
    if not orders:
        raise HTTPException(status_code=404, detail="Order not found")
    return [OrderSchema.model_validate(order) for order in orders]

def get_last_value(db: Session):
    latest_order = db.query(Order).order_by(desc(Order.log_time)).first()
    if latest_order:
        return OrderSchema.model_validate(latest_order)
    return None