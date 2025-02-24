
from sqlalchemy.orm import Session
from models import Order
from schemas import OrderSchema
from fastapi import HTTPException
from datetime import datetime
from sqlalchemy import and_

def get_order_after_last_value(db: Session, cur_value: int, last_value: int):
    orders = db.query(Order).filter(and_(Order.id > cur_value, Order.id <= last_value)).all()
    if not orders:
        raise HTTPException(status_code=404, detail="Order not found")
    return [OrderSchema.model_validate(order) for order in orders] 

def get_orders(db: Session):
    orders = db.query(Order).all()
    return [OrderSchema.model_validate(order) for order in orders] 