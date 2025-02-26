from pydantic import BaseModel
from typing import Optional, List
from pydantic import BaseModel
from models import Order  # 실제 프로젝트의 모델 import
from datetime import datetime

class OrderSchema(BaseModel):
    order_id: str
    customer_id: str
    order_status: str
    order_purchase_timestamp: datetime
    order_approved_at: Optional[str] = None
    order_delivered_carrier_date: Optional[str] = None
    order_delivered_customer_date: Optional[str] = None
    order_estimated_delivery_date: Optional[str] = None
    id: int

    class Config:
        from_attributes = True

class ResponseModel(BaseModel):
    orders: List[OrderSchema]
