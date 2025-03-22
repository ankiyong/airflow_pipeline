from pydantic import BaseModel
from typing import Optional,List
from pydantic import BaseModel
from models import Order
from datetime import datetime

class OrderSchema(BaseModel):
    log_time: datetime
    order_id: str
    customer_id: str
    order_status: str
    order_purchase_timestamp: datetime
    order_approved_at: Optional[datetime] = None
    order_delivered_carrier_date: Optional[datetime] = None
    order_delivered_customer_date: Optional[datetime] = None
    order_estimated_delivery_date: Optional[datetime] = None
    payment_type: Optional[str] = None
    payment_sequential: int
    payment_installments: int
    payment_value: float
    order_item_id: int
    product_id: str
    seller_ids: Optional[List[str]] = None
    shipping_limit_date: Optional[datetime] = None
    price: float
    freight_value: float

    class Config:
        from_attributes = True