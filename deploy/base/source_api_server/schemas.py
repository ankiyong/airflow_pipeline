from pydantic import BaseModel
from typing import Optional,List
from pydantic import BaseModel
from models import Order
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
    payment_sequential: int
    payment_type:  Optional[str] = None
    payment_installments: int
    payment_value: float
    order_item_id:  Optional[int] = None
    product_id:  Optional[str] = None
    seller_id:  Optional[str] = None
    shipping_limit_date:  Optional[str] = None
    price: float
    freight_value: float
    class Config:
        from_attributes = True

class ResponseModel(BaseModel):
    orders: List[OrderSchema]
