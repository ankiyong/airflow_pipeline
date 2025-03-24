from sqlalchemy import Boolean, Column, Numeric, Integer, String, DateTime,Float,ARRAY
from sqlalchemy.orm import relationship
from database import Base


class Order(Base):
    __tablename__ = "order_log"
    log_time = Column(DateTime)
    order_id = Column(String, primary_key=True, index=False)
    customer_id = Column(String)
    order_status = Column(String)
    order_purchase_timestamp = Column(DateTime)
    order_approved_at = Column(String)
    order_delivered_carrier_date = Column(String)
    order_delivered_customer_date = Column(String)
    order_estimated_delivery_date = Column(String)
    payment_sequential = Column(Integer)
    payment_type = Column(String)
    payment_installments = Column(Integer)
    payment_value = Column(Float)
    order_item_id = Column(Integer)
    product_id = Column(ARRAY(String))
    seller_ids = Column(ARRAY(String))
    shipping_limit_date = Column(String)
    price = Column(Numeric(precision=3, scale=2), default=0)
    freight_value = Column(Numeric(precision=3, scale=2), default=0)