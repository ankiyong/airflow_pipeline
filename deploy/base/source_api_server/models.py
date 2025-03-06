from sqlalchemy import Boolean, Column, Numeric, Integer, String, DateTime,Float
from sqlalchemy.orm import relationship
from database import Base


class Order(Base):
    __tablename__ = "olist_orders"
    order_id = Column(String,primary_key=True,index=False)
    id = Column(Integer)
    customer_id = Column(String,primary_key=False)
    order_status = Column(String,primary_key=False)
    order_purchase_timestamp = Column(DateTime)
    order_approved_at = Column(String,primary_key=False)
    order_delivered_carrier_date = Column(String,primary_key=False)
    order_delivered_customer_date = Column(String,primary_key=False)
    order_estimated_delivery_date = Column(String,primary_key=False)
    payment_sequential= Column(Integer)
    payment_type= Column(String,primary_key=False)
    payment_installments= Column(Integer)
    payment_value=  Column(Float)
    order_item_id= Column(Integer)
    product_id= Column(String,primary_key=False)
    seller_id= Column(String,primary_key=False)
    shipping_limit_date= Column(String,primary_key=False)
    price= Column(Numeric(precision=3, scale=2), default=0)
    freight_value=  Column(Numeric(precision=3, scale=2), default=0)
