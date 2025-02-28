from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import relationship
from database import Base


class Order(Base):
    __tablename__ = "olist_orders_dataset"
    order_id = Column(String,primary_key=True,index=False)
    id = Column(Integer)
    customer_id = Column(String,primary_key=False)
    order_status = Column(String,primary_key=False)
    order_purchase_timestamp = Column(DateTime)
    order_approved_at = Column(String,primary_key=False)
    order_delivered_carrier_date = Column(String,primary_key=False)
    order_delivered_customer_date = Column(String,primary_key=False)
    order_estimated_delivery_date = Column(String,primary_key=False)
