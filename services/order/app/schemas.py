from __future__ import annotations

from pydantic import BaseModel, EmailStr, Field


class OrderLineIn(BaseModel):
    sku: str = Field(min_length=1, max_length=40)
    quantity: int = Field(gt=0, le=20)
    unit_price_cents: int = Field(gt=0)


class PlaceOrderRequest(BaseModel):
    customer_id: str = Field(min_length=1, max_length=64)
    customer_email: EmailStr
    # A token issued by the external payment provider's hosted card form.
    # Card numbers never reach MaltaShop, which keeps us out of most PCI DSS scope.
    payment_token: str = Field(min_length=3, max_length=64)
    items: list[OrderLineIn] = Field(min_length=1, max_length=50)


class OrderLineOut(BaseModel):
    sku: str
    quantity: int
    unit_price_cents: int


class OrderOut(BaseModel):
    order_id: str
    status: str
    total_cents: int
    currency: str
    failure_reason: str | None = None
    lines: list[OrderLineOut]
