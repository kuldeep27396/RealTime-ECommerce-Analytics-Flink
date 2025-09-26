"""
Data models for clickstream events with validation
"""
from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, validator
from enum import Enum


class InteractionType(str, Enum):
    """Types of user interactions"""
    VIEW = "view"
    ADD_TO_CART = "add_to_cart"
    REMOVE_FROM_CART = "remove_from_cart"
    PURCHASE = "purchase"
    WISHLIST = "wishlist"
    CLICK = "click"


class DeviceType(str, Enum):
    """Device types"""
    MOBILE = "mobile"
    DESKTOP = "desktop"
    TABLET = "tablet"


class SourceType(str, Enum):
    """Source types"""
    MOBILE_APP = "mobile_app"
    WEBSITE = "website"


class RawClickstreamEvent(BaseModel):
    """Raw event from API"""
    interaction_id: str
    user_id: str
    product_id: str
    interaction_type: InteractionType
    timestamp: str
    session_id: str
    duration: Optional[int] = None
    quantity: int = Field(default=1, ge=1)
    revenue: float = Field(default=0.0, ge=0.0)
    device_info: str
    page_url: str
    referrer: Optional[str] = None

    @validator('timestamp')
    def validate_timestamp(cls, v):
        try:
            # Validate timestamp format
            datetime.fromisoformat(v.replace('Z', '+00:00'))
            return v
        except ValueError:
            raise ValueError("Invalid timestamp format")


class TransformedClickstreamEvent(BaseModel):
    """Transformed event for Kafka with Flink-compatible schema"""
    user_id: str
    session_id: str
    timestamp: int  # Unix timestamp in milliseconds
    event_type: str
    product_id: str
    product_category: str
    price: float
    quantity: int
    source: SourceType
    page_url: str
    user_agent: str
    ip_address: str
    device_type: DeviceType
    interaction_duration: Optional[int] = None
    revenue: float

    # Additional computed fields for analytics
    hour_of_day: int = Field(ge=0, le=23)
    day_of_week: int = Field(ge=0, le=6)
    is_weekend: bool
    price_category: str  # budget, mid-range, premium, luxury

    @validator('price_category')
    def categorize_price(cls, v, values):
        price = values.get('price', 0)
        if price < 20:
            return "budget"
        elif price < 50:
            return "mid-range"
        elif price < 100:
            return "premium"
        else:
            return "luxury"


class ProducerMetrics(BaseModel):
    """Producer metrics for monitoring"""
    events_processed: int = 0
    events_failed: int = 0
    bytes_produced: int = 0
    last_event_time: Optional[datetime] = None
    api_calls_made: int = 0
    kafka_produces: int = 0

    @property
    def success_rate(self) -> float:
        """Calculate success rate"""
        total = self.events_processed + self.events_failed
        return (self.events_processed / total * 100) if total > 0 else 0.0

    @property
    def average_events_per_second(self) -> float:
        """Calculate average events per second"""
        return self.events_processed / max(1, self.api_calls_made)


class HealthStatus(BaseModel):
    """Health check response"""
    status: str  # healthy, degraded, unhealthy
    timestamp: datetime
    uptime_seconds: float
    metrics: ProducerMetrics
    version: str = "1.0.0"