"""
Data models for consumer service with analytical extensions
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator
from pydantic.types import DateTime, String, Float32, UInt16, UInt8, LowCardinality


class EventType(str, Enum):
    """Event types for processing"""

    PAGE_VIEW = "page_view"
    PRODUCT_VIEW = "product_view"
    ADD_TO_CART = "add_to_cart"
    REMOVE_FROM_CART = "remove_from_cart"
    PURCHASE = "purchase"
    WISHLIST = "wishlist"


class DeviceType(str, Enum):
    """Device types"""

    MOBILE = "mobile"
    DESKTOP = "desktop"
    TABLET = "tablet"


class SourceType(str, Enum):
    """Source types"""

    MOBILE_APP = "mobile_app"
    WEBSITE = "website"


class ClickstreamEvent(BaseModel):
    """Clickstream event model matching Kafka schema"""

    user_id: str
    session_id: str
    timestamp: int  # Unix timestamp in milliseconds
    event_type: str
    product_id: str
    product_category: str
    price: float
    quantity: int
    source: str
    page_url: str
    user_agent: str
    ip_address: str
    device_type: str
    interaction_duration: Optional[int] = None
    revenue: float
    hour_of_day: int = Field(ge=0, le=23)
    day_of_week: int = Field(ge=0, le=6)
    is_weekend: bool
    price_category: str

    @validator("event_type")
    def validate_event_type(cls, v):
        valid_types = [e.value for e in EventType]
        if v not in valid_types:
            raise ValueError(f"Invalid event type: {v}. Must be one of {valid_types}")
        return v


class UserSessionMetrics(BaseModel):
    """User session analytics"""

    session_id: str
    user_id: str
    event_count: int
    unique_products_viewed: int
    total_revenue: float
    average_session_duration: float
    device_type: DeviceType
    conversion_rate: float  # purchases / views
    first_event_time: datetime
    last_event_time: datetime

    @validator("conversion_rate")
    def validate_conversion_rate(cls, v):
        return max(0.0, min(1.0, v))


class ProductAnalytics(BaseModel):
    """Product performance metrics"""

    product_id: str
    product_category: str
    view_count: int
    add_to_cart_count: int
    purchase_count: int
    total_revenue: float
    average_price: float
    conversion_rate: float  # purchases / views
    popularity_score: float  # weighted score based on multiple metrics

    @validator("conversion_rate")
    def validate_conversion_rate(cls, v):
        return max(0.0, min(1.0, v))


class TimeWindowMetrics(BaseModel):
    """Time-based analytics"""

    window_start: datetime
    window_end: datetime
    total_events: int
    unique_users: int
    total_revenue: float
    average_order_value: float
    top_product_category: str
    device_distribution: Dict[str, float]  # device_type -> percentage
    event_type_distribution: Dict[str, float]  # event_type -> percentage

    @validator("device_distribution")
    def validate_device_distribution(cls, v):
        total = sum(v.values())
        if abs(total - 1.0) > 0.01:  # Allow small rounding errors
            raise ValueError("Device distribution must sum to 1.0")
        return v

    @validator("event_type_distribution")
    def validate_event_type_distribution(cls, v):
        total = sum(v.values())
        if abs(total - 1.0) > 0.01:
            raise ValueError("Event type distribution must sum to 1.0")
        return v


class RealTimeMetrics(BaseModel):
    """Real-time streaming metrics"""

    timestamp: datetime
    events_per_second: float
    users_per_second: float
    revenue_per_second: float
    average_processing_latency_ms: float
    error_rate: float  # percentage of failed events
    current_memory_usage_mb: float
    cpu_usage_percent: float

    @validator("error_rate")
    def validate_error_rate(cls, v):
        return max(0.0, min(100.0, v))


class FunnelMetrics(BaseModel):
    """Conversion funnel analytics"""

    window_start: datetime
    window_end: datetime
    product_views: int
    add_to_carts: int
    purchases: int

    @property
    def view_to_cart_rate(self) -> float:
        """Conversion rate from view to add to cart"""
        return (
            (self.add_to_carts / self.product_views * 100)
            if self.product_views > 0
            else 0.0
        )

    @property
    def cart_to_purchase_rate(self) -> float:
        """Conversion rate from add to cart to purchase"""
        return (
            (self.purchases / self.add_to_carts * 100) if self.add_to_carts > 0 else 0.0
        )

    @property
    def overall_conversion_rate(self) -> float:
        """Overall conversion rate from view to purchase"""
        return (
            (self.purchases / self.product_views * 100)
            if self.product_views > 0
            else 0.0
        )


class AnomalyDetection(BaseModel):
    """Anomaly detection results"""

    timestamp: datetime
    metric_name: str
    current_value: float
    expected_range: tuple[float, float]  # (min, max)
    severity: str  # low, medium, high, critical
    description: str
    potential_causes: List[str]


class ClickHouseRecord(BaseModel):
    """Record for ClickHouse insertion - Schema optimized for ClickHouse analytics"""

    timestamp: DateTime
    user_id: String
    session_id: String
    event_type: LowCardinality(String)
    product_id: String
    product_category: LowCardinality(String)
    price: Float32
    quantity: UInt16
    source: LowCardinality(String)
    device_type: LowCardinality(String)
    revenue: Float32
    hour_of_day: UInt8
    day_of_week: UInt8
    is_weekend: UInt8
    price_category: LowCardinality(String)
    processing_time: DateTime


class ConsumerHealth(BaseModel):
    """Consumer service health status"""

    status: str  # healthy, degraded, unhealthy
    timestamp: datetime
    kafka_lag_ms: int
    flink_job_status: str
    clickhouse_connection: bool
    events_processed_last_minute: int
    error_rate: float


# Learning: Advanced analytics models for machine learning integration
class MLFeatures(BaseModel):
    """Features for machine learning models"""

    user_id: str
    session_id: str
    # Behavioral features
    session_duration_seconds: float
    events_per_session: float
    unique_products_per_session: float
    average_time_between_events: float
    # Purchase patterns
    total_spent_this_session: float
    items_in_cart: int
    previous_purchases_count: int
    # Temporal features
    hour_of_day: int
    day_of_week: int
    is_weekend: bool
    # Device and source
    device_type: str
    source: str
    # Product preferences
    favorite_category: str
    price_sensitivity_score: float  # calculated from purchase history

    @validator("price_sensitivity_score")
    def validate_price_sensitivity(cls, v):
        return max(0.0, min(1.0, v))


class PredictionResult(BaseModel):
    """Machine learning prediction results"""

    timestamp: datetime
    user_id: str
    session_id: str
    prediction_type: str  # conversion_propensity, churn_probability, etc.
    prediction_value: float
    confidence: float
    model_version: str
    features_used: List[str]
