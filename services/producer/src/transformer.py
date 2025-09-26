"""
Event transformation logic with detailed learning comments
"""

import re
from datetime import datetime
from typing import Any, Dict
from urllib.parse import parse_qs, urlparse

from models import (
    DeviceType,
    RawClickstreamEvent,
    SourceType,
    TransformedClickstreamEvent,
)


class ClickstreamTransformer:
    """Transforms raw API events to Flink-compatible format"""

    def __init__(self):
        # Learning: Pre-compile regex for better performance
        self.product_category_pattern = re.compile(r"ProductCategory\.([A-Z_]+)")
        self.device_pattern = re.compile(r"(mobile|desktop|tablet)", re.IGNORECASE)

    def transform_event(self, raw_event: Dict[str, Any]) -> TransformedClickstreamEvent:
        """
        Transform raw API event to Flink-compatible format

        Learning: This is where we handle data cleaning, enrichment,
        and schema transformation for real-time processing
        """
        # First validate the raw event
        validated_event = RawClickstreamEvent(**raw_event)

        # Extract and transform fields
        timestamp_ms = self._parse_timestamp(validated_event.timestamp)
        device_type = self._extract_device_type(validated_event.device_info)
        source = self._extract_source(validated_event.device_info)
        product_category = self._extract_product_category(validated_event.page_url)

        # Calculate derived fields for analytics
        dt = datetime.fromtimestamp(timestamp_ms / 1000)
        hour_of_day = dt.hour
        day_of_week = dt.weekday()
        is_weekend = day_of_week >= 5  # Saturday=5, Sunday=6

        # Calculate price from revenue and quantity
        # Learning: Handle division by zero and negative values
        price = validated_event.revenue / max(1, validated_event.quantity)

        return TransformedClickstreamEvent(
            user_id=validated_event.user_id,
            session_id=validated_event.session_id,
            timestamp=timestamp_ms,
            event_type=validated_event.interaction_type.value,
            product_id=validated_event.product_id,
            product_category=product_category,
            price=price,
            quantity=validated_event.quantity,
            source=source,
            page_url=validated_event.page_url,
            user_agent=validated_event.device_info,
            ip_address="0.0.0.0",  # Not provided by API
            device_type=device_type,
            interaction_duration=validated_event.duration,
            revenue=validated_event.revenue,
            hour_of_day=hour_of_day,
            day_of_week=day_of_week,
            is_weekend=is_weekend,
            price_category=self._categorize_price(price),
        )

    def _parse_timestamp(self, timestamp_str: str) -> int:
        """
        Parse ISO timestamp to milliseconds

        Learning: Handle different timestamp formats gracefully
        """
        try:
            # Handle 'Z' timezone indicator
            if timestamp_str.endswith("Z"):
                timestamp_str = timestamp_str[:-1] + "+00:00"

            dt = datetime.fromisoformat(timestamp_str)
            return int(dt.timestamp() * 1000)
        except ValueError as e:
            # Fallback to current time if parsing fails
            print(f"Warning: Failed to parse timestamp '{timestamp_str}': {e}")
            return int(datetime.now().timestamp() * 1000)

    def _extract_device_type(self, device_info: str) -> DeviceType:
        """
        Extract device type from device info string

        Learning: Use regex for flexible pattern matching
        """
        match = self.device_pattern.search(device_info)
        if match:
            return DeviceType(match.group(1).lower())
        return DeviceType.DESKTOP  # Default fallback

    def _extract_source(self, device_info: str) -> SourceType:
        """
        Determine source type (mobile app vs website)

        Learning: Business logic based on device type
        """
        if "mobile" in device_info.lower():
            return SourceType.MOBILE_APP
        return SourceType.WEBSITE

    def _extract_product_category(self, page_url: str) -> str:
        """
        Extract product category from page URL

        Learning: URL parsing and pattern extraction
        """
        try:
            # Learning: Use regex to extract category from URL
            match = self.product_category_pattern.search(page_url)
            if match:
                category = match.group(1)
                # Convert SNAKE_CASE to Title Case
                return category.replace("_", " ").title()
        except Exception as e:
            print(f"Warning: Failed to extract category from URL '{page_url}': {e}")

        return "Unknown"

    def _categorize_price(self, price: float) -> str:
        """
        Categorize product price for analytics

        Learning: Business categorization logic
        """
        if price < 20:
            return "budget"
        elif price < 50:
            return "mid-range"
        elif price < 100:
            return "premium"
        else:
            return "luxury"

    def batch_transform(
        self, raw_events: list[Dict[str, Any]]
    ) -> list[TransformedClickstreamEvent]:
        """
        Transform multiple events in batch

        Learning: Batch processing for better performance
        """
        transformed = []
        errors = []

        for i, raw_event in enumerate(raw_events):
            try:
                transformed_event = self.transform_event(raw_event)
                transformed.append(transformed_event)
            except Exception as e:
                errors.append(f"Event {i}: {e}")
                print(f"Error transforming event {i}: {e}")

        if errors:
            print(
                f"Transformation errors: {len(errors)} out of {len(raw_events)} events"
            )

        return transformed


# Learning: Example of advanced transformation strategies
class AdvancedTransformer(ClickstreamTransformer):
    """
    Extended transformer with more sophisticated enrichment

    Learning: This shows how to extend the base transformer
    with additional business logic and data enrichment
    """

    def __init__(self):
        super().__init__()
        # Learning: Add more sophisticated patterns
        self.user_session_pattern = re.compile(r"user_([a-f0-9]+)")
        self.bot_patterns = [
            r"bot",
            r"crawler",
            r"spider",
            r"scanner",
            r"test",
            r"dev",
            r"selenium",
            r"phantom",
        ]

    def is_bot_user_agent(self, user_agent: str) -> bool:
        """Detect if user agent is a bot"""
        user_agent_lower = user_agent.lower()
        return any(bot in user_agent_lower for bot in self.bot_patterns)

    def extract_user_segment(self, user_id: str) -> str:
        """Segment users based on ID patterns"""
        if user_id.startswith("test_"):
            return "test"
        elif len(user_id) > 30:
            return "premium"
        else:
            return "regular"

    def transform_event(self, raw_event: Dict[str, Any]) -> TransformedClickstreamEvent:
        """Override to add more enrichment"""
        base_event = super().transform_event(raw_event)

        # Learning: Add more sophisticated enrichment
        # These would be added to the model in a real implementation
        enrichment = {
            "is_bot": self.is_bot_user_agent(base_event.user_agent),
            "user_segment": self.extract_user_segment(base_event.user_id),
            "session_length_estimate": self._estimate_session_length(base_event),
        }

        # For now, we'll just return the base event
        # In a real implementation, we'd extend the model
        return base_event

    def _estimate_session_length(self, event: TransformedClickstreamEvent) -> int:
        """Estimate session length based on event patterns"""
        # Learning: Simple heuristic based on event type
        if event.event_type == "purchase":
            return 300  # Purchases typically take longer
        elif event.event_type == "view":
            return 30  # Views are typically quick
        else:
            return 60  # Default estimate
