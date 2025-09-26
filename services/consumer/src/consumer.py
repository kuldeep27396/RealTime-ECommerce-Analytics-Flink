"""
Main consumer service orchestrating Flink processing and ClickHouse storage
"""

import json
import signal
import sys
import threading
import time
from datetime import datetime
from typing import Any, Dict, List

import structlog

from .clickhouse_manager import ClickHouseManager
from .config import config
from .flink_processor import FlinkClickstreamProcessor
from .models import RealTimeMetrics

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


class ClickstreamConsumer:
    """
    Main consumer service that orchestrates the complete analytics pipeline

    Learning: This demonstrates:
    - Service orchestration patterns
    - Monitoring and health checks
    - Graceful shutdown handling
    - Multi-threaded architecture
    - Error recovery strategies
    """

    def __init__(self):
        self.flink_processor = None
        self.clickhouse_manager = None
        self.running = False
        self.start_time = datetime.now()
        self.metrics = {
            "events_processed": 0,
            "clickhouse_inserts": 0,
            "errors": 0,
            "last_activity": datetime.now(),
        }

        # Learning: Threading for different concerns
        self.flink_thread = None
        self.monitoring_thread = None
        self.health_thread = None

        # Learning: Signal handling for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self._initialize_components()

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("Received shutdown signal, stopping consumer...")
        self.stop()

    def _initialize_components(self):
        """Initialize all processing components"""
        try:
            # Initialize ClickHouse manager
            self.clickhouse_manager = ClickHouseManager()
            if not self.clickhouse_manager.connected:
                logger.warning("ClickHouse not available, continuing without it")

            # Initialize Flink processor
            self.flink_processor = FlinkClickstreamProcessor()
            logger.info("✅ All components initialized")

        except Exception as e:
            logger.error(f"Failed to initialize components: {e}")
            raise

    def start_flink_processing(self):
        """Start Flink processing in a separate thread"""

        def flink_worker():
            try:
                logger.info("🚀 Starting Flink processing thread")
                self.flink_processor.execute_processing()
            except Exception as e:
                logger.error(f"Flink processing failed: {e}")
                self.metrics["errors"] += 1

        self.flink_thread = threading.Thread(
            target=flink_worker, name="flink-processor"
        )
        self.flink_thread.daemon = True
        self.flink_thread.start()

    def start_monitoring(self):
        """Start monitoring thread for metrics collection"""

        def monitoring_worker():
            logger.info("📊 Starting monitoring thread")

            while self.running:
                try:
                    # Learning: Collect and log metrics
                    self._collect_metrics()

                    # Learning: Periodic health checks
                    self._health_check()

                    # Learning: Database cleanup and maintenance
                    if int(time.time()) % 3600 == 0:  # Every hour
                        self._maintenance_tasks()

                    time.sleep(30)  # Check every 30 seconds

                except Exception as e:
                    logger.error(f"Monitoring error: {e}")
                    time.sleep(60)  # Wait longer on error

        self.monitoring_thread = threading.Thread(
            target=monitoring_worker, name="monitoring"
        )
        self.monitoring_thread.daemon = True
        self.monitoring_thread.start()

    def start_health_server(self):
        """Start health check server (simplified version)"""

        def health_worker():
            logger.info("🏥 Starting health check thread")

            while self.running:
                try:
                    # Learning: Simple health check implementation
                    # In production, you'd use a proper HTTP server
                    health_status = self.get_health_status()
                    logger.info(f"Health status: {health_status['status']}")

                    time.sleep(60)  # Check every minute

                except Exception as e:
                    logger.error(f"Health check error: {e}")
                    time.sleep(120)

        self.health_thread = threading.Thread(target=health_worker, name="health-check")
        self.health_thread.daemon = True
        self.health_thread.start()

    def _collect_metrics(self):
        """Collect and log performance metrics"""
        try:
            # Learning: Collect metrics from ClickHouse if available
            if self.clickhouse_manager and self.clickhouse_manager.connected:
                dashboard_data = self.clickhouse_manager.get_real_time_dashboard(
                    minutes=1
                )
                if dashboard_data:
                    logger.info(
                        "📈 Real-time metrics",
                        **{
                            "events_last_minute": dashboard_data.get("total_events", 0),
                            "active_users": dashboard_data.get("unique_users", 0),
                            "revenue": f"${dashboard_data.get('total_revenue', 0):.2f}",
                            "avg_order_value": f"${dashboard_data.get('avg_order_value', 0):.2f}",
                        },
                    )

                    # Update internal metrics
                    self.metrics["events_processed"] += dashboard_data.get(
                        "total_events", 0
                    )

            # Learning: Log service health
            uptime = (datetime.now() - self.start_time).total_seconds()
            logger.info(
                "🔧 Service health",
                **{
                    "uptime_seconds": uptime,
                    "total_events_processed": self.metrics["events_processed"],
                    "clickhouse_inserts": self.metrics["clickhouse_inserts"],
                    "errors": self.metrics["errors"],
                    "flink_status": (
                        "running"
                        if self.flink_thread and self.flink_thread.is_alive()
                        else "stopped"
                    ),
                },
            )

            self.metrics["last_activity"] = datetime.now()

        except Exception as e:
            logger.error(f"Error collecting metrics: {e}")
            self.metrics["errors"] += 1

    def _health_check(self):
        """Perform health checks on all components"""
        health_issues = []

        # Check Flink thread
        if not self.flink_thread or not self.flink_thread.is_alive():
            health_issues.append("Flink processor not running")

        # Check ClickHouse connection
        if self.clickhouse_manager and not self.clickhouse_manager.connected:
            health_issues.append("ClickHouse not connected")

        # Check error rate
        if self.metrics["errors"] > 10:
            health_issues.append(f"High error rate: {self.metrics['errors']} errors")

        if health_issues:
            logger.warning("⚠️ Health check issues", issues=health_issues)
        else:
            logger.debug("✅ Health check passed")

    def _maintenance_tasks(self):
        """Perform periodic maintenance tasks"""
        try:
            logger.info("🔧 Running maintenance tasks")

            # Cleanup old data
            if self.clickhouse_manager and self.clickhouse_manager.connected:
                self.clickhouse_manager.cleanup_old_data(days_to_keep=30)

            # Log table statistics
            if self.clickhouse_manager and self.clickhouse_manager.connected:
                table_stats = self.clickhouse_manager.get_table_stats()
                logger.info("📊 Table statistics", **table_stats)

            # Reset error counter if it's getting too high
            if self.metrics["errors"] > 100:
                logger.info("Resetting error counter after reaching threshold")
                self.metrics["errors"] = 0

        except Exception as e:
            logger.error(f"Maintenance tasks failed: {e}")

    def get_health_status(self) -> Dict[str, Any]:
        """Get comprehensive health status"""
        uptime = (datetime.now() - self.start_time).total_seconds()

        # Determine overall status
        status_issues = []
        if not (self.flink_thread and self.flink_thread.is_alive()):
            status_issues.append("flink_down")
        if self.clickhouse_manager and not self.clickhouse_manager.connected:
            status_issues.append("clickhouse_down")
        if self.metrics["errors"] > 5:
            status_issues.append("high_errors")

        if status_issues:
            overall_status = "unhealthy" if len(status_issues) > 2 else "degraded"
        else:
            overall_status = "healthy"

        return {
            "status": overall_status,
            "timestamp": datetime.now(),
            "uptime_seconds": uptime,
            "components": {
                "flink": (
                    "running"
                    if self.flink_thread and self.flink_thread.is_alive()
                    else "stopped"
                ),
                "clickhouse": (
                    "connected"
                    if self.clickhouse_manager and self.clickhouse_manager.connected
                    else "disconnected"
                ),
            },
            "metrics": self.metrics.copy(),
            "issues": status_issues,
        }

    def start(self):
        """Start the complete consumer service"""
        logger.info("🚀 Starting Clickstream Consumer Service")
        logger.info(
            "🔧 Configuration",
            **{
                "kafka_topic": config.KAFKA_TOPIC,
                "flink_parallelism": config.FLINK_PARALLELISM,
                "clickhouse_host": config.CLICKHOUSE_HOST,
                "window_size_minutes": config.WINDOW_SIZE_MINUTES,
            },
        )

        try:
            self.running = True

            # Start all processing threads
            self.start_flink_processing()
            self.start_monitoring()
            self.start_health_server()

            logger.info("✅ Consumer service started successfully")
            logger.info("💡 Learning: This service demonstrates:")
            logger.info("   • Real-time stream processing with Apache Flink")
            logger.info("   • Analytics storage with ClickHouse")
            logger.info("   • Multi-threaded service architecture")
            logger.info("   • Comprehensive monitoring and health checks")
            logger.info("   • Graceful shutdown and error handling")

            # Learning: Keep main thread alive
            while self.running:
                time.sleep(1)

        except Exception as e:
            logger.error(f"Consumer service failed: {e}")
            raise
        finally:
            self.stop()

    def stop(self):
        """Stop the consumer service gracefully"""
        logger.info("🛑 Stopping consumer service...")
        self.running = False

        # Stop Flink processor
        if self.flink_processor:
            try:
                self.flink_processor.stop()
            except Exception as e:
                logger.error(f"Error stopping Flink processor: {e}")

        # Close ClickHouse connection
        if self.clickhouse_manager:
            try:
                self.clickhouse_manager.close()
            except Exception as e:
                logger.error(f"Error closing ClickHouse connection: {e}")

        # Wait for threads to finish
        if self.flink_thread and self.flink_thread.is_alive():
            self.flink_thread.join(timeout=10)

        if self.monitoring_thread and self.monitoring_thread.is_alive():
            self.monitoring_thread.join(timeout=5)

        if self.health_thread and self.health_thread.is_alive():
            self.health_thread.join(timeout=5)

        logger.info("✅ Consumer service stopped")

    def get_analytics_summary(self) -> Dict[str, Any]:
        """Get analytics summary for dashboards"""
        try:
            summary = {
                "service_health": self.get_health_status(),
                "timestamp": datetime.now(),
            }

            # Add real-time metrics from ClickHouse
            if self.clickhouse_manager and self.clickhouse_manager.connected:
                # Real-time dashboard data
                dashboard_data = self.clickhouse_manager.get_real_time_dashboard(
                    minutes=5
                )
                summary["real_time"] = dashboard_data

                # Conversion funnel
                funnel_data = self.clickhouse_manager.get_conversion_funnel(hours=1)
                if funnel_data:
                    latest_funnel = funnel_data[-1] if funnel_data else {}
                    summary["conversion_funnel"] = {
                        "views": latest_funnel.get("views", 0),
                        "add_to_carts": latest_funnel.get("add_to_carts", 0),
                        "purchases": latest_funnel.get("purchases", 0),
                        "conversion_rate": (
                            (
                                latest_funnel.get("purchases", 0)
                                / latest_funnel.get("views", 0)
                                * 100
                            )
                            if latest_funnel.get("views", 0) > 0
                            else 0
                        ),
                    }

                # Top products
                top_products = self.clickhouse_manager.get_top_products(
                    hours=1, limit=5
                )
                summary["top_products"] = top_products

                # User behavior
                behavior = self.clickhouse_manager.get_user_behavior_analysis(hours=1)
                summary["user_behavior"] = behavior

            return summary

        except Exception as e:
            logger.error(f"Error getting analytics summary: {e}")
            return {"error": str(e)}


def main():
    """Main entry point for the consumer service"""
    try:
        consumer = ClickstreamConsumer()
        consumer.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Consumer service failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
