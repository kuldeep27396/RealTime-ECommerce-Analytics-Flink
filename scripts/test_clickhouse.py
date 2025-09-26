#!/usr/bin/env python3
"""
ClickHouse connection test script
Tests connection to ClickHouse Cloud using environment variables
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    import clickhouse_connect
    from dotenv import load_dotenv

    # Load environment variables
    load_dotenv()

    def test_clickhouse_connection():
        """Test connection to ClickHouse Cloud"""
        print("🔌 Testing ClickHouse Cloud connection...")

        # Get connection details from environment
        host = os.getenv('CLICKHOUSE_HOST')
        port = int(os.getenv('CLICKHOUSE_PORT', 8443))
        database = os.getenv('CLICKHOUSE_DATABASE', 'ecommerce')
        user = os.getenv('CLICKHOUSE_USER', 'default')
        password = os.getenv('CLICKHOUSE_PASSWORD')
        secure = os.getenv('CLICKHOUSE_SECURE', 'true').lower() == 'true'

        if not all([host, password]):
            print("❌ Missing required ClickHouse configuration in .env")
            print(f"Host: {host}")
            print(f"Password: {'*' * len(password) if password else 'Not set'}")
            return False

        try:
            # Create connection
            print(f"🔗 Connecting to ClickHouse Cloud...")
            print(f"   Host: {host}")
            print(f"   Port: {port}")
            print(f"   Database: {database}")
            print(f"   User: {user}")
            print(f"   Secure: {secure}")

            client = clickhouse_connect.get_client(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                secure=secure
            )

            # Test basic query
            print("📊 Testing basic query...")
            result = client.query("SELECT 1 as test_value")
            test_value = result.result_set[0][0]

            if test_value == 1:
                print("✅ ClickHouse Cloud connection successful!")

                # Test database creation
                print("🏗️ Testing database setup...")
                try:
                    client.command("CREATE DATABASE IF NOT EXISTS ecommerce")
                    print("✅ Database 'ecommerce' created/verified")
                except Exception as e:
                    print(f"⚠️  Database creation warning: {e}")

                # Show server info
                try:
                    server_info = client.query("SELECT version(), uptime()").result_set[0]
                    version, uptime = server_info
                    print(f"📈 Server Info:")
                    print(f"   Version: {version}")
                    print(f"   Uptime: {uptime} seconds")
                except Exception as e:
                    print(f"⚠️  Could not get server info: {e}")

                # Show tables
                try:
                    tables = client.query("SHOW TABLES FROM ecommerce").result_rows
                    if tables:
                        print(f"📋 Existing tables in 'ecommerce':")
                        for table in tables:
                            table_name = table[0]
                            print(f"   - {table_name}")
                    else:
                        print("📋 No existing tables in 'ecommerce' database")
                except Exception as e:
                    print(f"⚠️  Could not list tables: {e}")

                return True
            else:
                print(f"❌ Unexpected result: {test_value}")
                return False

        except Exception as e:
            print(f"❌ Connection failed: {e}")
            print("\n🔧 Troubleshooting:")
            print("1. Check your .env file for correct ClickHouse credentials")
            print("2. Verify your ClickHouse Cloud instance is running")
            print("3. Check network connectivity")
            print("4. Verify your IP is whitelisted in ClickHouse Cloud")
            return False
        finally:
            try:
                client.close()
                print("🔌 Connection closed")
            except:
                pass

    def test_sample_data():
        """Test creating sample data and queries"""
        print("\n🧪 Testing sample data operations...")

        try:
            client = clickhouse_connect.get_client(
                host=os.getenv('CLICKHOUSE_HOST'),
                port=int(os.getenv('CLICKHOUSE_PORT', 8443)),
                database=os.getenv('CLICKHOUSE_DATABASE', 'ecommerce'),
                user=os.getenv('CLICKHOUSE_USER', 'default'),
                password=os.getenv('CLICKHOUSE_PASSWORD'),
                secure=os.getenv('CLICKHOUSE_SECURE', 'true').lower() == 'true'
            )

            # Create test table
            print("📝 Creating test table...")
            client.command("""
                CREATE TABLE IF NOT EXISTS ecommerce.test_events (
                    timestamp DateTime,
                    event_type String,
                    user_id String,
                    value Float64
                ) ENGINE = MergeTree()
                ORDER BY timestamp
            """)

            # Insert sample data
            print("📥 Inserting sample data...")
            sample_data = [
                ('2024-01-01 10:00:00', 'view', 'user1', 100.0),
                ('2024-01-01 10:01:00', 'click', 'user1', 150.0),
                ('2024-01-01 10:02:00', 'purchase', 'user2', 200.0),
            ]

            client.insert('ecommerce.test_events', sample_data, column_names=['timestamp', 'event_type', 'user_id', 'value'])

            # Query the data
            print("📊 Querying sample data...")
            result = client.query("SELECT event_type, COUNT(*) as count, AVG(value) as avg_value FROM ecommerce.test_events GROUP BY event_type")

            print("📈 Results:")
            for row in result.result_rows:
                event_type, count, avg_value = row
                print(f"   {event_type}: {count} events, avg value: {avg_value:.2f}")

            # Clean up
            print("🧹 Cleaning up test data...")
            client.command("DROP TABLE IF EXISTS ecommerce.test_events")

            print("✅ Sample data test successful!")
            return True

        except Exception as e:
            print(f"❌ Sample data test failed: {e}")
            return False

    if __name__ == "__main__":
        print("🚀 ClickHouse Cloud Connection Test")
        print("=" * 50)

        # Test basic connection
        connection_ok = test_clickhouse_connection()

        if connection_ok:
            # Test sample data operations
            data_ok = test_sample_data()

            if data_ok:
                print("\n🎉 All tests passed! ClickHouse Cloud is ready!")
                sys.exit(0)
            else:
                print("\n❌ Data operations failed")
                sys.exit(1)
        else:
            print("\n❌ Connection test failed")
            sys.exit(1)

except ImportError as e:
    print(f"❌ Missing required packages: {e}")
    print("Please run 'make install' to install dependencies")
    sys.exit(1)