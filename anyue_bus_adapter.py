import sqlite3
import os
import json
import logging
import time
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database path
DB_PATH = os.environ.get("BUS_DB_PATH", os.path.join(os.path.dirname(__file__), "anyue_bus.db"))

def init_db():
    """Initialize the database with required tables"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create messages table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            message_id TEXT,
            content TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(message_id)
        )
    ''')
    
    # Create processed_messages table for deduplication
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_messages (
            message_id TEXT PRIMARY KEY
        )
    ''')
    
    conn.commit()
    conn.close()

def is_message_processed(message_id):
    """Check if a message has already been processed"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("SELECT message_id FROM processed_messages WHERE message_id=?", (message_id,))
        result = cursor.fetchone()
        
        conn.close()
        return result is not None
    except Exception as e:
        logger.error(f"Error checking if message is processed: {e}")
        return False

def mark_message_processed(message_id):
    """Mark a message as processed"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("INSERT OR IGNORE INTO processed_messages (message_id) VALUES (?)", (message_id,))
        conn.commit()
        conn.close()
        logger.info(f"Marked message {message_id} as processed")
    except Exception as e:
        logger.error(f"Error marking message as processed: {e}")

def add_message(source, message_id, content):
    """Add a message to the database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("INSERT OR IGNORE INTO messages (source, message_id, content) VALUES (?, ?, ?)", 
                   (source, message_id, content))
        conn.commit()
        conn.close()
        logger.info(f"Added message {message_id} from {source}")
    except Exception as e:
        logger.error(f"Error adding message: {e}")

def poll_messages():
    """Poll for new messages and process them with deduplication"""
    # This is a simplified implementation - in a real system, this would be replaced with actual message polling code
    logger.info("Polling for new messages...")
    # Simulate message processing with deduplication
    sample_message_id = "msg-001"
    sample_source = "agent_a"
    sample_content = "测试消息内容"
    
    # Check if message is already processed
    if not is_message_processed(sample_message_id):
        # Add message to database
        add_message(sample_source, sample_message_id, sample_content)
        # Mark as processed
        mark_message_processed(sample_message_id)
        logger.info(f"Processed new message: {sample_message_id}")
    else:
        logger.info(f"Skipping duplicate message: {sample_message_id}")

if __name__ == "__main__":
    # Initialize database
    init_db()
    
    # Poll for messages
    while True:
        poll_messages()
        time.sleep(10)  # Poll every 10 seconds