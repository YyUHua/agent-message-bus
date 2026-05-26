import sqlite3
import os
import logging
import json
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database path
DB_PATH = os.environ.get("BUS_DB_PATH", os.path.join(os.path.dirname(__file__), "anyue_bus.db"))

def init_db():
    """Initialize the database with required tables"""
    try:
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
                message_id TEXT PRIMARY KEY,
                processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")

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
        
        cursor.execute("INSERT OR IGNORE INTO processed_messages (message_id, processed_at) VALUES (?, ?)", 
                      (message_id, datetime.now()))
        conn.commit()
        conn.close()
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

def get_unprocessed_messages():
    """Get all messages that haven't been marked as processed"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, source, message_id, content, timestamp 
            FROM messages 
            WHERE message_id NOT IN (SELECT message_id FROM processed_messages)
        ''')
        
        results = cursor.fetchall()
        conn.close()
        
        # Convert to list of dictionaries
        messages = []
        for row in results:
            messages.append({
                'id': row[0],
                'source': row[1],
                'message_id': row[2],
                'content': row[3],
                'timestamp': row[4]
            })
        
        return messages
    except Exception as e:
        logger.error(f"Error getting unprocessed messages: {e}")
        return []