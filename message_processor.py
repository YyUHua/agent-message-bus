import sys
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.append(_PROJECT_ROOT)

from message_deduplicator import init_db, add_message, is_message_processed, mark_message_processed

def process_incoming_message(source, message_id, content):
    """
    Process an incoming message with deduplication
    Returns True if message was processed, False if it was a duplicate
    """
    # Initialize database if needed
    init_db()
    
    # Check if message is already processed
    if is_message_processed(message_id):
        logger.info(f"Skipping duplicate message: {message_id}")
        return False
    
    # Add message to database
    add_message(source, message_id, content)
    
    # Mark as processed
    mark_message_processed(message_id)
    
    return True