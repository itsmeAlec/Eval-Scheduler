#!/usr/bin/env python3
"""
Web App for Robot Status Dashboard
Shows robot status, current task, and next task from Google Sheets
Runs the scheduler on each refresh to update assignments
"""

from flask import Flask, render_template, jsonify
from datetime import datetime, date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging

# Import scheduler to run it on each refresh
from scheduler import main as run_scheduler

# Configuration (same as scheduler.py)
SPREADSHEET_NAME = "1 AGIBOT Evals (US)"
MODELS_SHEET_NAME = "EVAL QUEUE"
ROBOTS_SHEET_NAME = "Robots"
CREDENTIALS_FILE = "service_account.json"

# Column indices (1-indexed)
COL_REQUEST_DATE = 1   # A
COL_TASK = 14          # N

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_client():
    """Create and return a Google Sheets client."""
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        logger.error(f"Failed to authenticate: {e}")
        raise


def parse_date(date_str: str):
    """Parse date string from Google Sheets."""
    if not date_str or not date_str.strip():
        return None
    
    date_str = date_str.strip()
    
    # Try ISO format first
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        pass
    
    # Try US format
    try:
        return datetime.strptime(date_str, "%m/%d/%Y").date()
    except ValueError:
        pass
    
    # Try EU format
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").date()
    except ValueError:
        pass
    
    return None


def get_task_for_row(worksheet, row_num):
    """
    Get task (column N) for a specific row.
    Returns None if row doesn't exist or is out of date range.
    """
    if row_num is None:
        return None
    
    try:
        rows = worksheet.get_all_values()
        if row_num < 2 or row_num > len(rows):
            return None
        
        # Get the row (convert to 0-indexed)
        row = rows[row_num - 1]
        
        # Check if row has enough columns
        if len(row) < COL_TASK:
            return None
        
        # Check request date (filter to today/yesterday)
        today = date.today()
        yesterday = today - timedelta(days=1)
        
        request_date_str = row[COL_REQUEST_DATE - 1] if len(row) > COL_REQUEST_DATE - 1 else ""
        request_date = parse_date(request_date_str)
        
        if request_date and (request_date == today or request_date == yesterday):
            task = row[COL_TASK - 1] if len(row) > COL_TASK - 1 else ""
            return task.strip() if task else None
        
        return None
    except Exception as e:
        logger.error(f"Error getting task for row {row_num}: {e}")
        return None


def get_robot_statuses():
    """Get robot statuses and their current/next tasks."""
    try:
        client = get_client()
        spreadsheet = client.open(SPREADSHEET_NAME)
        
        # Get Robots sheet
        robots_worksheet = spreadsheet.worksheet(ROBOTS_SHEET_NAME)
        robots_rows = robots_worksheet.get_all_values()
        
        # Get Eval Queue sheet for task lookups
        eval_worksheet = spreadsheet.worksheet(MODELS_SHEET_NAME)
        
        robot_statuses = []
        
        # Skip header row (index 0), start from row 2 (1-indexed)
        for i, row in enumerate(robots_rows[1:], start=2):
            if not row or len(row) < 2:
                continue
            
            robot_id = (row[0] or "").strip()
            status = (row[1] or "").strip()
            current_row_str = row[2].strip() if len(row) > 2 else ""
            next_row_str = row[3].strip() if len(row) > 3 else ""
            
            if not robot_id:
                continue
            
            # Parse row numbers
            current_row = int(current_row_str) if current_row_str.isdigit() else None
            next_row = int(next_row_str) if next_row_str.isdigit() else None
            
            # Get tasks for current and next rows
            current_task = get_task_for_row(eval_worksheet, current_row)
            next_task = get_task_for_row(eval_worksheet, next_row)
            
            robot_statuses.append({
                'robot_id': robot_id,
                'status': status,
                'current_row': current_row,
                'current_task': current_task,
                'next_row': next_row,
                'next_task': next_task,
                'is_available': status.upper() in ("IDLE", "RUNNING")
            })
        
        return robot_statuses
    except Exception as e:
        logger.error(f"Error getting robot statuses: {e}")
        raise


@app.route('/')
def index():
    """Main dashboard page."""
    return render_template('index.html')


@app.route('/api/robots')
def api_robots():
    """API endpoint to get robot statuses. Runs scheduler before fetching status."""
    try:
        # Run scheduler to update assignments before fetching robot statuses
        logger.info("Running scheduler before fetching robot statuses...")
        try:
            # Capture scheduler output to avoid cluttering API response
            # The scheduler will update the sheets, which is what we want
            run_scheduler()
            logger.info("Scheduler completed successfully")
        except SystemExit:
            # Scheduler calls sys.exit() on errors - catch it so it doesn't kill the Flask app
            logger.warning("Scheduler exited with error, but continuing to serve dashboard")
        except Exception as scheduler_error:
            # Log scheduler errors but don't fail the API call
            # This allows the dashboard to still show current status even if scheduling fails
            logger.error(f"Scheduler error (non-fatal): {scheduler_error}")
        
        # Fetch updated robot statuses after scheduler runs
        robot_statuses = get_robot_statuses()
        return jsonify({
            'success': True,
            'robots': robot_statuses,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error in API: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
