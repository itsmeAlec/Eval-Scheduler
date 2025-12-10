#!/usr/bin/env python3
"""
Eval Scheduler - Coordinates evaluation jobs across 4 robots using Google Sheets

CONFIGURATION:
1. Set SPREADSHEET_NAME below to match your Google Sheet name
2. Place your service account JSON credentials file as "service_account.json" in the same directory
3. In the Robots sheet, set Status column (B) for each robot:
   - "IDLE" or "RUNNING" = robot is available and will receive assignments
   - "DOWN", "OFFLINE", or any other status = robot is unavailable (no assignments)
4. Run: python scheduler.py

ROBOT AVAILABILITY:
- The scheduler automatically detects available robots from the Robots sheet
- Only robots with Status="IDLE" or "RUNNING" receive NextRow assignments
- If a robot is marked as DOWN/OFFLINE, the scheduler will:
  * Clear any existing assignments for that robot in the Eval Queue
  * Reset "Evaluation In Progress" tasks back to "pending" if assigned to a down robot
  * Skip that robot when computing new assignments
- You can have any number of robots (3, 4, 5, etc.) - just add them to the Robots sheet

OPERATOR WORKFLOW:
1. When a robot starts a job:
   - Update Eval Queue row: Set Robot ID (col J) to the robot name, Status (col M) to "in progress" or "Evaluation In Progress"
   - Update Robots sheet: Set Status (col B) to "RUNNING" (optional - CurrentRow will be auto-updated)
   - Run the scheduler: CurrentRow (col C) will be automatically updated based on tasks in progress

2. When a robot finishes a job:
   - For ALL4 tasks (unseen/in-domain): 
     - IMPORTANT: Call mark_robot_done() or manually update Operator (col Q) to mark the robot as done:
       - Format: "robots_done: 25" or "robots_done: 25,27" etc.
     - Clear or update Robot ID (col J) - the next robot can now start
     - When all available robots are done, set Status (col M) to "Completed"
     - The scheduler checks robots_done to know when a robot has finished, allowing the next robot to be assigned
   - For normal tasks: Set Status (col M) to "Completed"
   - Update Robots sheet: Set Status (col B) to "IDLE" (CurrentRow will be auto-cleared when task is completed)

3. Run this scheduler script to:
   - Automatically update CurrentRow based on tasks in progress
   - Recompute NextRow assignments for all robots

SHEET STRUCTURE:

Eval Queue Sheet (columns):
  A: Request date
  B: Completion Date
  C: Model Name
  D: Comment (for requestor - DO NOT TOUCH)
  E: Checkpoint (format: "ip, port, (/path/...)" - port is second comma-separated value)
  F: Expected inference speed
  G: Actual inference speed
  H: Eval start time
  I: Requestor
  J: Robot ID (25-29) - set when job starts
  K: Flag
  L: Priority
  M: Status (dropdown: "pending", "Evaluation In Progress", "Completed", "Cancelled", "Vibe Check Requested")
  N: Task (dropdown; "unseen" and "in-domain" are ALL4 tasks requiring all 4 robots)
  O: Success rate
  Q: Operator name (also stores robots_done: metadata like "robots_done: 25,27")

Robots Sheet (columns):
  A: Robot (25, 26, 27, 28, 29, etc.)
  B: Status (IDLE, RUNNING, DOWN, OFFLINE, or any other status - maintained manually)
    - IDLE or RUNNING = robot is available for scheduling
    - DOWN, OFFLINE, or any other status = robot is unavailable
  C: CurrentRow (row index from Eval Queue - automatically updated by scheduler based on tasks in progress)
  D: NextRow (row index from Eval Queue - written by this script)

ROBOT AVAILABILITY:
- The scheduler automatically detects available robots from the Robots sheet
- Only robots with Status="IDLE" or "RUNNING" will receive assignments
- If a robot is marked as DOWN/OFFLINE, its assignments will be cleared from the Eval Queue
- You can have any number of robots (3, 4, 5, etc.) - just add them to the Robots sheet

SCHEDULING RULES:
1. Date filtering: Only tasks with Request date (column A) matching today or yesterday are scheduled
2. Status filtering: 
   - Normal tasks: Only tasks with Status="pending" are scheduled
   - ALL4 tasks: Can be scheduled even if Status="in progress" (allows multiple robots to work on same task)
3. Port conflict: Two robots must NOT run the same (Model Name, Port) simultaneously
4. ALL4 tasks: Tasks with Task="unseen" or "in-domain" must run once on each AVAILABLE robot
   - Can be scheduled with Status="pending" OR "in progress" (another robot may already be running it)
   - All available robots will eventually be assigned to complete the task (across multiple scheduling runs)
   - Assignment rule: Only ONE robot per ALL4 task per scheduling run (they run sequentially, not simultaneously)
   - Port conflict rule: Only one robot can run the same (Model Name, Port) combination at a time
   - Multiple robots CAN run simultaneously if they have different model names (even on same port)
   - Track completion via robots_done: in Operator column (Q)
   - When all available robots complete, set Status="Completed"
   - If a robot goes down mid-task, it will be cleared and can be reassigned when robot is back up
5. Status handling:
   - Normal tasks:
     - "pending" => eligible to be scheduled
     - "in progress" / "Evaluation In Progress" => not schedulable (already running)
   - ALL4 tasks:
     - "pending" or "in progress" => eligible to be scheduled (if robot hasn't completed it and no port conflict)
     - "Completed" => not schedulable (all robots done)
   - "Cancelled" / "Vibe Check Requested" => blocked, not schedulable
6. Priority: ALL4 tasks are scheduled before normal tasks
"""

import logging
import re
import sys
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Set, Tuple

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ============================================================================
# CONFIGURATION
# ============================================================================

SPREADSHEET_NAME = "1 AGIBOT Evals (US)"  # Change this to your Google Sheet name
MODELS_SHEET_NAME = "EVAL QUEUE"
ROBOTS_SHEET_NAME = "Robots"
CREDENTIALS_FILE = "service_account.json"

# Robot availability is automatically detected from the Robots sheet
# Only robots with Status="IDLE" or "RUNNING" are considered available
# You can have any number of robots - just add them to the Robots sheet
# Legacy: kept for validation, but actual list comes from Robots sheet
ROBOT_NAMES = ["25", "26", "27", "28", "29"]  # Robot IDs

# Column indices (1-indexed, converted to 0-indexed when accessing arrays)
COL_REQUEST_DATE = 1   # A
COL_MODEL_NAME = 3     # C
COL_CHECKPOINT = 5     # E
COL_ROBOT_ID = 10      # J
COL_STATUS = 13        # M
COL_TASK = 14          # N
COL_OPERATOR = 17      # Q (used for robots_done metadata)

ALL4_TASKS = {"unseen", "in-domain"}

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATA MODELS
# ============================================================================


@dataclass
class TaskRow:
    """Represents a single row from the Eval Queue sheet."""
    row: int  # 1-indexed row number in the sheet
    model: str
    checkpoint: str
    port: str
    all4: bool  # True if this is an ALL4 task (unseen/in-domain)
    status: str  # Normalized status: PENDING, IN_PROGRESS, COMPLETED, BLOCKED
    robot: str  # Current robot assigned (from col J)
    done_list: List[str]  # List of robots that have completed this task (for ALL4 tasks)
    task: str  # Original task value from col N
    request_date: Optional[date]  # Request date from column A


@dataclass
class AssignmentResult:
    """Result of scheduling assignment for a robot."""
    robot: str
    assigned_row: Optional[int]
    skip_reasons: List[str]  # Reasons why tasks were skipped


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def parse_port(checkpoint: str) -> str:
    """
    Extract port number from checkpoint string.
    
    Args:
        checkpoint: String like "10.78.4.150, 7000, (/path/...)"
    
    Returns:
        Port number as string, or empty string if not found
    """
    if not checkpoint:
        return ""
    parts = [p.strip() for p in checkpoint.split(",")]
    if len(parts) > 1 and parts[1].isdigit():
        return parts[1]
    return ""


def parse_date(date_str: str) -> Optional[date]:
    """
    Parse date string from Google Sheets into a date object.
    
    Handles various formats that Google Sheets might use:
    - "2024-01-15" (ISO format)
    - "1/15/2024" (US format)
    - "15/1/2024" (EU format)
    - Date serial numbers (if converted to string)
    
    Args:
        date_str: Date string from sheet
    
    Returns:
        date object, or None if parsing fails
    """
    if not date_str or not date_str.strip():
        return None
    
    date_str = date_str.strip()
    
    # Try ISO format first (YYYY-MM-DD)
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        pass
    
    # Try US format (M/D/YYYY or MM/DD/YYYY)
    try:
        return datetime.strptime(date_str, "%m/%d/%Y").date()
    except ValueError:
        pass
    
    # Try EU format (D/M/YYYY or DD/MM/YYYY)
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").date()
    except ValueError:
        pass
    
    # Try with dashes (M-D-YYYY)
    try:
        return datetime.strptime(date_str, "%m-%d-%Y").date()
    except ValueError:
        pass
    
    # Try with dashes (D-M-YYYY)
    try:
        return datetime.strptime(date_str, "%d-%m-%Y").date()
    except ValueError:
        pass
    
    logger.debug(f"Could not parse date: {date_str}")
    return None


def is_date_in_range(task_date: Optional[date], today: date, yesterday: date) -> bool:
    """
    Check if a task date is within the allowed range (today or yesterday).
    
    Args:
        task_date: The task's request date
        today: Today's date
        yesterday: Yesterday's date
    
    Returns:
        True if task_date is today or yesterday, False otherwise
    """
    if task_date is None:
        return False
    return task_date == today or task_date == yesterday


def normalize_status(status: str) -> str:
    """
    Normalize status string to internal representation.
    
    Args:
        status: Raw status string from sheet
    
    Returns:
        Normalized status: PENDING, IN_PROGRESS, COMPLETED, or BLOCKED
    """
    if not status:
        return "PENDING"
    s = status.lower().strip()
    if s in ("pending", "", "can run now"):
        return "PENDING"
    # Handle both "evaluation in progress" and "in progress"
    if s in ("evaluation in progress", "in progress"):
        return "IN_PROGRESS"
    if s == "completed":
        return "COMPLETED"
    if s in ("cancelled", "vibe check requested"):
        return "BLOCKED"
    return "BLOCKED"


def parse_done_list(operator_field: str) -> List[str]:
    """
    Parse robots_done: list from Operator column.
    
    Looks for:
    1. Pattern "robots_done: 25,27" (full format)
    2. Just robot IDs like "26" or "25,27" (simplified format)
    
    Args:
        operator_field: Content of Operator column (Q)
    
    Returns:
        List of robot IDs that have completed this task (e.g., ["25", "27"])
    """
    if not operator_field:
        return []
    
    text = operator_field.replace(" ", "")
    robots = []
    
    # First, try to match "robots_done:25,26,27" or "robots_done: 25, 26" format
    match = re.search(r"robots_done:([\d,]+)", text, re.I)
    if match:
        raw = match.group(1)
        robots = [r.strip() for r in raw.split(",") if r.strip()]
    else:
        # Look for standalone robot IDs (just numbers that match robot names)
        # Match patterns like "26" or "25,27" or "26 " (with spaces/commas)
        # Extract all valid robot IDs from the text
        for robot_id in ROBOT_NAMES:
            # Look for the robot ID as a standalone number (not part of a larger number)
            # Match word boundaries or comma-separated values
            pattern = r'\b' + re.escape(robot_id) + r'\b'
            if re.search(pattern, text):
                if robot_id not in robots:
                    robots.append(robot_id)
    
    # Validate robot names
    validated_robots = [r for r in robots if r in ROBOT_NAMES]
    
    # Legacy support: "all_done" means all robots completed
    if "all_done" in text.lower():
        return ROBOT_NAMES.copy()
    
    return validated_robots


def is_all4_task(task_val: str, operator_field: str) -> bool:
    """
    Determine if a task is an ALL4 task (must run on all 4 robots).
    
    Args:
        task_val: Task value from column N
        operator_field: Operator field from column Q (for legacy detection)
    
    Returns:
        True if this is an ALL4 task
    """
    if not task_val:
        return False
    if task_val.strip().lower() in ALL4_TASKS:
        return True
    
    # Legacy support: check operator field for hints
    if operator_field:
        op = operator_field.lower()
        if any(phrase in op for phrase in ["all robots", "4 robots", "all four robots"]):
            return True
    
    return False


def extract_robots_done_pattern(operator_field: str) -> Tuple[Optional[str], str]:
    """
    Extract robots_done: pattern and remaining text from operator field.
    
    Args:
        operator_field: Full content of Operator column
    
    Returns:
        Tuple of (robots_done_pattern, remaining_text)
        Example: ("25,27", "Operator: John Doe") or (None, "Operator: John Doe")
    """
    if not operator_field:
        return None, ""
    
    # Look for robots_done: pattern (matches digits and commas)
    match = re.search(r"robots_done:\s*([\d,\s]+)", operator_field, re.I)
    if match:
        robots_done = match.group(1).strip()
        # Remove the pattern from the text
        remaining = re.sub(r"robots_done:\s*[\d,\s]+", "", operator_field, flags=re.I).strip()
        return robots_done, remaining
    
    return None, operator_field


def mark_robot_done(worksheet: gspread.Worksheet, row: int, robot: str) -> None:
    """
    Mark a robot as done for a task by updating the Operator column (Q).
    
    This function:
    - Adds the robot to robots_done: list if not already present
    - Preserves any existing operator name text outside the robots_done: pattern
    - Handles the case where robots_done: doesn't exist yet
    
    Args:
        worksheet: The Eval Queue worksheet
        row: 1-indexed row number
        robot: Robot ID (25, 26, 27, 28, or 29)
    
    Raises:
        ValueError: If robot is not a valid robot name
        gspread.exceptions.APIError: If Google Sheets API call fails
    """
    if robot not in ROBOT_NAMES:
        raise ValueError(f"Invalid robot name: {robot}. Must be one of {ROBOT_NAMES}")
    
    # Get current operator field
    current_value = worksheet.cell(row, COL_OPERATOR).value or ""
    
    # Extract existing robots_done and remaining text
    robots_done_pattern, remaining_text = extract_robots_done_pattern(current_value)
    
    # Parse existing robots
    if robots_done_pattern:
        existing_robots = [r.strip() for r in robots_done_pattern.split(",") if r.strip()]
    else:
        existing_robots = []
    
    # Add robot if not already present
    if robot not in existing_robots:
        existing_robots.append(robot)
        # Sort numerically for consistency
        existing_robots.sort(key=lambda x: int(x) if x.isdigit() else 0)
    
    # Build new robots_done pattern
    robots_done_str = ",".join(existing_robots)
    
    # Reconstruct the operator field
    parts = []
    if remaining_text:
        parts.append(remaining_text)
    parts.append(f"robots_done: {robots_done_str}")
    
    new_value = " | ".join(parts) if remaining_text else f"robots_done: {robots_done_str}"
    
    # Update the cell
    worksheet.update_cell(row, COL_OPERATOR, new_value)
    logger.info(f"Updated row {row} Operator column: added {robot} to robots_done. New value: {new_value}")


# ============================================================================
# GOOGLE SHEETS CLIENT
# ============================================================================


def get_client() -> gspread.Client:
    """
    Create and return a Google Sheets client using service account credentials.
    
    Returns:
        Authorized gspread client
    
    Raises:
        FileNotFoundError: If credentials file doesn't exist
        gspread.exceptions.SpreadsheetNotFound: If spreadsheet doesn't exist
    """
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        logger.info(f"Successfully authenticated with Google Sheets")
        return client
    except FileNotFoundError:
        logger.error(f"Credentials file not found: {CREDENTIALS_FILE}")
        raise
    except Exception as e:
        logger.error(f"Failed to authenticate: {e}")
        raise


def load_models(client: gspread.Client) -> Tuple[gspread.Worksheet, List[TaskRow]]:
    """
    Load all task rows from the Eval Queue sheet.
    
    Args:
        client: Google Sheets client
    
    Returns:
        Tuple of (worksheet, list of TaskRow objects)
    
    Raises:
        gspread.exceptions.WorksheetNotFound: If worksheet doesn't exist
        gspread.exceptions.APIError: If API call fails
    """
    try:
        spreadsheet = client.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(MODELS_SHEET_NAME)
        rows = worksheet.get_all_values()
        
        tasks = []
        today = date.today()
        yesterday = today - timedelta(days=1)
        
        # Skip header row (index 0), start from row 2 (1-indexed)
        for i, row in enumerate(rows[1:], start=2):
            # Handle rows that might be shorter than expected
            if len(row) < max(COL_MODEL_NAME, COL_CHECKPOINT, COL_STATUS, COL_TASK, COL_OPERATOR):
                continue
            
            # Read request date from column A
            request_date_str = row[COL_REQUEST_DATE - 1] if len(row) > COL_REQUEST_DATE - 1 else ""
            request_date = parse_date(request_date_str)
            
            # Filter: only include tasks from today or yesterday
            if not is_date_in_range(request_date, today, yesterday):
                continue
            
            model = (row[COL_MODEL_NAME - 1] or "").strip()
            checkpoint = (row[COL_CHECKPOINT - 1] or "").strip()
            operator_field = row[COL_OPERATOR - 1] if len(row) > COL_OPERATOR - 1 else ""
            status = normalize_status(row[COL_STATUS - 1] if len(row) > COL_STATUS - 1 else "")
            robot = (row[COL_ROBOT_ID - 1] if len(row) > COL_ROBOT_ID - 1 else "").strip()
            task_val = row[COL_TASK - 1] if len(row) > COL_TASK - 1 else ""
            
            # Skip empty rows
            if not model and not checkpoint:
                continue
            
            tasks.append(TaskRow(
                row=i,
                model=model,
                checkpoint=checkpoint,
                port=parse_port(checkpoint),
                all4=is_all4_task(task_val, operator_field),
                status=status,
                robot=robot,
                done_list=parse_done_list(operator_field),
                task=task_val,
                request_date=request_date,
            ))
        
        logger.info(f"Loaded {len(tasks)} tasks from Eval Queue sheet (filtered to today and yesterday only)")
        return worksheet, tasks
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"Worksheet '{MODELS_SHEET_NAME}' not found in spreadsheet '{SPREADSHEET_NAME}'")
        raise
    except Exception as e:
        logger.error(f"Failed to load models: {e}")
        raise


def load_robots(client: gspread.Client) -> gspread.Worksheet:
    """
    Load the Robots worksheet.
    
    Args:
        client: Google Sheets client
    
    Returns:
        Robots worksheet
    
    Raises:
        gspread.exceptions.WorksheetNotFound: If worksheet doesn't exist
    """
    try:
        spreadsheet = client.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(ROBOTS_SHEET_NAME)
        logger.info(f"Loaded Robots sheet")
        return worksheet
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"Worksheet '{ROBOTS_SHEET_NAME}' not found in spreadsheet '{SPREADSHEET_NAME}'")
        raise


@dataclass
class RobotStatus:
    """Represents a robot's status from the Robots sheet."""
    robot_id: str
    status: str  # IDLE, RUNNING, DOWN, OFFLINE, etc.
    current_row: Optional[int]
    next_row: Optional[int]
    is_available: bool  # True if status is IDLE or RUNNING


def get_available_robots(worksheet: gspread.Worksheet) -> Tuple[List[str], List[RobotStatus], List[str]]:
    """
    Get list of available robots from the Robots sheet.
    
    Available robots are those with Status="IDLE" or "RUNNING".
    All other statuses (DOWN, OFFLINE, etc.) are considered unavailable.
    
    Args:
        worksheet: Robots worksheet
    
    Returns:
        Tuple of (available_robot_ids, all_robot_statuses, unavailable_robot_ids)
    """
    rows = worksheet.get_all_values()
    if len(rows) < 2:  # Need at least header + 1 data row
        logger.warning("Robots sheet has no data rows")
        return [], [], []
    
    available = []
    unavailable = []
    all_statuses = []
    
    # Skip header row (index 0), start from row 2 (1-indexed)
    for i, row in enumerate(rows[1:], start=2):
        if not row or len(row) < 2:
            continue
        
        robot_id = (row[0] or "").strip()
        status = (row[1] or "").strip().upper()
        current_row_str = row[2].strip() if len(row) > 2 else ""
        next_row_str = row[3].strip() if len(row) > 3 else ""
        
        if not robot_id:
            continue
        
        # Parse row numbers
        current_row = int(current_row_str) if current_row_str.isdigit() else None
        next_row = int(next_row_str) if next_row_str.isdigit() else None
        
        # Determine availability: IDLE and RUNNING are available, everything else is not
        is_available = status in ("IDLE", "RUNNING")
        
        robot_status = RobotStatus(
            robot_id=robot_id,
            status=status,
            current_row=current_row,
            next_row=next_row,
            is_available=is_available
        )
        all_statuses.append(robot_status)
        
        if is_available:
            available.append(robot_id)
        else:
            unavailable.append(robot_id)
    
    logger.info(f"Available robots: {available} ({len(available)} total)")
    if unavailable:
        logger.warning(f"Unavailable robots: {unavailable} ({len(unavailable)} total)")
    
    return available, all_statuses, unavailable


def handle_robot_downtime(
    models_worksheet: gspread.Worksheet,
    tasks: List[TaskRow],
    unavailable_robots: List[str]
) -> int:
    """
    Handle robot downtime by clearing assignments for unavailable robots.
    
    For tasks assigned to unavailable robots:
    - If status is "Evaluation In Progress" and assigned to a down robot, reset to "pending"
    - Clear the Robot ID (col J) assignment
    
    Args:
        models_worksheet: Eval Queue worksheet
        tasks: List of all tasks
        unavailable_robots: List of robot IDs that are down/unavailable
    
    Returns:
        Number of tasks updated
    """
    if not unavailable_robots:
        return 0
    
    unavailable_set = set(robot for robot in unavailable_robots)
    updates = []
    updated_count = 0
    
    for task in tasks:
        # Check if this task is assigned to an unavailable robot
        if task.robot and task.robot in unavailable_set:
            # Clear Robot ID (col J)
            robot_cell = models_worksheet.cell(task.row, COL_ROBOT_ID)
            robot_cell.value = ""
            updates.append(robot_cell)
            
            # If status is IN_PROGRESS and assigned to down robot, reset to pending
            if task.status == "IN_PROGRESS":
                status_cell = models_worksheet.cell(task.row, COL_STATUS)
                status_cell.value = "pending"
                updates.append(status_cell)
                logger.info(
                    f"Row {task.row} ({task.model}): Reset from IN_PROGRESS to pending "
                    f"(robot {task.robot} is unavailable)"
                )
                updated_count += 1
            else:
                logger.debug(f"Row {task.row}: Cleared Robot ID assignment (robot {task.robot} is unavailable)")
                updated_count += 1
    
    if updates:
        models_worksheet.update_cells(updates, value_input_option="USER_ENTERED")
        logger.info(f"Cleared assignments for {updated_count} tasks assigned to unavailable robots")
    
    return updated_count


# ============================================================================
# SCHEDULING LOGIC
# ============================================================================


def get_running_ports(tasks: List[TaskRow]) -> Dict[Tuple[str, str], str]:
    """
    Get a map of (model, port) -> robot for currently running tasks.
    
    Args:
        tasks: List of all tasks
    
    Returns:
        Dictionary mapping (model, port) tuples to robot IDs
    """
    used = {}
    for task in tasks:
        if task.status == "IN_PROGRESS" and task.port:
            key = (task.model, task.port)
            used[key] = task.robot
    logger.debug(f"Found {len(used)} active port conflicts")
    return used


def find_next_task(
    robot: str,
    tasks: List[TaskRow],
    used_ports: Dict[Tuple[str, str], str],
    skip_reasons: List[str],
    exclude_rows: Optional[Set[int]] = None
) -> Optional[int]:
    """
    Find the next task to assign to a robot.
    
    Priority:
    1. ALL4 tasks that this robot hasn't completed yet (must be PENDING)
    2. Normal pending tasks
    
    Only tasks with PENDING status are scheduled.
    
    Args:
        robot: Robot ID (25-29)
        tasks: List of all tasks
        used_ports: Map of (model, port) -> robot for active tasks
        skip_reasons: List to append skip reasons to (for logging)
        exclude_rows: Set of row numbers to exclude (already assigned in this run)
    
    Returns:
        Row number (1-indexed) of next task, or None if no task available
    """
    if exclude_rows is None:
        exclude_rows = set()
    
    # Phase 1: ALL4 tasks first
    # For ALL4 tasks, we allow scheduling even if status is "IN_PROGRESS" (another robot running it)
    # as long as this robot hasn't completed it and there's no port conflict
    for task in tasks:
        if not task.all4:
            continue
        # For ALL4 tasks, we allow multiple robots to be assigned in the same run
        # (they'll run sequentially, with notifications about when to start)
        # Only exclude if this is a normal task that's already assigned
        if task.row in exclude_rows:
            continue
        # Skip if COMPLETED (all robots done) or BLOCKED
        if task.status == "COMPLETED":
            skip_reasons.append(f"Row {task.row} ({task.model}): ALL4 task completed by all robots")
            continue
        if task.status == "BLOCKED":
            skip_reasons.append(f"Row {task.row} ({task.model}): ALL4 task is blocked")
            continue
        # Allow PENDING and IN_PROGRESS status for ALL4 tasks
        # (IN_PROGRESS means another robot is running it, which is OK for ALL4 tasks)
        if robot in task.done_list:
            skip_reasons.append(f"Row {task.row} ({task.model}): ALL4 task already done by {robot}")
            continue
        
        # For ALL4 tasks, we allow assignment even if another robot is running it (they run sequentially)
        # Only check port conflict if this specific robot is already using that port for another task
        if (task.model, task.port) in used_ports:
            other_robot = used_ports[(task.model, task.port)]
            # If the port is being used by THIS robot for a different task, skip (robot can't run two tasks simultaneously)
            if other_robot == robot:
                skip_reasons.append(
                    f"Row {task.row} ({task.model}): Robot {robot} already using model {task.model} on port {task.port}"
                )
                continue
            # If another robot is using it, that's OK for ALL4 tasks - they'll run sequentially
            # Log it but don't skip
            logger.debug(
                f"Row {task.row} ({task.model}): ALL4 task - port {task.port} busy with {other_robot}, "
                f"but allowing assignment to {robot} (will run sequentially)"
            )
        
        # Check if another robot is currently running this ALL4 task
        # Add a notification message for when the next robot can start
        if task.status == "IN_PROGRESS" and task.robot and task.robot != robot:
            if task.robot not in task.done_list:
                # Another robot is running it - add notification message
                skip_reasons.append(
                    f"Row {task.row} ({task.model}): ⚠️ NOTE - Robot {task.robot} is currently running this ALL4 task. "
                    f"Wait for Robot {task.robot} to add '{task.robot}' to Operator column (Q) before starting."
                )
                # Still assign it, but with the notification
                logger.info(
                    f"Row {task.row} ({task.model}): Assigned to {robot}, but Robot {task.robot} is still running. "
                    f"Wait for '{task.robot}' to be added to Operator column before starting."
                )
        # Found a valid ALL4 task (can be PENDING or IN_PROGRESS)
        return task.row
    
    # Phase 2: Normal pending tasks
    for task in tasks:
        if task.all4:
            continue
        # Skip if already assigned in this run
        if task.row in exclude_rows:
            skip_reasons.append(f"Row {task.row} ({task.model}): Already assigned to another robot in this run")
            continue
        if task.status != "PENDING":
            if task.status == "IN_PROGRESS":
                skip_reasons.append(f"Row {task.row} ({task.model}): Already in progress")
            elif task.status == "COMPLETED":
                skip_reasons.append(f"Row {task.row} ({task.model}): Already completed")
            else:
                skip_reasons.append(f"Row {task.row} ({task.model}): Status is {task.status}")
            continue
        if (task.model, task.port) in used_ports:
            other_robot = used_ports[(task.model, task.port)]
            skip_reasons.append(
                f"Row {task.row} ({task.model}): Port {task.port} busy (used by {other_robot})"
            )
            continue
        # Found a valid normal task
        return task.row
    
    return None


def update_eval_queue_for_assignments(
    worksheet: gspread.Worksheet,
    assignments: Dict[str, Optional[int]]
) -> None:
    """
    Update Eval Queue sheet for assigned tasks:
    - Set Robot ID (col J) if not already set
    - Set Status (col M) to "pending" if empty
    
    Args:
        worksheet: Eval Queue worksheet
        assignments: Dictionary mapping robot IDs to row numbers (or None)
    
    Raises:
        gspread.exceptions.APIError: If API update fails
    """
    try:
        updates = []
        assigned_rows = set()
        
        # Collect all assigned rows
        for robot, row_num in assignments.items():
            if row_num is not None:
                assigned_rows.add((row_num, robot))
        
        if not assigned_rows:
            return
        
        # Get current values for assigned rows
        for row_num, robot in assigned_rows:
            # Get current Robot ID and Status
            robot_cell = worksheet.cell(row_num, COL_ROBOT_ID)
            status_cell = worksheet.cell(row_num, COL_STATUS)
            
            current_robot = (robot_cell.value or "").strip()
            current_status = (status_cell.value or "").strip()
            
            # Update Robot ID if empty
            if not current_robot:
                robot_cell.value = robot
                updates.append(robot_cell)
                logger.debug(f"Row {row_num}: Set Robot ID to {robot}")
            
            # Update Status to "pending" if empty
            if not current_status:
                status_cell.value = "pending"
                updates.append(status_cell)
                logger.debug(f"Row {row_num}: Set Status to 'pending'")
        
        if updates:
            worksheet.update_cells(updates, value_input_option="USER_ENTERED")
            logger.info(f"Updated Eval Queue for {len(assigned_rows)} assigned tasks")
    except Exception as e:
        logger.error(f"Failed to update Eval Queue: {e}")
        raise


def update_current_rows(
    robots_worksheet: gspread.Worksheet,
    tasks: List[TaskRow]
) -> None:
    """
    Update CurrentRow column (C) and Status column (B) in Robots sheet based on tasks currently in progress.
    
    Scans tasks for those with status "IN_PROGRESS" and updates the CurrentRow
    for the corresponding robot in the Robots sheet. Also updates Status to "RUNNING" 
    when a robot has a CurrentRow, and "IDLE" when CurrentRow is cleared.
    
    Args:
        robots_worksheet: Robots worksheet
        tasks: List of all tasks from Eval Queue
    
    Raises:
        gspread.exceptions.APIError: If API update fails
    """
    try:
        # Find all robots currently running tasks
        robot_to_row = {}
        for task in tasks:
            if task.status == "IN_PROGRESS" and task.robot:
                robot_to_row[task.robot] = task.row
        
        # Get all robots from the sheet
        rows = robots_worksheet.get_all_values()
        if len(rows) < 2:
            logger.warning("Robots sheet has no data rows")
            return
        
        updates = []
        # Skip header row (index 0), start from row 2 (1-indexed)
        for i, row in enumerate(rows[1:], start=2):
            if not row:
                continue
            robot_name = row[0].strip() if row[0] else ""
            if not robot_name:
                continue
            
            current_row_cell = robots_worksheet.cell(i, 3)  # Column C = CurrentRow
            status_cell = robots_worksheet.cell(i, 2)  # Column B = Status
            current_value = (current_row_cell.value or "").strip()
            current_status = (status_cell.value or "").strip().upper()
            
            if robot_name in robot_to_row:
                # Robot is running a task - update CurrentRow
                new_value = str(robot_to_row[robot_name])
                if current_value != new_value:
                    current_row_cell.value = new_value
                    updates.append(current_row_cell)
                    logger.info(f"Updated CurrentRow for robot {robot_name} to {new_value}")
                    # Update the current_value for status check below
                    current_value = new_value
            else:
                # Robot is not running a task - clear CurrentRow if it's set
                if current_value:
                    current_row_cell.value = ""
                    updates.append(current_row_cell)
                    logger.debug(f"Cleared CurrentRow for robot {robot_name} (no active task)")
                    current_value = ""  # Update for status check below
            
            # Update status based on CurrentRow value (regardless of robot_to_row)
            # This handles cases where CurrentRow is set but task might not be IN_PROGRESS
            if current_value:
                # Robot has a CurrentRow - should be RUNNING (unless DOWN/OFFLINE)
                if current_status not in ("RUNNING", "DOWN", "OFFLINE"):
                    status_cell.value = "RUNNING"
                    updates.append(status_cell)
                    logger.info(f"Updated Status for robot {robot_name} to RUNNING (has CurrentRow: {current_value})")
            else:
                # Robot has no CurrentRow - should be IDLE (if currently RUNNING)
                if current_status == "RUNNING":
                    status_cell.value = "IDLE"
                    updates.append(status_cell)
                    logger.info(f"Updated Status for robot {robot_name} to IDLE (no CurrentRow)")
        
        if updates:
            robots_worksheet.update_cells(updates, value_input_option="USER_ENTERED")
            logger.info(f"Updated CurrentRow and Status for {len(set([u.row for u in updates]))} robots")
    except Exception as e:
        logger.error(f"Failed to update CurrentRow: {e}")
        raise


def update_robot_sheet(
    worksheet: gspread.Worksheet,
    assignments: Dict[str, Optional[int]]
) -> None:
    """
    Update NextRow column (D) in Robots sheet with new assignments.
    
    Updates all robots in the sheet:
    - If robot is in assignments, use that value (or empty string if None)
    - If robot is not in assignments, clear NextRow (set to empty string)
    
    Args:
        worksheet: Robots worksheet
        assignments: Dictionary mapping robot IDs to row numbers (or None)
    
    Raises:
        gspread.exceptions.APIError: If API update fails
    """
    try:
        rows = worksheet.get_all_values()
        if len(rows) < 2:  # Need at least header + 1 data row
            logger.warning("Robots sheet has no data rows")
            return
        
        updates = []
        # Skip header row (index 0), start from row 2 (1-indexed)
        for i, row in enumerate(rows[1:], start=2):
            if not row:
                continue
            robot_name = row[0].strip() if row[0] else ""
            if not robot_name:
                continue
            
            cell = worksheet.cell(i, 4)  # Column D = NextRow
            current_value = (cell.value or "").strip()
            
            if robot_name in assignments:
                # Robot is in assignments - update to the assigned value
                val = assignments[robot_name]
                new_value = "" if val is None else str(val)
            else:
                # Robot is not in assignments - clear NextRow
                new_value = ""
            
            # Only update if the value has changed
            if current_value != new_value:
                cell.value = new_value
                updates.append(cell)
                if new_value:
                    logger.debug(f"Updated NextRow for robot {robot_name} to {new_value}")
                else:
                    logger.debug(f"Cleared NextRow for robot {robot_name}")
        
        if updates:
            worksheet.update_cells(updates, value_input_option="USER_ENTERED")
            logger.info(f"Updated NextRow for {len(updates)} robots in Robots sheet")
        else:
            logger.debug("No NextRow updates needed (all values already correct)")
    except Exception as e:
        logger.error(f"Failed to update robot sheet: {e}")
        raise


def update_all4_task_status(
    models_worksheet: gspread.Worksheet,
    tasks: List[TaskRow],
    available_robots: List[str]
) -> int:
    """
    Update status of ALL4 tasks to "Can run now" when appropriate and assign next robot.
    
    For ALL4 tasks:
    - If status is "pending" or "in progress"
    - AND at least one robot has completed (robots_done has entries)
    - AND not all available robots are done yet
    - Then update status to "Can run now" and assign next available robot to column J
    
    Args:
        models_worksheet: Eval Queue worksheet
        tasks: List of all tasks
        available_robots: List of available robot IDs
    
    Returns:
        Number of tasks updated
    """
    try:
        updates = []
        updated_count = 0
        available_robots_set = set(available_robots)
        
        for task in tasks:
            if not task.all4:
                continue
            
            # Only update if status is PENDING or IN_PROGRESS
            if task.status not in ("PENDING", "IN_PROGRESS"):
                continue
            
            # Check if at least one robot has completed
            if not task.done_list:
                continue
            
            # Check if all available robots are done
            done_set = set(task.done_list)
            all_available_done = available_robots_set.issubset(done_set)
            
            # If not all available robots are done, check if we need to update status
            if not all_available_done:
                # Get current Robot ID from the sheet
                robot_cell = models_worksheet.cell(task.row, COL_ROBOT_ID)
                current_robot_id = (robot_cell.value or "").strip()
                
                # Check current status value in sheet (not normalized)
                status_cell = models_worksheet.cell(task.row, COL_STATUS)
                current_status = (status_cell.value or "").strip().lower()
                
                # Only update to "Can run now" if:
                # 1. Current robot has finished (is in done_list), OR
                # 2. No robot is assigned and status is pending/in_progress
                # Don't change status if current robot is still running (not in done_list)
                current_robot_finished = current_robot_id and current_robot_id in done_set
                should_update_status = (
                    current_robot_finished or  # Current robot finished
                    (not current_robot_id and current_status in ("pending", "in progress", "evaluation in progress"))  # No robot assigned
                )
                
                if should_update_status and current_status in ("pending", "in progress", "evaluation in progress"):
                    old_status = status_cell.value  # Keep original for logging
                    status_cell.value = "Can run now"
                    updates.append(status_cell)
                    
                    # Find next available robot that hasn't completed this task
                    next_robot = None
                    for robot in available_robots:
                        if robot not in done_set:
                            next_robot = robot
                            break
                    
                    # Update Robot ID (column J) to next robot
                    # Only update if current robot finished or no robot assigned
                    if next_robot and (current_robot_finished or not current_robot_id):
                        robot_cell.value = next_robot
                        updates.append(robot_cell)
                        logger.info(
                            f"Row {task.row} ({task.model}): Updated status from '{old_status}' to 'Can run now' "
                            f"and assigned Robot ID from '{current_robot_id}' to {next_robot} "
                            f"(robots done: {','.join(task.done_list)}, available: {','.join(sorted(available_robots))})"
                        )
                    elif not next_robot:
                        logger.warning(
                            f"Row {task.row} ({task.model}): No available robot found to assign "
                            f"(robots done: {','.join(task.done_list)}, available: {','.join(sorted(available_robots))})"
                        )
                    
                    updated_count += 1
                elif current_robot_id and current_robot_id not in done_set:
                    # Current robot is still running - don't change status
                    logger.debug(
                        f"Row {task.row} ({task.model}): Robot {current_robot_id} is still running, "
                        f"not updating status (robots done: {','.join(task.done_list)})"
                    )
        
        if updates:
            models_worksheet.update_cells(updates, value_input_option="USER_ENTERED")
            logger.info(f"Updated status to 'Can run now' and Robot ID for {updated_count} ALL4 tasks")
        
        return updated_count
    except Exception as e:
        logger.error(f"Failed to update ALL4 task status: {e}")
        raise


# ============================================================================
# MAIN SCHEDULER
# ============================================================================


def schedule_tasks(
    tasks: List[TaskRow],
    used_ports: Dict[Tuple[str, str], str],
    available_robots: List[str]
) -> Dict[str, AssignmentResult]:
    """
    Optimized scheduler that assigns tasks to minimize idle time and respect all constraints.
    
    Scheduling Rules:
    1. Model + Port conflict: Same (model, port) = only one robot at a time
    2. Port-only: Same port, different model = can run in parallel
    3. ALL4 tasks: Must run on all robots, but only one robot per ALL4 task per run (sequential)
    4. Optimization: Minimize idle time, balance load, finish in minimum total time
    
    Args:
        tasks: List of all tasks
        used_ports: Map of active (model, port) -> robot
        available_robots: List of robot IDs that are available for scheduling
    
    Returns:
        Dictionary mapping robot IDs to AssignmentResult objects
    """
    assignments = {}
    assigned_tasks = set()  # All tasks assigned in this run (to prevent duplicates)
    assigned_all4_tasks = set()  # ALL4 tasks assigned in this run (only one robot per ALL4 task)
    assigned_ports = used_ports.copy()  # Track (model, port) assignments
    assigned_ports_only = set()  # Track ports in use (for port-only conflict checking)
    
    # Separate tasks into ALL4 and normal
    all4_tasks = [t for t in tasks if t.all4 and t.status not in ("COMPLETED", "BLOCKED")]
    normal_tasks = [t for t in tasks if not t.all4 and t.status == "PENDING"]
    
    # Build assignments for each robot
    for robot in available_robots:
        skip_reasons = []
        assigned_row = None
        
        # Strategy: Try to assign different tasks to different robots to maximize parallelism
        # Priority: ALL4 tasks first (but only one robot per ALL4 task), then normal tasks
        
        # Phase 1: Try to assign an ALL4 task that this robot hasn't completed
        # But only if no other robot has been assigned this ALL4 task in this run
        for task in all4_tasks:
            if task.row in assigned_all4_tasks:
                continue  # This ALL4 task already assigned to another robot in this run
            if robot in task.done_list:
                skip_reasons.append(f"Row {task.row} ({task.model}): ALL4 task already done by {robot}")
                continue
            
            # Check if this robot can run it (model+port conflict)
            if task.port and (task.model, task.port) in assigned_ports:
                other_robot = assigned_ports[(task.model, task.port)]
                if other_robot == robot:
                    # This robot is already using this (model, port) for another task
                    skip_reasons.append(
                        f"Row {task.row} ({task.model}): Robot {robot} already using model {task.model} on port {task.port}"
                    )
                    continue
            
            # Check if another robot is currently running this ALL4 task
            if task.status == "IN_PROGRESS" and task.robot and task.robot != robot:
                if task.robot not in task.done_list:
                    # Another robot is running it - add notification but still assign
                    skip_reasons.append(
                        f"Row {task.row} ({task.model}): ⚠️ NOTE - Robot {task.robot} is currently running this ALL4 task. "
                        f"Wait for Robot {task.robot} to add '{task.robot}' to Operator column (Q) before starting."
                    )
            
            # Can assign this ALL4 task to this robot
            assigned_row = task.row
            assigned_all4_tasks.add(task.row)
            assigned_tasks.add(task.row)
            if task.port:
                assigned_ports[(task.model, task.port)] = robot
                assigned_ports_only.add(task.port)
            break
        
        # Phase 2: If no ALL4 task assigned, try normal tasks
        # Assign different tasks to different robots to maximize parallelism
        if not assigned_row:
            for task in normal_tasks:
                if task.row in assigned_tasks:
                    skip_reasons.append(f"Row {task.row} ({task.model}): Already assigned to another robot in this run")
                    continue
                
                # Check model+port conflict: same (model, port) = only one robot
                if task.port and (task.model, task.port) in assigned_ports:
                    other_robot = assigned_ports[(task.model, task.port)]
                    skip_reasons.append(
                        f"Row {task.row} ({task.model}): Model {task.model} on port {task.port} busy (used by {other_robot})"
                    )
                    continue
                
                # Port-only conflict: same port, different model = OK (can run in parallel)
                # This is already handled - we only check (model, port) conflicts above
                
                # Can assign this normal task to this robot
                assigned_row = task.row
                assigned_tasks.add(task.row)
                if task.port:
                    assigned_ports[(task.model, task.port)] = robot
                    assigned_ports_only.add(task.port)
                break
        
        assignments[robot] = AssignmentResult(
            robot=robot,
            assigned_row=assigned_row,
            skip_reasons=skip_reasons
        )
    
    return assignments


def print_summary(
    assignments: Dict[str, AssignmentResult],
    available_robots: List[str],
    unavailable_robots: List[str]
) -> None:
    """
    Print a summary of scheduling results.
    
    Args:
        assignments: Dictionary of assignment results per robot
        available_robots: List of available robot IDs
        unavailable_robots: List of unavailable robot IDs
    """
    print("\n" + "=" * 80)
    print("SCHEDULING SUMMARY")
    print("=" * 80)
    
    if unavailable_robots:
        print(f"\n⚠ Unavailable robots (will not receive assignments): {', '.join(unavailable_robots)}")
    
    assigned_count = 0
    for robot, result in assignments.items():
        if result.assigned_row:
            # Check if there are any notifications about waiting for another robot
            notifications = [r for r in result.skip_reasons if "NOTE" in r or "Wait for" in r]
            if notifications:
                print(f"\n✓ {robot}: Assigned to row {result.assigned_row}")
                for note in notifications:
                    print(f"  {note}")
            else:
                print(f"\n✓ {robot}: Assigned to row {result.assigned_row}")
            assigned_count += 1
        else:
            print(f"\n✗ {robot}: No task assigned")
            if result.skip_reasons:
                print(f"  Skipped tasks:")
                for reason in result.skip_reasons[:5]:  # Show first 5 reasons
                    print(f"    - {reason}")
                if len(result.skip_reasons) > 5:
                    print(f"    ... and {len(result.skip_reasons) - 5} more")
    
    print(f"\n{'=' * 80}")
    print(f"Total assignments: {assigned_count}/{len(available_robots)} available robots")
    print("=" * 80 + "\n")


def main() -> None:
    """Main entry point for the scheduler."""
    try:
        logger.info("Starting scheduler...")
        
        # Connect to Google Sheets
        client = get_client()
        
        # Load data
        ws_models, tasks = load_models(client)
        ws_robots = load_robots(client)
        
        # Get available and unavailable robots from Robots sheet
        available_robots, all_robot_statuses, unavailable_robots = get_available_robots(ws_robots)
        
        if not available_robots:
            logger.warning("No available robots found! Please check the Robots sheet.")
            print("\n⚠ WARNING: No available robots found in Robots sheet.")
            print("Robots must have Status='IDLE' or 'RUNNING' to receive assignments.")
            print("Other statuses (DOWN, OFFLINE, etc.) mark robots as unavailable.\n")
            return
        
        # Handle robot downtime: clear assignments for unavailable robots
        if unavailable_robots:
            updated_count = handle_robot_downtime(ws_models, tasks, unavailable_robots)
            if updated_count > 0:
                # Reload tasks after clearing assignments
                ws_models, tasks = load_models(client)
                logger.info(f"Reloaded tasks after clearing {updated_count} assignments for unavailable robots")
        
        # Update ALL4 task status to "Can run now" when appropriate
        all4_status_updates = update_all4_task_status(ws_models, tasks, available_robots)
        if all4_status_updates > 0:
            # Reload tasks after status updates
            ws_models, tasks = load_models(client)
            logger.info(f"Reloaded tasks after updating {all4_status_updates} ALL4 task statuses")
        
        # Get active port conflicts
        used_ports = get_running_ports(tasks)
        
        # Schedule tasks only for available robots
        assignments = schedule_tasks(tasks, used_ports, available_robots)
        
        # Update CurrentRow in Robots sheet based on tasks currently in progress
        update_current_rows(ws_robots, tasks)
        
        # Update Robots sheet (only for available robots)
        assignment_dict = {result.robot: result.assigned_row for result in assignments.values()}
        # Also set NextRow to empty for unavailable robots
        for robot_status in all_robot_statuses:
            if not robot_status.is_available:
                assignment_dict[robot_status.robot_id] = None
        update_robot_sheet(ws_robots, assignment_dict)
        
        # Update Eval Queue: set Robot ID and Status for assigned tasks
        update_eval_queue_for_assignments(ws_models, assignment_dict)
        
        # Print summary
        print_summary(assignments, available_robots, unavailable_robots)
        
        logger.info("Scheduler completed successfully")
        
    except FileNotFoundError as e:
        logger.error(f"Configuration error: {e}")
        logger.error(f"Please ensure '{CREDENTIALS_FILE}' exists in the current directory")
        sys.exit(1)
    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"Spreadsheet '{SPREADSHEET_NAME}' not found")
        logger.error("Please check SPREADSHEET_NAME in the script configuration")
        sys.exit(1)
    except gspread.exceptions.WorksheetNotFound as e:
        logger.error(f"Worksheet not found: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
