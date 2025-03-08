import os
import sys
import json
import time
import logging
import traceback
from datetime import datetime
from typing import Dict, Any, Optional, Union, List, Tuple
from functools import wraps

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Configure main logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/lead_generation.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Create error logger
error_logger = logging.getLogger('error_logger')
error_logger.setLevel(logging.ERROR)
error_handler = logging.FileHandler('logs/error_log.txt')
error_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
error_logger.addHandler(error_handler)

# Create activity logger for metrics
activity_logger = logging.getLogger('activity_logger')
activity_logger.setLevel(logging.INFO)
activity_handler = logging.FileHandler('logs/activity_metrics.jsonl')
activity_logger.addHandler(activity_handler)

# Create performance logger
performance_logger = logging.getLogger('performance_logger')
performance_logger.setLevel(logging.INFO)
performance_handler = logging.FileHandler('logs/performance_metrics.log')
performance_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
performance_logger.addHandler(performance_handler)


class LogManager:
    """
    Central logging management system for lead generation scripts.
    Provides standardized logging across all modules.
    """
    
    def __init__(self, module_name: str):
        """
        Initialize the log manager for a specific module.
        
        Args:
            module_name: Name of the module using this logger
        """
        self.module_name = module_name
        self.logger = logging.getLogger(module_name)
        self.start_time = None
        self.metrics = {}
        
    def start_operation(self, operation_name: str) -> None:
        """
        Log the start of an operation and record start time.
        
        Args:
            operation_name: Name of the operation being started
        """
        self.start_time = time.time()
        self.operation_name = operation_name
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Initialize metrics for this operation
        self.metrics = {
            "module": self.module_name,
            "operation": operation_name,
            "status": "started",
            "start_time": timestamp,
            "end_time": None,
            "duration_seconds": None,
            "leads_scraped": 0,
            "messages_generated": 0,
            "emails_sent": 0,
            "leads_scored": 0,
            "high_priority_leads": 0,
            "errors": 0,
            "details": {}
        }
        
        # Log start of operation
        self.logger.info(f"Starting {operation_name}")
        
        # Log metrics
        activity_logger.info(json.dumps(self.metrics))
        
    def end_operation(self, success: bool = True, details: Dict[str, Any] = None) -> None:
        """
        Log the end of an operation and record metrics.
        
        Args:
            success: Whether the operation was successful
            details: Additional details about the operation
        """
        if self.start_time is None:
            self.logger.warning("end_operation called without start_operation")
            return
            
        end_time = time.time()
        duration = end_time - self.start_time
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Update metrics
        self.metrics["status"] = "completed" if success else "failed"
        self.metrics["end_time"] = timestamp
        self.metrics["duration_seconds"] = round(duration, 2)
        
        if details:
            self.metrics["details"].update(details)
        
        # Log end of operation
        status_msg = "successfully" if success else "with errors"
        self.logger.info(f"Finished {self.operation_name} {status_msg} in {duration:.2f} seconds")
        
        # Log performance metrics
        performance_logger.info(
            f"{self.module_name} - {self.operation_name}: {duration:.2f} seconds"
        )
        
        # Log activity metrics
        activity_logger.info(json.dumps(self.metrics))
        
        # Reset start time
        self.start_time = None
        
    def update_metrics(self, metrics_update: Dict[str, Any]) -> None:
        """
        Update operation metrics during execution.
        
        Args:
            metrics_update: Dictionary with metrics to update
        """
        if not self.metrics:
            self.logger.warning("update_metrics called without active operation")
            return
            
        # Update metrics
        for key, value in metrics_update.items():
            if key in self.metrics:
                self.metrics[key] = value
            else:
                self.metrics["details"][key] = value
        
        # Log interim metrics
        activity_logger.info(json.dumps(self.metrics))
        
    def log_error(self, error: Union[str, Exception], details: Dict[str, Any] = None) -> None:
        """
        Log an error to both regular and error logs.
        
        Args:
            error: Error message or exception
            details: Additional context for the error
        """
        # Increment error count in metrics
        self.metrics["errors"] = self.metrics.get("errors", 0) + 1
        
        # Format error message
        if isinstance(error, Exception):
            error_msg = f"{type(error).__name__}: {str(error)}"
            stack_trace = traceback.format_exc()
        else:
            error_msg = str(error)
            stack_trace = "".join(traceback.format_stack()[:-1])
            
        # Add context details
        context = {
            "module": self.module_name,
            "operation": getattr(self, "operation_name", "unknown"),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        if details:
            context.update(details)
            
        context_str = json.dumps(context, indent=2)
        
        # Log to regular logger
        self.logger.error(f"Error: {error_msg}")
        
        # Log to error logger with full context
        error_logger.error(
            f"ERROR: {error_msg}\n"
            f"CONTEXT: {context_str}\n"
            f"TRACEBACK: {stack_trace}\n"
            f"{'-' * 80}"
        )
        
    def log_info(self, message: str) -> None:
        """
        Log an informational message.
        
        Args:
            message: Message to log
        """
        self.logger.info(message)
        
    def log_warning(self, message: str) -> None:
        """
        Log a warning message.
        
        Args:
            message: Warning message to log
        """
        self.logger.warning(message)
        
    def log_debug(self, message: str) -> None:
        """
        Log a debug message.
        
        Args:
            message: Debug message to log
        """
        self.logger.debug(message)


def operation_logger(func):
    """
    Decorator to automatically log operation start/end and handle errors.
    
    Args:
        func: Function to wrap with logging
        
    Returns:
        Wrapped function with logging
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Get module name from function
        module_name = func.__module__
        
        # Create log manager
        log_manager = LogManager(module_name)
        
        # Add log_manager to kwargs if not already present
        if 'log_manager' not in kwargs:
            kwargs['log_manager'] = log_manager
            
        # Start operation
        operation_name = func.__name__
        log_manager.start_operation(operation_name)
        
        try:
            # Call the function
            result = func(*args, **kwargs)
            
            # Check if result contains metrics to log
            if isinstance(result, dict) and any(k in result for k in [
                'leads_scraped', 'messages_generated', 'emails_sent', 'leads_scored'
            ]):
                log_manager.update_metrics(result)
                
            # End operation
            log_manager.end_operation(success=True, details={'result': str(result)[:1000]})
            
            return result
            
        except Exception as e:
            # Log error
            log_manager.log_error(e)
            
            # End operation with failure
            log_manager.end_operation(success=False, details={'error': str(e)})
            
            # Re-raise the exception
            raise
            
    return wrapper


class MetricsTracker:
    """
    Tracks metrics across operations and generates reports.
    """
    
    @staticmethod
    def get_daily_metrics(date_str: Optional[str] = None) -> Dict[str, Any]:
        """
        Get metrics for a specific day.
        
        Args:
            date_str: Date string in format YYYY-MM-DD (default: today)
            
        Returns:
            Dictionary with aggregated metrics
        """
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
            
        metrics = {
            "date": date_str,
            "total_operations": 0,
            "successful_operations": 0,
            "failed_operations": 0,
            "leads_scraped": 0,
            "messages_generated": 0,
            "emails_sent": 0,
            "leads_scored": 0,
            "high_priority_leads": 0,
            "total_errors": 0,
            "operation_counts": {},
            "module_usage": {}
        }
        
        try:
            with open('logs/activity_metrics.jsonl', 'r') as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        
                        # Skip if no start_time (malformed entry)
                        if "start_time" not in data:
                            continue
                            
                        # Check if this entry is from the requested date
                        entry_date = data["start_time"].split()[0]
                        if entry_date != date_str:
                            continue
                            
                        # Count operations
                        metrics["total_operations"] += 1
                        
                        if data["status"] == "completed":
                            metrics["successful_operations"] += 1
                        elif data["status"] == "failed":
                            metrics["failed_operations"] += 1
                            
                        # Count errors
                        metrics["total_errors"] += data.get("errors", 0)
                        
                        # Aggregate metrics
                        metrics["leads_scraped"] += data.get("leads_scraped", 0)
                        metrics["messages_generated"] += data.get("messages_generated", 0)
                        metrics["emails_sent"] += data.get("emails_sent", 0)
                        metrics["leads_scored"] += data.get("leads_scored", 0)
                        metrics["high_priority_leads"] += data.get("high_priority_leads", 0)
                        
                        # Count by operation type
                        operation = data.get("operation", "unknown")
                        metrics["operation_counts"][operation] = metrics["operation_counts"].get(operation, 0) + 1
                        
                        # Count by module
                        module = data.get("module", "unknown")
                        metrics["module_usage"][module] = metrics["module_usage"].get(module, 0) + 1
                        
                    except json.JSONDecodeError:
                        continue
                        
            return metrics
            
        except FileNotFoundError:
            return metrics
            
    @staticmethod
    def get_error_summary(days: int = 1) -> List[Dict[str, Any]]:
        """
        Get summary of recent errors.
        
        Args:
            days: Number of days to look back
            
        Returns:
            List of error entries
        """
        cutoff_date = (datetime.now() - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
        errors = []
        
        try:
            with open('logs/error_log.txt', 'r') as f:
                current_error = None
                
                for line in f:
                    if line.startswith("ERROR:"):
                        # Start of new error
                        if current_error:
                            errors.append(current_error)
                            
                        current_error = {
                            "message": line[7:].strip(),
                            "context": {},
                            "traceback": []
                        }
                        
                    elif current_error:
                        if line.startswith("CONTEXT:"):
                            try:
                                # Try to parse context JSON
                                context_str = line[9:].strip()
                                context_lines = []
                                
                                # Handle multi-line context
                                while not context_str.endswith('}'):
                                    context_lines.append(context_str)
                                    line = next(f, "")
                                    context_str = line.strip()
                                    
                                context_lines.append(context_str)
                                context_str = " ".join(context_lines)
                                
                                current_error["context"] = json.loads(context_str)
                                
                            except (json.JSONDecodeError, StopIteration):
                                current_error["context"] = {"raw": line[9:].strip()}
                                
                        elif line.startswith("TRACEBACK:"):
                            current_error["traceback"].append(line[11:].strip())
                            
                        elif line.startswith("---"):
                            # End of error
                            errors.append(current_error)
                            current_error = None
                            
                        elif current_error.get("traceback"):
                            # Continue traceback
                            current_error["traceback"].append(line.strip())
                
                # Add last error if any
                if current_error:
                    errors.append(current_error)
                    
            # Filter by date
            filtered_errors = []
            for error in errors:
                error_date = error.get("context", {}).get("timestamp", "").split()[0]
                if error_date >= cutoff_date:
                    filtered_errors.append(error)
                    
            return filtered_errors
            
        except FileNotFoundError:
            return []
            
    @staticmethod
    def generate_report(days: int = 7) -> str:
        """
        Generate a comprehensive report of recent activity.
        
        Args:
            days: Number of days to include in the report
            
        Returns:
            Report as a formatted string
        """
        report = []
        report.append("=" * 80)
        report.append(f"LEAD GENERATION ACTIVITY REPORT - Last {days} days")
        report.append("=" * 80)
        
        # Get metrics for each day
        today = datetime.now()
        daily_metrics = []
        
        for i in range(days):
            date = (today - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
            metrics = MetricsTracker.get_daily_metrics(date)
            daily_metrics.append(metrics)
            
        # Overall summary
        total_leads = sum(m["leads_scraped"] for m in daily_metrics)
        total_messages = sum(m["messages_generated"] for m in daily_metrics)
        total_emails = sum(m["emails_sent"] for m in daily_metrics)
        total_scored = sum(m["leads_scored"] for m in daily_metrics)
        total_high_priority = sum(m["high_priority_leads"] for m in daily_metrics)
        total_errors = sum(m["total_errors"] for m in daily_metrics)
        
        report.append("\nOVERALL SUMMARY:")
        report.append(f"Total Leads Scraped:     {total_leads}")
        report.append(f"Total Messages Generated: {total_messages}")
        report.append(f"Total Emails Sent:       {total_emails}")
        report.append(f"Total Leads Scored:      {total_scored}")
        report.append(f"High Priority Leads:     {total_high_priority}")
        report.append(f"Total Errors:            {total_errors}")
        
        # Daily breakdown
        report.append("\nDAILY BREAKDOWN:")
        for metrics in daily_metrics:
            date = metrics["date"]
            report.append(f"\n{date}:")
            report.append(f"  Operations:      {metrics['successful_operations']} successful, {metrics['failed_operations']} failed")
            report.append(f"  Leads Scraped:   {metrics['leads_scraped']}")
            report.append(f"  Messages:        {metrics['messages_generated']}")
            report.append(f"  Emails:          {metrics['emails_sent']}")
            report.append(f"  Leads Scored:    {metrics['leads_scored']}")
            report.append(f"  High Priority:   {metrics['high_priority_leads']}")
            report.append(f"  Errors:          {metrics['total_errors']}")
            
        # Error summary
        errors = MetricsTracker.get_error_summary(days)
        report.append(f"\nERROR SUMMARY ({len(errors)} errors in the last {days} days):")
        
        for i, error in enumerate(errors[:10]):  # Show only first 10 errors
            message = error.get("message", "Unknown error")
            context = error.get("context", {})
            module = context.get("module", "unknown")
            operation = context.get("operation", "unknown")
            timestamp = context.get("timestamp", "unknown")
            
            report.append(f"\n{i+1}. {timestamp} - {module}.{operation}:")
            report.append(f"   {message}")
            
        if len(errors) > 10:
            report.append(f"\n... and {len(errors) - 10} more errors. See error_log.txt for details.")
            
        report.append("\n" + "=" * 80)
        
        return "\n".join(report)


# Example usage
if __name__ == "__main__":
    # Generate sample report
    report = MetricsTracker.generate_report(days=7)
    print(report)
    
    # Save report to file
    with open('logs/activity_report.txt', 'w') as f:
        f.write(report)
    
    # Example of usage with decorator
    @operation_logger
    def sample_linkedin_scrape(max_leads: int = 10, log_manager: LogManager = None):
        log_manager.log_info(f"Attempting to scrape {max_leads} leads from LinkedIn")
        
        # Simulate work
        time.sleep(2)
        
        # Update metrics during operation
        log_manager.update_metrics({"leads_scraped": 5})
        
        # More work
        time.sleep(1)
        
        # Final metrics
        return {
            "leads_scraped": 10,
            "linkedin_leads": 10,
            "success": True
        }
    
    # Run sample function
    try:
        result = sample_linkedin_scrape(max_leads=15)
        print(f"Result: {result}")
    except Exception as e:
        print(f"Error: {e}")
