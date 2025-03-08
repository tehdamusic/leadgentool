import os
import sys
import time
import logging
import threading
import webbrowser
import subprocess
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from datetime import datetime
from typing import Callable, Dict, Any, List

from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='lead_generation_gui.log'
)
logger = logging.getLogger('lead_generation_gui')

# Load environment variables
load_dotenv()

class RedirectText:
    """Class for redirecting print outputs to a tkinter Text widget."""
    
    def __init__(self, text_widget):
        self.text_widget = text_widget
        self.buffer = ""
        
    def write(self, string):
        self.buffer += string
        self.text_widget.config(state=tk.NORMAL)
        self.text_widget.insert(tk.END, string)
        self.text_widget.see(tk.END)  # Auto-scroll
        self.text_widget.config(state=tk.DISABLED)
        
    def flush(self):
        pass


class TaskManager:
    """Manages the execution of lead generation tasks."""
    
    def __init__(self):
        """Initialize the task manager."""
        self.tasks = {}
        
    def register_task(self, name: str, func: Callable, **kwargs):
        """
        Register a task with the task manager.
        
        Args:
            name: Task name
            func: Function to execute
            kwargs: Additional keyword arguments for the function
        """
        self.tasks[name] = {
            'func': func,
            'kwargs': kwargs
        }
        
    def run_task(self, name: str) -> Any:
        """
        Run a registered task.
        
        Args:
            name: Task name
            
        Returns:
            Return value from the task function
        """
        if name not in self.tasks:
            raise ValueError(f"Task '{name}' not registered")
            
        task = self.tasks[name]
        return task['func'](**task['kwargs'])


class LeadGenerationGUI:
    """GUI for controlling lead generation scripts."""
    
    def __init__(self, root):
        """
        Initialize the GUI.
        
        Args:
            root: Tkinter root window
        """
        self.root = root
        self.root.title("Lead Generation Control Panel")
        self.root.geometry("800x600")
        self.root.minsize(700, 500)
        
        # Set icon if available
        try:
            self.root.iconbitmap("lead_gen_icon.ico")
        except:
            pass  # Ignore if icon file not found
        
        # Configure colors and styles
        self.bg_color = "#f5f5f5"
        self.header_color = "#2c3e50"
        self.button_color = "#3498db"
        self.button_active_color = "#2980b9"
        self.section_bg = "#ffffff"
        
        self.root.configure(bg=self.bg_color)
        
        # Create task manager
        self.task_manager = TaskManager()
        
        # Create and register tasks
        self.register_tasks()
        
        # Create GUI elements
        self.create_widgets()
        
        # Track running tasks
        self.running_tasks = []
        
        # Setup redirection for print statements
        self.setup_console_redirect()
        
        logger.info("GUI initialized")
        
    def setup_console_redirect(self):
        """Set up redirection of print statements to the log console."""
        self.stdout_backup = sys.stdout
        self.redirect = RedirectText(self.log_console)
        sys.stdout = self.redirect
        
    def restore_console(self):
        """Restore original stdout."""
        sys.stdout = self.stdout_backup
        
    def register_tasks(self):
        """Register all the tasks with the task manager."""
        # Register all tasks
        # Import dependencies here to avoid circular imports
        try:
            # Import your modules here
            from scrapers.linkedin_scraper import run_linkedin_scraper
            from scrapers.reddit_scraper import run_reddit_scraper
            from ai.message_generator import run_message_generator
            from utils.lead_scorer import run_lead_scorer
            from utils.email_reporter import run_email_reporter
            from utils.sheets_manager import get_sheets_client
            
            # Get sheets client once to reuse
            self.sheets_client = get_sheets_client()
            
            # Register LinkedIn scraper task
            self.task_manager.register_task(
                "run_linkedin_scraper",
                run_linkedin_scraper,
                sheets_client=self.sheets_client,
                max_leads=20,
                headless=True
            )
            
            # Register Reddit scraper task
            self.task_manager.register_task(
                "run_reddit_scraper",
                run_reddit_scraper,
                sheets_client=self.sheets_client,
                max_leads=20,
                save_csv=True
            )
            
            # Register message generator task
            self.task_manager.register_task(
                "run_message_generator",
                run_message_generator,
                sheets_client=self.sheets_client,
                max_linkedin_leads=10,
                max_reddit_leads=10,
                model="gpt-4"
            )
            
            # Register lead scorer task
            self.task_manager.register_task(
                "run_lead_scorer",
                run_lead_scorer,
                sheets_client=self.sheets_client,
                max_linkedin_leads=20,
                max_reddit_leads=20,
                use_ai_analysis=True
            )
            
            # Register email reporter task
            self.task_manager.register_task(
                "run_email_reporter",
                run_email_reporter,
                sheets_client=self.sheets_client,
                days_back=1,
                response_days=7
            )
            
            logger.info("Tasks registered successfully")
            
        except Exception as e:
            logger.error(f"Error registering tasks: {str(e)}")
            messagebox.showerror("Error", f"Error registering tasks: {str(e)}")
        
    def create_widgets(self):
        """Create and arrange all GUI widgets."""
        # Main container
        main_frame = ttk.Frame(self.root, padding=10)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title frame
        title_frame = ttk.Frame(main_frame)
        title_frame.pack(fill=tk.X, pady=(0, 10))
        
        title_label = ttk.Label(
            title_frame, 
            text="Lead Generation Control Panel", 
            font=("Arial", 18, "bold")
        )
        title_label.pack(side=tk.LEFT, pady=5)
        
        # Create a notebook for tabs
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        # Create main tab
        main_tab = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(main_tab, text="Main Controls")
        
        # Create settings tab
        settings_tab = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(settings_tab, text="Settings")
        
        # Create logs tab
        logs_tab = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(logs_tab, text="Logs")
        
        # Main tab content
        self.create_main_tab(main_tab)
        
        # Settings tab content
        self.create_settings_tab(settings_tab)
        
        # Logs tab content
        self.create_logs_tab(logs_tab)
        
        # Status bar
        status_frame = ttk.Frame(main_frame)
        status_frame.pack(fill=tk.X, pady=(10, 0))
        
        self.status_label = ttk.Label(
            status_frame, 
            text="Ready", 
            relief=tk.SUNKEN, 
            anchor=tk.W
        )
        self.status_label.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        version_label = ttk.Label(
            status_frame, 
            text=f"v1.0.0", 
            relief=tk.SUNKEN, 
            anchor=tk.E
        )
        version_label.pack(side=tk.RIGHT)
        
    def create_main_tab(self, parent):
        """Create content for the main controls tab."""
        # Control buttons frame
        control_frame = ttk.LabelFrame(parent, text="Actions", padding=10)
        control_frame.pack(fill=tk.BOTH, expand=False, padx=5, pady=5)
        
        # Create 2x3 grid for buttons
        for i in range(2):
            control_frame.rowconfigure(i, weight=1)
        for i in range(3):
            control_frame.columnconfigure(i, weight=1)
        
        # LinkedIn Scraper button
        linkedin_btn = ttk.Button(
            control_frame,
            text="Run LinkedIn Scraper",
            command=lambda: self.run_task_async("run_linkedin_scraper", "LinkedIn Scraper")
        )
        linkedin_btn.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        
        # Reddit Scraper button
        reddit_btn = ttk.Button(
            control_frame,
            text="Run Reddit Scraper",
            command=lambda: self.run_task_async("run_reddit_scraper", "Reddit Scraper")
        )
        reddit_btn.grid(row=0, column=1, padx=5, pady=5, sticky="nsew")
        
        # Generate Messages button
        messages_btn = ttk.Button(
            control_frame,
            text="Generate AI Messages",
            command=lambda: self.run_task_async("run_message_generator", "AI Message Generator")
        )
        messages_btn.grid(row=0, column=2, padx=5, pady=5, sticky="nsew")
        
        # Score Leads button
        score_btn = ttk.Button(
            control_frame,
            text="Score Leads",
            command=lambda: self.run_task_async("run_lead_scorer", "Lead Scorer")
        )
        score_btn.grid(row=1, column=0, padx=5, pady=5, sticky="nsew")
        
        # Send Email Report button
        email_btn = ttk.Button(
            control_frame,
            text="Send Email Report",
            command=lambda: self.run_task_async("run_email_reporter", "Email Reporter")
        )
        email_btn.grid(row=1, column=1, padx=5, pady=5, sticky="nsew")
        
        # Open Google Sheets button
        sheets_btn = ttk.Button(
            control_frame,
            text="Open Google Sheets",
            command=self.open_google_sheets
        )
        sheets_btn.grid(row=1, column=2, padx=5, pady=5, sticky="nsew")
        
        # Progress container
        progress_frame = ttk.LabelFrame(parent, text="Progress", padding=10)
        progress_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Progress bar
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(
            progress_frame,
            variable=self.progress_var,
            mode='indeterminate'
        )
        self.progress_bar.pack(fill=tk.X, padx=5, pady=5)
        
        # Console output
        self.log_console = scrolledtext.ScrolledText(
            progress_frame,
            height=10,
            wrap=tk.WORD,
            background="#f8f8f8",
            font=("Consolas", 10)
        )
        self.log_console.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.log_console.config(state=tk.DISABLED)
        
        # Quick actions frame
        quick_frame = ttk.Frame(parent)
        quick_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # Clear log button
        clear_log_btn = ttk.Button(
            quick_frame,
            text="Clear Log",
            command=self.clear_log
        )
        clear_log_btn.pack(side=tk.LEFT, padx=5)
        
        # Run all button
        run_all_btn = ttk.Button(
            quick_frame,
            text="Run Full Pipeline",
            command=self.run_full_pipeline
        )
        run_all_btn.pack(side=tk.RIGHT, padx=5)
        
    def create_settings_tab(self, parent):
        """Create content for the settings tab."""
        # Create a scrollable frame for settings
        settings_canvas = tk.Canvas(parent, bg=self.bg_color)
        settings_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        scrollbar = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=settings_canvas.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        settings_canvas.configure(yscrollcommand=scrollbar.set)
        
        settings_frame = ttk.Frame(settings_canvas)
        settings_canvas.create_window((0, 0), window=settings_frame, anchor=tk.NW)
        
        # LinkedIn Settings
        linkedin_frame = ttk.LabelFrame(settings_frame, text="LinkedIn Scraper Settings", padding=10)
        linkedin_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # LinkedIn leads limit
        ttk.Label(linkedin_frame, text="Max Leads:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.linkedin_leads_var = tk.StringVar(value="20")
        ttk.Entry(linkedin_frame, textvariable=self.linkedin_leads_var, width=10).grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Headless mode
        self.linkedin_headless_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(linkedin_frame, text="Headless Mode", variable=self.linkedin_headless_var).grid(row=0, column=2, sticky=tk.W, padx=5, pady=5)
        
        # Reddit Settings
        reddit_frame = ttk.LabelFrame(settings_frame, text="Reddit Scraper Settings", padding=10)
        reddit_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # Reddit leads limit
        ttk.Label(reddit_frame, text="Max Leads:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.reddit_leads_var = tk.StringVar(value="20")
        ttk.Entry(reddit_frame, textvariable=self.reddit_leads_var, width=10).grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Save CSV
        self.reddit_csv_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(reddit_frame, text="Save CSV", variable=self.reddit_csv_var).grid(row=0, column=2, sticky=tk.W, padx=5, pady=5)
        
        # Message Generator Settings
        message_frame = ttk.LabelFrame(settings_frame, text="Message Generator Settings", padding=10)
        message_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # LinkedIn messages limit
        ttk.Label(message_frame, text="LinkedIn Messages:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.linkedin_messages_var = tk.StringVar(value="10")
        ttk.Entry(message_frame, textvariable=self.linkedin_messages_var, width=10).grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Reddit messages limit
        ttk.Label(message_frame, text="Reddit Messages:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        self.reddit_messages_var = tk.StringVar(value="10")
        ttk.Entry(message_frame, textvariable=self.reddit_messages_var, width=10).grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        
        # OpenAI Model
        ttk.Label(message_frame, text="OpenAI Model:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        self.openai_model_var = tk.StringVar(value="gpt-4")
        model_combo = ttk.Combobox(message_frame, textvariable=self.openai_model_var, values=["gpt-4", "gpt-3.5-turbo"])
        model_combo.grid(row=2, column=1, sticky=tk.W, padx=5, pady=5)
        model_combo.state(["readonly"])
        
        # Lead Scorer Settings
        scorer_frame = ttk.LabelFrame(settings_frame, text="Lead Scorer Settings", padding=10)
        scorer_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # LinkedIn leads to score
        ttk.Label(scorer_frame, text="LinkedIn Leads:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.linkedin_score_var = tk.StringVar(value="20")
        ttk.Entry(scorer_frame, textvariable=self.linkedin_score_var, width=10).grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Reddit leads to score
        ttk.Label(scorer_frame, text="Reddit Leads:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        self.reddit_score_var = tk.StringVar(value="20")
        ttk.Entry(scorer_frame, textvariable=self.reddit_score_var, width=10).grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Use AI analysis
        self.use_ai_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(scorer_frame, text="Use AI Analysis", variable=self.use_ai_var).grid(row=2, column=0, columnspan=2, sticky=tk.W, padx=5, pady=5)
        
        # Email Reporter Settings
        email_frame = ttk.LabelFrame(settings_frame, text="Email Reporter Settings", padding=10)
        email_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # Days back
        ttk.Label(email_frame, text="Days Back:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.days_back_var = tk.StringVar(value="1")
        ttk.Entry(email_frame, textvariable=self.days_back_var, width=10).grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Response days
        ttk.Label(email_frame, text="Response Days:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        self.response_days_var = tk.StringVar(value="7")
        ttk.Entry(email_frame, textvariable=self.response_days_var, width=10).grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Recipient email
        ttk.Label(email_frame, text="Recipient Email:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        self.email_recipient_var = tk.StringVar(value=os.getenv('EMAIL_RECIPIENT', ''))
        ttk.Entry(email_frame, textvariable=self.email_recipient_var, width=30).grid(row=2, column=1, columnspan=2, sticky=tk.W, padx=5, pady=5)
        
        # Pipeline Settings
        pipeline_frame = ttk.LabelFrame(settings_frame, text="Pipeline Settings", padding=10)
        pipeline_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # Which tasks to run in pipeline
        ttk.Label(pipeline_frame, text="Pipeline Tasks:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        
        # Task checkboxes
        self.run_linkedin_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(pipeline_frame, text="LinkedIn Scraper", variable=self.run_linkedin_var).grid(row=1, column=0, sticky=tk.W, padx=25, pady=2)
        
        self.run_reddit_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(pipeline_frame, text="Reddit Scraper", variable=self.run_reddit_var).grid(row=2, column=0, sticky=tk.W, padx=25, pady=2)
        
        self.run_scorer_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(pipeline_frame, text="Lead Scorer", variable=self.run_scorer_var).grid(row=3, column=0, sticky=tk.W, padx=25, pady=2)
        
        self.run_messages_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(pipeline_frame, text="Message Generator", variable=self.run_messages_var).grid(row=4, column=0, sticky=tk.W, padx=25, pady=2)
        
        self.run_email_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(pipeline_frame, text="Email Reporter", variable=self.run_email_var).grid(row=5, column=0, sticky=tk.W, padx=25, pady=2)
        
        # Save settings button
        save_btn = ttk.Button(
            settings_frame,
            text="Apply Settings",
            command=self.update_task_settings
        )
        save_btn.pack(pady=10)
        
        # Update the scroll region
        settings_frame.update_idletasks()
        settings_canvas.config(scrollregion=settings_canvas.bbox(tk.ALL))
        
    def create_logs_tab(self, parent):
        """Create content for the logs tab."""
        # Create log viewer frame
        log_frame = ttk.Frame(parent, padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True)
        
        # Add buttons to view different log files
        btn_frame = ttk.Frame(log_frame)
        btn_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(
            btn_frame, 
            text="GUI Logs",
            command=lambda: self.view_log_file("lead_generation_gui.log")
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            btn_frame, 
            text="LinkedIn Logs",
            command=lambda: self.view_log_file("linkedin_scraper.log")
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            btn_frame, 
            text="Reddit Logs",
            command=lambda: self.view_log_file("reddit_scraper.log")
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            btn_frame, 
            text="Message Logs",
            command=lambda: self.view_log_file("message_generator.log")
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            btn_frame, 
            text="Scorer Logs",
            command=lambda: self.view_log_file("lead_scorer.log")
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            btn_frame, 
            text="Email Logs",
            command=lambda: self.view_log_file("email_reporter.log")
        ).pack(side=tk.LEFT, padx=5)
        
        # Create log viewer
        self.log_viewer = scrolledtext.ScrolledText(
            log_frame,
            wrap=tk.WORD,
            background="#f8f8f8",
            font=("Consolas", 10)
        )
        self.log_viewer.pack(fill=tk.BOTH, expand=True, pady=5)
        self.log_viewer.config(state=tk.DISABLED)
        
        # Add refresh and clear buttons
        btn_frame2 = ttk.Frame(log_frame)
        btn_frame2.pack(fill=tk.X, pady=5)
        
        ttk.Button(
            btn_frame2, 
            text="Refresh",
            command=lambda: self.view_log_file(self.current_log_file)
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            btn_frame2, 
            text="Clear Log File",
            command=self.clear_log_file
        ).pack(side=tk.RIGHT, padx=5)
        
        # Track current log file
        self.current_log_file = "lead_generation_gui.log"
        self.view_log_file(self.current_log_file)
        
    def view_log_file(self, filename: str):
        """
        View contents of a log file.
        
        Args:
            filename: Log file name
        """
        self.current_log_file = filename
        
        try:
            with open(filename, 'r') as file:
                content = file.read()
                
            self.log_viewer.config(state=tk.NORMAL)
            self.log_viewer.delete(1.0, tk.END)
            self.log_viewer.insert(tk.END, content)
            self.log_viewer.config(state=tk.DISABLED)
            
        except Exception as e:
            self.log_viewer.config(state=tk.NORMAL)
            self.log_viewer.delete(1.0, tk.END)
            self.log_viewer.insert(tk.END, f"Error opening log file {filename}: {str(e)}")
            self.log_viewer.config(state=tk.DISABLED)
            
    def clear_log_file(self):
        """Clear the current log file."""
        if not self.current_log_file:
            return
            
        try:
            # Confirm with user
            confirm = messagebox.askyesno(
                "Confirm",
                f"Are you sure you want to clear {self.current_log_file}?"
            )
            
            if confirm:
                # Clear the file
                with open(self.current_log_file, 'w') as file:
                    file.write(f"Log cleared on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                
                # Refresh the view
                self.view_log_file(self.current_log_file)
                
        except Exception as e:
            messagebox.showerror("Error", f"Error clearing log file: {str(e)}")
        
    def update_task_settings(self):
        """Update task settings based on user input."""
        try:
            # Get values from settings UI
            linkedin_leads = int(self.linkedin_leads_var.get())
            linkedin_headless = self.linkedin_headless_var.get()
            reddit_leads = int(self.reddit_leads_var.get())
            reddit_csv = self.reddit_csv_var.get()
            linkedin_messages = int(self.linkedin_messages_var.get())
            reddit_messages = int(self.reddit_messages_var.get())
            openai_model = self.openai_model_var.get()
            linkedin_score = int(self.linkedin_score_var.get())
            reddit_score = int(self.reddit_score_var.get())
            use_ai = self.use_ai_var.get()
            days_back = int(self.days_back_var.get())
            response_days = int(self.response_days_var.get())
            
            # Update the email recipient in environment
            os.environ['EMAIL_RECIPIENT'] = self.email_recipient_var.get()
            
            # Update task kwargs
            self.task_manager.tasks["run_linkedin_scraper"]["kwargs"].update({
                "max_leads": linkedin_leads,
                "headless": linkedin_headless
            })
            
            self.task_manager.tasks["run_reddit_scraper"]["kwargs"].update({
                "max_leads": reddit_leads,
                "save_csv": reddit_csv
            })
            
            self.task_manager.tasks["run_message_generator"]["kwargs"].update({
                "max_linkedin_leads": linkedin_messages,
                "max_reddit_leads": reddit_messages,
                "model": openai_model
            })
            
            self.task_manager.tasks["run_lead_scorer"]["kwargs"].update({
                "max_linkedin_leads": linkedin_score,
                "max_reddit_leads": reddit_score,
                "use_ai_analysis": use_ai
            })
            
            self.task_manager.tasks["run_email_reporter"]["kwargs"].update({
                "days_back": days_back,
                "response_days": response_days
            })
            
            messagebox.showinfo("Success", "Settings updated successfully")
            logger.info("Task settings updated successfully")
            
        except ValueError as e:
            messagebox.showerror("Invalid Value", f"Please enter valid numbers for all fields: {str(e)}")
            logger.error(f"Error updating settings: {str(e)}")
        except Exception as e:
            messagebox.showerror("Error", f"Error updating settings: {str(e)}")
            logger.error(f"Error updating settings: {str(e)}")
        
    def run_task_async(self, task_name: str, display_name: str):
        """
        Run a task in a separate thread.
        
        Args:
            task_name: Name of the task to run
            display_name: Display name of the task
        """
        if task_name in self.running_tasks:
            messagebox.showinfo("Task Running", f"{display_name} is already running")
            return
            
        # Start progress bar
        self.progress_bar.start()
        self.status_label.config(text=f"Running {display_name}...")
        
        # Add to running tasks
        self.running_tasks.append(task_name)
        
        # Create and start thread
        thread = threading.Thread(
            target=self._run_task_thread,
            args=(task_name, display_name)
        )
        thread.daemon = True
        thread.start()
        
    def _run_task_thread(self, task_name: str, display_name: str):
        """
        Thread function to run a task.
        
        Args:
            task_name: Name of the task to run
            display_name: Display name of the task
        """
        try:
            print(f"\n--- Starting {display_name} ---\n")
            
            # Run the task
            result = self.task_manager.run_task(task_name)
            
            # Update UI
            self.root.after(0, self._task_completed, task_name, display_name, result, None)
            
        except Exception as e:
            logger.error(f"Error running {display_name}: {str(e)}")
            
            # Update UI with error
            self.root.after(0, self._task_completed, task_name, display_name, None, str(e))
    
    def _task_completed(self, task_name: str, display_name: str, result: Any, error: str):
        """
        Handle task completion (called from the main thread).
        
        Args:
            task_name: Name of the task that completed
            display_name: Display name of the task
            result: Result from the task
            error: Error message, if any
        """
        # Stop progress bar
        self.progress_bar.stop()
        
        # Remove from running tasks
        if task_name in self.running_tasks:
            self.running_tasks.remove(task_name)
        
        # Update status
        if error:
            self.status_label.config(text=f"Error running {display_name}")
            print(f"\n--- Error in {display_name}: {error} ---\n")
            messagebox.showerror("Error", f"Error running {display_name}: {error}")
        else:
            self.status_label.config(text=f"{display_name} completed successfully")
            print(f"\n--- {display_name} completed successfully ---\n")
            
            # Show summary of results if available
            if result and isinstance(result, dict):
                summary = "\nResults Summary:\n"
                for key, value in result.items():
                    summary += f"- {key}: {value}\n"
                print(summary)
    
    def run_full_pipeline(self):
        """Run the full lead generation pipeline."""
        # Confirm with user
        confirm = messagebox.askyesno(
            "Confirm",
            "Are you sure you want to run the full pipeline? This may take some time."
        )
        
        if not confirm:
            return
            
        # Get selected tasks
        tasks_to_run = []
        
        if self.run_linkedin_var.get():
            tasks_to_run.append(("run_linkedin_scraper", "LinkedIn Scraper"))
            
        if self.run_reddit_var.get():
            tasks_to_run.append(("run_reddit_scraper", "Reddit Scraper"))
            
        if self.run_scorer_var.get():
            tasks_to_run.append(("run_lead_scorer", "Lead Scorer"))
            
        if self.run_messages_var.get():
            tasks_to_run.append(("run_message_generator", "Message Generator"))
            
        if self.run_email_var.get():
            tasks_to_run.append(("run_email_reporter", "Email Reporter"))
        
        if not tasks_to_run:
            messagebox.showinfo("No Tasks", "No tasks selected to run")
            return
            
        # Start pipeline thread
        thread = threading.Thread(
            target=self._run_pipeline_thread,
            args=(tasks_to_run,)
        )
        thread.daemon = True
        thread.start()
        
        # Update UI
        self.progress_bar.start()
        self.status_label.config(text="Running pipeline...")
        
    def _run_pipeline_thread(self, tasks: List[Tuple[str, str]]):
        """
        Thread function to run multiple tasks in sequence.
        
        Args:
            tasks: List of (task_name, display_name) tuples
        """
        results = {}
        error = None
        
        try:
            print("\n=== Starting Full Pipeline ===\n")
            
            for task_name, display_name in tasks:
                # Check if we should stop due to an error
                if error:
                    break
                    
                # Add to running tasks
                self.running_tasks.append(task_name)
                
                # Update status
                self.root.after(0, self.status_label.config, {"text": f"Running {display_name}..."})
                
                try:
                    print(f"\n--- Starting {display_name} ---\n")
                    
                    # Run the task
                    result = self.task_manager.run_task(task_name)
                    results[task_name] = result
                    
                    print(f"\n--- {display_name} completed successfully ---\n")
                    
                except Exception as e:
                    logger.error(f"Error running {display_name}: {str(e)}")
                    error = f"Error in {display_name}: {str(e)}"
                    break
                    
                finally:
                    # Remove from running tasks
                    if task_name in self.running_tasks:
                        self.running_tasks.remove(task_name)
            
            # Update UI when all tasks complete
            self.root.after(0, self._pipeline_completed, results, error)
            
        except Exception as e:
            logger.error(f"Error running pipeline: {str(e)}")
            
            # Update UI with error
            self.root.after(0, self._pipeline_completed, results, str(e))
    
    def _pipeline_completed(self, results: Dict[str, Any], error: str):
        """
        Handle pipeline completion (called from the main thread).
        
        Args:
            results: Dictionary of results from each task
            error: Error message, if any
        """
        # Stop progress bar
        self.progress_bar.stop()
        
        # Update status
        if error:
            self.status_label.config(text="Pipeline failed")
            print(f"\n=== Pipeline failed: {error} ===\n")
            messagebox.showerror("Pipeline Error", error)
        else:
            self.status_label.config(text="Pipeline completed successfully")
            print("\n=== Full Pipeline completed successfully ===\n")
            
            # Show summary of results
            summary = "\nPipeline Results Summary:\n"
            for task, result in results.items():
                if isinstance(result, dict):
                    summary += f"\n{task}:\n"
                    for key, value in result.items():
                        summary += f"- {key}: {value}\n"
                else:
                    summary += f"\n{task}: {result}\n"
            
            print(summary)
    
    def open_google_sheets(self):
        """Open the Google Sheets in the browser."""
        try:
            # Get the spreadsheet ID
            spreadsheet_id = os.getenv('GOOGLE_SHEETS_SPREADSHEET_ID')
            
            if not spreadsheet_id:
                # Try to get it from the sheets client
                try:
                    from utils.sheets_manager import get_spreadsheet_id
                    spreadsheet_id = get_spreadsheet_id()
                except:
                    pass
            
            if spreadsheet_id:
                url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
                webbrowser.open(url)
                self.status_label.config(text="Opened Google Sheets in browser")
            else:
                # Just open Google Sheets
                webbrowser.open("https://docs.google.com/spreadsheets")
                self.status_label.config(text="Opened Google Sheets in browser (ID not found)")
                
        except Exception as e:
            logger.error(f"Error opening Google Sheets: {str(e)}")
            messagebox.showerror("Error", f"Error opening Google Sheets: {str(e)}")
    
    def clear_log(self):
        """Clear the console log."""
        self.log_console.config(state=tk.NORMAL)
        self.log_console.delete(1.0, tk.END)
        self.log_console.config(state=tk.DISABLED)
        self.status_label.config(text="Console log cleared")
    
    def on_close(self):
        """Handle window close event."""
        # Check if any tasks are running
        if self.running_tasks:
            confirm = messagebox.askyesno(
                "Tasks Running",
                "There are tasks still running. Are you sure you want to exit?"
            )
            
            if not confirm:
                return
        
        # Restore console
        self.restore_console()
        
        # Close the window
        self.root.destroy()


def run_gui():
    """Run the lead generation GUI."""
    root = tk.Tk()
    app = LeadGenerationGUI(root)
    
    # Set up close handler
    root.protocol("WM_DELETE_WINDOW", app.on_close)
    
    # Start the main loop
    root.mainloop()


if __name__ == "__main__":
    run_gui()