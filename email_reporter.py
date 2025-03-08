import os
import logging
import smtplib
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import json

from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='email_reporter.log'
)
logger = logging.getLogger('email_reporter')

# Load environment variables
load_dotenv()

class EmailReporter:
    """
    Generates and sends daily email reports for lead generation activities,
    including new leads, response likelihood, and engagement metrics.
    """
    
    def __init__(self):
        """Initialize the email reporter."""
        # Email configuration
        self.sender_email = os.getenv('EMAIL_ADDRESS')
        self.sender_password = os.getenv('EMAIL_PASSWORD')
        self.recipient_email = os.getenv('EMAIL_RECIPIENT', self.sender_email)
        
        if not self.sender_email or not self.sender_password:
            logger.error("Email credentials missing in environment variables")
            raise ValueError("Email credentials missing in environment variables")
        
        logger.info("Email reporter initialized")
    
    def get_new_linkedin_leads(self, sheets_client, days: int = 1) -> List[Dict[str, Any]]:
        """
        Retrieve LinkedIn leads added in the last n days.
        
        Args:
            sheets_client: Google Sheets client
            days: Number of days to look back
            
        Returns:
            List of dictionaries containing lead data
        """
        try:
            # Calculate the cutoff date
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            logger.info(f"Getting LinkedIn leads since {cutoff_date}")
            
            # Get LinkedIn leads
            worksheet = sheets_client.open('LeadGenerationData').worksheet('Leads')
            all_values = worksheet.get_all_values()
            
            if not all_values or len(all_values) <= 1:
                logger.warning("No LinkedIn leads found")
                return []
            
            # Extract header and data
            headers = [h.strip() for h in all_values[0]]
            data_rows = all_values[1:]
            
            # Find date column index
            date_col_idx = None
            for i, header in enumerate(headers):
                if 'date' in header.lower() and 'added' in header.lower():
                    date_col_idx = i
                    break
            
            if date_col_idx is None:
                logger.warning("Could not find date column in LinkedIn leads sheet")
                date_col_idx = -1  # Default to the last column
            
            # Convert to list of dictionaries and filter by date
            leads = []
            for row in data_rows:
                lead = {}
                for i, header in enumerate(headers):
                    if i < len(row):
                        lead[header] = row[i]
                    else:
                        lead[header] = ""
                
                # Filter by date
                lead_date = lead.get(headers[date_col_idx], "")
                if lead_date and lead_date >= cutoff_date:
                    leads.append(lead)
                    
            logger.info(f"Found {len(leads)} new LinkedIn leads")
            return leads
            
        except Exception as e:
            logger.error(f"Error retrieving LinkedIn leads: {str(e)}")
            return []
    
    def get_new_reddit_leads(self, sheets_client, days: int = 1) -> List[Dict[str, Any]]:
        """
        Retrieve Reddit leads added in the last n days.
        
        Args:
            sheets_client: Google Sheets client
            days: Number of days to look back
            
        Returns:
            List of dictionaries containing lead data
        """
        try:
            # Calculate the cutoff date
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            logger.info(f"Getting Reddit leads since {cutoff_date}")
            
            # Get Reddit leads
            worksheet = sheets_client.open('LeadGenerationData').worksheet('RedditLeads')
            all_values = worksheet.get_all_values()
            
            if not all_values or len(all_values) <= 1:
                logger.warning("No Reddit leads found")
                return []
            
            # Extract header and data
            headers = [h.strip() for h in all_values[0]]
            data_rows = all_values[1:]
            
            # Find date column index
            date_col_idx = None
            for i, header in enumerate(headers):
                if 'date' in header.lower() and 'added' in header.lower():
                    date_col_idx = i
                    break
            
            if date_col_idx is None:
                logger.warning("Could not find date column in Reddit leads sheet")
                date_col_idx = -1  # Default to the last column
            
            # Convert to list of dictionaries and filter by date
            leads = []
            for row in data_rows:
                lead = {}
                for i, header in enumerate(headers):
                    if i < len(row):
                        lead[header] = row[i]
                    else:
                        lead[header] = ""
                
                # Filter by date
                lead_date = lead.get(headers[date_col_idx], "")
                if lead_date and lead_date >= cutoff_date:
                    leads.append(lead)
                    
            logger.info(f"Found {len(leads)} new Reddit leads")
            return leads
            
        except Exception as e:
            logger.error(f"Error retrieving Reddit leads: {str(e)}")
            return []
    
    def get_linkedin_scores(self, sheets_client) -> Dict[str, float]:
        """
        Retrieve LinkedIn lead scores.
        
        Args:
            sheets_client: Google Sheets client
            
        Returns:
            Dictionary mapping profile URLs to scores
        """
        try:
            # Get LinkedIn lead scores
            worksheet = sheets_client.open('LeadGenerationData').worksheet('LinkedInLeadScores')
            all_values = worksheet.get_all_values()
            
            if not all_values or len(all_values) <= 1:
                logger.warning("No LinkedIn scores found")
                return {}
            
            # Extract header and data
            headers = [h.strip() for h in all_values[0]]
            data_rows = all_values[1:]
            
            # Find URL and score column indices
            url_idx, score_idx = None, None
            for i, header in enumerate(headers):
                header_lower = header.lower()
                if 'url' in header_lower:
                    url_idx = i
                elif 'final' in header_lower and 'score' in header_lower:
                    score_idx = i
            
            if url_idx is None or score_idx is None:
                logger.warning("Could not find URL or score column in LinkedIn scores sheet")
                return {}
            
            # Create URL to score mapping
            scores = {}
            for row in data_rows:
                if len(row) > max(url_idx, score_idx):
                    url = row[url_idx]
                    try:
                        score = float(row[score_idx])
                        scores[url] = score
                    except (ValueError, TypeError):
                        # Skip if score is not a valid number
                        pass
                        
            logger.info(f"Retrieved {len(scores)} LinkedIn scores")
            return scores
            
        except Exception as e:
            logger.error(f"Error retrieving LinkedIn scores: {str(e)}")
            return {}
    
    def get_reddit_scores(self, sheets_client) -> Dict[str, float]:
        """
        Retrieve Reddit lead scores.
        
        Args:
            sheets_client: Google Sheets client
            
        Returns:
            Dictionary mapping post URLs to scores
        """
        try:
            # Get Reddit lead scores
            worksheet = sheets_client.open('LeadGenerationData').worksheet('RedditLeadScores')
            all_values = worksheet.get_all_values()
            
            if not all_values or len(all_values) <= 1:
                logger.warning("No Reddit scores found")
                return {}
            
            # Extract header and data
            headers = [h.strip() for h in all_values[0]]
            data_rows = all_values[1:]
            
            # Find URL and score column indices
            url_idx, score_idx = None, None
            for i, header in enumerate(headers):
                header_lower = header.lower()
                if 'url' in header_lower:
                    url_idx = i
                elif 'final' in header_lower and 'score' in header_lower:
                    score_idx = i
            
            if url_idx is None or score_idx is None:
                logger.warning("Could not find URL or score column in Reddit scores sheet")
                return {}
            
            # Create URL to score mapping
            scores = {}
            for row in data_rows:
                if len(row) > max(url_idx, score_idx):
                    url = row[url_idx]
                    try:
                        score = float(row[score_idx])
                        scores[url] = score
                    except (ValueError, TypeError):
                        # Skip if score is not a valid number
                        pass
                        
            logger.info(f"Retrieved {len(scores)} Reddit scores")
            return scores
            
        except Exception as e:
            logger.error(f"Error retrieving Reddit scores: {str(e)}")
            return {}
    
    def get_linkedin_messages(self, sheets_client) -> Dict[str, str]:
        """
        Retrieve LinkedIn messages.
        
        Args:
            sheets_client: Google Sheets client
            
        Returns:
            Dictionary mapping profile URLs to messages
        """
        try:
            # Get LinkedIn messages
            worksheet = sheets_client.open('LeadGenerationData').worksheet('LinkedInMessages')
            all_values = worksheet.get_all_values()
            
            if not all_values or len(all_values) <= 1:
                logger.warning("No LinkedIn messages found")
                return {}
            
            # Extract header and data
            headers = [h.strip() for h in all_values[0]]
            data_rows = all_values[1:]
            
            # Find URL and message column indices
            url_idx, message_idx = None, None
            for i, header in enumerate(headers):
                header_lower = header.lower()
                if 'url' in header_lower:
                    url_idx = i
                elif 'message' in header_lower and 'generated' in header_lower:
                    message_idx = i
            
            if url_idx is None or message_idx is None:
                logger.warning("Could not find URL or message column in LinkedIn messages sheet")
                return {}
            
            # Create URL to message mapping
            messages = {}
            for row in data_rows:
                if len(row) > max(url_idx, message_idx):
                    url = row[url_idx]
                    message = row[message_idx]
                    messages[url] = message
                        
            logger.info(f"Retrieved {len(messages)} LinkedIn messages")
            return messages
            
        except Exception as e:
            logger.error(f"Error retrieving LinkedIn messages: {str(e)}")
            return {}
    
    def get_reddit_messages(self, sheets_client) -> Dict[str, str]:
        """
        Retrieve Reddit messages.
        
        Args:
            sheets_client: Google Sheets client
            
        Returns:
            Dictionary mapping post URLs to messages
        """
        try:
            # Get Reddit messages
            worksheet = sheets_client.open('LeadGenerationData').worksheet('RedditMessages')
            all_values = worksheet.get_all_values()
            
            if not all_values or len(all_values) <= 1:
                logger.warning("No Reddit messages found")
                return {}
            
            # Extract header and data
            headers = [h.strip() for h in all_values[0]]
            data_rows = all_values[1:]
            
            # Find URL and message column indices
            url_idx, message_idx = None, None
            for i, header in enumerate(headers):
                header_lower = header.lower()
                if 'url' in header_lower:
                    url_idx = i
                elif 'message' in header_lower and 'generated' in header_lower:
                    message_idx = i
            
            if url_idx is None or message_idx is None:
                logger.warning("Could not find URL or message column in Reddit messages sheet")
                return {}
            
            # Create URL to message mapping
            messages = {}
            for row in data_rows:
                if len(row) > max(url_idx, message_idx):
                    url = row[url_idx]
                    message = row[message_idx]
                    messages[url] = message
                        
            logger.info(f"Retrieved {len(messages)} Reddit messages")
            return messages
            
        except Exception as e:
            logger.error(f"Error retrieving Reddit messages: {str(e)}")
            return {}
    
    def get_message_responses(self, sheets_client, days: int = 7) -> List[Dict[str, Any]]:
        """
        Retrieve responses to messages in the last n days.
        
        Args:
            sheets_client: Google Sheets client
            days: Number of days to look back
            
        Returns:
            List of dictionaries containing response data
        """
        try:
            # Calculate the cutoff date
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            logger.info(f"Getting message responses since {cutoff_date}")
            
            # Get LinkedIn responses
            linkedin_responses = self._get_linkedin_responses(sheets_client, cutoff_date)
            
            # Get Reddit responses
            reddit_responses = self._get_reddit_responses(sheets_client, cutoff_date)
            
            # Combine responses
            all_responses = linkedin_responses + reddit_responses
            
            logger.info(f"Found {len(all_responses)} responses in total")
            return all_responses
            
        except Exception as e:
            logger.error(f"Error retrieving message responses: {str(e)}")
            return []
    
    def _get_linkedin_responses(self, sheets_client, cutoff_date: str) -> List[Dict[str, Any]]:
        """
        Retrieve LinkedIn responses since the cutoff date.
        
        Args:
            sheets_client: Google Sheets client
            cutoff_date: Cutoff date string in format YYYY-MM-DD
            
        Returns:
            List of dictionaries containing response data
        """
        try:
            # Get LinkedIn messages
            worksheet = sheets_client.open('LeadGenerationData').worksheet('LinkedInMessages')
            all_values = worksheet.get_all_values()
            
            if not all_values or len(all_values) <= 1:
                return []
            
            # Extract header and data
            headers = [h.strip() for h in all_values[0]]
            data_rows = all_values[1:]
            
            # Find relevant column indices
            name_idx, response_idx, status_idx, date_sent_idx = None, None, None, None
            for i, header in enumerate(headers):
                header_lower = header.lower()
                if header_lower == 'name':
                    name_idx = i
                elif 'response' in header_lower:
                    response_idx = i
                elif 'status' in header_lower:
                    status_idx = i
                elif 'date' in header_lower and 'sent' in header_lower:
                    date_sent_idx = i
            
            if None in [name_idx, response_idx, status_idx, date_sent_idx]:
                logger.warning("Could not find required columns in LinkedIn messages sheet")
                return []
            
            # Extract responses
            responses = []
            for row in data_rows:
                if len(row) > max(name_idx, response_idx, status_idx, date_sent_idx):
                    # Check if there's a response and it was after the cutoff date
                    response = row[response_idx]
                    status = row[status_idx]
                    date_sent = row[date_sent_idx]
                    
                    if response and status.lower() == 'responded' and date_sent >= cutoff_date:
                        responses.append({
                            'platform': 'LinkedIn',
                            'name': row[name_idx],
                            'response': response,
                            'date': date_sent
                        })
            
            logger.info(f"Found {len(responses)} LinkedIn responses")
            return responses
            
        except Exception as e:
            logger.error(f"Error retrieving LinkedIn responses: {str(e)}")
            return []
    
    def _get_reddit_responses(self, sheets_client, cutoff_date: str) -> List[Dict[str, Any]]:
        """
        Retrieve Reddit responses since the cutoff date.
        
        Args:
            sheets_client: Google Sheets client
            cutoff_date: Cutoff date string in format YYYY-MM-DD
            
        Returns:
            List of dictionaries containing response data
        """
        try:
            # Get Reddit messages
            worksheet = sheets_client.open('LeadGenerationData').worksheet('RedditMessages')
            all_values = worksheet.get_all_values()
            
            if not all_values or len(all_values) <= 1:
                return []
            
            # Extract header and data
            headers = [h.strip() for h in all_values[0]]
            data_rows = all_values[1:]
            
            # Find relevant column indices
            username_idx, response_idx, status_idx, date_sent_idx = None, None, None, None
            for i, header in enumerate(headers):
                header_lower = header.lower()
                if 'username' in header_lower:
                    username_idx = i
                elif 'response' in header_lower:
                    response_idx = i
                elif 'status' in header_lower:
                    status_idx = i
                elif 'date' in header_lower and 'sent' in header_lower:
                    date_sent_idx = i
            
            if None in [username_idx, response_idx, status_idx, date_sent_idx]:
                logger.warning("Could not find required columns in Reddit messages sheet")
                return []
            
            # Extract responses
            responses = []
            for row in data_rows:
                if len(row) > max(username_idx, response_idx, status_idx, date_sent_idx):
                    # Check if there's a response and it was after the cutoff date
                    response = row[response_idx]
                    status = row[status_idx]
                    date_sent = row[date_sent_idx]
                    
                    if response and status.lower() == 'responded' and date_sent >= cutoff_date:
                        responses.append({
                            'platform': 'Reddit',
                            'name': row[username_idx],
                            'response': response,
                            'date': date_sent
                        })
            
            logger.info(f"Found {len(responses)} Reddit responses")
            return responses
            
        except Exception as e:
            logger.error(f"Error retrieving Reddit responses: {str(e)}")
            return []
    
    def get_engagement_metrics(self, sheets_client, days: int = 30) -> Dict[str, Any]:
        """
        Calculate engagement metrics for the last n days.
        
        Args:
            sheets_client: Google Sheets client
            days: Number of days to analyze
            
        Returns:
            Dictionary with engagement metrics
        """
        try:
            # Calculate the cutoff date
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            
            # Initialize metrics
            metrics = {
                'total_leads': 0,
                'linkedin_leads': 0,
                'reddit_leads': 0,
                'messages_sent': 0,
                'response_rate': 0,
                'high_priority_leads': 0,
                'replies_received': 0
            }
            
            # Get LinkedIn leads count
            try:
                worksheet = sheets_client.open('LeadGenerationData').worksheet('Leads')
                linkedin_leads = len(worksheet.get_all_values()) - 1  # Subtract header row
                metrics['linkedin_leads'] = max(0, linkedin_leads)
            except:
                pass
            
            # Get Reddit leads count
            try:
                worksheet = sheets_client.open('LeadGenerationData').worksheet('RedditLeads')
                reddit_leads = len(worksheet.get_all_values()) - 1  # Subtract header row
                metrics['reddit_leads'] = max(0, reddit_leads)
            except:
                pass
            
            # Calculate total leads
            metrics['total_leads'] = metrics['linkedin_leads'] + metrics['reddit_leads']
            
            # Get LinkedIn messages metrics
            linkedin_sent, linkedin_responses = self._get_message_metrics(
                sheets_client, 'LinkedInMessages', cutoff_date
            )
            
            # Close HTML
            html += """
                <div class="footer">
                    <p>This is an automated report. Do not reply to this email.</p>
                    <p>Generated on {} UTC</p>
                </div>
            </body>
            </html>
            """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            
            logger.info("Generated email HTML content")
            return html
            
        except Exception as e:
            logger.error(f"Error generating email HTML: {str(e)}")
            return f"""
            <html>
            <body>
                <h1>Lead Generation Daily Report</h1>
                <p>There was an error generating the report: {str(e)}</p>
                <p>Please check the logs for more information.</p>
            </body>
            </html>
            """
    
    def send_email(self, subject: str, html_content: str) -> bool:
        """
        Send an email with the provided HTML content.
        
        Args:
            subject: Email subject
            html_content: HTML content of the email
            
        Returns:
            True if the email was sent successfully, False otherwise
        """
        try:
            # Create message
            message = MIMEMultipart("alternative")
            message["Subject"] = subject
            message["From"] = self.sender_email
            message["To"] = self.recipient_email
            
            # Attach HTML content
            html_part = MIMEText(html_content, "html")
            message.attach(html_part)
            
            # Send email
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(self.sender_email, self.sender_password)
                server.sendmail(
                    self.sender_email, self.recipient_email, message.as_string()
                )
            
            logger.info(f"Email sent successfully to {self.recipient_email}")
            return True
            
        except Exception as e:
            logger.error(f"Error sending email: {str(e)}")
            return False
    
    def generate_and_send_report(self, sheets_client, days_back: int = 1, 
                                response_days: int = 7) -> bool:
        """
        Generate and send a daily report.
        
        Args:
            sheets_client: Google Sheets client
            days_back: Number of days to look back for new leads
            response_days: Number of days to look back for responses
            
        Returns:
            True if the report was generated and sent successfully, False otherwise
        """
        try:
            # Get all data
            data = {
                'new_linkedin_leads': self.get_new_linkedin_leads(sheets_client, days=days_back),
                'new_reddit_leads': self.get_new_reddit_leads(sheets_client, days=days_back),
                'linkedin_scores': self.get_linkedin_scores(sheets_client),
                'reddit_scores': self.get_reddit_scores(sheets_client),
                'linkedin_messages': self.get_linkedin_messages(sheets_client),
                'reddit_messages': self.get_reddit_messages(sheets_client),
                'responses': self.get_message_responses(sheets_client, days=response_days),
                'metrics': self.get_engagement_metrics(sheets_client, days=30)
            }
            
            # Generate email HTML
            html_content = self.generate_email_html(data)
            
            # Generate subject with date
            today = datetime.now().strftime("%Y-%m-%d")
            subject = f"Lead Generation Daily Report - {today}"
            
            # Send email
            return self.send_email(subject, html_content)
            
        except Exception as e:
            logger.error(f"Error generating and sending report: {str(e)}")
            return False


def run_email_reporter(sheets_client, days_back: int = 1, response_days: int = 7) -> bool:
    """
    Run the email reporter as a standalone function.
    
    Args:
        sheets_client: Google Sheets client
        days_back: Number of days to look back for new leads
        response_days: Number of days to look back for responses
        
    Returns:
        True if the report was generated and sent successfully, False otherwise
    """
    try:
        reporter = EmailReporter()
        return reporter.generate_and_send_report(
            sheets_client, 
            days_back=days_back,
            response_days=response_days
        )
    except Exception as e:
        logger.error(f"Error running email reporter: {str(e)}")
        return False


if __name__ == "__main__":
    # This allows the script to be run directly for testing
    from utils.sheets_manager import get_sheets_client
    
    # Get Google Sheets client
    try:
        sheets_client = get_sheets_client()
    except Exception as e:
        logger.error(f"Could not connect to Google Sheets: {str(e)}")
        sheets_client = None
        print(f"Error: {str(e)}")
        exit(1)
    
    # Run the email reporter
    success = run_email_reporter(
        sheets_client,
        days_back=1,  # Look for leads from the last day
        response_days=7  # Look for responses from the last week
    )
    
    if success:
        print("Daily report email sent successfully!")
    else:
        print("Error sending daily report email. Check the logs for details.")
            
            # Get Reddit messages metrics
            reddit_sent, reddit_responses = self._get_message_metrics(
                sheets_client, 'RedditMessages', cutoff_date
            )
            
            # Calculate total messages and responses
            metrics['messages_sent'] = linkedin_sent + reddit_sent
            metrics['replies_received'] = linkedin_responses + reddit_responses
            
            # Calculate response rate
            if metrics['messages_sent'] > 0:
                metrics['response_rate'] = round(
                    (metrics['replies_received'] / metrics['messages_sent']) * 100, 1
                )
            
            # Get high priority leads count
            try:
                # LinkedIn high priority
                worksheet = sheets_client.open('LeadGenerationData').worksheet('LinkedInLeadScores')
                all_values = worksheet.get_all_values()
                
                if len(all_values) > 1:
                    headers = [h.strip().lower() for h in all_values[0]]
                    data_rows = all_values[1:]
                    
                    # Find priority column index
                    priority_idx = None
                    for i, header in enumerate(headers):
                        if 'priority' in header:
                            priority_idx = i
                            break
                    
                    if priority_idx is not None:
                        high_priority = sum(
                            1 for row in data_rows if len(row) > priority_idx 
                            and row[priority_idx].lower() == 'high_priority'
                        )
                        metrics['high_priority_leads'] += high_priority
            except:
                pass
            
            try:
                # Reddit high priority
                worksheet = sheets_client.open('LeadGenerationData').worksheet('RedditLeadScores')
                all_values = worksheet.get_all_values()
                
                if len(all_values) > 1:
                    headers = [h.strip().lower() for h in all_values[0]]
                    data_rows = all_values[1:]
                    
                    # Find priority column index
                    priority_idx = None
                    for i, header in enumerate(headers):
                        if 'priority' in header:
                            priority_idx = i
                            break
                    
                    if priority_idx is not None:
                        high_priority = sum(
                            1 for row in data_rows if len(row) > priority_idx 
                            and row[priority_idx].lower() == 'high_priority'
                        )
                        metrics['high_priority_leads'] += high_priority
            except:
                pass
            
            logger.info(f"Calculated engagement metrics for the past {days} days")
            return metrics
            
        except Exception as e:
            logger.error(f"Error calculating engagement metrics: {str(e)}")
            return {
                'total_leads': 0,
                'linkedin_leads': 0,
                'reddit_leads': 0,
                'messages_sent': 0,
                'response_rate': 0,
                'high_priority_leads': 0,
                'replies_received': 0
            }
    
    def _get_message_metrics(self, sheets_client, worksheet_name: str, cutoff_date: str) -> Tuple[int, int]:
        """
        Get message metrics from a worksheet.
        
        Args:
            sheets_client: Google Sheets client
            worksheet_name: Name of the worksheet
            cutoff_date: Cutoff date string in format YYYY-MM-DD
            
        Returns:
            Tuple of (messages sent, responses received)
        """
        try:
            messages_sent = 0
            responses = 0
            
            # Get messages
            worksheet = sheets_client.open('LeadGenerationData').worksheet(worksheet_name)
            all_values = worksheet.get_all_values()
            
            if len(all_values) <= 1:
                return 0, 0
            
            # Extract header and data
            headers = [h.strip().lower() for h in all_values[0]]
            data_rows = all_values[1:]
            
            # Find relevant column indices
            status_idx, date_sent_idx = None, None
            for i, header in enumerate(headers):
                if 'status' in header:
                    status_idx = i
                elif 'date' in header and 'sent' in header:
                    date_sent_idx = i
            
            if status_idx is None or date_sent_idx is None:
                return 0, 0
            
            # Count messages sent and responses
            for row in data_rows:
                if len(row) > max(status_idx, date_sent_idx):
                    date_sent = row[date_sent_idx]
                    
                    # Check if the message was sent after the cutoff date
                    if date_sent and date_sent >= cutoff_date:
                        status = row[status_idx].lower()
                        
                        if status in ['sent', 'responded']:
                            messages_sent += 1
                        
                        if status == 'responded':
                            responses += 1
            
            return messages_sent, responses
            
        except Exception as e:
            logger.error(f"Error getting message metrics from {worksheet_name}: {str(e)}")
            return 0, 0
    
    def generate_email_html(self, data: Dict[str, Any]) -> str:
        """
        Generate the HTML content for the email.
        
        Args:
            data: Dictionary with all the data for the email
            
        Returns:
            HTML string for the email
        """
        try:
            # Extract data
            new_linkedin_leads = data.get('new_linkedin_leads', [])
            new_reddit_leads = data.get('new_reddit_leads', [])
            linkedin_scores = data.get('linkedin_scores', {})
            reddit_scores = data.get('reddit_scores', {})
            linkedin_messages = data.get('linkedin_messages', {})
            reddit_messages = data.get('reddit_messages', {})
            responses = data.get('responses', [])
            metrics = data.get('metrics', {})
            
            # Get date range
            today = datetime.now().strftime("%B %d, %Y")
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%B %d, %Y")
            
            # Start building HTML
            html = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Lead Generation Daily Report</title>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        line-height: 1.6;
                        color: #333;
                        max-width: 800px;
                        margin: 0 auto;
                        padding: 20px;
                    }}
                    h1, h2, h3 {{
                        color: #2c3e50;
                    }}
                    h1 {{
                        border-bottom: 2px solid #3498db;
                        padding-bottom: 10px;
                    }}
                    h2 {{
                        border-bottom: 1px solid #ddd;
                        padding-bottom: 5px;
                        margin-top: 30px;
                    }}
                    .lead-card {{
                        border: 1px solid #ddd;
                        border-radius: 5px;
                        padding: 15px;
                        margin-bottom: 15px;
                        background-color: #f9f9f9;
                    }}
                    .lead-card h3 {{
                        margin-top: 0;
                        margin-bottom: 10px;
                    }}
                    .lead-platform {{
                        display: inline-block;
                        padding: 3px 8px;
                        border-radius: 3px;
                        font-size: 12px;
                        font-weight: bold;
                        margin-right: 10px;
                    }}
                    .lead-platform.linkedin {{
                        background-color: #0077b5;
                        color: white;
                    }}
                    .lead-platform.reddit {{
                        background-color: #ff4500;
                        color: white;
                    }}
                    .lead-score {{
                        float: right;
                        padding: 3px 8px;
                        border-radius: 3px;
                        font-weight: bold;
                    }}
                    .high-priority {{
                        background-color: #27ae60;
                        color: white;
                    }}
                    .medium-priority {{
                        background-color: #f39c12;
                        color: white;
                    }}
                    .low-priority {{
                        background-color: #e74c3c;
                        color: white;
                    }}
                    .metrics-container {{
                        display: flex;
                        flex-wrap: wrap;
                        justify-content: space-between;
                        margin-bottom: 20px;
                    }}
                    .metric-box {{
                        width: 30%;
                        background-color: #f5f5f5;
                        border-radius: 5px;
                        padding: 15px;
                        margin-bottom: 15px;
                        text-align: center;
                    }}
                    .metric-value {{
                        font-size: 24px;
                        font-weight: bold;
                        margin: 10px 0;
                        color: #2c3e50;
                    }}
                    .metric-label {{
                        font-size: 14px;
                        color: #7f8c8d;
                    }}
                    .message-preview {{
                        border-left: 3px solid #3498db;
                        padding-left: 15px;
                        margin: 10px 0;
                        font-style: italic;
                        color: #555;
                    }}
                    .message-preview p {{
                        margin: 5px 0;
                    }}
                    .response {{
                        border-left: 3px solid #27ae60;
                    }}
                    table {{
                        width: 100%;
                        border-collapse: collapse;
                        margin: 20px 0;
                    }}
                    th, td {{
                        padding: 12px 15px;
                        text-align: left;
                        border-bottom: 1px solid #ddd;
                    }}
                    th {{
                        background-color: #f2f2f2;
                        font-weight: bold;
                    }}
                    tr:hover {{
                        background-color: #f5f5f5;
                    }}
                    .footer {{
                        margin-top: 40px;
                        border-top: 1px solid #ddd;
                        padding-top: 20px;
                        text-align: center;
                        font-size: 12px;
                        color: #7f8c8d;
                    }}
                </style>
            </head>
            <body>
                <h1>Lead Generation Daily Report</h1>
                <p>Report for {yesterday} to {today}</p>
                
                <div class="metrics-container">
                    <div class="metric-box">
                        <div class="metric-value">{metrics.get('total_leads', 0)}</div>
                        <div class="metric-label">Total Leads</div>
                    </div>
                    <div class="metric-box">
                        <div class="metric-value">{metrics.get('high_priority_leads', 0)}</div>
                        <div class="metric-label">High Priority Leads</div>
                    </div>
                    <div class="metric-box">
                        <div class="metric-value">{metrics.get('response_rate', 0)}%</div>
                        <div class="metric-label">Response Rate</div>
                    </div>
                    <div class="metric-box">
                        <div class="metric-value">{len(new_linkedin_leads) + len(new_reddit_leads)}</div>
                        <div class="metric-label">New Leads Today</div>
                    </div>
                    <div class="metric-box">
                        <div class="metric-value">{metrics.get('messages_sent', 0)}</div>
                        <div class="metric-label">Messages Sent</div>
                    </div>
                    <div class="metric-box">
                        <div class="metric-value">{metrics.get('replies_received', 0)}</div>
                        <div class="metric-label">Replies Received</div>
                    </div>
                </div>
            """