import os
import time
import logging
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import json

from openai import Client
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='message_generator.log'
)
logger = logging.getLogger('message_generator')

# Load environment variables
load_dotenv()

class MessageGenerator:
    """
    Generates personalized outreach messages for leads using OpenAI's API.
    Takes lead data from Google Sheets and crafts human-like conversation starters.
    """
    
    def __init__(self, model: str = "gpt-4"):
        """
        Initialize the message generator.
        
        Args:
            model: OpenAI model to use for message generation
        """
        self.model = model
        self._init_openai_client()
        logger.info(f"Message generator initialized with model: {model}")
        
    def _init_openai_client(self) -> None:
        """Initialize the OpenAI API client."""
        try:
            openai_api_key = os.getenv('OPENAI_API_KEY')
            
            if not openai_api_key:
                raise ValueError("OpenAI API key missing in environment variables")
            
            self.client = Client(api_key=openai_api_key)
            logger.info("Successfully connected to OpenAI API")
            
        except Exception as e:
            logger.error(f"Error initializing OpenAI client: {str(e)}")
            raise
    
    def get_linkedin_leads(self, sheets_client, worksheet_name: str = "Leads") -> List[Dict[str, Any]]:
        """
        Retrieve LinkedIn leads from Google Sheets.
        
        Args:
            sheets_client: Google Sheets client
            worksheet_name: Name of the worksheet containing LinkedIn leads
            
        Returns:
            List of dictionaries containing lead data
        """
        try:
            logger.info(f"Retrieving LinkedIn leads from worksheet: {worksheet_name}")
            
            # Get the worksheet
            worksheet = sheets_client.open('LeadGenerationData').worksheet(worksheet_name)
            
            # Get all values including header row
            all_values = worksheet.get_all_values()
            
            if not all_values:
                logger.warning(f"No LinkedIn leads found in worksheet: {worksheet_name}")
                return []
            
            # Extract header and data
            headers = all_values[0]
            data_rows = all_values[1:]
            
            # Convert to list of dictionaries
            leads = []
            for row in data_rows:
                lead = {}
                for i, header in enumerate(headers):
                    if i < len(row):
                        lead[header.strip()] = row[i]
                    else:
                        lead[header.strip()] = ""
                
                # Parse JSON fields if they exist
                for field in ['contact_info', 'recent_posts']:
                    if field in lead and lead[field]:
                        try:
                            lead[field] = json.loads(lead[field])
                        except (json.JSONDecodeError, TypeError):
                            # If it's not valid JSON, keep as string or handle accordingly
                            if field == 'recent_posts' and isinstance(lead[field], str):
                                # Split by semicolon if it's a string
                                lead[field] = lead[field].split(';')
                
                leads.append(lead)
            
            logger.info(f"Retrieved {len(leads)} LinkedIn leads")
            return leads
            
        except Exception as e:
            logger.error(f"Error retrieving LinkedIn leads: {str(e)}")
            return []
    
    def get_reddit_leads(self, sheets_client, worksheet_name: str = "RedditLeads") -> List[Dict[str, Any]]:
        """
        Retrieve Reddit leads from Google Sheets.
        
        Args:
            sheets_client: Google Sheets client
            worksheet_name: Name of the worksheet containing Reddit leads
            
        Returns:
            List of dictionaries containing lead data
        """
        try:
            logger.info(f"Retrieving Reddit leads from worksheet: {worksheet_name}")
            
            # Get the worksheet
            worksheet = sheets_client.open('LeadGenerationData').worksheet(worksheet_name)
            
            # Get all values including header row
            all_values = worksheet.get_all_values()
            
            if not all_values:
                logger.warning(f"No Reddit leads found in worksheet: {worksheet_name}")
                return []
            
            # Extract header and data
            headers = all_values[0]
            data_rows = all_values[1:]
            
            # Convert to list of dictionaries
            leads = []
            for row in data_rows:
                lead = {}
                for i, header in enumerate(headers):
                    if i < len(row):
                        lead[header.strip()] = row[i]
                    else:
                        lead[header.strip()] = ""
                leads.append(lead)
            
            logger.info(f"Retrieved {len(leads)} Reddit leads")
            return leads
            
        except Exception as e:
            logger.error(f"Error retrieving Reddit leads: {str(e)}")
            return []
    
    def generate_linkedin_message(self, lead: Dict[str, Any]) -> Tuple[str, str]:
        """
        Generate a personalized outreach message for a LinkedIn lead.
        
        Args:
            lead: Dictionary containing LinkedIn lead data
            
        Returns:
            Tuple of (message, reasoning)
        """
        try:
            name = lead.get('name', 'professional')
            job_title = lead.get('job_title', '')
            industry = lead.get('industry', '')
            bio_snippet = lead.get('bio_snippet', '')
            
            # Extract recent posts if available
            recent_posts = lead.get('recent_posts', [])
            if isinstance(recent_posts, str):
                # Handle case where it's a string instead of a list
                recent_posts = [recent_posts]
            
            posts_text = ""
            if recent_posts:
                posts_text = "\n- " + "\n- ".join(recent_posts[:3])  # Limit to first 3 posts
            
            # Craft the system message
            system_message = """You are a thoughtful professional reaching out to make a genuine connection. 
            Your goal is to write a short, natural-sounding first message that feels like a real human wrote it.
            
            Key guidelines:
            - Keep the message conversational, warm, and brief (3-5 sentences)
            - Avoid sounding salesy, pushy, or overly formal
            - Include a specific reference to their background, content, or industry to show you've done your homework
            - Ask a thoughtful open-ended question related to their experience or interests
            - Your goal is to start a conversation, not to sell anything
            - Make it feel like a message from one professional to another, not an automated outreach
            - Focus on providing value or insight, not asking for something
            - Be authentic and human - avoid corporate jargon or buzzwords"""
            
            # Create the prompt for the AI
            user_message = f"""This is a professional on LinkedIn:
            
            Name: {name}
            Current role: {job_title}
            Industry: {industry}
            Bio snippet: {bio_snippet}
            
            Recent LinkedIn content they've posted: {posts_text}
            
            Write a personalized, conversational first message to send on LinkedIn. This should sound like a genuine human connection attempt, not automated outreach. Keep it brief, warm, and include a thoughtful question.
            
            Also explain your reasoning for why this message will be effective (the reasoning part will not be sent to them)."""

            # Call the OpenAI API
            response = self.client.chat.completions.create(
                model=self.model,
                temperature=0.7,
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": user_message}
                ],
                max_tokens=750
            )
            
            full_response = response.choices[0].message.content
            
            # Separate the message from the reasoning
            # Most likely the model will separate them clearly with a line break and heading
            parts = full_response.split("\n\n", 1)
            if len(parts) == 2 and ("Reasoning" in parts[1] or "Why this works" in parts[1]):
                message = parts[0].strip()
                reasoning = parts[1].strip()
            else:
                # If not cleanly separated, make a best guess
                split_points = [
                    "\nReasoning:",
                    "\nWhy this works:",
                    "\nEffectiveness:",
                    "\nStrategy:"
                ]
                message = full_response
                reasoning = ""
                
                for split_point in split_points:
                    if split_point in full_response:
                        parts = full_response.split(split_point, 1)
                        message = parts[0].strip()
                        reasoning = split_point.strip() + " " + parts[1].strip()
                        break
                
                # If we still couldn't split it, return the full response as the message
                if not reasoning:
                    message = full_response
                    reasoning = "No explicit reasoning provided."
            
            logger.info(f"Generated LinkedIn message for {name} ({job_title})")
            return message, reasoning
            
        except Exception as e:
            logger.error(f"Error generating LinkedIn message: {str(e)}")
            return "Error generating message.", f"Error: {str(e)}"
    
    def generate_reddit_message(self, lead: Dict[str, Any]) -> Tuple[str, str]:
        """
        Generate a personalized outreach message for a Reddit lead.
        
        Args:
            lead: Dictionary containing Reddit lead data
            
        Returns:
            Tuple of (message, reasoning)
        """
        try:
            username = lead.get('username', 'Redditor')
            subreddit = lead.get('subreddit', '')
            post_title = lead.get('post_title', '')
            post_content = lead.get('post_content', '')
            matched_keywords = lead.get('matched_keywords', '')
            
            # Craft the system message
            system_message = """You are a thoughtful professional reaching out to someone from Reddit who has posted about work challenges.
            Your goal is to write a brief, empathetic, and natural-sounding DM that feels like a real human wrote it.
            
            Key guidelines:
            - Keep the message conversational, warm, and brief (3-5 sentences)
            - Be empathetic and understanding about their situation
            - Reference their specific post in a non-intrusive way
            - Avoid sounding salesy, pushy, or too formal
            - Include a specific insight related to their post to show you've read it
            - Ask a thoughtful open-ended question related to their situation
            - Make it feel like a message from one person to another, not automated outreach
            - Focus on being helpful and supportive, not selling anything
            - Avoid being presumptuous about their situation"""
            
            # Create the prompt for the AI
            user_message = f"""This is a person who posted on Reddit:
            
            Username: {username}
            Subreddit: r/{subreddit}
            Post Title: {post_title}
            Post Content: {post_content[:1000]}  # Limit length for API
            Keywords matched: {matched_keywords}
            
            Write a personalized, conversational first Reddit DM that feels like a genuine human reaching out. Be empathetic but not presumptuous. Keep it brief, warm, and include a thoughtful question.
            
            Also explain your reasoning for why this message will be effective (the reasoning part will not be sent to them)."""

            # Call the OpenAI API
            response = self.client.chat.completions.create(
                model=self.model,
                temperature=0.7,
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": user_message}
                ],
                max_tokens=750
            )
            
            full_response = response.choices[0].message.content
            
            # Separate the message from the reasoning
            parts = full_response.split("\n\n", 1)
            if len(parts) == 2 and ("Reasoning" in parts[1] or "Why this works" in parts[1]):
                message = parts[0].strip()
                reasoning = parts[1].strip()
            else:
                # If not cleanly separated, make a best guess
                split_points = [
                    "\nReasoning:",
                    "\nWhy this works:",
                    "\nEffectiveness:",
                    "\nStrategy:"
                ]
                message = full_response
                reasoning = ""
                
                for split_point in split_points:
                    if split_point in full_response:
                        parts = full_response.split(split_point, 1)
                        message = parts[0].strip()
                        reasoning = split_point.strip() + " " + parts[1].strip()
                        break
                
                # If we still couldn't split it, return the full response as the message
                if not reasoning:
                    message = full_response
                    reasoning = "No explicit reasoning provided."
            
            logger.info(f"Generated Reddit message for {username} in r/{subreddit}")
            return message, reasoning
            
        except Exception as e:
            logger.error(f"Error generating Reddit message: {str(e)}")
            return "Error generating message.", f"Error: {str(e)}"
    
    def save_linkedin_messages(self, sheets_client, leads_with_messages: List[Dict[str, Any]], 
                               worksheet_name: str = "LinkedInMessages") -> bool:
        """
        Save generated LinkedIn messages to Google Sheets.
        
        Args:
            sheets_client: Google Sheets client
            leads_with_messages: List of leads with generated messages
            worksheet_name: Name of the worksheet to save messages to
            
        Returns:
            True if saving was successful, False otherwise
        """
        try:
            logger.info(f"Saving LinkedIn messages to worksheet: {worksheet_name}")
            
            # Check if worksheet exists, create if not
            try:
                worksheet = sheets_client.open('LeadGenerationData').worksheet(worksheet_name)
            except:
                worksheet = sheets_client.open('LeadGenerationData').add_worksheet(
                    title=worksheet_name, rows=1000, cols=10
                )
                # Add headers
                headers = [
                    "Name", "Job Title", "Industry", "Profile URL", 
                    "Generated Message", "Reasoning", "Status", "Response", 
                    "Date Generated", "Date Sent"
                ]
                worksheet.append_row(headers)
            
            # Prepare rows for Google Sheets
            rows = []
            for lead in leads_with_messages:
                row = [
                    lead.get('name', ''),
                    lead.get('job_title', ''),
                    lead.get('industry', ''),
                    lead.get('profile_url', ''),
                    lead.get('generated_message', ''),
                    lead.get('reasoning', ''),
                    "Pending Review",  # Default status
                    "",  # Empty response column
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Date generated
                    ""  # Empty date sent column
                ]
                rows.append(row)
            
            # Append to Google Sheet
            for row in rows:
                worksheet.append_row(row)
                
            logger.info(f"Successfully saved {len(rows)} LinkedIn messages to Google Sheets")
            return True
            
        except Exception as e:
            logger.error(f"Error saving LinkedIn messages to Google Sheets: {str(e)}")
            return False
    
    def save_reddit_messages(self, sheets_client, leads_with_messages: List[Dict[str, Any]], 
                             worksheet_name: str = "RedditMessages") -> bool:
        """
        Save generated Reddit messages to Google Sheets.
        
        Args:
            sheets_client: Google Sheets client
            leads_with_messages: List of leads with generated messages
            worksheet_name: Name of the worksheet to save messages to
            
        Returns:
            True if saving was successful, False otherwise
        """
        try:
            logger.info(f"Saving Reddit messages to worksheet: {worksheet_name}")
            
            # Check if worksheet exists, create if not
            try:
                worksheet = sheets_client.open('LeadGenerationData').worksheet(worksheet_name)
            except:
                worksheet = sheets_client.open('LeadGenerationData').add_worksheet(
                    title=worksheet_name, rows=1000, cols=10
                )
                # Add headers
                headers = [
                    "Username", "Subreddit", "Post Title", "Post URL", 
                    "Generated Message", "Reasoning", "Status", "Response", 
                    "Date Generated", "Date Sent"
                ]
                worksheet.append_row(headers)
            
            # Prepare rows for Google Sheets
            rows = []
            for lead in leads_with_messages:
                row = [
                    lead.get('username', ''),
                    lead.get('subreddit', ''),
                    lead.get('post_title', ''),
                    lead.get('post_url', ''),
                    lead.get('generated_message', ''),
                    lead.get('reasoning', ''),
                    "Pending Review",  # Default status
                    "",  # Empty response column
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Date generated
                    ""  # Empty date sent column
                ]
                rows.append(row)
            
            # Append to Google Sheet
            for row in rows:
                worksheet.append_row(row)
                
            logger.info(f"Successfully saved {len(rows)} Reddit messages to Google Sheets")
            return True
            
        except Exception as e:
            logger.error(f"Error saving Reddit messages to Google Sheets: {str(e)}")
            return False
    
    def process_linkedin_leads(self, sheets_client, max_leads: int = 10,
                              skip_existing: bool = True) -> List[Dict[str, Any]]:
        """
        Process LinkedIn leads by generating personalized messages.
        
        Args:
            sheets_client: Google Sheets client
            max_leads: Maximum number of leads to process
            skip_existing: Whether to skip leads that already have messages
            
        Returns:
            List of leads with generated messages
        """
        processed_leads = []
        
        try:
            # Get LinkedIn leads
            leads = self.get_linkedin_leads(sheets_client)
            
            if not leads:
                logger.warning("No LinkedIn leads found to process")
                return []
            
            # Get existing messages to avoid duplicates if needed
            existing_messages = set()
            if skip_existing:
                try:
                    messages_worksheet = sheets_client.open('LeadGenerationData').worksheet('LinkedInMessages')
                    all_values = messages_worksheet.get_all_values()
                    
                    # Extract profile URLs from existing messages (assuming column 4)
                    if all_values and len(all_values) > 1:  # Check if there's data besides header
                        existing_messages = set(row[3] for row in all_values[1:] if len(row) > 3)
                        
                except Exception as e:
                    logger.warning(f"Could not retrieve existing messages: {str(e)}")
            
            # Process leads and generate messages
            count = 0
            for lead in leads:
                # Skip if we've reached the maximum
                if count >= max_leads:
                    break
                
                # Skip if already processed
                profile_url = lead.get('profile_url', '')
                if skip_existing and profile_url in existing_messages:
                    logger.info(f"Skipping existing lead: {lead.get('name', '')}")
                    continue
                
                # Generate message
                message, reasoning = self.generate_linkedin_message(lead)
                
                # Add message to lead data
                lead_with_message = lead.copy()
                lead_with_message['generated_message'] = message
                lead_with_message['reasoning'] = reasoning
                
                processed_leads.append(lead_with_message)
                count += 1
                
                # Sleep to avoid API rate limits
                time.sleep(1)
            
            # Save generated messages
            if processed_leads:
                self.save_linkedin_messages(sheets_client, processed_leads)
            
            logger.info(f"Processed {len(processed_leads)} LinkedIn leads")
            return processed_leads
            
        except Exception as e:
            logger.error(f"Error processing LinkedIn leads: {str(e)}")
            return processed_leads
    
    def process_reddit_leads(self, sheets_client, max_leads: int = 10,
                            skip_existing: bool = True) -> List[Dict[str, Any]]:
        """
        Process Reddit leads by generating personalized messages.
        
        Args:
            sheets_client: Google Sheets client
            max_leads: Maximum number of leads to process
            skip_existing: Whether to skip leads that already have messages
            
        Returns:
            List of leads with generated messages
        """
        processed_leads = []
        
        try:
            # Get Reddit leads
            leads = self.get_reddit_leads(sheets_client)
            
            if not leads:
                logger.warning("No Reddit leads found to process")
                return []
            
            # Get existing messages to avoid duplicates if needed
            existing_messages = set()
            if skip_existing:
                try:
                    messages_worksheet = sheets_client.open('LeadGenerationData').worksheet('RedditMessages')
                    all_values = messages_worksheet.get_all_values()
                    
                    # Extract post URLs from existing messages (assuming column 4)
                    if all_values and len(all_values) > 1:  # Check if there's data besides header
                        existing_messages = set(row[3] for row in all_values[1:] if len(row) > 3)
                        
                except Exception as e:
                    logger.warning(f"Could not retrieve existing messages: {str(e)}")
            
            # Process leads and generate messages
            count = 0
            for lead in leads:
                # Skip if we've reached the maximum
                if count >= max_leads:
                    break
                
                # Skip if already processed
                post_url = lead.get('post_url', '')
                if skip_existing and post_url in existing_messages:
                    logger.info(f"Skipping existing lead: {lead.get('username', '')}")
                    continue
                
                # Generate message
                message, reasoning = self.generate_reddit_message(lead)
                
                # Add message to lead data
                lead_with_message = lead.copy()
                lead_with_message['generated_message'] = message
                lead_with_message['reasoning'] = reasoning
                
                processed_leads.append(lead_with_message)
                count += 1
                
                # Sleep to avoid API rate limits
                time.sleep(1)
            
            # Save generated messages
            if processed_leads:
                self.save_reddit_messages(sheets_client, processed_leads)
            
            logger.info(f"Processed {len(processed_leads)} Reddit leads")
            return processed_leads
            
        except Exception as e:
            logger.error(f"Error processing Reddit leads: {str(e)}")
            return processed_leads
    
    def process_all_leads(self, sheets_client, max_linkedin_leads: int = 10,
                         max_reddit_leads: int = 10) -> Dict[str, int]:
        """
        Process both LinkedIn and Reddit leads.
        
        Args:
            sheets_client: Google Sheets client
            max_linkedin_leads: Maximum number of LinkedIn leads to process
            max_reddit_leads: Maximum number of Reddit leads to process
            
        Returns:
            Dictionary with counts of processed leads by platform
        """
        results = {
            'linkedin_leads_processed': 0,
            'reddit_leads_processed': 0
        }
        
        try:
            # Process LinkedIn leads
            linkedin_leads = self.process_linkedin_leads(
                sheets_client, 
                max_leads=max_linkedin_leads
            )
            results['linkedin_leads_processed'] = len(linkedin_leads)
            
            # Process Reddit leads
            reddit_leads = self.process_reddit_leads(
                sheets_client, 
                max_leads=max_reddit_leads
            )
            results['reddit_leads_processed'] = len(reddit_leads)
            
            logger.info(f"Processed a total of {results['linkedin_leads_processed'] + results['reddit_leads_processed']} leads")
            return results
            
        except Exception as e:
            logger.error(f"Error processing all leads: {str(e)}")
            return results


def run_message_generator(sheets_client, max_linkedin_leads: int = 10,
                         max_reddit_leads: int = 10, model: str = "gpt-4") -> Dict[str, int]:
    """
    Run the message generator as a standalone function.
    
    Args:
        sheets_client: Google Sheets client for retrieving leads and saving messages
        max_linkedin_leads: Maximum number of LinkedIn leads to process
        max_reddit_leads: Maximum number of Reddit leads to process
        model: OpenAI model to use
        
    Returns:
        Dictionary with counts of processed leads by platform
    """
    generator = MessageGenerator(model=model)
    
    try:
        results = generator.process_all_leads(
            sheets_client,
            max_linkedin_leads=max_linkedin_leads,
            max_reddit_leads=max_reddit_leads
        )
        return results
    except Exception as e:
        logger.error(f"Error running message generator: {str(e)}")
        return {
            'linkedin_leads_processed': 0,
            'reddit_leads_processed': 0,
            'error': str(e)
        }


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
    
    # Run the message generator
    results = run_message_generator(
        sheets_client,
        max_linkedin_leads=5,
        max_reddit_leads=5,
        model="gpt-4"  # You can change to "gpt-3.5-turbo" for faster/cheaper results
    )
    
    print(f"Results: {results}")
