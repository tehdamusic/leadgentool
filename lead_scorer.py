import os
import re
import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import json

from openai import Client
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='lead_scorer.log'
)
logger = logging.getLogger('lead_scorer')

# Load environment variables
load_dotenv()

class LeadScorer:
    """
    Scores leads based on their likelihood to respond based on content analysis,
    urgency of pain points, and other configurable factors.
    """
    
    def __init__(self, use_ai_analysis: bool = True, model: str = "gpt-4"):
        """
        Initialize the lead scorer.
        
        Args:
            use_ai_analysis: Whether to use OpenAI for content analysis
            model: OpenAI model to use for content analysis
        """
        self.use_ai_analysis = use_ai_analysis
        self.model = model
        
        # Initialize OpenAI client if AI analysis is enabled
        if use_ai_analysis:
            self._init_openai_client()
        
        # Define scoring criteria - making these class attributes allows for easy tweaking later
        self.scoring_config = {
            # Keywords indicating pain points, with weights
            'pain_point_keywords': {
                'high_urgency': {
                    'need help': 3.0,
                    'desperate': 3.0,
                    'urgent': 3.0,
                    'struggling': 2.5,
                    'can\'t take it': 2.5,
                    'at my limit': 2.5,
                    'breaking point': 2.5,
                    'emergency': 3.0,
                    'crisis': 3.0,
                    'suicidal': 0.0,  # We don't want to score suicidal content high
                    'help me': 2.0,
                    'please advise': 2.0,
                    'what should i do': 2.0,
                    'lost my job': 2.0,
                    'fired': 2.0,
                    'burnout': 2.0,
                    'burning out': 2.0,
                    'exhausted': 1.5,
                },
                'medium_urgency': {
                    'stressed': 1.5,
                    'anxiety': 1.5,
                    'anxious': 1.5,
                    'overwhelmed': 1.5,
                    'frustrated': 1.0,
                    'tired of': 1.0,
                    'fed up': 1.0,
                    'hate my job': 1.5,
                    'toxic workplace': 1.5,
                    'looking for advice': 1.5,
                    'need advice': 1.5,
                    'career change': 1.0,
                    'new job': 1.0,
                    'work-life balance': 1.0,
                    'coaching': 1.0,
                    'mentor': 1.0,
                },
                'low_urgency': {
                    'improvement': 0.5,
                    'better': 0.5,
                    'learning': 0.5,
                    'growth': 0.5,
                    'tips': 0.5,
                    'productivity': 0.5,
                    'efficiency': 0.5,
                    'strategy': 0.5,
                    'curious': 0.5,
                    'wondering': 0.5,
                }
            },
            
            # Tone indicators with weights
            'tone_indicators': {
                'positive': {
                    'weight': -0.5,  # Lower priority - they're doing okay
                    'patterns': [
                        r'happy', r'excited', r'grateful', r'thankful', 
                        r'optimistic', r'hopeful', r'looking forward'
                    ]
                },
                'negative': {
                    'weight': 1.0,  # Higher priority - they're struggling
                    'patterns': [
                        r'sad', r'depressed', r'miserable', r'unhappy', 
                        r'angry', r'furious', r'hate', r'dislike'
                    ]
                },
                'desperate': {
                    'weight': 2.0,  # Highest priority - they need help now
                    'patterns': [
                        r'please help', r'i need', r'desperate', r'urgently', 
                        r'asap', r'emergency', r'crisis', r'immediately'
                    ]
                },
                'reflective': {
                    'weight': 0.7,  # Medium priority - they're contemplating
                    'patterns': [
                        r'thinking about', r'considering', r'reflecting', 
                        r'wondering if', r'not sure if', r'should i'
                    ]
                }
            },
            
            # Question patterns indicating they're seeking help
            'question_patterns': {
                'weight': 1.5,
                'patterns': [
                    r'\?',
                    r'how (can|do|should)',
                    r'what (should|can|do)',
                    r'any advice',
                    r'any suggestions',
                    r'recommend',
                    r'anyone have experience'
                ]
            },
            
            # Length multipliers - weight content based on length
            'content_length': {
                'short': {'max_chars': 100, 'weight': 0.8},  # Brief posts get lower weight
                'medium': {'max_chars': 500, 'weight': 1.0},  # Medium posts get normal weight
                'long': {'max_chars': 1000, 'weight': 1.2},   # Detailed posts get higher weight
                'very_long': {'min_chars': 1000, 'weight': 1.4}  # Very detailed posts get highest weight
            },
            
            # Activity recency (in days)
            'activity_recency': {
                'recent': {'max_days': 7, 'weight': 1.3},      # Within a week
                'medium': {'max_days': 30, 'weight': 1.0},     # Within a month
                'old': {'max_days': 90, 'weight': 0.7},        # Within three months
                'very_old': {'min_days': 90, 'weight': 0.5}    # Older than three months
            },
            
            # Platform-specific weights
            'platform_weights': {
                'linkedin': 1.0,      # LinkedIn leads
                'reddit': 1.2         # Reddit leads - tends to have more direct questions/needs
            },
            
            # Score thresholds
            'score_thresholds': {
                'high_priority': 7.0,   # Scores 9-10
                'medium_priority': 4.0, # Scores 6-8
                'low_priority': 2.0,    # Scores 3-5
                'very_low_priority': 0  # Scores 1-2
            }
        }
        
        logger.info("Lead scorer initialized")
    
    def _init_openai_client(self) -> None:
        """Initialize the OpenAI API client for advanced content analysis."""
        try:
            openai_api_key = os.getenv('OPENAI_API_KEY')
            
            if not openai_api_key:
                raise ValueError("OpenAI API key missing in environment variables")
            
            self.client = Client(api_key=openai_api_key)
            logger.info("Successfully connected to OpenAI API")
            
        except Exception as e:
            logger.error(f"Error initializing OpenAI client: {str(e)}")
            self.use_ai_analysis = False
            logger.info("Falling back to rule-based scoring only")
    
    def _check_keyword_matches(self, text: str) -> Dict[str, List[str]]:
        """
        Check for keyword matches in the provided text.
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary of matched keywords by urgency category
        """
        if not text:
            return {'high_urgency': [], 'medium_urgency': [], 'low_urgency': []}
        
        text = text.lower()
        matches = {
            'high_urgency': [],
            'medium_urgency': [],
            'low_urgency': []
        }
        
        # Check each urgency category
        for urgency, keywords in self.scoring_config['pain_point_keywords'].items():
            for keyword in keywords:
                if keyword in text:
                    matches[urgency].append(keyword)
        
        return matches
    
    def _analyze_tone(self, text: str) -> Dict[str, int]:
        """
        Analyze the tone of the provided text.
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with counts of tone indicators
        """
        if not text:
            return {}
        
        text = text.lower()
        tone_matches = {}
        
        # Check each tone category
        for tone, tone_info in self.scoring_config['tone_indicators'].items():
            count = 0
            for pattern in tone_info['patterns']:
                count += len(re.findall(pattern, text))
            
            if count > 0:
                tone_matches[tone] = count
        
        return tone_matches
    
    def _check_questions(self, text: str) -> int:
        """
        Check for question patterns in the text.
        
        Args:
            text: Text to analyze
            
        Returns:
            Count of question patterns found
        """
        if not text:
            return 0
        
        text = text.lower()
        question_count = 0
        
        for pattern in self.scoring_config['question_patterns']['patterns']:
            question_count += len(re.findall(pattern, text))
        
        return question_count
    
    def _calculate_content_length_multiplier(self, text: str) -> float:
        """
        Calculate the content length multiplier.
        
        Args:
            text: Text to analyze
            
        Returns:
            Content length multiplier (weight)
        """
        if not text:
            return 1.0
        
        text_length = len(text)
        
        # Apply appropriate multiplier based on length
        if text_length >= self.scoring_config['content_length']['very_long']['min_chars']:
            return self.scoring_config['content_length']['very_long']['weight']
        elif text_length >= self.scoring_config['content_length']['long']['max_chars']:
            return self.scoring_config['content_length']['long']['weight']
        elif text_length >= self.scoring_config['content_length']['medium']['max_chars']:
            return self.scoring_config['content_length']['medium']['weight']
        else:
            return self.scoring_config['content_length']['short']['weight']
    
    def _calculate_recency_multiplier(self, date_str: str) -> float:
        """
        Calculate the recency multiplier based on the post date.
        
        Args:
            date_str: Date string in format YYYY-MM-DD or YYYY-MM-DD HH:MM:SS
            
        Returns:
            Recency multiplier (weight)
        """
        if not date_str:
            return 1.0
        
        try:
            # Try different date formats
            try:
                post_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    post_date = datetime.strptime(date_str, "%Y-%m-%d")
                except ValueError:
                    # Default to current date if parsing fails
                    return 1.0
            
            # Calculate days since post
            days_diff = (datetime.now() - post_date).days
            
            # Apply appropriate multiplier based on recency
            if days_diff <= self.scoring_config['activity_recency']['recent']['max_days']:
                return self.scoring_config['activity_recency']['recent']['weight']
            elif days_diff <= self.scoring_config['activity_recency']['medium']['max_days']:
                return self.scoring_config['activity_recency']['medium']['weight']
            elif days_diff <= self.scoring_config['activity_recency']['old']['max_days']:
                return self.scoring_config['activity_recency']['old']['weight']
            else:
                return self.scoring_config['activity_recency']['very_old']['weight']
                
        except Exception as e:
            logger.error(f"Error calculating recency multiplier: {str(e)}")
            return 1.0
    
    def _analyze_with_ai(self, text: str) -> Dict[str, Any]:
        """
        Analyze text using OpenAI API for more nuanced understanding.
        
        Args:
            text: Text to analyze
            
        Returns:
            Analysis results
        """
        if not self.use_ai_analysis or not text:
            return {'ai_score': 0, 'reasoning': 'AI analysis disabled or no text provided'}
        
        try:
            # Craft the system message
            system_message = """You are an AI lead scoring assistant. Your task is to analyze the given 
            text and determine the likelihood that the person would respond positively to outreach from 
            a professional coach or mentor who could help them with their work-related challenges.
            
            Consider these factors:
            1. Urgency of pain points (high, medium, low)
            2. Tone (desperate, negative, reflective, positive)
            3. Whether they're actively seeking advice or help
            4. Specificity of their problem
            5. Openness to external input
            
            Respond with:
            1. A score from 0 to 10 (with 10 being extremely likely to respond)
            2. A brief explanation of your reasoning
            3. Key pain points you identified
            
            Format your response as a JSON object with keys: score, reasoning, pain_points"""
            
            # User message
            user_message = f"Please analyze this content for lead scoring:\n\n{text[:2000]}"  # Limit length for API
            
            # Call the OpenAI API
            response = self.client.chat.completions.create(
                model=self.model,
                response_format={"type": "json_object"},
                temperature=0.1,  # Lower temperature for more consistent scoring
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": user_message}
                ],
                max_tokens=500
            )
            
            # Parse the response
            analysis_text = response.choices[0].message.content
            analysis = json.loads(analysis_text)
            
            # Ensure expected fields exist
            if 'score' not in analysis:
                analysis['score'] = 5  # Default middle score
            
            if 'reasoning' not in analysis:
                analysis['reasoning'] = "No reasoning provided"
                
            if 'pain_points' not in analysis:
                analysis['pain_points'] = []
            
            return {
                'ai_score': float(analysis['score']),
                'ai_reasoning': analysis['reasoning'],
                'ai_pain_points': analysis['pain_points']
            }
            
        except Exception as e:
            logger.error(f"Error during AI analysis: {str(e)}")
            return {'ai_score': 0, 'reasoning': f'Error during AI analysis: {str(e)}'}
    
    def score_linkedin_lead(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        """
        Score a LinkedIn lead based on their profile and posts.
        
        Args:
            lead: Dictionary containing LinkedIn lead data
            
        Returns:
            Lead dictionary with added score data
        """
        try:
            logger.info(f"Scoring LinkedIn lead: {lead.get('name', 'Unknown')}")
            
            # Extract relevant text for analysis
            name = lead.get('name', '')
            job_title = lead.get('job_title', '')
            bio_snippet = lead.get('bio_snippet', '')
            
            # Get recent posts
            recent_posts = lead.get('recent_posts', [])
            if isinstance(recent_posts, str):
                # Handle case where it's a string instead of a list
                if ';' in recent_posts:
                    recent_posts = recent_posts.split(';')
                else:
                    recent_posts = [recent_posts]
            
            # Combine all posts
            all_posts_text = ' '.join(recent_posts)
            
            # Combine all text for analysis
            combined_text = f"{bio_snippet} {all_posts_text}"
            
            # Base score initialization
            base_score = 5.0  # Start at neutral midpoint
            score_factors = {}
            
            # Apply platform weight
            platform_weight = self.scoring_config['platform_weights']['linkedin']
            score_factors['platform_weight'] = platform_weight
            
            # Check keyword matches
            keyword_matches = self._check_keyword_matches(combined_text)
            
            # Calculate score adjustment for keywords
            keyword_score = 0
            for urgency, keywords in keyword_matches.items():
                for keyword in keywords:
                    if urgency == 'high_urgency':
                        keyword_score += self.scoring_config['pain_point_keywords']['high_urgency'][keyword]
                    elif urgency == 'medium_urgency':
                        keyword_score += self.scoring_config['pain_point_keywords']['medium_urgency'][keyword]
                    elif urgency == 'low_urgency':
                        keyword_score += self.scoring_config['pain_point_keywords']['low_urgency'][keyword]
            
            score_factors['keyword_score'] = keyword_score
            
            # Analyze tone
            tone_matches = self._analyze_tone(combined_text)
            tone_score = 0
            for tone, count in tone_matches.items():
                tone_score += self.scoring_config['tone_indicators'][tone]['weight'] * count
            
            score_factors['tone_score'] = tone_score
            
            # Check for questions
            question_count = self._check_questions(combined_text)
            question_score = question_count * self.scoring_config['question_patterns']['weight']
            score_factors['question_score'] = question_score
            
            # Apply content length multiplier
            content_length_multiplier = self._calculate_content_length_multiplier(combined_text)
            score_factors['content_length_multiplier'] = content_length_multiplier
            
            # Apply recency multiplier if date is available
            recency_multiplier = 1.0
            if 'date_added' in lead:
                recency_multiplier = self._calculate_recency_multiplier(lead['date_added'])
            score_factors['recency_multiplier'] = recency_multiplier
            
            # Calculate rule-based score
            rule_based_score = (base_score + keyword_score + tone_score + question_score) * content_length_multiplier * recency_multiplier * platform_weight
            
            # Apply AI analysis if enabled
            ai_analysis = {}
            if self.use_ai_analysis:
                ai_analysis = self._analyze_with_ai(combined_text)
                
                # Create a weighted average between rule-based and AI score
                # We'll trust AI a bit more with a 60/40 split
                final_score = (0.4 * rule_based_score) + (0.6 * ai_analysis.get('ai_score', 0))
            else:
                final_score = rule_based_score
            
            # Ensure score is within 1-10 range
            final_score = max(1, min(10, final_score))
            
            # Determine priority level
            priority_level = "very_low_priority"
            for level, threshold in sorted(self.scoring_config['score_thresholds'].items(), key=lambda x: x[1], reverse=True):
                if final_score >= threshold:
                    priority_level = level
                    break
            
            # Prepare score data
            score_data = {
                'rule_based_score': round(rule_based_score, 2),
                'score_factors': score_factors,
                'keyword_matches': keyword_matches,
                'tone_matches': tone_matches,
                'question_count': question_count,
                'ai_analysis': ai_analysis,
                'final_score': round(final_score, 1),
                'priority_level': priority_level
            }
            
            # Add score data to lead
            scored_lead = lead.copy()
            scored_lead['score_data'] = score_data
            scored_lead['response_score'] = round(final_score, 1)
            scored_lead['priority_level'] = priority_level
            
            logger.info(f"Scored LinkedIn lead {lead.get('name', 'Unknown')}: {scored_lead['response_score']}")
            
            return scored_lead
            
        except Exception as e:
            logger.error(f"Error scoring LinkedIn lead: {str(e)}")
            return {**lead, 'response_score': 5.0, 'priority_level': 'error', 'error': str(e)}
    
    def score_reddit_lead(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        """
        Score a Reddit lead based on their post content.
        
        Args:
            lead: Dictionary containing Reddit lead data
            
        Returns:
            Lead dictionary with added score data
        """
        try:
            logger.info(f"Scoring Reddit lead: {lead.get('username', 'Unknown')}")
            
            # Extract relevant text for analysis
            username = lead.get('username', '')
            post_title = lead.get('post_title', '')
            post_content = lead.get('post_content', '')
            matched_keywords = lead.get('matched_keywords', '')
            subreddit = lead.get('subreddit', '')
            
            # Combine all text for analysis
            combined_text = f"{post_title} {post_content}"
            
            # Base score initialization
            base_score = 5.0  # Start at neutral midpoint
            score_factors = {}
            
            # Apply platform weight
            platform_weight = self.scoring_config['platform_weights']['reddit']
            score_factors['platform_weight'] = platform_weight
            
            # Check keyword matches
            keyword_matches = self._check_keyword_matches(combined_text)
            
            # Calculate score adjustment for keywords
            keyword_score = 0
            for urgency, keywords in keyword_matches.items():
                for keyword in keywords:
                    if urgency == 'high_urgency':
                        keyword_score += self.scoring_config['pain_point_keywords']['high_urgency'][keyword]
                    elif urgency == 'medium_urgency':
                        keyword_score += self.scoring_config['pain_point_keywords']['medium_urgency'][keyword]
                    elif urgency == 'low_urgency':
                        keyword_score += self.scoring_config['pain_point_keywords']['low_urgency'][keyword]
            
            score_factors['keyword_score'] = keyword_score
            
            # Analyze tone
            tone_matches = self._analyze_tone(combined_text)
            tone_score = 0
            for tone, count in tone_matches.items():
                tone_score += self.scoring_config['tone_indicators'][tone]['weight'] * count
            
            score_factors['tone_score'] = tone_score
            
            # Check for questions
            question_count = self._check_questions(combined_text)
            question_score = question_count * self.scoring_config['question_patterns']['weight']
            score_factors['question_score'] = question_score
            
            # Apply content length multiplier
            content_length_multiplier = self._calculate_content_length_multiplier(combined_text)
            score_factors['content_length_multiplier'] = content_length_multiplier
            
            # Apply recency multiplier if date is available
            recency_multiplier = 1.0
            if 'created_utc' in lead:
                recency_multiplier = self._calculate_recency_multiplier(lead['created_utc'])
            score_factors['recency_multiplier'] = recency_multiplier
            
            # Include boost for high-engagement posts
            engagement_boost = 0
            if 'score' in lead and lead['score']:
                try:
                    post_score = int(lead['score'])
                    if post_score > 100:
                        engagement_boost = 1.0
                    elif post_score > 50:
                        engagement_boost = 0.7
                    elif post_score > 10:
                        engagement_boost = 0.3
                except (ValueError, TypeError):
                    pass
            
            score_factors['engagement_boost'] = engagement_boost
            
            # Calculate rule-based score
            rule_based_score = (base_score + keyword_score + tone_score + question_score + engagement_boost) * content_length_multiplier * recency_multiplier * platform_weight
            
            # Apply AI analysis if enabled
            ai_analysis = {}
            if self.use_ai_analysis:
                ai_analysis = self._analyze_with_ai(combined_text)
                
                # Create a weighted average between rule-based and AI score
                # We'll trust AI a bit more with a 60/40 split
                final_score = (0.4 * rule_based_score) + (0.6 * ai_analysis.get('ai_score', 0))
            else:
                final_score = rule_based_score
            
            # Ensure score is within 1-10 range
            final_score = max(1, min(10, final_score))
            
            # Determine priority level
            priority_level = "very_low_priority"
            for level, threshold in sorted(self.scoring_config['score_thresholds'].items(), key=lambda x: x[1], reverse=True):
                if final_score >= threshold:
                    priority_level = level
                    break
            
            # Prepare score data
            score_data = {
                'rule_based_score': round(rule_based_score, 2),
                'score_factors': score_factors,
                'keyword_matches': keyword_matches,
                'tone_matches': tone_matches,
                'question_count': question_count,
                'ai_analysis': ai_analysis,
                'final_score': round(final_score, 1),
                'priority_level': priority_level
            }
            
            # Add score data to lead
            scored_lead = lead.copy()
            scored_lead['score_data'] = score_data
            scored_lead['response_score'] = round(final_score, 1)
            scored_lead['priority_level'] = priority_level
            
            logger.info(f"Scored Reddit lead {lead.get('username', 'Unknown')}: {scored_lead['response_score']}")
            
            return scored_lead
            
        except Exception as e:
            logger.error(f"Error scoring Reddit lead: {str(e)}")
            return {**lead, 'response_score': 5.0, 'priority_level': 'error', 'error': str(e)}
    
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
    
    def process_all_leads(self, sheets_client, max_linkedin_leads: int = 50, 
                         max_reddit_leads: int = 50) -> Dict[str, Any]:
        """
        Process both LinkedIn and Reddit leads.
        
        Args:
            sheets_client: Google Sheets client
            max_linkedin_leads: Maximum number of LinkedIn leads to process
            max_reddit_leads: Maximum number of Reddit leads to process
            
        Returns:
            Dictionary with results summary
        """
        results = {
            'linkedin_leads_scored': 0,
            'reddit_leads_scored': 0,
            'high_priority_leads': 0,
            'medium_priority_leads': 0,
            'low_priority_leads': 0,
            'very_low_priority_leads': 0
        }
        
        try:
            # Process LinkedIn leads
            linkedin_leads = self.process_linkedin_leads(sheets_client, max_leads=max_linkedin_leads)
            results['linkedin_leads_scored'] = len(linkedin_leads)
            
            # Process Reddit leads
            reddit_leads = self.process_reddit_leads(sheets_client, max_leads=max_reddit_leads)
            results['reddit_leads_scored'] = len(reddit_leads)
            
            # Count priority levels
            all_leads = linkedin_leads + reddit_leads
            for lead in all_leads:
                priority = lead.get('priority_level', '')
                if priority == 'high_priority':
                    results['high_priority_leads'] += 1
                elif priority == 'medium_priority':
                    results['medium_priority_leads'] += 1
                elif priority == 'low_priority':
                    results['low_priority_leads'] += 1
                elif priority == 'very_low_priority':
                    results['very_low_priority_leads'] += 1
            
            logger.info(f"Processed a total of {len(all_leads)} leads")
            logger.info(f"High priority: {results['high_priority_leads']}, Medium: {results['medium_priority_leads']}, Low: {results['low_priority_leads']}, Very Low: {results['very_low_priority_leads']}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error processing all leads: {str(e)}")
            return results
    
    def update_manual_scores(self, sheets_client, platform: str = "linkedin", 
                            updated_scores: List[Dict[str, Any]] = None) -> bool:
        """
        Update lead scores with manual adjustments.
        
        Args:
            sheets_client: Google Sheets client
            platform: Either "linkedin" or "reddit"
            updated_scores: List of dictionaries with updated scores
                Each dict should have 'url', 'manual_adjustment', 'final_score', and 'notes' keys
            
        Returns:
            True if updating was successful, False otherwise
        """
        if not updated_scores:
            logger.warning("No scores provided for manual update")
            return False
            
        try:
            # Determine worksheet name based on platform
            worksheet_name = "LinkedInLeadScores" if platform.lower() == "linkedin" else "RedditLeadScores"
            
            # Get the worksheet
            worksheet = sheets_client.open('LeadGenerationData').worksheet(worksheet_name)
            
            # Get all values including header row
            all_values = worksheet.get_all_values()
            
            if not all_values or len(all_values) <= 1:
                logger.warning(f"No data found in {worksheet_name}")
                return False
                
            # Extract header and data
            headers = all_values[0]
            data_rows = all_values[1:]
            
            # Find column indices
            url_idx = 3  # Default URL column index
            manual_adj_idx = 11  # Default manual adjustment column index
            final_score_idx = 12  # Default final score column index
            notes_idx = 13  # Default notes column index
            
            for i, header in enumerate(headers):
                header_lower = header.lower()
                if 'url' in header_lower:
                    url_idx = i
                elif 'manual' in header_lower and 'adjustment' in header_lower:
                    manual_adj_idx = i
                elif 'final' in header_lower and 'score' in header_lower:
                    final_score_idx = i
                elif 'note' in header_lower:
                    notes_idx = i
            
            # Update scores
            updates_count = 0
            for update in updated_scores:
                url = update.get('url', '')
                if not url:
                    continue
                    
                # Find the row for this URL
                for row_idx, row in enumerate(data_rows):
                    if row_idx < len(data_rows) and url_idx < len(row) and row[url_idx] == url:
                        # Update cells
                        # Row index is +2 because of 0-indexing and header row
                        actual_row = row_idx + 2
                        
                        # Update manual adjustment
                        if 'manual_adjustment' in update:
                            worksheet.update_cell(actual_row, manual_adj_idx + 1, str(update['manual_adjustment']))
                            
                        # Update final score
                        if 'final_score' in update:
                            worksheet.update_cell(actual_row, final_score_idx + 1, str(update['final_score']))
                            
                        # Update notes
                        if 'notes' in update:
                            worksheet.update_cell(actual_row, notes_idx + 1, update['notes'])
                            
                        updates_count += 1
                        break
            
            logger.info(f"Updated {updates_count} lead scores with manual adjustments")
            return True
            
        except Exception as e:
            logger.error(f"Error updating manual scores: {str(e)}")
            return False


def run_lead_scorer(sheets_client, max_linkedin_leads: int = 50, max_reddit_leads: int = 50,
                   use_ai_analysis: bool = True, model: str = "gpt-4") -> Dict[str, Any]:
    """
    Run the lead scorer as a standalone function.
    
    Args:
        sheets_client: Google Sheets client
        max_linkedin_leads: Maximum number of LinkedIn leads to process
        max_reddit_leads: Maximum number of Reddit leads to process
        use_ai_analysis: Whether to use OpenAI for content analysis
        model: OpenAI model to use for content analysis
        
    Returns:
        Dictionary with results summary
    """
    # Create the scorer
    scorer = LeadScorer(use_ai_analysis=use_ai_analysis, model=model)
    
    # Process all leads
    results = scorer.process_all_leads(
        sheets_client,
        max_linkedin_leads=max_linkedin_leads,
        max_reddit_leads=max_reddit_leads
    )
    
    return results


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
    
    # Run the lead scorer
    results = run_lead_scorer(
        sheets_client,
        max_linkedin_leads=20,
        max_reddit_leads=20,
        use_ai_analysis=True
    )
    
    print(f"Results: {results}")
            
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
    
    def save_linkedin_scores(self, sheets_client, scored_leads: List[Dict[str, Any]], 
                             output_worksheet_name: str = "LinkedInLeadScores") -> bool:
        """
        Save scored LinkedIn leads to Google Sheets.
        
        Args:
            sheets_client: Google Sheets client
            scored_leads: List of leads with score data
            output_worksheet_name: Name of the worksheet to save scores to
            
        Returns:
            True if saving was successful, False otherwise
        """
        try:
            logger.info(f"Saving LinkedIn lead scores to worksheet: {output_worksheet_name}")
            
            # Check if worksheet exists, create if not
            try:
                worksheet = sheets_client.open('LeadGenerationData').worksheet(output_worksheet_name)
            except:
                worksheet = sheets_client.open('LeadGenerationData').add_worksheet(
                    title=output_worksheet_name, rows=1000, cols=15
                )
                # Add headers
                headers = [
                    "Name", "Job Title", "Industry", "Profile URL", "Response Score", 
                    "Priority Level", "High Urgency Keywords", "Medium Urgency Keywords", 
                    "Low Urgency Keywords", "Question Count", "AI Reasoning", 
                    "Manual Adjustment", "Final Score", "Notes", "Date Scored"
                ]
                worksheet.append_row(headers)
            
            # Prepare rows for Google Sheets
            rows = []
            for lead in scored_leads:
                # Format keyword matches for readability
                high_urgency = ", ".join(lead.get('score_data', {}).get('keyword_matches', {}).get('high_urgency', []))
                medium_urgency = ", ".join(lead.get('score_data', {}).get('keyword_matches', {}).get('medium_urgency', []))
                low_urgency = ", ".join(lead.get('score_data', {}).get('keyword_matches', {}).get('low_urgency', []))
                
                # Get AI reasoning if available
                ai_reasoning = lead.get('score_data', {}).get('ai_analysis', {}).get('ai_reasoning', '')
                
                # Create a row for each lead
                row = [
                    lead.get('name', ''),
                    lead.get('job_title', ''),
                    lead.get('industry', ''),
                    lead.get('profile_url', ''),
                    str(lead.get('response_score', 0)),
                    lead.get('priority_level', ''),
                    high_urgency,
                    medium_urgency,
                    low_urgency,
                    str(lead.get('score_data', {}).get('question_count', 0)),
                    ai_reasoning,
                    "",  # Manual adjustment column (empty by default)
                    str(lead.get('response_score', 0)),  # Final score (same as response score initially)
                    "",  # Notes column (empty by default)
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ]
                rows.append(row)
            
            # Append to Google Sheet
            for row in rows:
                worksheet.append_row(row)
                
            logger.info(f"Successfully saved {len(rows)} LinkedIn lead scores to Google Sheets")
            return True
            
        except Exception as e:
            logger.error(f"Error saving LinkedIn lead scores to Google Sheets: {str(e)}")
            return False
    
    def save_reddit_scores(self, sheets_client, scored_leads: List[Dict[str, Any]], 
                           output_worksheet_name: str = "RedditLeadScores") -> bool:
        """
        Save scored Reddit leads to Google Sheets.
        
        Args:
            sheets_client: Google Sheets client
            scored_leads: List of leads with score data
            output_worksheet_name: Name of the worksheet to save scores to
            
        Returns:
            True if saving was successful, False otherwise
        """
        try:
            logger.info(f"Saving Reddit lead scores to worksheet: {output_worksheet_name}")
            
            # Check if worksheet exists, create if not
            try:
                worksheet = sheets_client.open('LeadGenerationData').worksheet(output_worksheet_name)
            except:
                worksheet = sheets_client.open('LeadGenerationData').add_worksheet(
                    title=output_worksheet_name, rows=1000, cols=15
                )
                # Add headers
                headers = [
                    "Username", "Subreddit", "Post Title", "Post URL", "Response Score", 
                    "Priority Level", "High Urgency Keywords", "Medium Urgency Keywords", 
                    "Low Urgency Keywords", "Question Count", "AI Reasoning", 
                    "Manual Adjustment", "Final Score", "Notes", "Date Scored"
                ]
                worksheet.append_row(headers)
            
            # Prepare rows for Google Sheets
            rows = []
            for lead in scored_leads:
                # Format keyword matches for readability
                high_urgency = ", ".join(lead.get('score_data', {}).get('keyword_matches', {}).get('high_urgency', []))
                medium_urgency = ", ".join(lead.get('score_data', {}).get('keyword_matches', {}).get('medium_urgency', []))
                low_urgency = ", ".join(lead.get('score_data', {}).get('keyword_matches', {}).get('low_urgency', []))
                
                # Get AI reasoning if available
                ai_reasoning = lead.get('score_data', {}).get('ai_analysis', {}).get('ai_reasoning', '')
                
                # Create a row for each lead
                row = [
                    lead.get('username', ''),
                    lead.get('subreddit', ''),
                    lead.get('post_title', '')[:100],  # Limit title length
                    lead.get('post_url', ''),
                    str(lead.get('response_score', 0)),
                    lead.get('priority_level', ''),
                    high_urgency,
                    medium_urgency,
                    low_urgency,
                    str(lead.get('score_data', {}).get('question_count', 0)),
                    ai_reasoning,
                    "",  # Manual adjustment column (empty by default)
                    str(lead.get('response_score', 0)),  # Final score (same as response score initially)
                    "",  # Notes column (empty by default)
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ]
                rows.append(row)
            
            # Append to Google Sheet
            for row in rows:
                worksheet.append_row(row)
                
            logger.info(f"Successfully saved {len(rows)} Reddit lead scores to Google Sheets")
            return True
            
        except Exception as e:
            logger.error(f"Error saving Reddit lead scores to Google Sheets: {str(e)}")
            return False
    
    def process_linkedin_leads(self, sheets_client, max_leads: int = 50) -> List[Dict[str, Any]]:
        """
        Process LinkedIn leads by scoring them based on likelihood to respond.
        
        Args:
            sheets_client: Google Sheets client
            max_leads: Maximum number of leads to process
            
        Returns:
            List of scored leads
        """
        try:
            # Get LinkedIn leads
            all_leads = self.get_linkedin_leads(sheets_client)
            
            if not all_leads:
                logger.warning("No LinkedIn leads found to process")
                return []
            
            # Get existing scores to avoid duplicates
            existing_scores = set()
            try:
                scores_worksheet = sheets_client.open('LeadGenerationData').worksheet('LinkedInLeadScores')
                all_values = scores_worksheet.get_all_values()
                
                # Extract profile URLs from existing scores (assuming column 4)
                if all_values and len(all_values) > 1:  # Check if there's data besides header
                    existing_scores = set(row[3] for row in all_values[1:] if len(row) > 3)
                    
            except Exception as e:
                logger.warning(f"Could not retrieve existing scores: {str(e)}")
            
            # Filter leads that haven't been scored yet
            leads_to_score = []
            for lead in all_leads:
                profile_url = lead.get('profile_url', '')
                if profile_url not in existing_scores:
                    leads_to_score.append(lead)
            
            # Limit to max_leads
            leads_to_score = leads_to_score[:max_leads]
            logger.info(f"Processing {len(leads_to_score)} LinkedIn leads")
            
            # Score leads
            scored_leads = []
            for lead in leads_to_score:
                scored_lead = self.score_linkedin_lead(lead)
                scored_leads.append(scored_lead)
            
            # Save scores to Google Sheets
            if scored_leads:
                self.save_linkedin_scores(sheets_client, scored_leads)
            
            return scored_leads
            
        except Exception as e:
            logger.error(f"Error processing LinkedIn leads: {str(e)}")
            return []
    
    def process_reddit_leads(self, sheets_client, max_leads: int = 50) -> List[Dict[str, Any]]:
        """
        Process Reddit leads by scoring them based on likelihood to respond.
        
        Args:
            sheets_client: Google Sheets client
            max_leads: Maximum number of leads to process
            
        Returns:
            List of scored leads
        """
        try:
            # Get Reddit leads
            all_leads = self.get_reddit_leads(sheets_client)
            
            if not all_leads:
                logger.warning("No Reddit leads found to process")
                return []
            
            # Get existing scores to avoid duplicates
            existing_scores = set()
            try:
                scores_worksheet = sheets_client.open('LeadGenerationData').worksheet('RedditLeadScores')
                all_values = scores_worksheet.get_all_values()
                
                # Extract post URLs from existing scores (assuming column 4)
                if all_values and len(all_values) > 1:  # Check if there's data besides header
                    existing_scores = set(row[3] for row in all_values[1:] if len(row) > 3)
                    
            except Exception as e:
                logger.warning(f"Could not retrieve existing scores: {str(e)}")
            
            # Filter leads that haven't been scored yet
            leads_to_score = []
            for lead in all_leads:
                post_url = lead.get('post_url', '')
                if post_url not in existing_scores:
                    leads_to_score.append(lead)
            
            # Limit to max_leads
            leads_to_score = leads_to_score[:max_leads]
            logger.info(f"Processing {len(leads_to_score)} Reddit leads")
            
            # Score leads
            scored_leads = []
            for lead in leads_to_score:
                scored_lead = self.score_reddit_lead(lead)
                scored_leads.append(scored_lead)
            
            # Save scores to Google Sheets
            if scored_leads:
                self.save_reddit_scores(sheets_client, scored_leads)
            
            return scored_leads
            
        except Exception as e:
            logger.error(f"Error processing Reddit leads: {str(e)}")
            return []