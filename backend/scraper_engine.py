"""
Advanced Web Scraping Engine for IndiaBix Aptitude Questions
Implements robust anti-detection measures and error recovery
"""

import asyncio
import logging
import random
import time
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime, timedelta
import json
from urllib.parse import urljoin, urlparse

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.common.action_chains import ActionChains

from bs4 import BeautifulSoup
import requests
from fake_useragent import UserAgent
import textdistance

from models import Question, ScrapingJob, ScrapingProgress, QuestionQuality, DifficultyLevel, ScrapingStatus
from scraper_config import INDIABIX_CONFIG, INDIABIX_SELECTORS, QUALITY_THRESHOLDS, DEFAULT_SCRAPING_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IndiaBixScraper:
    """
    Advanced scraper for IndiaBix aptitude questions with anti-detection measures
    """
    
    def __init__(self, config=None):
        self.config = config or DEFAULT_SCRAPING_CONFIG
        self.driver = None
        self.session = requests.Session()
        self.scraped_questions = []
        self.duplicate_count = 0
        self.error_count = 0
        self.success_count = 0
        
        # Anti-detection state
        self.current_user_agent = None
        self.last_request_time = 0
        
        # Initialize user agent rotation
        self.ua = UserAgent()
        self.setup_session()
    
    def setup_session(self):
        """Setup requests session with headers"""
        self.current_user_agent = random.choice(self.config.USER_AGENTS)
        self.session.headers.update({
            'User-Agent': self.current_user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def create_driver(self) -> webdriver.Chrome:
        """Create and configure Chrome WebDriver with anti-detection options"""
        try:
            chrome_options = Options()
            
            # Basic options
            if self.config.HEADLESS:
                chrome_options.add_argument('--headless')
            chrome_options.add_argument(f'--window-size={self.config.WINDOW_SIZE}')
            
            # Anti-detection options
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Performance options
            if self.config.DISABLE_IMAGES:
                prefs = {"profile.managed_default_content_settings.images": 2}
                chrome_options.add_experimental_option("prefs", prefs)
            
            # User agent
            chrome_options.add_argument(f'--user-agent={self.current_user_agent}')
            
            # Create driver
            service = Service('/usr/bin/chromedriver')
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Execute script to remove automation indicators
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            return driver
            
        except Exception as e:
            logger.error(f"Failed to create Chrome driver: {e}")
            raise
    
    async def random_delay(self, min_delay: Optional[float] = None, max_delay: Optional[float] = None):
        """Implement random delay to mimic human behavior"""
        min_delay = min_delay or self.config.MIN_DELAY
        max_delay = max_delay or self.config.MAX_DELAY
        delay = random.uniform(min_delay, max_delay)
        
        # Ensure minimum time between requests
        time_since_last = time.time() - self.last_request_time
        if time_since_last < delay:
            await asyncio.sleep(delay - time_since_last)
        
        self.last_request_time = time.time()
    
    def simulate_human_behavior(self):
        """Simulate human-like mouse movements and actions"""
        if not self.driver:
            return
            
        try:
            # Random mouse movement
            actions = ActionChains(self.driver)
            
            # Get window size
            size = self.driver.get_window_size()
            width, height = size['width'], size['height']
            
            # Random coordinates
            x = random.randint(100, width - 100)
            y = random.randint(100, height - 100)
            
            # Move mouse to random position
            actions.move_by_offset(x, y)
            actions.perform()
            
            # Random scroll
            scroll_y = random.randint(-300, 300)
            self.driver.execute_script(f"window.scrollBy(0, {scroll_y});")
            
        except Exception as e:
            logger.debug(f"Human behavior simulation error: {e}")
    
    def extract_question_from_page(self, page_source: str, url: str) -> Optional[Dict[str, Any]]:
        """Extract question data from HTML page source"""
        try:
            soup = BeautifulSoup(page_source, 'lxml')
            
            # Extract question text
            question_element = soup.select_one(INDIABIX_SELECTORS["question_text"])
            if not question_element:
                logger.warning(f"No question text found on {url}")
                return None
            
            question_text = question_element.get_text(strip=True)
            
            # Extract options
            option_elements = soup.select(INDIABIX_SELECTORS["option"])
            if len(option_elements) < 4:
                logger.warning(f"Insufficient options found on {url}")
                return None
            
            options = []
            for opt in option_elements[:4]:  # Take first 4 options
                option_text = opt.get_text(strip=True)
                if option_text:
                    options.append(option_text)
            
            if len(options) != 4:
                logger.warning(f"Could not extract 4 options from {url}")
                return None
            
            # Extract correct answer (this might need adjustment based on IndiaBix structure)
            answer_element = soup.select_one(INDIABIX_SELECTORS["answer_container"])
            correct_answer = ""
            if answer_element:
                # Try to find the correct answer text
                answer_text = answer_element.get_text(strip=True)
                # Match with one of the options
                for option in options:
                    if option.lower() in answer_text.lower() or answer_text.lower() in option.lower():
                        correct_answer = option
                        break
            
            # If no match found, take first option as default (needs manual review)
            if not correct_answer and options:
                correct_answer = options[0]
                logger.warning(f"Could not determine correct answer for {url}, using first option")
            
            # Extract explanation
            explanation_element = soup.select_one(INDIABIX_SELECTORS["explanation"])
            explanation = ""
            if explanation_element:
                explanation = explanation_element.get_text(strip=True)
            
            # Basic validation
            if not self.validate_question_quality(question_text, options, correct_answer):
                return None
            
            return {
                'question_text': question_text,
                'options': options,
                'correct_answer': correct_answer,
                'explanation': explanation,
                'source_url': url
            }
            
        except Exception as e:
            logger.error(f"Error extracting question from {url}: {e}")
            return None
    
    def validate_question_quality(self, question_text: str, options: List[str], correct_answer: str) -> bool:
        """Validate question meets quality standards"""
        try:
            # Check question length
            if not (QUALITY_THRESHOLDS["min_question_length"] <= len(question_text) <= QUALITY_THRESHOLDS["max_question_length"]):
                return False
            
            # Check options
            if len(options) != QUALITY_THRESHOLDS["required_options"]:
                return False
            
            for option in options:
                if not (QUALITY_THRESHOLDS["min_option_length"] <= len(option) <= QUALITY_THRESHOLDS["max_option_length"]):
                    return False
            
            # Check if correct answer is in options
            if correct_answer not in options:
                return False
            
            # Check for basic completeness
            if not question_text.strip() or any(not opt.strip() for opt in options):
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating question quality: {e}")
            return False
    
    def check_duplicate(self, question_text: str) -> bool:
        """Check if question is duplicate using text similarity"""
        try:
            for existing_q in self.scraped_questions:
                similarity = textdistance.cosine.normalized_similarity(
                    question_text.lower(), 
                    existing_q['question_text'].lower()
                )
                if similarity > QUALITY_THRESHOLDS["similarity_threshold"]:
                    return True
            return False
        except Exception as e:
            logger.error(f"Error checking duplicate: {e}")
            return False
    
    async def scrape_category_page(self, category: str, subcategory: str, page_url: str) -> List[Dict[str, Any]]:
        """Scrape questions from a specific category page"""
        questions_extracted = []
        
        try:
            logger.info(f"Scraping {category}/{subcategory} from {page_url}")
            
            # Navigate to page
            self.driver.get(page_url)
            await self.random_delay()
            
            # Wait for page to load
            wait = WebDriverWait(self.driver, self.config.REQUEST_TIMEOUT)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, INDIABIX_SELECTORS["question_text"])))
            
            # Simulate human behavior
            self.simulate_human_behavior()
            
            # Extract question from current page
            page_source = self.driver.page_source
            question_data = self.extract_question_from_page(page_source, page_url)
            
            if question_data:
                # Check for duplicates
                if not self.check_duplicate(question_data['question_text']):
                    # Add category and subcategory information
                    question_data.update({
                        'category': category,
                        'subcategory': subcategory,
                        'source': 'indiabix',
                        'difficulty': self.estimate_difficulty(question_data['question_text']),
                        'concepts': self.extract_concepts(category, subcategory, question_data['question_text']),
                        'tags': [category, subcategory]
                    })
                    
                    questions_extracted.append(question_data)
                    self.scraped_questions.append(question_data)
                    self.success_count += 1
                    logger.info(f"Successfully extracted question from {page_url}")
                else:
                    self.duplicate_count += 1
                    logger.info(f"Duplicate question found, skipping: {page_url}")
            else:
                self.error_count += 1
                logger.warning(f"Failed to extract question from {page_url}")
            
        except TimeoutException:
            logger.error(f"Timeout loading page: {page_url}")
            self.error_count += 1
        except Exception as e:
            logger.error(f"Error scraping page {page_url}: {e}")
            self.error_count += 1
        
        return questions_extracted
    
    def estimate_difficulty(self, question_text: str) -> DifficultyLevel:
        """Estimate question difficulty based on text complexity"""
        try:
            # Simple heuristic based on length and complexity keywords
            text_length = len(question_text.split())
            
            # Keywords that indicate difficulty
            hard_keywords = ['calculate', 'determine', 'analyze', 'complex', 'advanced', 'comprehensive']
            medium_keywords = ['find', 'compute', 'solve', 'identify']
            
            hard_count = sum(1 for keyword in hard_keywords if keyword in question_text.lower())
            medium_count = sum(1 for keyword in medium_keywords if keyword in question_text.lower())
            
            # Scoring
            if text_length > 50 or hard_count >= 2:
                return DifficultyLevel.HARD
            elif text_length > 20 or medium_count >= 1 or hard_count >= 1:
                return DifficultyLevel.MEDIUM
            else:
                return DifficultyLevel.EASY
                
        except Exception:
            return DifficultyLevel.MEDIUM
    
    def extract_concepts(self, category: str, subcategory: str, question_text: str) -> List[str]:
        """Extract relevant concepts from question text"""
        concepts = [category, subcategory]
        
        # Category-specific concept extraction
        concept_keywords = {
            'quantitative_aptitude': ['percentage', 'profit', 'loss', 'interest', 'time', 'work', 'speed', 'distance'],
            'logical_reasoning': ['series', 'pattern', 'analogy', 'coding', 'blood relation', 'direction'],
            'verbal_ability': ['synonym', 'antonym', 'grammar', 'comprehension', 'vocabulary']
        }
        
        if category in concept_keywords:
            for keyword in concept_keywords[category]:
                if keyword in question_text.lower():
                    concepts.append(keyword)
        
        return list(set(concepts))  # Remove duplicates
    
    async def scrape_subcategory(self, category: str, subcategory_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Scrape all questions from a subcategory"""
        subcategory = list(subcategory_info.keys())[0]
        config = subcategory_info[subcategory]
        
        base_url = INDIABIX_CONFIG["base_url"]
        category_url = urljoin(base_url, config["url"])
        target_questions = config.get("target_questions", 100)
        
        logger.info(f"Starting to scrape {category}/{subcategory} - Target: {target_questions} questions")
        
        all_questions = []
        page_number = 1
        consecutive_failures = 0
        
        try:
            while len(all_questions) < target_questions and consecutive_failures < 5:
                # Construct page URL (IndiaBix pagination format)
                if page_number == 1:
                    page_url = category_url
                else:
                    page_url = f"{category_url}{page_number}"
                
                # Scrape current page
                questions = await self.scrape_category_page(category, subcategory, page_url)
                
                if questions:
                    all_questions.extend(questions)
                    consecutive_failures = 0
                    logger.info(f"Page {page_number}: Extracted {len(questions)} questions. Total: {len(all_questions)}")
                else:
                    consecutive_failures += 1
                    logger.warning(f"Page {page_number}: No questions extracted. Failures: {consecutive_failures}")
                
                page_number += 1
                
                # Random delay between pages
                await self.random_delay(3, 10)
                
                # Try to navigate to next page
                try:
                    next_button = self.driver.find_element(By.CSS_SELECTOR, INDIABIX_SELECTORS["next_button"])
                    if next_button.is_enabled():
                        self.simulate_human_behavior()
                        next_button.click()
                        await self.random_delay()
                    else:
                        logger.info(f"Reached end of pages for {category}/{subcategory}")
                        break
                except NoSuchElementException:
                    logger.info(f"No next button found for {category}/{subcategory}")
                    break
        
        except Exception as e:
            logger.error(f"Error scraping subcategory {category}/{subcategory}: {e}")
        
        logger.info(f"Completed {category}/{subcategory}: {len(all_questions)} questions extracted")
        return all_questions
    
    async def start_scraping(self, target_categories: List[str] = None, target_total: int = 5000) -> Dict[str, Any]:
        """Start the main scraping process"""
        start_time = datetime.utcnow()
        
        try:
            # Initialize driver
            self.driver = self.create_driver()
            logger.info("Chrome driver initialized successfully")
            
            # Default to all categories if none specified
            if not target_categories:
                target_categories = list(INDIABIX_CONFIG["categories"].keys())
            
            all_extracted_questions = []
            stats = {
                'start_time': start_time,
                'categories_processed': [],
                'total_questions': 0,
                'success_count': 0,
                'duplicate_count': 0,
                'error_count': 0
            }
            
            for category_name in target_categories:
                if category_name not in INDIABIX_CONFIG["categories"]:
                    logger.warning(f"Category {category_name} not found in config")
                    continue
                
                category_config = INDIABIX_CONFIG["categories"][category_name]
                logger.info(f"Processing category: {category_config['display_name']}")
                
                category_questions = []
                
                # Process each subcategory
                for subcategory_name, subcategory_config in category_config["subcategories"].items():
                    try:
                        subcategory_info = {subcategory_name: subcategory_config}
                        questions = await self.scrape_subcategory(category_name, subcategory_info)
                        category_questions.extend(questions)
                        
                        logger.info(f"Subcategory {subcategory_name} completed: {len(questions)} questions")
                        
                        # Check if we've reached the target
                        if len(all_extracted_questions) + len(category_questions) >= target_total:
                            logger.info(f"Reached target of {target_total} questions")
                            break
                            
                    except Exception as e:
                        logger.error(f"Error processing subcategory {subcategory_name}: {e}")
                        continue
                
                all_extracted_questions.extend(category_questions)
                stats['categories_processed'].append({
                    'category': category_name,
                    'questions_count': len(category_questions)
                })
                
                logger.info(f"Category {category_name} completed: {len(category_questions)} questions")
                
                # Check if we've reached the target
                if len(all_extracted_questions) >= target_total:
                    break
            
            # Update final stats
            stats.update({
                'end_time': datetime.utcnow(),
                'total_questions': len(all_extracted_questions),
                'success_count': self.success_count,
                'duplicate_count': self.duplicate_count,
                'error_count': self.error_count,
                'duration': (datetime.utcnow() - start_time).total_seconds()
            })
            
            logger.info(f"Scraping completed: {len(all_extracted_questions)} questions extracted")
            
            return {
                'questions': all_extracted_questions,
                'stats': stats
            }
            
        except Exception as e:
            logger.error(f"Fatal error during scraping: {e}")
            raise
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("Chrome driver closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver:
            self.driver.quit()