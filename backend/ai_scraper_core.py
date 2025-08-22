"""
Super-Intelligent Medical Scraper Core System
Advanced AI-powered scraping architecture with massive parallel processing capabilities
"""

import asyncio
import aiohttp
import random
import time
import logging
from typing import List, Dict, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
import hashlib
import uuid
from urllib.parse import urljoin, urlparse, parse_qs
from collections import defaultdict, deque
import statistics

# AI and ML imports
import numpy as np
from fake_useragent import UserAgent

# Advanced logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ScrapingPriority(Enum):
    """Scraping priority levels for intelligent task scheduling"""
    CRITICAL = 1
    HIGH = 2
    MEDIUM = 3
    LOW = 4
    BACKGROUND = 5

class ContentType(Enum):
    """Types of medical content for specialized processing"""
    MEDICAL_ARTICLE = "medical_article"
    RESEARCH_PAPER = "research_paper"
    CLINICAL_TRIAL = "clinical_trial"
    DRUG_INFORMATION = "drug_information"
    DISEASE_INFO = "disease_info"
    SYMPTOM_INFO = "symptom_info"
    TREATMENT_INFO = "treatment_info"
    MEDICAL_NEWS = "medical_news"
    CLINICAL_GUIDELINE = "clinical_guideline"
    MEDICAL_DATABASE = "medical_database"

class ScrapingTier(Enum):
    """Medical data source tiers"""
    TIER_1_GOVERNMENT = "government"
    TIER_2_INTERNATIONAL = "international"
    TIER_3_ACADEMIC = "academic"
    TIER_4_JOURNALS = "journals"
    TIER_5_DATABASES = "databases"
    TIER_6_MEDICAL_SITES = "medical_sites"
    TIER_7_APIS = "apis"
    TIER_8_DISEASE_ORGS = "disease_orgs"
    TIER_9_NEWS = "news"
    TIER_10_INTERNATIONAL_MISC = "international_misc"

@dataclass
class ScrapingTask:
    """Individual scraping task with AI-powered metadata"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    url: str = ""
    source_name: str = ""
    tier: ScrapingTier = ScrapingTier.TIER_6_MEDICAL_SITES
    content_type: ContentType = ContentType.MEDICAL_ARTICLE
    priority: ScrapingPriority = ScrapingPriority.MEDIUM
    retry_count: int = 0
    max_retries: int = 3
    created_at: datetime = field(default_factory=datetime.utcnow)
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    estimated_processing_time: float = 30.0  # seconds
    success_probability: float = 0.8
    content_quality_score: float = 0.0

@dataclass
class ScrapingResult:
    """Comprehensive scraping result with AI analysis"""
    task_id: str
    url: str
    success: bool
    content: Optional[str] = None
    extracted_data: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    processing_time: float = 0.0
    content_length: int = 0
    quality_score: float = 0.0
    confidence_score: float = 0.0
    extracted_entities: List[str] = field(default_factory=list)
    medical_concepts: List[str] = field(default_factory=list)
    error_details: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)

class ContentDiscoveryAI:
    """AI system for intelligent content discovery and URL generation"""
    
    def __init__(self):
        self.discovered_urls = set()
        self.url_patterns = defaultdict(list)
        self.content_signatures = set()
        
    async def discover_medical_urls(self, base_url: str, medical_category: str) -> List[str]:
        """AI-powered medical URL discovery"""
        discovered = []
        
        # Pattern-based URL generation
        patterns = await self._generate_medical_url_patterns(base_url, medical_category)
        
        for pattern in patterns:
            urls = await self._expand_url_pattern(pattern, base_url)
            discovered.extend(urls)
            
        # Remove duplicates and validate
        unique_urls = list(set(discovered) - self.discovered_urls)
        validated_urls = await self._validate_medical_urls(unique_urls)
        
        self.discovered_urls.update(validated_urls)
        return validated_urls
    
    async def _generate_medical_url_patterns(self, base_url: str, category: str) -> List[str]:
        """Generate intelligent URL patterns for medical content"""
        medical_keywords = [
            'diseases', 'conditions', 'symptoms', 'treatments', 'medications',
            'procedures', 'tests', 'health-topics', 'medical-conditions',
            'patient-education', 'clinical-trials', 'research', 'studies'
        ]
        
        patterns = []
        for keyword in medical_keywords:
            patterns.extend([
                f"{keyword}/",
                f"{keyword}/a-z/",
                f"{keyword}/categories/",
                f"health/{keyword}/",
                f"medical/{keyword}/",
                f"patient-care/{keyword}/",
                f"specialties/{keyword}/",
                f"departments/{keyword}/"
            ])
            
        return patterns
    
    async def _expand_url_pattern(self, pattern: str, base_url: str) -> List[str]:
        """Expand URL patterns into actual URLs"""
        urls = []
        full_pattern = urljoin(base_url, pattern)
        
        # Add alphabetical expansions for A-Z listings
        if 'a-z' in pattern:
            for letter in 'abcdefghijklmnopqrstuvwxyz':
                urls.append(f"{full_pattern}{letter}/")
                urls.append(f"{full_pattern}?letter={letter}")
        
        # Add numerical expansions for paginated content
        for page in range(1, 21):  # Check first 20 pages
            urls.extend([
                f"{full_pattern}?page={page}",
                f"{full_pattern}/page/{page}/",
                f"{full_pattern}/{page}/"
            ])
            
        urls.append(full_pattern)
        return urls
    
    async def _validate_medical_urls(self, urls: List[str]) -> List[str]:
        """Validate URLs contain medical content"""
        validated = []
        
        async with aiohttp.ClientSession() as session:
            semaphore = asyncio.Semaphore(20)
            
            async def check_url(url):
                async with semaphore:
                    try:
                        async with session.head(url, timeout=10) as response:
                            if response.status == 200:
                                # Additional content type validation could be added here
                                return url
                    except:
                        pass
                    return None
            
            tasks = [check_url(url) for url in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            validated = [url for url in results if url and isinstance(url, str)]
            
        return validated

class ScraperOptimizationAI:
    """AI system for optimizing scraping strategies and performance"""
    
    def __init__(self):
        self.performance_history = deque(maxlen=10000)
        self.success_rates = defaultdict(list)
        self.timing_patterns = defaultdict(list)
        
    async def optimize_scraping_strategy(self, tasks: List[ScrapingTask]) -> Dict[str, Any]:
        """AI-powered scraping strategy optimization"""
        
        # Analyze task distribution
        tier_distribution = self._analyze_tier_distribution(tasks)
        
        # Optimize concurrency levels
        optimal_concurrency = await self._calculate_optimal_concurrency(tasks)
        
        # Determine optimal scheduling
        scheduling_strategy = await self._optimize_task_scheduling(tasks)
        
        # Calculate resource allocation
        resource_allocation = await self._optimize_resource_allocation(tasks)
        
        return {
            'tier_distribution': tier_distribution,
            'optimal_concurrency': optimal_concurrency,
            'scheduling_strategy': scheduling_strategy,
            'resource_allocation': resource_allocation,
            'estimated_completion_time': self._estimate_completion_time(tasks, optimal_concurrency)
        }
    
    def _analyze_tier_distribution(self, tasks: List[ScrapingTask]) -> Dict[str, int]:
        """Analyze distribution of tasks across tiers"""
        distribution = defaultdict(int)
        for task in tasks:
            distribution[task.tier.value] += 1
        return dict(distribution)
    
    async def _calculate_optimal_concurrency(self, tasks: List[ScrapingTask]) -> Dict[str, int]:
        """Calculate optimal concurrency levels per tier"""
        
        # Base concurrency levels per tier
        base_concurrency = {
            ScrapingTier.TIER_1_GOVERNMENT.value: 50,
            ScrapingTier.TIER_2_INTERNATIONAL.value: 40,
            ScrapingTier.TIER_3_ACADEMIC.value: 60,
            ScrapingTier.TIER_4_JOURNALS.value: 80,
            ScrapingTier.TIER_5_DATABASES.value: 45,
            ScrapingTier.TIER_6_MEDICAL_SITES.value: 70,
            ScrapingTier.TIER_7_APIS.value: 100,
            ScrapingTier.TIER_8_DISEASE_ORGS.value: 35,
            ScrapingTier.TIER_9_NEWS.value: 55,
            ScrapingTier.TIER_10_INTERNATIONAL_MISC.value: 30
        }
        
        # Adjust based on success rates and performance history
        optimized_concurrency = {}
        for tier, base_count in base_concurrency.items():
            success_rate = self._get_tier_success_rate(tier)
            performance_factor = self._get_tier_performance_factor(tier)
            
            # Adjust concurrency based on performance
            adjusted_count = int(base_count * success_rate * performance_factor)
            optimized_concurrency[tier] = max(10, min(adjusted_count, 200))  # Bounds: 10-200
            
        return optimized_concurrency
    
    async def _optimize_task_scheduling(self, tasks: List[ScrapingTask]) -> Dict[str, Any]:
        """Optimize task scheduling strategy"""
        
        # Priority-based scheduling
        high_priority_tasks = [t for t in tasks if t.priority in [ScrapingPriority.CRITICAL, ScrapingPriority.HIGH]]
        medium_priority_tasks = [t for t in tasks if t.priority == ScrapingPriority.MEDIUM]
        low_priority_tasks = [t for t in tasks if t.priority in [ScrapingPriority.LOW, ScrapingPriority.BACKGROUND]]
        
        return {
            'strategy': 'priority_weighted',
            'high_priority_allocation': 60,  # 60% of resources
            'medium_priority_allocation': 30,  # 30% of resources
            'low_priority_allocation': 10,   # 10% of resources
            'batch_size_high': 100,
            'batch_size_medium': 200,
            'batch_size_low': 500
        }
    
    async def _optimize_resource_allocation(self, tasks: List[ScrapingTask]) -> Dict[str, Any]:
        """Optimize resource allocation across tiers"""
        
        total_tasks = len(tasks)
        tier_counts = defaultdict(int)
        
        for task in tasks:
            tier_counts[task.tier.value] += 1
            
        # Calculate resource allocation percentages
        allocations = {}
        for tier, count in tier_counts.items():
            allocations[tier] = (count / total_tasks) * 100
            
        return allocations
    
    def _get_tier_success_rate(self, tier: str) -> float:
        """Get historical success rate for a tier"""
        if tier in self.success_rates and self.success_rates[tier]:
            return statistics.mean(self.success_rates[tier])
        return 0.8  # Default success rate
    
    def _get_tier_performance_factor(self, tier: str) -> float:
        """Get performance factor for a tier"""
        if tier in self.timing_patterns and self.timing_patterns[tier]:
            avg_time = statistics.mean(self.timing_patterns[tier])
            # Normalize to a factor between 0.5 and 1.5
            return max(0.5, min(1.5, 30.0 / avg_time))
        return 1.0  # Default performance factor
    
    def _estimate_completion_time(self, tasks: List[ScrapingTask], concurrency: Dict[str, int]) -> float:
        """Estimate total completion time"""
        
        tier_times = {}
        for tier_name, concurrent_count in concurrency.items():
            tier_tasks = [t for t in tasks if t.tier.value == tier_name]
            if tier_tasks:
                total_time = sum(t.estimated_processing_time for t in tier_tasks)
                tier_times[tier_name] = total_time / concurrent_count
                
        return max(tier_times.values()) if tier_times else 3600  # Default 1 hour

class AntiDetectionAI:
    """Advanced AI-powered anti-detection system"""
    
    def __init__(self):
        self.ua = UserAgent()
        self.session_patterns = {}
        self.detection_signals = []
        
    async def get_optimized_headers(self, url: str, previous_requests: int = 0) -> Dict[str, str]:
        """Generate optimized headers with anti-detection measures"""
        
        domain = urlparse(url).netloc
        
        # Rotate user agents intelligently based on domain and patterns
        user_agent = await self._select_optimal_user_agent(domain, previous_requests)
        
        headers = {
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': self._get_random_accept_language(),
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
        
        # Add domain-specific headers
        domain_headers = await self._get_domain_specific_headers(domain)
        headers.update(domain_headers)
        
        # Add randomized optional headers
        optional_headers = await self._get_randomized_optional_headers()
        headers.update(optional_headers)
        
        return headers
    
    async def _select_optimal_user_agent(self, domain: str, request_count: int) -> str:
        """Select optimal user agent based on domain and request patterns"""
        
        # Rotate user agents every 10-50 requests
        rotation_interval = random.randint(10, 50)
        
        if request_count % rotation_interval == 0:
            # Medical site friendly user agents
            medical_friendly_uas = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
            return random.choice(medical_friendly_uas)
        
        return self.session_patterns.get(domain, self.ua.chrome)
    
    def _get_random_accept_language(self) -> str:
        """Get randomized Accept-Language header"""
        languages = [
            'en-US,en;q=0.9',
            'en-US,en;q=0.8,es;q=0.7',
            'en-GB,en;q=0.9,en-US;q=0.8',
            'en-US,en;q=0.9,fr;q=0.8',
            'en-US,en;q=0.9,de;q=0.8'
        ]
        return random.choice(languages)
    
    async def _get_domain_specific_headers(self, domain: str) -> Dict[str, str]:
        """Get domain-specific headers for better compatibility"""
        
        domain_headers = {}
        
        # Government domains (.gov)
        if '.gov' in domain:
            domain_headers.update({
                'Sec-GPC': '1',
                'Pragma': 'no-cache'
            })
        
        # Academic domains (.edu)
        elif '.edu' in domain:
            domain_headers.update({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            })
        
        # Medical organization domains
        elif any(med_domain in domain for med_domain in ['mayo', 'cleveland', 'johns', 'harvard', 'stanford']):
            domain_headers.update({
                'Sec-Fetch-User': '?1'
            })
            
        return domain_headers
    
    async def _get_randomized_optional_headers(self) -> Dict[str, str]:
        """Get randomized optional headers"""
        
        optional = {}
        
        # Randomly add some optional headers
        if random.random() < 0.3:
            optional['X-Requested-With'] = 'XMLHttpRequest'
            
        if random.random() < 0.2:
            optional['Origin'] = 'https://www.google.com'
            
        if random.random() < 0.4:
            optional['Referer'] = random.choice([
                'https://www.google.com/',
                'https://scholar.google.com/',
                'https://pubmed.ncbi.nlm.nih.gov/'
            ])
            
        return optional
    
    async def calculate_intelligent_delay(self, domain: str, success_rate: float, recent_response_times: List[float]) -> float:
        """Calculate intelligent delay between requests"""
        
        # Base delays by domain type
        base_delays = {
            '.gov': (2.0, 8.0),      # Government sites - be respectful
            '.edu': (1.5, 6.0),      # Academic sites
            '.org': (1.0, 5.0),      # Organizations
            '.com': (0.5, 4.0),      # Commercial sites
            'who.int': (3.0, 10.0),  # WHO - be extra respectful
            'nih.gov': (2.5, 8.0),   # NIH - government medical
        }
        
        # Determine base delay range
        delay_range = base_delays.get('.com', (0.5, 4.0))  # Default
        for domain_pattern, range_values in base_delays.items():
            if domain_pattern in domain:
                delay_range = range_values
                break
        
        min_delay, max_delay = delay_range
        
        # Adjust based on success rate
        if success_rate < 0.7:
            # Increase delays if success rate is low
            min_delay *= 1.5
            max_delay *= 2.0
        elif success_rate > 0.95:
            # Decrease delays if success rate is very high
            min_delay *= 0.8
            max_delay *= 0.9
            
        # Adjust based on recent response times
        if recent_response_times:
            avg_response_time = statistics.mean(recent_response_times)
            if avg_response_time > 5.0:  # Slow responses
                min_delay *= 1.3
                max_delay *= 1.5
                
        # Add some randomization
        base_delay = random.uniform(min_delay, max_delay)
        
        # Add jitter (±20%)
        jitter = random.uniform(-0.2, 0.2) * base_delay
        final_delay = max(0.1, base_delay + jitter)
        
        return final_delay

class ContentQualityAI:
    """AI system for assessing medical content quality"""
    
    def __init__(self):
        self.medical_keywords = set([
            'disease', 'condition', 'symptom', 'treatment', 'medication', 'diagnosis',
            'therapy', 'clinical', 'medical', 'health', 'patient', 'doctor',
            'hospital', 'research', 'study', 'trial', 'drug', 'pharmaceutical'
        ])
        
    async def assess_content_quality(self, content: str, url: str, metadata: Dict[str, Any] = None) -> float:
        """Assess medical content quality using AI analysis"""
        
        if not content or len(content) < 100:
            return 0.0
            
        scores = []
        
        # Content length and structure score
        length_score = self._calculate_length_score(content)
        scores.append(length_score)
        
        # Medical relevance score
        relevance_score = self._calculate_medical_relevance(content)
        scores.append(relevance_score)
        
        # Source credibility score
        credibility_score = self._calculate_source_credibility(url)
        scores.append(credibility_score)
        
        # Content completeness score
        completeness_score = self._calculate_completeness_score(content)
        scores.append(completeness_score)
        
        # Technical quality score
        technical_score = self._calculate_technical_quality(content)
        scores.append(technical_score)
        
        # Weighted average (credibility weighted higher)
        weights = [0.15, 0.25, 0.30, 0.20, 0.10]
        weighted_score = sum(score * weight for score, weight in zip(scores, weights))
        
        return min(1.0, max(0.0, weighted_score))
    
    def _calculate_length_score(self, content: str) -> float:
        """Calculate score based on content length"""
        length = len(content)
        
        if length < 200:
            return 0.2
        elif length < 500:
            return 0.4
        elif length < 1000:
            return 0.6
        elif length < 2000:
            return 0.8
        elif length < 10000:
            return 1.0
        else:
            return 0.9  # Very long content might be less focused
    
    def _calculate_medical_relevance(self, content: str) -> float:
        """Calculate medical relevance score"""
        content_lower = content.lower()
        
        # Count medical keywords
        medical_count = sum(1 for keyword in self.medical_keywords if keyword in content_lower)
        
        # Calculate density
        words = content_lower.split()
        total_words = len(words)
        
        if total_words == 0:
            return 0.0
            
        density = medical_count / total_words
        
        # Score based on density
        if density >= 0.05:  # 5% medical keywords
            return 1.0
        elif density >= 0.03:  # 3% medical keywords
            return 0.8
        elif density >= 0.02:  # 2% medical keywords
            return 0.6
        elif density >= 0.01:  # 1% medical keywords
            return 0.4
        else:
            return 0.2
    
    def _calculate_source_credibility(self, url: str) -> float:
        """Calculate source credibility score based on URL"""
        domain = urlparse(url).netloc.lower()
        
        # High credibility sources
        high_credibility = [
            'nih.gov', 'cdc.gov', 'fda.gov', 'who.int', 'mayoclinic.org',
            'clevelandclinic.org', 'hopkinsmedicine.org', 'harvard.edu',
            'stanford.edu', 'nature.com', 'nejm.org', 'bmj.com',
            'pubmed.ncbi.nlm.nih.gov', 'cochranelibrary.com'
        ]
        
        # Medium credibility sources
        medium_credibility = [
            'webmd.com', 'healthline.com', 'medicalnewstoday.com',
            '.edu', '.org', 'nhs.uk', 'cancer.org', 'heart.org'
        ]
        
        # Check high credibility
        for source in high_credibility:
            if source in domain:
                return 1.0
                
        # Check medium credibility
        for source in medium_credibility:
            if source in domain:
                return 0.7
                
        # Government domains
        if '.gov' in domain:
            return 0.9
            
        # Academic domains
        if '.edu' in domain:
            return 0.8
            
        # Default score
        return 0.5
    
    def _calculate_completeness_score(self, content: str) -> float:
        """Calculate content completeness score"""
        
        # Check for common medical content sections
        sections = [
            'symptom', 'cause', 'treatment', 'diagnosis', 'prevention',
            'overview', 'description', 'definition', 'background'
        ]
        
        content_lower = content.lower()
        found_sections = sum(1 for section in sections if section in content_lower)
        
        # Score based on number of sections found
        completeness = found_sections / len(sections)
        return min(1.0, completeness * 1.5)  # Boost score slightly
    
    def _calculate_technical_quality(self, content: str) -> float:
        """Calculate technical quality score"""
        
        # Check for technical indicators
        technical_indicators = [
            'study', 'research', 'clinical trial', 'peer-reviewed',
            'evidence', 'data', 'analysis', 'results', 'conclusion',
            'methodology', 'randomized', 'controlled', 'systematic review'
        ]
        
        content_lower = content.lower()
        found_indicators = sum(1 for indicator in technical_indicators if indicator in content_lower)
        
        # Score based on technical depth
        if found_indicators >= 5:
            return 1.0
        elif found_indicators >= 3:
            return 0.8
        elif found_indicators >= 2:
            return 0.6
        elif found_indicators >= 1:
            return 0.4
        else:
            return 0.2

class IntelligentTaskScheduler:
    """AI-powered intelligent task scheduling system"""
    
    def __init__(self):
        self.task_queues = {priority: deque() for priority in ScrapingPriority}
        self.running_tasks = {}
        self.completed_tasks = []
        self.failed_tasks = []
        self.performance_metrics = defaultdict(list)
        
    async def schedule_tasks(self, tasks: List[ScrapingTask]) -> Dict[str, List[ScrapingTask]]:
        """Intelligently schedule tasks across queues"""
        
        # Clear existing queues
        for queue in self.task_queues.values():
            queue.clear()
            
        # Sort and distribute tasks
        for task in tasks:
            # Adjust priority based on AI analysis
            adjusted_priority = await self._analyze_task_priority(task)
            task.priority = adjusted_priority
            
            # Add to appropriate queue
            self.task_queues[task.priority].append(task)
            
        # Optimize queue order within each priority
        for priority, queue in self.task_queues.items():
            optimized_queue = await self._optimize_queue_order(list(queue))
            queue.clear()
            queue.extend(optimized_queue)
            
        return {priority.name: list(queue) for priority, queue in self.task_queues.items()}
    
    async def _analyze_task_priority(self, task: ScrapingTask) -> ScrapingPriority:
        """Analyze and adjust task priority using AI"""
        
        current_priority = task.priority
        
        # Government sources get higher priority
        if task.tier in [ScrapingTier.TIER_1_GOVERNMENT, ScrapingTier.TIER_2_INTERNATIONAL]:
            if current_priority.value > ScrapingPriority.HIGH.value:
                return ScrapingPriority.HIGH
                
        # High-quality medical sites get priority boost
        quality_domains = ['mayo', 'cleveland', 'hopkins', 'harvard', 'stanford', 'who', 'nih', 'cdc']
        if any(domain in task.url.lower() for domain in quality_domains):
            if current_priority.value > ScrapingPriority.MEDIUM.value:
                return ScrapingPriority.MEDIUM
                
        # API tasks get higher priority due to rate limits
        if task.tier == ScrapingTier.TIER_7_APIS:
            if current_priority.value > ScrapingPriority.HIGH.value:
                return ScrapingPriority.HIGH
                
        return current_priority
    
    async def _optimize_queue_order(self, tasks: List[ScrapingTask]) -> List[ScrapingTask]:
        """Optimize order of tasks within a priority queue"""
        
        # Sort by multiple factors
        def sort_key(task):
            return (
                -task.success_probability,  # Higher success probability first
                task.estimated_processing_time,  # Shorter tasks first
                -task.content_quality_score,  # Higher quality first
                task.created_at  # Older tasks first
            )
            
        return sorted(tasks, key=sort_key)
    
    async def get_next_batch(self, batch_size: int = 100) -> List[ScrapingTask]:
        """Get next batch of tasks to process"""
        
        batch = []
        
        # Priority-based distribution
        priority_allocation = {
            ScrapingPriority.CRITICAL: 0.4,  # 40% of batch
            ScrapingPriority.HIGH: 0.3,      # 30% of batch
            ScrapingPriority.MEDIUM: 0.2,    # 20% of batch
            ScrapingPriority.LOW: 0.1,       # 10% of batch
            ScrapingPriority.BACKGROUND: 0.0  # 0% of batch (background only)
        }
        
        for priority, allocation in priority_allocation.items():
            queue = self.task_queues[priority]
            count = int(batch_size * allocation)
            
            while len(batch) < len(batch) + count and queue:
                task = queue.popleft()
                batch.append(task)
                
        # Fill remaining slots with any available tasks
        remaining_slots = batch_size - len(batch)
        for priority in ScrapingPriority:
            queue = self.task_queues[priority]
            while remaining_slots > 0 and queue:
                task = queue.popleft()
                batch.append(task)
                remaining_slots -= 1
                
        return batch
    
    def record_task_completion(self, task: ScrapingTask, result: ScrapingResult):
        """Record task completion for learning"""
        
        self.running_tasks.pop(task.id, None)
        
        if result.success:
            self.completed_tasks.append(task)
        else:
            self.failed_tasks.append(task)
            
        # Update performance metrics
        self.performance_metrics[task.tier.value].append({
            'success': result.success,
            'processing_time': result.processing_time,
            'quality_score': result.quality_score,
            'timestamp': result.timestamp
        })

class AdaptiveRateLimiter:
    """Adaptive rate limiting system with AI-powered adjustment"""
    
    def __init__(self):
        self.domain_limits = {}
        self.domain_stats = defaultdict(lambda: {
            'success_count': 0,
            'error_count': 0,
            'total_requests': 0,
            'avg_response_time': 0.0,
            'last_request_time': 0.0,
            'consecutive_errors': 0,
            'rate_limit_detected': False
        })
        
    async def acquire_permit(self, url: str) -> bool:
        """Acquire permission to make request with adaptive rate limiting"""
        
        domain = urlparse(url).netloc
        current_time = time.time()
        
        # Initialize domain stats if not exists
        if domain not in self.domain_stats:
            self.domain_stats[domain] = {
                'success_count': 0,
                'error_count': 0,
                'total_requests': 0,
                'avg_response_time': 0.0,
                'last_request_time': 0.0,
                'consecutive_errors': 0,
                'rate_limit_detected': False
            }
            
        stats = self.domain_stats[domain]
        
        # Calculate required delay
        required_delay = await self._calculate_adaptive_delay(domain, stats)
        
        # Check if enough time has passed
        time_since_last = current_time - stats['last_request_time']
        
        if time_since_last < required_delay:
            return False  # Need to wait longer
            
        # Update last request time
        stats['last_request_time'] = current_time
        stats['total_requests'] += 1
        
        return True
    
    async def _calculate_adaptive_delay(self, domain: str, stats: Dict[str, Any]) -> float:
        """Calculate adaptive delay based on domain performance"""
        
        # Base delays by domain type
        base_delays = {
            '.gov': 3.0,
            '.edu': 2.0,
            '.org': 1.5,
            'who.int': 4.0,
            'nih.gov': 3.5,
            'cdc.gov': 3.0,
            'fda.gov': 3.0
        }
        
        # Determine base delay
        base_delay = 1.0  # Default
        for pattern, delay in base_delays.items():
            if pattern in domain:
                base_delay = delay
                break
                
        # Adjust based on success rate
        total_requests = stats['total_requests']
        if total_requests > 10:
            success_rate = stats['success_count'] / total_requests
            
            if success_rate < 0.5:
                base_delay *= 3.0  # Increase delay significantly
            elif success_rate < 0.7:
                base_delay *= 2.0  # Double the delay
            elif success_rate < 0.9:
                base_delay *= 1.5  # Increase delay moderately
            else:
                base_delay *= 0.8  # Slightly reduce delay for good performance
                
        # Adjust for consecutive errors
        consecutive_errors = stats['consecutive_errors']
        if consecutive_errors > 0:
            error_multiplier = min(5.0, 1.5 ** consecutive_errors)
            base_delay *= error_multiplier
            
        # Adjust for rate limiting detection
        if stats['rate_limit_detected']:
            base_delay *= 10.0  # Significantly increase delay
            
        # Adjust for response time
        avg_response_time = stats['avg_response_time']
        if avg_response_time > 10.0:  # Slow responses
            base_delay *= 1.5
        elif avg_response_time > 5.0:
            base_delay *= 1.2
            
        return base_delay
    
    def record_request_result(self, url: str, success: bool, response_time: float, status_code: int = None):
        """Record request result for adaptive learning"""
        
        domain = urlparse(url).netloc
        stats = self.domain_stats[domain]
        
        if success:
            stats['success_count'] += 1
            stats['consecutive_errors'] = 0
            stats['rate_limit_detected'] = False
        else:
            stats['error_count'] += 1
            stats['consecutive_errors'] += 1
            
            # Detect rate limiting
            if status_code in [429, 503, 502] or stats['consecutive_errors'] >= 3:
                stats['rate_limit_detected'] = True
                
        # Update average response time
        total_responses = stats['success_count'] + stats['error_count']
        if total_responses > 1:
            stats['avg_response_time'] = (
                (stats['avg_response_time'] * (total_responses - 1) + response_time) / total_responses
            )
        else:
            stats['avg_response_time'] = response_time

class IntelligentProxyRotator:
    """Intelligent proxy rotation system"""
    
    def __init__(self):
        self.proxies = []
        self.proxy_stats = {}
        self.current_proxy_index = 0
        
    async def add_proxies(self, proxy_list: List[str]):
        """Add proxies to rotation pool"""
        
        for proxy in proxy_list:
            if proxy not in [p['proxy'] for p in self.proxies]:
                self.proxies.append({
                    'proxy': proxy,
                    'success_count': 0,
                    'error_count': 0,
                    'last_used': 0.0,
                    'avg_response_time': 0.0,
                    'is_working': True
                })
                
    async def get_best_proxy(self, domain: str = None) -> Optional[str]:
        """Get best available proxy for domain"""
        
        if not self.proxies:
            return None
            
        # Filter working proxies
        working_proxies = [p for p in self.proxies if p['is_working']]
        
        if not working_proxies:
            # Reset all proxies if none are working
            for proxy in self.proxies:
                proxy['is_working'] = True
            working_proxies = self.proxies
            
        # Select best proxy based on performance
        best_proxy = min(working_proxies, key=lambda p: (
            p['error_count'],
            p['avg_response_time'],
            -p['success_count']
        ))
        
        # Update usage
        best_proxy['last_used'] = time.time()
        
        return best_proxy['proxy']
    
    def record_proxy_result(self, proxy: str, success: bool, response_time: float):
        """Record proxy performance"""
        
        for proxy_info in self.proxies:
            if proxy_info['proxy'] == proxy:
                if success:
                    proxy_info['success_count'] += 1
                else:
                    proxy_info['error_count'] += 1
                    
                    # Mark as not working after 5 consecutive errors
                    if proxy_info['error_count'] % 5 == 0:
                        proxy_info['is_working'] = False
                        
                # Update average response time
                total_requests = proxy_info['success_count'] + proxy_info['error_count']
                if total_requests > 1:
                    proxy_info['avg_response_time'] = (
                        (proxy_info['avg_response_time'] * (total_requests - 1) + response_time) / total_requests
                    )
                else:
                    proxy_info['avg_response_time'] = response_time
                    
                break

class AdvancedDeduplicator:
    """Advanced content deduplication system"""
    
    def __init__(self):
        self.content_hashes = set()
        self.similarity_threshold = 0.85
        self.url_patterns = set()
        
    async def is_duplicate(self, content: str, url: str, metadata: Dict[str, Any] = None) -> bool:
        """Check if content is duplicate using multiple methods"""
        
        # Method 1: Exact hash matching
        content_hash = hashlib.md5(content.encode()).hexdigest()
        if content_hash in self.content_hashes:
            return True
            
        # Method 2: URL pattern matching
        url_pattern = self._extract_url_pattern(url)
        if url_pattern in self.url_patterns:
            return True
            
        # Method 3: Content similarity (simplified)
        if await self._is_similar_content(content):
            return True
            
        # Method 4: Title and key content matching
        if metadata and await self._is_duplicate_by_metadata(metadata):
            return True
            
        # Not a duplicate - record signatures
        self.content_hashes.add(content_hash)
        self.url_patterns.add(url_pattern)
        
        return False
    
    def _extract_url_pattern(self, url: str) -> str:
        """Extract URL pattern for duplicate detection"""
        
        parsed = urlparse(url)
        
        # Remove query parameters and fragments
        clean_path = parsed.path.rstrip('/')
        
        # Replace numbers with placeholders for pattern matching
        import re
        pattern_path = re.sub(r'/\d+', '/[ID]', clean_path)
        pattern_path = re.sub(r'page=\d+', 'page=[NUM]', pattern_path)
        
        return f"{parsed.netloc}{pattern_path}"
    
    async def _is_similar_content(self, content: str) -> bool:
        """Check content similarity (simplified implementation)"""
        
        # This is a simplified implementation
        # In production, you might use more sophisticated similarity measures
        
        content_words = set(content.lower().split())
        
        # Compare against stored content signatures (simplified)
        # This would typically use more advanced techniques like MinHash or SimHash
        
        return False  # Placeholder - implement actual similarity checking
    
    async def _is_duplicate_by_metadata(self, metadata: Dict[str, Any]) -> bool:
        """Check for duplicates based on metadata"""
        
        # Check title similarity
        title = metadata.get('title', '')
        if title and len(title) > 10:
            # Simplified title matching
            # In production, implement more sophisticated title similarity
            pass
            
        return False  # Placeholder

# Export classes for use in other modules
__all__ = [
    'ScrapingTask', 'ScrapingResult', 'ScrapingPriority', 'ContentType', 'ScrapingTier',
    'ContentDiscoveryAI', 'ScraperOptimizationAI', 'AntiDetectionAI', 'ContentQualityAI',
    'IntelligentTaskScheduler', 'AdaptiveRateLimiter', 'IntelligentProxyRotator', 'AdvancedDeduplicator'
]