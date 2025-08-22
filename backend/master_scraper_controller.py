"""
Master Scraper Controller - World-Class Medical Data Extraction System
Orchestrates massive parallel scraping operations across all medical data tiers
"""

import asyncio
import aiohttp
import logging
from typing import List, Dict, Optional, Any, Union
from datetime import datetime, timedelta
import json
from collections import defaultdict, deque
import statistics
import random
import time

from ai_scraper_core import (
    ScrapingTask, ScrapingResult, ScrapingPriority, ContentType, ScrapingTier,
    ContentDiscoveryAI, ScraperOptimizationAI, AntiDetectionAI, ContentQualityAI,
    IntelligentTaskScheduler, AdaptiveRateLimiter, IntelligentProxyRotator, AdvancedDeduplicator
)

logger = logging.getLogger(__name__)

class TierScraperBase:
    """Base class for all tier-specific scrapers"""
    
    def __init__(self, tier: ScrapingTier, max_concurrent: int = 50):
        self.tier = tier
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # AI systems
        self.content_discovery = ContentDiscoveryAI()
        self.anti_detection = AntiDetectionAI()
        self.content_quality = ContentQualityAI()
        self.deduplicator = AdvancedDeduplicator()
        
        # Performance tracking
        self.processed_urls = set()
        self.success_count = 0
        self.error_count = 0
        self.total_content_size = 0
        
    async def scrape_complete_tier(self) -> List[ScrapingResult]:
        """Scrape complete tier - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement scrape_complete_tier")
    
    async def extract_content_from_url(self, url: str, session: aiohttp.ClientSession, 
                                     retry_count: int = 0) -> ScrapingResult:
        """Extract content from a single URL with advanced processing"""
        
        task_id = f"{self.tier.value}_{hash(url)}"
        
        async with self.semaphore:
            try:
                # Get optimized headers
                headers = await self.anti_detection.get_optimized_headers(url, len(self.processed_urls))
                
                # Make request with timeout and retries
                async with session.get(url, headers=headers, timeout=30) as response:
                    start_time = time.time()
                    
                    if response.status == 200:
                        content = await response.text()
                        processing_time = time.time() - start_time
                        
                        # Check for duplicates
                        if await self.deduplicator.is_duplicate(content, url):
                            return ScrapingResult(
                                task_id=task_id,
                                url=url,
                                success=False,
                                error_details="Duplicate content detected"
                            )
                        
                        # Extract structured data
                        extracted_data = await self._extract_structured_data(content, url)
                        
                        # Assess content quality
                        quality_score = await self.content_quality.assess_content_quality(content, url)
                        
                        # Build result
                        result = ScrapingResult(
                            task_id=task_id,
                            url=url,
                            success=True,
                            content=content,
                            extracted_data=extracted_data,
                            processing_time=processing_time,
                            content_length=len(content),
                            quality_score=quality_score,
                            confidence_score=0.9,  # High confidence for successful extraction
                            timestamp=datetime.utcnow()
                        )
                        
                        self.success_count += 1
                        self.total_content_size += len(content)
                        
                        return result
                        
                    else:
                        # Handle error response
                        return ScrapingResult(
                            task_id=task_id,
                            url=url,
                            success=False,
                            error_details=f"HTTP {response.status}: {response.reason}",
                            timestamp=datetime.utcnow()
                        )
                        
            except Exception as e:
                self.error_count += 1
                
                # Retry logic
                if retry_count < 3:
                    await asyncio.sleep(2 ** retry_count)  # Exponential backoff
                    return await self.extract_content_from_url(url, session, retry_count + 1)
                
                return ScrapingResult(
                    task_id=task_id,
                    url=url,
                    success=False,
                    error_details=str(e),
                    timestamp=datetime.utcnow()
                )
                
    async def _extract_structured_data(self, content: str, url: str) -> Dict[str, Any]:
        """Extract structured medical data from content"""
        
        from bs4 import BeautifulSoup
        
        try:
            soup = BeautifulSoup(content, 'lxml')
            
            extracted = {
                'title': '',
                'description': '',
                'medical_content': {},
                'metadata': {},
                'links': []
            }
            
            # Extract title
            title_tag = soup.find('title')
            if title_tag:
                extracted['title'] = title_tag.get_text(strip=True)
            
            # Extract meta description
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc:
                extracted['description'] = meta_desc.get('content', '')
            
            # Extract headings structure
            headings = []
            for tag in ['h1', 'h2', 'h3', 'h4']:
                for heading in soup.find_all(tag):
                    headings.append({
                        'level': tag,
                        'text': heading.get_text(strip=True)
                    })
            extracted['medical_content']['headings'] = headings
            
            # Extract main content paragraphs
            paragraphs = []
            for p in soup.find_all('p'):
                text = p.get_text(strip=True)
                if len(text) > 50:  # Filter out short paragraphs
                    paragraphs.append(text)
            extracted['medical_content']['paragraphs'] = paragraphs[:10]  # Top 10 paragraphs
            
            # Extract lists (symptoms, treatments, etc.)
            lists = []
            for ul in soup.find_all(['ul', 'ol']):
                list_items = [li.get_text(strip=True) for li in ul.find_all('li')]
                if len(list_items) >= 2:  # At least 2 items
                    lists.append(list_items)
            extracted['medical_content']['lists'] = lists[:5]  # Top 5 lists
            
            # Extract internal links
            links = []
            for a in soup.find_all('a', href=True):
                href = a.get('href')
                if href and not href.startswith(('http', '#', 'mailto', 'javascript')):
                    full_url = urljoin(url, href)
                    links.append({
                        'url': full_url,
                        'text': a.get_text(strip=True)
                    })
            extracted['links'] = links[:20]  # Top 20 links
            
            # Extract metadata
            extracted['metadata'] = {
                'word_count': len(content.split()),
                'paragraph_count': len(paragraphs),
                'heading_count': len(headings),
                'list_count': len(lists),
                'link_count': len(links)
            }
            
            return extracted
            
        except Exception as e:
            logger.error(f"Error extracting structured data from {url}: {e}")
            return {'error': str(e)}

class GovernmentScraper(TierScraperBase):
    """Tier 1: Government sources scraper (NIH, CDC, FDA, etc.)"""
    
    def __init__(self):
        super().__init__(ScrapingTier.TIER_1_GOVERNMENT, max_concurrent=100)
        self.government_sources = {
            'medlineplus': {
                'base_url': 'https://medlineplus.gov',
                'endpoints': [
                    '/encyclopedia/',
                    '/healthtopics/',
                    '/druginformation.html'
                ]
            },
            'cdc': {
                'base_url': 'https://www.cdc.gov',
                'endpoints': [
                    '/diseasesconditions/',
                    '/health/',
                    '/vaccines/'
                ]
            },
            'fda': {
                'base_url': 'https://www.fda.gov',
                'endpoints': [
                    '/drugs/',
                    '/medical-devices/',
                    '/safety/'
                ]
            },
            'nih': {
                'base_url': 'https://www.nih.gov',
                'endpoints': [
                    '/health-information/',
                    '/news-events/'
                ]
            }
        }
    
    async def scrape_complete_tier(self) -> List[ScrapingResult]:
        """Scrape all government medical sources"""
        
        logger.info(f"Starting {self.tier.value} scraping with {self.max_concurrent} concurrent workers")
        
        all_results = []
        
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
            connector=aiohttp.TCPConnector(limit=200, limit_per_host=50)
        ) as session:
            
            for source_name, source_config in self.government_sources.items():
                logger.info(f"Scraping {source_name}...")
                
                # Discover URLs for this source
                source_urls = await self._discover_source_urls(source_name, source_config)
                
                # Create scraping tasks
                tasks = [
                    self.extract_content_from_url(url, session)
                    for url in source_urls[:5000]  # Limit per source
                ]
                
                # Execute in batches
                batch_size = 100
                for i in range(0, len(tasks), batch_size):
                    batch = tasks[i:i + batch_size]
                    batch_results = await asyncio.gather(*batch, return_exceptions=True)
                    
                    # Filter successful results
                    valid_results = [r for r in batch_results if isinstance(r, ScrapingResult)]
                    all_results.extend(valid_results)
                    
                    logger.info(f"{source_name} batch {i//batch_size + 1}: {len(valid_results)} successful extractions")
                    
                    # Adaptive delay between batches
                    delay = await self._calculate_batch_delay(source_name)
                    await asyncio.sleep(delay)
        
        logger.info(f"Completed {self.tier.value} scraping: {len(all_results)} total extractions")
        return all_results
    
    async def _discover_source_urls(self, source_name: str, source_config: Dict[str, Any]) -> List[str]:
        """Discover URLs for a government source"""
        
        base_url = source_config['base_url']
        endpoints = source_config['endpoints']
        
        discovered_urls = []
        
        for endpoint in endpoints:
            full_endpoint = base_url + endpoint
            
            # Use AI to discover medical URLs
            medical_urls = await self.content_discovery.discover_medical_urls(
                full_endpoint, 
                f"government_{source_name}"
            )
            
            discovered_urls.extend(medical_urls)
        
        return list(set(discovered_urls))  # Remove duplicates
    
    async def _calculate_batch_delay(self, source_name: str) -> float:
        """Calculate appropriate delay between batches for government sites"""
        
        # Government sites require respectful delays
        base_delays = {
            'medlineplus': 2.0,
            'cdc': 2.5,
            'fda': 3.0,
            'nih': 2.0
        }
        
        base_delay = base_delays.get(source_name, 2.0)
        
        # Add randomization
        jitter = random.uniform(-0.5, 1.0)
        return base_delay + jitter

class InternationalScraper(TierScraperBase):
    """Tier 2: International organizations scraper (WHO, EMA, etc.)"""
    
    def __init__(self):
        super().__init__(ScrapingTier.TIER_2_INTERNATIONAL, max_concurrent=80)
        self.international_sources = {
            'who': {
                'base_url': 'https://www.who.int',
                'endpoints': ['/health-topics/', '/emergencies/', '/publications/']
            },
            'ema': {
                'base_url': 'https://www.ema.europa.eu',
                'endpoints': ['/en/medicines/', '/en/news/']
            },
            'nhs': {
                'base_url': 'https://www.nhs.uk',
                'endpoints': ['/conditions/', '/live-well/']
            }
        }
    
    async def scrape_complete_tier(self) -> List[ScrapingResult]:
        """Scrape all international medical sources"""
        
        logger.info(f"Starting {self.tier.value} scraping")
        
        all_results = []
        
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
            connector=aiohttp.TCPConnector(limit=150, limit_per_host=40)
        ) as session:
            
            for source_name, source_config in self.international_sources.items():
                logger.info(f"Scraping international source: {source_name}")
                
                source_urls = await self._discover_international_urls(source_name, source_config)
                
                tasks = [
                    self.extract_content_from_url(url, session)
                    for url in source_urls[:3000]  # Limit per international source
                ]
                
                # Execute in smaller batches for international sites
                batch_size = 50
                for i in range(0, len(tasks), batch_size):
                    batch = tasks[i:i + batch_size]
                    batch_results = await asyncio.gather(*batch, return_exceptions=True)
                    
                    valid_results = [r for r in batch_results if isinstance(r, ScrapingResult)]
                    all_results.extend(valid_results)
                    
                    # Longer delays for international sites
                    await asyncio.sleep(random.uniform(3.0, 8.0))
        
        return all_results
    
    async def _discover_international_urls(self, source_name: str, source_config: Dict[str, Any]) -> List[str]:
        """Discover URLs for international sources"""
        
        base_url = source_config['base_url']
        endpoints = source_config['endpoints']
        
        discovered_urls = []
        
        for endpoint in endpoints:
            full_endpoint = base_url + endpoint
            medical_urls = await self.content_discovery.discover_medical_urls(
                full_endpoint, 
                f"international_{source_name}"
            )
            discovered_urls.extend(medical_urls)
        
        return list(set(discovered_urls))

class AcademicScraper(TierScraperBase):
    """Tier 3: Academic medical centers scraper"""
    
    def __init__(self):
        super().__init__(ScrapingTier.TIER_3_ACADEMIC, max_concurrent=120)
        self.academic_sources = {
            'mayo_clinic': {
                'base_url': 'https://www.mayoclinic.org',
                'endpoints': ['/diseases-conditions/', '/tests-procedures/', '/drugs-supplements/']
            },
            'cleveland_clinic': {
                'base_url': 'https://my.clevelandclinic.org',
                'endpoints': ['/health/', '/treatments/']
            },
            'johns_hopkins': {
                'base_url': 'https://www.hopkinsmedicine.org',
                'endpoints': ['/health/', '/conditions-and-diseases/']
            },
            'harvard_health': {
                'base_url': 'https://www.health.harvard.edu',
                'endpoints': ['/topics/', '/conditions-and-diseases/']
            }
        }
    
    async def scrape_complete_tier(self) -> List[ScrapingResult]:
        """Scrape all academic medical centers"""
        
        logger.info(f"Starting {self.tier.value} scraping")
        
        all_results = []
        
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
            connector=aiohttp.TCPConnector(limit=200, limit_per_host=60)
        ) as session:
            
            for source_name, source_config in self.academic_sources.items():
                logger.info(f"Scraping academic source: {source_name}")
                
                source_urls = await self._discover_academic_urls(source_name, source_config)
                
                tasks = [
                    self.extract_content_from_url(url, session)
                    for url in source_urls[:4000]  # Limit per academic source
                ]
                
                batch_size = 80
                for i in range(0, len(tasks), batch_size):
                    batch = tasks[i:i + batch_size]
                    batch_results = await asyncio.gather(*batch, return_exceptions=True)
                    
                    valid_results = [r for r in batch_results if isinstance(r, ScrapingResult)]
                    all_results.extend(valid_results)
                    
                    # Moderate delays for academic sites
                    await asyncio.sleep(random.uniform(1.0, 3.0))
        
        return all_results
    
    async def _discover_academic_urls(self, source_name: str, source_config: Dict[str, Any]) -> List[str]:
        """Discover URLs for academic sources"""
        
        base_url = source_config['base_url']
        endpoints = source_config['endpoints']
        
        discovered_urls = []
        
        for endpoint in endpoints:
            full_endpoint = base_url + endpoint
            medical_urls = await self.content_discovery.discover_medical_urls(
                full_endpoint, 
                f"academic_{source_name}"
            )
            discovered_urls.extend(medical_urls)
        
        return list(set(discovered_urls))

class WorldClassMedicalScraper:
    """Master controller for the world's most advanced medical scraper system"""
    
    def __init__(self):
        # Initialize tier scrapers
        self.tier_scrapers = {
            ScrapingTier.TIER_1_GOVERNMENT: GovernmentScraper(),
            ScrapingTier.TIER_2_INTERNATIONAL: InternationalScraper(),
            ScrapingTier.TIER_3_ACADEMIC: AcademicScraper(),
            # Additional tiers will be added in subsequent phases
        }
        
        # AI systems
        self.scraper_optimization = ScraperOptimizationAI()
        self.task_scheduler = IntelligentTaskScheduler()
        self.rate_limiter = AdaptiveRateLimiter()
        
        # Performance tracking
        self.total_processed = 0
        self.total_success = 0
        self.total_errors = 0
        self.start_time = None
        self.tier_results = {}
        
    async def execute_massive_scraping_operation(self, target_tiers: List[ScrapingTier] = None) -> Dict[str, Any]:
        """Execute coordinated massive scraping across all tiers"""
        
        self.start_time = datetime.utcnow()
        logger.info("🚀 Starting World-Class Medical Data Extraction Operation")
        
        # Default to first 3 tiers for Phase 1
        if target_tiers is None:
            target_tiers = [
                ScrapingTier.TIER_1_GOVERNMENT,
                ScrapingTier.TIER_2_INTERNATIONAL,
                ScrapingTier.TIER_3_ACADEMIC
            ]
        
        # Generate scraping tasks
        all_tasks = await self._generate_scraping_tasks(target_tiers)
        
        # Optimize scraping strategy
        optimization_strategy = await self.scraper_optimization.optimize_scraping_strategy(all_tasks)
        logger.info(f"📊 Optimization complete: {optimization_strategy['estimated_completion_time']:.1f}s estimated")
        
        # Schedule tasks intelligently
        scheduled_tasks = await self.task_scheduler.schedule_tasks(all_tasks)
        logger.info(f"📅 Task scheduling complete: {sum(len(queue) for queue in scheduled_tasks.values())} tasks queued")
        
        # Execute tier scraping operations
        tier_execution_tasks = []
        for tier in target_tiers:
            if tier in self.tier_scrapers:
                scraper = self.tier_scrapers[tier]
                tier_execution_tasks.append(self._execute_tier_scraping(tier, scraper))
        
        # Run all tiers in parallel
        logger.info(f"🔄 Launching parallel execution across {len(tier_execution_tasks)} tiers")
        tier_results_list = await asyncio.gather(*tier_execution_tasks, return_exceptions=True)
        
        # Process results
        final_results = await self._process_final_results(tier_results_list, target_tiers)
        
        execution_time = (datetime.utcnow() - self.start_time).total_seconds()
        logger.info(f"✅ Scraping operation completed in {execution_time:.1f}s")
        
        return final_results
    
    async def _generate_scraping_tasks(self, target_tiers: List[ScrapingTier]) -> List[ScrapingTask]:
        """Generate scraping tasks for target tiers"""
        
        all_tasks = []
        
        for tier in target_tiers:
            # Generate tier-specific tasks
            tier_tasks = await self._generate_tier_tasks(tier)
            all_tasks.extend(tier_tasks)
            
        logger.info(f"📝 Generated {len(all_tasks)} scraping tasks across {len(target_tiers)} tiers")
        return all_tasks
    
    async def _generate_tier_tasks(self, tier: ScrapingTier) -> List[ScrapingTask]:
        """Generate tasks for a specific tier"""
        
        # This is a simplified task generation
        # In the full implementation, this would discover URLs and create specific tasks
        
        base_urls = {
            ScrapingTier.TIER_1_GOVERNMENT: [
                'https://medlineplus.gov/encyclopedia/',
                'https://www.cdc.gov/diseasesconditions/',
                'https://www.fda.gov/drugs/'
            ],
            ScrapingTier.TIER_2_INTERNATIONAL: [
                'https://www.who.int/health-topics/',
                'https://www.nhs.uk/conditions/'
            ],
            ScrapingTier.TIER_3_ACADEMIC: [
                'https://www.mayoclinic.org/diseases-conditions/',
                'https://my.clevelandclinic.org/health/'
            ]
        }
        
        urls = base_urls.get(tier, [])
        tasks = []
        
        for url in urls:
            task = ScrapingTask(
                url=url,
                source_name=f"{tier.value}_source",
                tier=tier,
                content_type=ContentType.MEDICAL_ARTICLE,
                priority=ScrapingPriority.HIGH if tier == ScrapingTier.TIER_1_GOVERNMENT else ScrapingPriority.MEDIUM
            )
            tasks.append(task)
            
        return tasks
    
    async def _execute_tier_scraping(self, tier: ScrapingTier, scraper: TierScraperBase) -> Dict[str, Any]:
        """Execute scraping for a specific tier"""
        
        logger.info(f"🎯 Starting {tier.value} scraping")
        start_time = time.time()
        
        try:
            results = await scraper.scrape_complete_tier()
            
            execution_time = time.time() - start_time
            success_count = sum(1 for r in results if r.success)
            error_count = len(results) - success_count
            
            tier_summary = {
                'tier': tier.value,
                'total_processed': len(results),
                'success_count': success_count,
                'error_count': error_count,
                'success_rate': success_count / len(results) if results else 0,
                'execution_time': execution_time,
                'avg_quality_score': statistics.mean([r.quality_score for r in results if r.success and r.quality_score > 0]) if success_count > 0 else 0,
                'total_content_size': sum(r.content_length for r in results if r.success),
                'results': results
            }
            
            logger.info(f"✅ {tier.value} completed: {success_count}/{len(results)} successful in {execution_time:.1f}s")
            return tier_summary
            
        except Exception as e:
            logger.error(f"❌ {tier.value} scraping failed: {e}")
            return {
                'tier': tier.value,
                'error': str(e),
                'total_processed': 0,
                'success_count': 0,
                'error_count': 1,
                'results': []
            }
    
    async def _process_final_results(self, tier_results_list: List[Dict[str, Any]], 
                                   target_tiers: List[ScrapingTier]) -> Dict[str, Any]:
        """Process and summarize final results"""
        
        total_processed = 0
        total_success = 0
        total_errors = 0
        total_content_size = 0
        all_results = []
        tier_summaries = {}
        
        for tier_result in tier_results_list:
            if isinstance(tier_result, dict) and 'tier' in tier_result:
                tier_name = tier_result['tier']
                tier_summaries[tier_name] = tier_result
                
                total_processed += tier_result.get('total_processed', 0)
                total_success += tier_result.get('success_count', 0)
                total_errors += tier_result.get('error_count', 0)
                total_content_size += tier_result.get('total_content_size', 0)
                
                if 'results' in tier_result:
                    all_results.extend(tier_result['results'])
        
        # Calculate overall statistics
        execution_time = (datetime.utcnow() - self.start_time).total_seconds()
        success_rate = total_success / total_processed if total_processed > 0 else 0
        processing_rate = total_processed / execution_time if execution_time > 0 else 0
        
        # Calculate average quality score
        quality_scores = [r.quality_score for r in all_results if isinstance(r, ScrapingResult) and r.success and r.quality_score > 0]
        avg_quality_score = statistics.mean(quality_scores) if quality_scores else 0
        
        final_summary = {
            'operation_summary': {
                'total_processed': total_processed,
                'total_success': total_success,
                'total_errors': total_errors,
                'success_rate': success_rate,
                'execution_time': execution_time,
                'processing_rate': processing_rate,
                'avg_quality_score': avg_quality_score,
                'total_content_size_mb': total_content_size / (1024 * 1024),
                'tiers_processed': len(target_tiers)
            },
            'tier_summaries': tier_summaries,
            'performance_metrics': {
                'documents_per_second': processing_rate,
                'mb_per_second': (total_content_size / (1024 * 1024)) / execution_time if execution_time > 0 else 0,
                'high_quality_documents': len([r for r in all_results if isinstance(r, ScrapingResult) and r.quality_score >= 0.8]),
                'medium_quality_documents': len([r for r in all_results if isinstance(r, ScrapingResult) and 0.5 <= r.quality_score < 0.8]),
                'low_quality_documents': len([r for r in all_results if isinstance(r, ScrapingResult) and 0 < r.quality_score < 0.5])
            },
            'extracted_results': all_results
        }
        
        # Log final statistics
        logger.info("=" * 80)
        logger.info("🎯 WORLD-CLASS MEDICAL SCRAPER - PHASE 1 RESULTS")
        logger.info("=" * 80)
        logger.info(f"📊 Total Documents Processed: {total_processed:,}")
        logger.info(f"✅ Successful Extractions: {total_success:,} ({success_rate:.1%})")
        logger.info(f"❌ Failed Extractions: {total_errors:,}")
        logger.info(f"⏱️  Total Execution Time: {execution_time:.1f} seconds")
        logger.info(f"🚀 Processing Rate: {processing_rate:.1f} documents/second")
        logger.info(f"⭐ Average Quality Score: {avg_quality_score:.3f}")
        logger.info(f"💾 Total Content Size: {total_content_size / (1024 * 1024):.1f} MB")
        logger.info("=" * 80)
        
        return final_summary

# Export main class
__all__ = ['WorldClassMedicalScraper', 'TierScraperBase', 'GovernmentScraper', 'InternationalScraper', 'AcademicScraper']