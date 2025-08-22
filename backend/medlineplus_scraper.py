"""
Phase 2: MedlinePlus Comprehensive Scraper
Advanced comprehensive scraper for MedlinePlus with AI-powered discovery and extraction
Target: 17,000+ medical articles across all MedlinePlus sections
"""

import asyncio
import aiohttp
import logging
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime
import json
import random
import time
from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup
import re
from collections import defaultdict

from ai_scraper_core import (
    ScrapingTask, ScrapingResult, ScrapingPriority, ContentType, ScrapingTier,
    ContentDiscoveryAI, AntiDetectionAI, ContentQualityAI, AdvancedDeduplicator
)

logger = logging.getLogger(__name__)

class MedlinePlusAdvancedScraper:
    """
    Comprehensive MedlinePlus scraper with AI-powered content discovery
    Targets all major MedlinePlus sections for maximum medical content extraction
    """
    
    def __init__(self):
        self.base_urls = {
            'encyclopedia': 'https://medlineplus.gov/encyclopedia/',
            'health_topics': 'https://medlineplus.gov/healthtopics/',
            'drug_info': 'https://medlineplus.gov/druginformation.html',
            'supplements': 'https://medlineplus.gov/vitaminsandsupplements.html',
            'medical_tests': 'https://medlineplus.gov/lab-tests/',
            'surgery': 'https://medlineplus.gov/surgery.html',
            'anatomy': 'https://medlineplus.gov/anatomy/',
            'medical_encyclopedia': 'https://medlineplus.gov/medicalencyclopedia.html',
            'easy_read': 'https://medlineplus.gov/all_easytoread.html',
            'games': 'https://medlineplus.gov/games/',
            'videos': 'https://medlineplus.gov/medlineplus-videos/'
        }
        
        # Advanced scraping capabilities
        self.url_discoverer = URLDiscoveryAI()
        self.content_extractor = AdvancedContentExtractor()
        self.structure_analyzer = PageStructureAnalyzer()
        
        # Anti-detection measures
        self.session_rotator = SessionRotator()
        self.header_randomizer = HeaderRandomizer()
        self.timing_humanizer = TimingHumanizer()
        
        # AI systems from core
        self.content_discovery = ContentDiscoveryAI()
        self.anti_detection = AntiDetectionAI()
        self.content_quality = ContentQualityAI()
        self.deduplicator = AdvancedDeduplicator()
        
        # Performance tracking
        self.processed_urls = set()
        self.success_count = 0
        self.error_count = 0
        self.total_content_size = 0
        self.section_stats = defaultdict(lambda: {'processed': 0, 'successful': 0, 'errors': 0})
    
    async def scrape_complete_medlineplus(self) -> Dict[str, Any]:
        """Scrape entire MedlinePlus knowledge base with intelligent coordination"""
        
        logger.info("🚀 Starting MedlinePlus Comprehensive Scraping Operation")
        start_time = datetime.utcnow()
        
        scraping_tasks = [
            self.scrape_encyclopedia_complete(),      # Target: 8,000+ articles
            self.scrape_health_topics_complete(),     # Target: 4,000+ topics  
            self.scrape_drug_database_complete(),     # Target: 2,500+ drugs
            self.scrape_supplements_complete(),       # Target: 1,200+ supplements
            self.scrape_medical_tests_complete(),     # Target: 800+ tests
            self.scrape_surgery_info_complete(),      # Target: 600+ procedures
            self.scrape_anatomy_complete(),           # Target: 300+ anatomy pages
            self.scrape_easy_read_complete(),         # Target: 400+ easy read articles
            self.scrape_videos_complete()             # Target: 200+ video resources
        ]
        
        # Execute all sections in parallel with intelligent coordination
        logger.info(f"📊 Launching {len(scraping_tasks)} parallel scraping operations")
        results = await asyncio.gather(*scraping_tasks, return_exceptions=True)
        
        # Process and integrate results
        processed_content = await self.process_and_store_content(results)
        
        execution_time = (datetime.utcnow() - start_time).total_seconds()
        logger.info(f"✅ MedlinePlus comprehensive scraping completed in {execution_time:.1f}s")
        
        return processed_content
    
    async def scrape_encyclopedia_complete(self) -> List[ScrapingResult]:
        """Scrape all encyclopedia entries with intelligent discovery"""
        
        logger.info("📚 Starting Encyclopedia section scraping")
        section_name = "encyclopedia"
        
        # AI discovers all possible encyclopedia URLs
        encyclopedia_urls = await self._discover_encyclopedia_urls()
        logger.info(f"🔍 Discovered {len(encyclopedia_urls)} encyclopedia URLs")
        
        scraped_articles = []
        
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
            connector=aiohttp.TCPConnector(limit=100, limit_per_host=25)
        ) as session:
            
            # Intelligent batching for optimal performance
            batch_size = 50
            for i in range(0, len(encyclopedia_urls), batch_size):
                batch_urls = encyclopedia_urls[i:i + batch_size]
                
                logger.info(f"📖 Processing encyclopedia batch {i//batch_size + 1}/{len(encyclopedia_urls)//batch_size + 1}")
                
                # Parallel scraping with anti-detection
                batch_results = await self._scrape_url_batch_with_protection(
                    batch_urls, session, section_name
                )
                scraped_articles.extend(batch_results)
                
                # Update section stats
                successful = sum(1 for r in batch_results if r.success)
                self.section_stats[section_name]['processed'] += len(batch_results)
                self.section_stats[section_name]['successful'] += successful
                self.section_stats[section_name]['errors'] += len(batch_results) - successful
                
                # Intelligent delay between batches
                delay = await self.timing_humanizer.calculate_adaptive_delay(
                    "medlineplus.gov", successful / len(batch_results) if batch_results else 0
                )
                await asyncio.sleep(delay)
        
        logger.info(f"✅ Encyclopedia section complete: {len(scraped_articles)} articles processed")
        return scraped_articles
    
    async def scrape_health_topics_complete(self) -> List[ScrapingResult]:
        """Scrape all health topics with comprehensive coverage"""
        
        logger.info("🏥 Starting Health Topics section scraping")
        section_name = "health_topics"
        
        # Discover health topic URLs using multiple strategies
        health_topic_urls = await self._discover_health_topic_urls()
        logger.info(f"🔍 Discovered {len(health_topic_urls)} health topic URLs")
        
        scraped_topics = []
        
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
            connector=aiohttp.TCPConnector(limit=100, limit_per_host=25)
        ) as session:
            
            batch_size = 40  # Smaller batches for health topics
            for i in range(0, len(health_topic_urls), batch_size):
                batch_urls = health_topic_urls[i:i + batch_size]
                
                logger.info(f"🏥 Processing health topics batch {i//batch_size + 1}/{len(health_topic_urls)//batch_size + 1}")
                
                batch_results = await self._scrape_url_batch_with_protection(
                    batch_urls, session, section_name
                )
                scraped_topics.extend(batch_results)
                
                # Update stats
                successful = sum(1 for r in batch_results if r.success)
                self.section_stats[section_name]['processed'] += len(batch_results)
                self.section_stats[section_name]['successful'] += successful
                self.section_stats[section_name]['errors'] += len(batch_results) - successful
                
                # Adaptive delay
                delay = await self.timing_humanizer.calculate_adaptive_delay(
                    "medlineplus.gov", successful / len(batch_results) if batch_results else 0
                )
                await asyncio.sleep(delay)
        
        logger.info(f"✅ Health Topics section complete: {len(scraped_topics)} topics processed")
        return scraped_topics
    
    async def scrape_drug_database_complete(self) -> List[ScrapingResult]:
        """Scrape comprehensive drug information database"""
        
        logger.info("💊 Starting Drug Information section scraping")
        section_name = "drug_info"
        
        # Discover drug information URLs
        drug_urls = await self._discover_drug_information_urls()
        logger.info(f"🔍 Discovered {len(drug_urls)} drug information URLs")
        
        scraped_drugs = []
        
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
            connector=aiohttp.TCPConnector(limit=80, limit_per_host=20)
        ) as session:
            
            batch_size = 35
            for i in range(0, len(drug_urls), batch_size):
                batch_urls = drug_urls[i:i + batch_size]
                
                logger.info(f"💊 Processing drugs batch {i//batch_size + 1}/{len(drug_urls)//batch_size + 1}")
                
                batch_results = await self._scrape_url_batch_with_protection(
                    batch_urls, session, section_name
                )
                scraped_drugs.extend(batch_results)
                
                # Update stats
                successful = sum(1 for r in batch_results if r.success)
                self.section_stats[section_name]['processed'] += len(batch_results)
                self.section_stats[section_name]['successful'] += successful
                self.section_stats[section_name]['errors'] += len(batch_results) - successful
                
                # Longer delay for drug information (more sensitive)
                delay = await self.timing_humanizer.calculate_adaptive_delay(
                    "medlineplus.gov", successful / len(batch_results) if batch_results else 0
                )
                await asyncio.sleep(delay + 1.0)  # Extra delay for drug content
        
        logger.info(f"✅ Drug Information section complete: {len(scraped_drugs)} drugs processed")
        return scraped_drugs
    
    async def scrape_supplements_complete(self) -> List[ScrapingResult]:
        """Scrape vitamins and supplements database"""
        
        logger.info("🌿 Starting Supplements section scraping")
        section_name = "supplements"
        
        supplement_urls = await self._discover_supplement_urls()
        logger.info(f"🔍 Discovered {len(supplement_urls)} supplement URLs")
        
        return await self._execute_section_scraping(
            supplement_urls, section_name, "Supplements", batch_size=30
        )
    
    async def scrape_medical_tests_complete(self) -> List[ScrapingResult]:
        """Scrape medical tests and lab procedures"""
        
        logger.info("🧪 Starting Medical Tests section scraping")
        section_name = "medical_tests"
        
        test_urls = await self._discover_medical_test_urls()
        logger.info(f"🔍 Discovered {len(test_urls)} medical test URLs")
        
        return await self._execute_section_scraping(
            test_urls, section_name, "Medical Tests", batch_size=25
        )
    
    async def scrape_surgery_info_complete(self) -> List[ScrapingResult]:
        """Scrape surgery and procedure information"""
        
        logger.info("⚕️ Starting Surgery section scraping")
        section_name = "surgery"
        
        surgery_urls = await self._discover_surgery_urls()
        logger.info(f"🔍 Discovered {len(surgery_urls)} surgery URLs")
        
        return await self._execute_section_scraping(
            surgery_urls, section_name, "Surgery", batch_size=20
        )
    
    async def scrape_anatomy_complete(self) -> List[ScrapingResult]:
        """Scrape anatomy and body systems information"""
        
        logger.info("🫀 Starting Anatomy section scraping")
        section_name = "anatomy"
        
        anatomy_urls = await self._discover_anatomy_urls()
        logger.info(f"🔍 Discovered {len(anatomy_urls)} anatomy URLs")
        
        return await self._execute_section_scraping(
            anatomy_urls, section_name, "Anatomy", batch_size=15
        )
    
    async def scrape_easy_read_complete(self) -> List[ScrapingResult]:
        """Scrape easy-to-read health information"""
        
        logger.info("📖 Starting Easy Read section scraping")
        section_name = "easy_read"
        
        easy_read_urls = await self._discover_easy_read_urls()
        logger.info(f"🔍 Discovered {len(easy_read_urls)} easy read URLs")
        
        return await self._execute_section_scraping(
            easy_read_urls, section_name, "Easy Read", batch_size=25
        )
    
    async def scrape_videos_complete(self) -> List[ScrapingResult]:
        """Scrape video resources and multimedia content"""
        
        logger.info("🎥 Starting Videos section scraping")
        section_name = "videos"
        
        video_urls = await self._discover_video_urls()
        logger.info(f"🔍 Discovered {len(video_urls)} video URLs")
        
        return await self._execute_section_scraping(
            video_urls, section_name, "Videos", batch_size=20
        )
    
    async def _execute_section_scraping(self, urls: List[str], section_name: str, 
                                      display_name: str, batch_size: int = 30) -> List[ScrapingResult]:
        """Generic section scraping execution with consistent handling"""
        
        scraped_results = []
        
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
            connector=aiohttp.TCPConnector(limit=80, limit_per_host=20)
        ) as session:
            
            for i in range(0, len(urls), batch_size):
                batch_urls = urls[i:i + batch_size]
                
                logger.info(f"{display_name} batch {i//batch_size + 1}/{len(urls)//batch_size + 1}")
                
                batch_results = await self._scrape_url_batch_with_protection(
                    batch_urls, session, section_name
                )
                scraped_results.extend(batch_results)
                
                # Update stats
                successful = sum(1 for r in batch_results if r.success)
                self.section_stats[section_name]['processed'] += len(batch_results)
                self.section_stats[section_name]['successful'] += successful
                self.section_stats[section_name]['errors'] += len(batch_results) - successful
                
                # Adaptive delay
                delay = await self.timing_humanizer.calculate_adaptive_delay(
                    "medlineplus.gov", successful / len(batch_results) if batch_results else 0
                )
                await asyncio.sleep(delay)
        
        logger.info(f"✅ {display_name} section complete: {len(scraped_results)} items processed")
        return scraped_results
    
    async def _scrape_url_batch_with_protection(self, urls: List[str], session: aiohttp.ClientSession,
                                               section: str) -> List[ScrapingResult]:
        """Scrape batch of URLs with advanced protection and error handling"""
        
        tasks = []
        semaphore = asyncio.Semaphore(25)  # Limit concurrent requests
        
        async def scrape_single_url(url: str) -> ScrapingResult:
            async with semaphore:
                return await self._extract_medlineplus_content(url, session, section)
        
        # Create tasks for all URLs
        for url in urls:
            task = scrape_single_url(url)
            tasks.append(task)
        
        # Execute with error handling
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter and process results
        valid_results = []
        for result in results:
            if isinstance(result, ScrapingResult):
                valid_results.append(result)
            elif isinstance(result, Exception):
                logger.warning(f"Scraping error: {result}")
        
        return valid_results
    
    async def _extract_medlineplus_content(self, url: str, session: aiohttp.ClientSession, 
                                         section: str) -> ScrapingResult:
        """Extract content from single MedlinePlus URL with advanced processing"""
        
        task_id = f"medlineplus_{section}_{hash(url)}"
        
        try:
            # Get optimized headers for MedlinePlus
            headers = await self.anti_detection.get_optimized_headers(url, len(self.processed_urls))
            
            # Add MedlinePlus-specific headers
            headers.update({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none'
            })
            
            start_time = time.time()
            
            async with session.get(url, headers=headers, timeout=30) as response:
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
                    
                    # Extract structured MedlinePlus data
                    extracted_data = await self._extract_medlineplus_structured_data(content, url, section)
                    
                    # Assess content quality with MedlinePlus-specific scoring
                    quality_score = await self.content_quality.assess_content_quality(content, url)
                    
                    # Enhance quality score for MedlinePlus (high-authority source)
                    enhanced_quality_score = min(1.0, quality_score * 1.2)
                    
                    result = ScrapingResult(
                        task_id=task_id,
                        url=url,
                        success=True,
                        content=content,
                        extracted_data=extracted_data,
                        processing_time=processing_time,
                        content_length=len(content),
                        quality_score=enhanced_quality_score,
                        confidence_score=0.95,  # High confidence for MedlinePlus
                        timestamp=datetime.utcnow()
                    )
                    
                    self.success_count += 1
                    self.total_content_size += len(content)
                    self.processed_urls.add(url)
                    
                    return result
                    
                else:
                    return ScrapingResult(
                        task_id=task_id,
                        url=url,
                        success=False,
                        error_details=f"HTTP {response.status}: {response.reason}",
                        timestamp=datetime.utcnow()
                    )
                    
        except Exception as e:
            self.error_count += 1
            return ScrapingResult(
                task_id=task_id,
                url=url,
                success=False,
                error_details=str(e),
                timestamp=datetime.utcnow()
            )
    
    async def _extract_medlineplus_structured_data(self, content: str, url: str, section: str) -> Dict[str, Any]:
        """Extract structured data specifically tailored for MedlinePlus content"""
        
        try:
            soup = BeautifulSoup(content, 'lxml')
            
            extracted = {
                'title': '',
                'summary': '',
                'medlineplus_section': section,
                'medical_content': {},
                'medlineplus_specific': {},
                'metadata': {},
                'links': []
            }
            
            # Extract MedlinePlus-specific title
            title_selectors = [
                'h1.page-title',
                'h1#pagetitle',
                'h1',
                'title'
            ]
            
            for selector in title_selectors:
                title_elem = soup.select_one(selector)
                if title_elem:
                    extracted['title'] = title_elem.get_text(strip=True)
                    break
            
            # Extract MedlinePlus summary/overview
            summary_selectors = [
                '.summary',
                '.overview',
                '.page-summary',
                'div[data-module="Summary"]',
                '.mplus-summary'
            ]
            
            for selector in summary_selectors:
                summary_elem = soup.select_one(selector)
                if summary_elem:
                    extracted['summary'] = summary_elem.get_text(strip=True)
                    break
            
            # Extract MedlinePlus-specific medical sections
            medical_sections = {}
            
            # Look for key medical information sections
            section_keywords = [
                ('symptoms', ['symptoms', 'signs']),
                ('causes', ['causes', 'risk factors']),
                ('diagnosis', ['diagnosis', 'testing']),
                ('treatment', ['treatment', 'therapy']),
                ('prevention', ['prevention', 'avoiding']),
                ('complications', ['complications', 'side effects']),
                ('outlook', ['outlook', 'prognosis'])
            ]
            
            for section_key, keywords in section_keywords:
                section_content = []
                
                for keyword in keywords:
                    # Look for sections containing these keywords
                    sections = soup.find_all(['div', 'section'], 
                        text=re.compile(keyword, re.IGNORECASE))
                    
                    for sect in sections:
                        # Get the parent container and extract content
                        parent = sect.parent if sect.parent else sect
                        paragraphs = parent.find_all('p')
                        section_text = [p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 50]
                        section_content.extend(section_text)
                
                if section_content:
                    medical_sections[section_key] = section_content[:3]  # Top 3 relevant paragraphs
            
            extracted['medical_content']['sections'] = medical_sections
            
            # Extract MedlinePlus-specific elements
            medlineplus_specific = {}
            
            # Extract "Also called" information
            also_called = soup.select_one('.also-called, .alternative-names')
            if also_called:
                medlineplus_specific['also_called'] = also_called.get_text(strip=True)
            
            # Extract related topics
            related_topics = []
            related_selectors = [
                '.related-topics a',
                '.see-also a',
                '.related-links a'
            ]
            
            for selector in related_selectors:
                links = soup.select(selector)
                for link in links[:10]:  # Top 10 related topics
                    topic_text = link.get_text(strip=True)
                    topic_href = link.get('href', '')
                    if topic_text and len(topic_text) > 3:
                        related_topics.append({
                            'title': topic_text,
                            'url': urljoin(url, topic_href) if topic_href else ''
                        })
            
            medlineplus_specific['related_topics'] = related_topics
            
            # Extract key statistics or numbers
            numbers_text = soup.get_text()
            statistics = re.findall(r'(\d+(?:\.\d+)?)\s*(?:%|percent|million|thousand|cases?|patients?)', 
                                  numbers_text, re.IGNORECASE)
            medlineplus_specific['statistics'] = statistics[:5]  # Top 5 statistics
            
            extracted['medlineplus_specific'] = medlineplus_specific
            
            # Extract main content paragraphs
            content_paragraphs = []
            main_content_selectors = [
                '.main-content p',
                '#main p',
                '.article-content p',
                '.page-content p',
                'p'
            ]
            
            for selector in main_content_selectors:
                paragraphs = soup.select(selector)
                if paragraphs:
                    for p in paragraphs:
                        text = p.get_text(strip=True)
                        if len(text) > 100:  # Substantial paragraphs only
                            content_paragraphs.append(text)
                    if content_paragraphs:
                        break  # Use first successful selector
            
            extracted['medical_content']['paragraphs'] = content_paragraphs[:15]  # Top 15 paragraphs
            
            # Extract internal MedlinePlus links
            internal_links = []
            for a in soup.find_all('a', href=True):
                href = a.get('href')
                text = a.get_text(strip=True)
                
                # Filter for MedlinePlus internal links
                if (href and 
                    ('medlineplus.gov' in href or href.startswith('/')) and
                    text and 
                    len(text) > 5 and
                    not href.startswith('#')):
                    
                    full_url = urljoin(url, href)
                    internal_links.append({
                        'url': full_url,
                        'text': text
                    })
            
            extracted['links'] = internal_links[:25]  # Top 25 internal links
            
            # Extract comprehensive metadata
            extracted['metadata'] = {
                'word_count': len(content.split()),
                'paragraph_count': len(content_paragraphs),
                'medical_sections_count': len(medical_sections),
                'related_topics_count': len(related_topics),
                'internal_links_count': len(internal_links),
                'statistics_count': len(statistics),
                'content_depth_score': self._calculate_content_depth(medical_sections, content_paragraphs),
                'extracted_at': datetime.utcnow().isoformat(),
                'source_authority': 'medlineplus.gov',
                'government_source': True
            }
            
            return extracted
            
        except Exception as e:
            logger.error(f"Error extracting MedlinePlus structured data from {url}: {e}")
            return {'error': str(e), 'url': url, 'section': section}
    
    def _calculate_content_depth(self, medical_sections: Dict[str, List[str]], 
                                paragraphs: List[str]) -> float:
        """Calculate content depth score based on medical information completeness"""
        
        depth_score = 0.0
        
        # Score based on medical sections presence
        section_weights = {
            'symptoms': 0.2,
            'causes': 0.15,
            'diagnosis': 0.15,
            'treatment': 0.25,
            'prevention': 0.1,
            'complications': 0.1,
            'outlook': 0.05
        }
        
        for section, weight in section_weights.items():
            if section in medical_sections and medical_sections[section]:
                depth_score += weight
        
        # Score based on content volume
        content_volume_score = min(0.3, len(paragraphs) * 0.02)  # Max 0.3 for volume
        depth_score += content_volume_score
        
        return min(1.0, depth_score)
    
    # URL Discovery Methods
    async def _discover_encyclopedia_urls(self) -> List[str]:
        """Discover all encyclopedia URLs using multiple strategies"""
        
        base_url = self.base_urls['encyclopedia']
        discovered_urls = set()
        
        # Strategy 1: A-Z browsing
        for letter in 'abcdefghijklmnopqrstuvwxyz0123456789':
            az_urls = await self.content_discovery.discover_medical_urls(
                f"{base_url}{letter}.html",
                f"encyclopedia_{letter}"
            )
            discovered_urls.update(az_urls)
        
        # Strategy 2: Category browsing
        categories = ['anatomy', 'diseases', 'symptoms', 'tests', 'treatments']
        for category in categories:
            cat_urls = await self.content_discovery.discover_medical_urls(
                f"{base_url}{category}/",
                f"encyclopedia_{category}"
            )
            discovered_urls.update(cat_urls)
        
        # Strategy 3: Search-based discovery
        search_terms = [
            'disease', 'condition', 'symptoms', 'treatment', 'diagnosis',
            'syndrome', 'disorder', 'infection', 'cancer', 'diabetes'
        ]
        for term in search_terms:
            search_urls = await self._discover_search_based_urls(base_url, term)
            discovered_urls.update(search_urls)
        
        return list(discovered_urls)
    
    async def _discover_health_topic_urls(self) -> List[str]:
        """Discover health topic URLs comprehensively"""
        
        base_url = self.base_urls['health_topics']
        discovered_urls = set()
        
        # A-Z health topics
        for letter in 'abcdefghijklmnopqrstuvwxyz':
            topic_urls = await self.content_discovery.discover_medical_urls(
                f"{base_url}{letter}.html",
                f"health_topics_{letter}"
            )
            discovered_urls.update(topic_urls)
        
        return list(discovered_urls)
    
    async def _discover_drug_information_urls(self) -> List[str]:
        """Discover drug information URLs"""
        
        discovered_urls = set()
        
        # A-Z drug browsing
        base_drug_url = "https://medlineplus.gov/druginfo/"
        for letter in 'abcdefghijklmnopqrstuvwxyz':
            drug_urls = await self.content_discovery.discover_medical_urls(
                f"{base_drug_url}{letter}.html",
                f"drugs_{letter}"
            )
            discovered_urls.update(drug_urls)
        
        return list(discovered_urls)
    
    async def _discover_supplement_urls(self) -> List[str]:
        """Discover supplement URLs"""
        
        return await self.content_discovery.discover_medical_urls(
            self.base_urls['supplements'],
            "supplements"
        )
    
    async def _discover_medical_test_urls(self) -> List[str]:
        """Discover medical test URLs"""
        
        return await self.content_discovery.discover_medical_urls(
            self.base_urls['medical_tests'],
            "medical_tests"
        )
    
    async def _discover_surgery_urls(self) -> List[str]:
        """Discover surgery and procedure URLs"""
        
        return await self.content_discovery.discover_medical_urls(
            self.base_urls['surgery'],
            "surgery"
        )
    
    async def _discover_anatomy_urls(self) -> List[str]:
        """Discover anatomy URLs"""
        
        return await self.content_discovery.discover_medical_urls(
            self.base_urls['anatomy'],
            "anatomy"
        )
    
    async def _discover_easy_read_urls(self) -> List[str]:
        """Discover easy read URLs"""
        
        return await self.content_discovery.discover_medical_urls(
            self.base_urls['easy_read'],
            "easy_read"
        )
    
    async def _discover_video_urls(self) -> List[str]:
        """Discover video resource URLs"""
        
        return await self.content_discovery.discover_medical_urls(
            self.base_urls['videos'],
            "videos"
        )
    
    async def _discover_search_based_urls(self, base_url: str, search_term: str) -> List[str]:
        """Discover URLs through search-based exploration"""
        
        search_urls = []
        
        # Simulate search patterns
        search_patterns = [
            f"{base_url}?search={search_term}",
            f"{base_url}search/{search_term}",
            f"{base_url}{search_term}/"
        ]
        
        for pattern in search_patterns:
            urls = await self.content_discovery.discover_medical_urls(
                pattern,
                f"search_{search_term}"
            )
            search_urls.extend(urls)
        
        return search_urls
    
    async def process_and_store_content(self, results: List[Any]) -> Dict[str, Any]:
        """Process and analyze all scraped content from MedlinePlus"""
        
        logger.info("🔄 Processing and analyzing MedlinePlus scraped content")
        
        all_results = []
        section_summaries = {}
        
        # Flatten and process results
        for i, section_results in enumerate(results):
            if isinstance(section_results, list):
                all_results.extend(section_results)
                
                # Create section summary
                successful = sum(1 for r in section_results if r.success)
                section_name = list(self.base_urls.keys())[i] if i < len(self.base_urls) else f"section_{i}"
                
                section_summaries[section_name] = {
                    'total_processed': len(section_results),
                    'successful': successful,
                    'success_rate': successful / len(section_results) if section_results else 0,
                    'avg_quality': sum(r.quality_score for r in section_results if r.success) / max(successful, 1)
                }
        
        # Calculate overall statistics
        total_processed = len(all_results)
        successful_results = [r for r in all_results if r.success]
        total_successful = len(successful_results)
        
        # Quality analysis
        high_quality = len([r for r in successful_results if r.quality_score >= 0.8])
        medium_quality = len([r for r in successful_results if 0.6 <= r.quality_score < 0.8])
        low_quality = total_successful - high_quality - medium_quality
        
        # Content analysis
        total_content_size = sum(r.content_length for r in successful_results)
        avg_processing_time = sum(r.processing_time for r in successful_results) / max(total_successful, 1)
        
        final_summary = {
            'medlineplus_scraping_summary': {
                'operation_type': 'MedlinePlus Comprehensive Scraping',
                'total_urls_processed': total_processed,
                'successful_extractions': total_successful,
                'failed_extractions': total_processed - total_successful,
                'overall_success_rate': total_successful / total_processed if total_processed > 0 else 0,
                'total_content_size_mb': total_content_size / (1024 * 1024),
                'average_processing_time': avg_processing_time,
                'content_authority_score': 0.95  # High authority for MedlinePlus
            },
            'quality_distribution': {
                'high_quality_documents': high_quality,
                'medium_quality_documents': medium_quality,
                'low_quality_documents': low_quality,
                'quality_percentages': {
                    'high': (high_quality / max(total_successful, 1)) * 100,
                    'medium': (medium_quality / max(total_successful, 1)) * 100,
                    'low': (low_quality / max(total_successful, 1)) * 100
                }
            },
            'section_performance': section_summaries,
            'performance_metrics': {
                'documents_per_second': total_processed / max(avg_processing_time * total_processed, 1),
                'mb_per_second': (total_content_size / (1024 * 1024)) / max(avg_processing_time * total_processed, 1),
                'government_source_reliability': 0.98,
                'content_medical_relevance': sum(r.quality_score for r in successful_results) / max(total_successful, 1)
            },
            'extracted_content': successful_results
        }
        
        logger.info("=" * 80)
        logger.info("🏆 MEDLINEPLUS COMPREHENSIVE SCRAPING - FINAL RESULTS")
        logger.info("=" * 80)
        logger.info(f"📊 Total URLs Processed: {total_processed:,}")
        logger.info(f"✅ Successful Extractions: {total_successful:,} ({(total_successful/total_processed)*100:.1f}%)")
        logger.info(f"💾 Total Content Size: {total_content_size / (1024 * 1024):.1f} MB")
        logger.info(f"⭐ High Quality Documents: {high_quality:,}")
        logger.info(f"🎯 Content Authority Score: 0.95 (Government Source)")
        logger.info("=" * 80)
        
        return final_summary


# Advanced helper classes
class AdvancedContentExtractor:
    """Advanced content extraction specifically for medical content"""
    
    async def extract_medical_entities(self, content: str) -> List[str]:
        """Extract medical entities from content"""
        # Simplified implementation - in production use advanced NLP
        medical_patterns = [
            r'\b\d+\s*(?:mg|ml|g|kg|units?)\b',  # Dosages
            r'\b(?:syndrome|disease|disorder|condition)\b',  # Medical conditions
            r'\b(?:symptoms?|signs?)\b',  # Clinical presentations
        ]
        
        entities = []
        for pattern in medical_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            entities.extend(matches)
        
        return list(set(entities))


class PageStructureAnalyzer:
    """Analyze page structure for optimal content extraction"""
    
    async def analyze_medlineplus_structure(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Analyze MedlinePlus page structure"""
        
        structure = {
            'has_main_content': bool(soup.select('.main-content, #main')),
            'has_navigation': bool(soup.select('nav, .navigation')),
            'has_related_topics': bool(soup.select('.related, .see-also')),
            'content_sections': len(soup.select('section, .section')),
            'paragraph_count': len(soup.select('p')),
            'heading_count': len(soup.select('h1, h2, h3, h4, h5, h6'))
        }
        
        return structure


class SessionRotator:
    """Rotate sessions to avoid detection"""
    
    def __init__(self):
        self.sessions = []
        self.current_index = 0
    
    async def get_session(self) -> aiohttp.ClientSession:
        """Get next session in rotation"""
        # Simplified - in production implement full session rotation
        return aiohttp.ClientSession()


class HeaderRandomizer:
    """Randomize headers to appear more human-like"""
    
    async def get_random_headers(self) -> Dict[str, str]:
        """Generate randomized headers"""
        
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        ]
        
        return {
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }


class TimingHumanizer:
    """Humanize request timing to avoid detection"""
    
    async def calculate_adaptive_delay(self, domain: str, success_rate: float) -> float:
        """Calculate adaptive delay based on success rate"""
        
        base_delays = {
            'medlineplus.gov': 2.5,
            'default': 2.0
        }
        
        base_delay = base_delays.get(domain, base_delays['default'])
        
        # Adjust based on success rate
        if success_rate < 0.5:
            base_delay *= 3.0
        elif success_rate < 0.7:
            base_delay *= 2.0
        elif success_rate > 0.9:
            base_delay *= 0.8
        
        # Add randomization
        jitter = random.uniform(-0.5, 1.0)
        return max(1.0, base_delay + jitter)

# Export main class
__all__ = ['MedlinePlusAdvancedScraper']