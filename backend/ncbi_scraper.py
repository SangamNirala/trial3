"""
Phase 2: NCBI Comprehensive Database Scraper
Advanced scraper for NCBI ecosystem including PubMed, PMC, and other databases
Target: 50,000+ medical articles and research papers
"""

import asyncio
import aiohttp
import logging
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta
import json
import random
import time
from urllib.parse import urljoin, urlparse, parse_qs, quote
from bs4 import BeautifulSoup
import re
from collections import defaultdict
import xml.etree.ElementTree as ET

from ai_scraper_core import (
    ScrapingTask, ScrapingResult, ScrapingPriority, ContentType, ScrapingTier,
    ContentDiscoveryAI, AntiDetectionAI, ContentQualityAI, AdvancedDeduplicator
)

logger = logging.getLogger(__name__)

class NCBIAdvancedScraper:
    """
    Comprehensive NCBI scraper with advanced API integration and web scraping
    Covers PubMed, PMC, NCBI Bookshelf, ClinVar, OMIM, and MeSH databases
    """
    
    def __init__(self):
        self.ncbi_endpoints = {
            'eutils_base': 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/',
            'pubmed': 'https://pubmed.ncbi.nlm.nih.gov/',
            'pmc': 'https://www.ncbi.nlm.nih.gov/pmc/',
            'bookshelf': 'https://www.ncbi.nlm.nih.gov/books/',
            'clinvar': 'https://www.ncbi.nlm.nih.gov/clinvar/',
            'omim': 'https://www.ncbi.nlm.nih.gov/omim/',
            'mesh': 'https://www.ncbi.nlm.nih.gov/mesh/',
            'gene': 'https://www.ncbi.nlm.nih.gov/gene/',
            'snp': 'https://www.ncbi.nlm.nih.gov/snp/',
            'gwas': 'https://www.ncbi.nlm.nih.gov/gap/'
        }
        
        # NCBI-specific scraping tools
        self.eutils_client = EUtilsAdvancedClient()
        self.api_rate_manager = NCBIRateManager()
        self.query_generator = MedicalQueryGenerator()
        self.citation_parser = CitationParser()
        
        # AI systems from core
        self.content_discovery = ContentDiscoveryAI()
        self.anti_detection = AntiDetectionAI()
        self.content_quality = ContentQualityAI()
        self.deduplicator = AdvancedDeduplicator()
        
        # Performance tracking
        self.processed_queries = set()
        self.success_count = 0
        self.error_count = 0
        self.total_articles_found = 0
        self.api_calls_made = 0
        self.database_stats = defaultdict(lambda: {'processed': 0, 'successful': 0, 'errors': 0})
        
        # NCBI API configuration
        self.api_key = None  # Optional API key for higher rate limits
        self.email = "medical.scraper@research.ai"  # Required for NCBI API
        self.tool = "MedicalScraperPhase2"
        
    async def scrape_pubmed_massive_dataset(self) -> Dict[str, Any]:
        """Scrape massive PubMed dataset using AI-generated queries"""
        
        logger.info("🔬 Starting PubMed Massive Dataset Extraction")
        start_time = datetime.utcnow()
        
        # Generate comprehensive medical queries using AI
        medical_queries = await self.query_generator.generate_comprehensive_medical_queries()
        logger.info(f"📝 Generated {len(medical_queries)} comprehensive medical queries")
        
        pubmed_articles = []
        
        # Process queries in intelligent batches
        batch_size = 10  # Process 10 queries at a time
        for i in range(0, len(medical_queries), batch_size):
            batch_queries = medical_queries[i:i + batch_size]
            
            logger.info(f"🔍 Processing PubMed query batch {i//batch_size + 1}/{len(medical_queries)//batch_size + 1}")
            
            batch_results = await self._process_pubmed_query_batch(batch_queries)
            pubmed_articles.extend(batch_results)
            
            # Respect API rate limits intelligently
            await self.api_rate_manager.wait_for_rate_limit()
            
            # Progress update
            logger.info(f"📊 PubMed progress: {len(pubmed_articles)} articles extracted so far")
        
        # Advanced deduplication and quality filtering
        unique_quality_articles = await self._deduplicate_and_filter_quality(pubmed_articles)
        
        execution_time = (datetime.utcnow() - start_time).total_seconds()
        
        return {
            'database': 'PubMed',
            'total_articles': len(unique_quality_articles),
            'execution_time': execution_time,
            'queries_processed': len(medical_queries),
            'api_calls': self.api_calls_made,
            'articles': unique_quality_articles
        }
    
    async def scrape_pmc_open_access_articles(self) -> Dict[str, Any]:
        """Scrape PMC open access full-text articles"""
        
        logger.info("📖 Starting PMC Open Access Articles Extraction")
        
        # Discover all open access article IDs
        open_access_ids = await self._discover_pmc_open_access_articles()
        logger.info(f"🔍 Discovered {len(open_access_ids)} open access article IDs")
        
        full_text_articles = []
        
        # Batch processing for efficiency
        batch_size = 100
        for i in range(0, len(open_access_ids), batch_size):
            batch_ids = open_access_ids[i:i + batch_size]
            
            logger.info(f"📄 Processing PMC batch {i//batch_size + 1}/{len(open_access_ids)//batch_size + 1}")
            
            # Extract full text content in parallel
            batch_articles = await self._extract_pmc_full_text_batch(batch_ids)
            full_text_articles.extend(batch_articles)
            
            # Rate limiting
            await self.api_rate_manager.wait_for_rate_limit()
        
        return {
            'database': 'PMC',
            'total_articles': len(full_text_articles),
            'open_access_ids': len(open_access_ids),
            'articles': full_text_articles
        }
    
    async def scrape_ncbi_bookshelf_complete(self) -> Dict[str, Any]:
        """Scrape NCBI Bookshelf medical books and resources"""
        
        logger.info("📚 Starting NCBI Bookshelf Extraction")
        
        # Discover bookshelf content
        book_urls = await self._discover_bookshelf_content()
        logger.info(f"📖 Discovered {len(book_urls)} bookshelf resources")
        
        bookshelf_content = []
        
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
            connector=aiohttp.TCPConnector(limit=50, limit_per_host=15)
        ) as session:
            
            # Process books in batches
            batch_size = 20
            for i in range(0, len(book_urls), batch_size):
                batch_urls = book_urls[i:i + batch_size]
                
                logger.info(f"📚 Processing bookshelf batch {i//batch_size + 1}/{len(book_urls)//batch_size + 1}")
                
                batch_results = await self._scrape_bookshelf_batch(batch_urls, session)
                bookshelf_content.extend(batch_results)
                
                # Respectful delay for NCBI
                await asyncio.sleep(random.uniform(2.0, 4.0))
        
        return {
            'database': 'NCBI_Bookshelf',
            'total_resources': len(bookshelf_content),
            'book_urls': len(book_urls),
            'content': bookshelf_content
        }
    
    async def scrape_clinvar_genetic_data(self) -> Dict[str, Any]:
        """Scrape ClinVar genetic variant data"""
        
        logger.info("🧬 Starting ClinVar Genetic Data Extraction")
        
        # Generate genetic variation queries
        genetic_queries = await self.query_generator.generate_genetic_queries()
        clinvar_variants = []
        
        for query in genetic_queries:
            variants = await self._search_clinvar_variants(query)
            clinvar_variants.extend(variants)
            
            await self.api_rate_manager.wait_for_rate_limit()
        
        return {
            'database': 'ClinVar',
            'total_variants': len(clinvar_variants),
            'variants': clinvar_variants
        }
    
    async def scrape_mesh_medical_terms(self) -> Dict[str, Any]:
        """Scrape MeSH medical terminology and concepts"""
        
        logger.info("🏷️ Starting MeSH Medical Terms Extraction")
        
        mesh_terms = await self._extract_mesh_terminology()
        
        return {
            'database': 'MeSH',
            'total_terms': len(mesh_terms),
            'terms': mesh_terms
        }
    
    async def scrape_complete_ncbi_ecosystem(self) -> Dict[str, Any]:
        """Comprehensive NCBI ecosystem scraping coordinator"""
        
        logger.info("🚀 Starting Complete NCBI Ecosystem Extraction")
        start_time = datetime.utcnow()
        
        # Launch parallel extractions across all NCBI databases
        extraction_tasks = [
            self.scrape_pubmed_massive_dataset(),
            self.scrape_pmc_open_access_articles(),
            self.scrape_ncbi_bookshelf_complete(),
            self.scrape_clinvar_genetic_data(),
            self.scrape_mesh_medical_terms()
        ]
        
        logger.info(f"⚡ Launching {len(extraction_tasks)} parallel NCBI database extractions")
        results = await asyncio.gather(*extraction_tasks, return_exceptions=True)
        
        # Process and consolidate results
        consolidated_results = await self._consolidate_ncbi_results(results)
        
        execution_time = (datetime.utcnow() - start_time).total_seconds()
        
        # Generate comprehensive summary
        total_documents = sum(
            result.get('total_articles', result.get('total_resources', result.get('total_variants', result.get('total_terms', 0))))
            for result in results if isinstance(result, dict)
        )
        
        final_summary = {
            'ncbi_scraping_summary': {
                'operation_type': 'NCBI Comprehensive Ecosystem Scraping',
                'databases_processed': len([r for r in results if isinstance(r, dict)]),
                'total_documents_extracted': total_documents,
                'total_api_calls': self.api_calls_made,
                'execution_time': execution_time,
                'success_rate': len([r for r in results if isinstance(r, dict)]) / len(extraction_tasks),
                'research_authority_score': 0.98  # Very high authority for NCBI
            },
            'database_results': consolidated_results,
            'performance_metrics': {
                'documents_per_second': total_documents / execution_time if execution_time > 0 else 0,
                'api_efficiency': total_documents / max(self.api_calls_made, 1),
                'research_quality_score': 0.95,  # High quality for peer-reviewed research
                'scientific_relevance': 0.97
            }
        }
        
        logger.info("=" * 80)
        logger.info("🏆 NCBI COMPREHENSIVE ECOSYSTEM - FINAL RESULTS")
        logger.info("=" * 80)
        logger.info(f"📊 Total Documents Extracted: {total_documents:,}")
        logger.info(f"🗃️ Databases Processed: {len([r for r in results if isinstance(r, dict)])}")
        logger.info(f"🔬 Research Authority Score: 0.98")
        logger.info(f"⏱️ Total Execution Time: {execution_time:.1f} seconds")
        logger.info(f"🚀 API Calls Made: {self.api_calls_made:,}")
        logger.info("=" * 80)
        
        return final_summary
    
    # Internal processing methods
    async def _process_pubmed_query_batch(self, queries: List[str]) -> List[Dict[str, Any]]:
        """Process batch of PubMed queries"""
        
        batch_articles = []
        
        for query in queries:
            try:
                # Search PubMed with intelligent pagination
                search_results = await self.eutils_client.search_pubmed_comprehensive(
                    query=query,
                    max_results=5000,  # 5000 per query
                    batch_size=1000
                )
                
                # Extract article details
                if search_results:
                    article_details = await self.eutils_client.fetch_article_details_batch(search_results)
                    batch_articles.extend(article_details)
                
                self.api_calls_made += 2  # Search + fetch
                
            except Exception as e:
                logger.warning(f"Error processing query '{query}': {e}")
                self.error_count += 1
        
        return batch_articles
    
    async def _discover_pmc_open_access_articles(self) -> List[str]:
        """Discover PMC open access article IDs"""
        
        # Use various strategies to discover open access articles
        strategies = [
            self._discover_pmc_by_journal(),
            self._discover_pmc_by_keywords(),
            self._discover_pmc_by_date_range(),
            self._discover_pmc_by_subject()
        ]
        
        all_ids = set()
        
        for strategy in strategies:
            ids = await strategy
            all_ids.update(ids)
        
        return list(all_ids)
    
    async def _discover_pmc_by_journal(self) -> List[str]:
        """Discover PMC articles by high-impact journals"""
        
        high_impact_journals = [
            "Nature", "Science", "Cell", "NEJM", "Lancet", "BMJ", "JAMA",
            "Nature Medicine", "Nature Genetics", "PLOS ONE", "PLOS Medicine"
        ]
        
        article_ids = []
        
        for journal in high_impact_journals:
            query = f'"{journal}"[Journal] AND ("open access"[Filter] OR "pmc"[Filter])'
            ids = await self.eutils_client.search_pmc(query, max_results=1000)
            article_ids.extend(ids)
        
        return article_ids
    
    async def _discover_pmc_by_keywords(self) -> List[str]:
        """Discover PMC articles by medical keywords"""
        
        medical_keywords = [
            "cancer treatment", "cardiovascular disease", "diabetes", "neurology",
            "infectious disease", "immunology", "pharmacology", "genetics",
            "clinical trial", "epidemiology", "public health", "mental health"
        ]
        
        article_ids = []
        
        for keyword in medical_keywords:
            query = f'"{keyword}"[All Fields] AND "open access"[Filter]'
            ids = await self.eutils_client.search_pmc(query, max_results=800)
            article_ids.extend(ids)
        
        return article_ids
    
    async def _discover_pmc_by_date_range(self) -> List[str]:
        """Discover recent PMC articles"""
        
        # Focus on recent high-quality articles
        current_year = datetime.now().year
        years = [current_year, current_year-1, current_year-2]
        
        article_ids = []
        
        for year in years:
            query = f'"free full text"[Filter] AND {year}[Publication Date]'
            ids = await self.eutils_client.search_pmc(query, max_results=2000)
            article_ids.extend(ids)
        
        return article_ids
    
    async def _discover_pmc_by_subject(self) -> List[str]:
        """Discover PMC articles by medical subjects"""
        
        medical_subjects = [
            "Medicine", "Surgery", "Pharmacology", "Pathology", "Radiology",
            "Cardiology", "Oncology", "Neurology", "Psychiatry", "Pediatrics"
        ]
        
        article_ids = []
        
        for subject in medical_subjects:
            query = f'"{subject}"[MeSH] AND "open access"[Filter]'
            ids = await self.eutils_client.search_pmc(query, max_results=1000)
            article_ids.extend(ids)
        
        return article_ids
    
    async def _extract_pmc_full_text_batch(self, article_ids: List[str]) -> List[Dict[str, Any]]:
        """Extract full text from batch of PMC articles"""
        
        full_text_articles = []
        
        # Process in smaller sub-batches
        sub_batch_size = 20
        for i in range(0, len(article_ids), sub_batch_size):
            sub_batch = article_ids[i:i + sub_batch_size]
            
            # Fetch full text for each article
            for article_id in sub_batch:
                try:
                    full_text = await self.eutils_client.fetch_pmc_full_text(article_id)
                    if full_text:
                        full_text_articles.append(full_text)
                        self.success_count += 1
                    
                    self.api_calls_made += 1
                    
                except Exception as e:
                    logger.warning(f"Error fetching PMC article {article_id}: {e}")
                    self.error_count += 1
            
            # Small delay between sub-batches
            await asyncio.sleep(0.5)
        
        return full_text_articles
    
    async def _discover_bookshelf_content(self) -> List[str]:
        """Discover NCBI Bookshelf content URLs"""
        
        bookshelf_urls = []
        
        # Medical book categories
        categories = [
            "medicine", "surgery", "pharmacology", "anatomy", "physiology",
            "pathology", "microbiology", "immunology", "genetics", "public-health"
        ]
        
        for category in categories:
            category_urls = await self.content_discovery.discover_medical_urls(
                f"{self.ncbi_endpoints['bookshelf']}browse/{category}/",
                f"bookshelf_{category}"
            )
            bookshelf_urls.extend(category_urls)
        
        return bookshelf_urls
    
    async def _scrape_bookshelf_batch(self, urls: List[str], session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        """Scrape batch of bookshelf content"""
        
        bookshelf_content = []
        
        for url in urls:
            try:
                content = await self._extract_bookshelf_content(url, session)
                if content:
                    bookshelf_content.append(content)
                    self.success_count += 1
                
            except Exception as e:
                logger.warning(f"Error scraping bookshelf URL {url}: {e}")
                self.error_count += 1
        
        return bookshelf_content
    
    async def _extract_bookshelf_content(self, url: str, session: aiohttp.ClientSession) -> Dict[str, Any]:
        """Extract content from single bookshelf URL"""
        
        headers = await self.anti_detection.get_optimized_headers(url, len(self.processed_queries))
        
        async with session.get(url, headers=headers, timeout=30) as response:
            if response.status == 200:
                content = await response.text()
                
                # Parse bookshelf content
                soup = BeautifulSoup(content, 'lxml')
                
                extracted = {
                    'url': url,
                    'title': '',
                    'authors': [],
                    'publication_info': {},
                    'chapters': [],
                    'abstract': '',
                    'content': content,
                    'extracted_at': datetime.utcnow().isoformat()
                }
                
                # Extract title
                title_elem = soup.find('h1') or soup.find('title')
                if title_elem:
                    extracted['title'] = title_elem.get_text(strip=True)
                
                # Extract authors
                author_elems = soup.find_all(class_=re.compile('author'))
                extracted['authors'] = [elem.get_text(strip=True) for elem in author_elems]
                
                # Extract chapters/sections
                chapter_elems = soup.find_all(['h2', 'h3'], class_=re.compile('chapter|section'))
                extracted['chapters'] = [elem.get_text(strip=True) for elem in chapter_elems]
                
                # Quality assessment
                quality_score = await self.content_quality.assess_content_quality(content, url)
                extracted['quality_score'] = quality_score
                
                return extracted
        
        return None
    
    async def _search_clinvar_variants(self, query: str) -> List[Dict[str, Any]]:
        """Search ClinVar for genetic variants"""
        
        # Use NCBI API to search ClinVar
        search_url = f"{self.ncbi_endpoints['eutils_base']}esearch.fcgi"
        params = {
            'db': 'clinvar',
            'term': query,
            'retmax': 1000,
            'retmode': 'json',
            'email': self.email,
            'tool': self.tool
        }
        
        if self.api_key:
            params['api_key'] = self.api_key
        
        variants = []
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(search_url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        variant_ids = data.get('esearchresult', {}).get('idlist', [])
                        
                        # Fetch variant details
                        if variant_ids:
                            variants = await self._fetch_clinvar_details(variant_ids)
                        
                        self.api_calls_made += 1
        
        except Exception as e:
            logger.warning(f"Error searching ClinVar: {e}")
        
        return variants
    
    async def _fetch_clinvar_details(self, variant_ids: List[str]) -> List[Dict[str, Any]]:
        """Fetch detailed information for ClinVar variants"""
        
        fetch_url = f"{self.ncbi_endpoints['eutils_base']}efetch.fcgi"
        params = {
            'db': 'clinvar',
            'id': ','.join(variant_ids[:100]),  # Limit to 100 IDs
            'retmode': 'xml',
            'email': self.email,
            'tool': self.tool
        }
        
        if self.api_key:
            params['api_key'] = self.api_key
        
        variants = []
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(fetch_url, params=params) as response:
                    if response.status == 200:
                        xml_content = await response.text()
                        
                        # Parse XML and extract variant information
                        variants = self._parse_clinvar_xml(xml_content)
                        self.api_calls_made += 1
        
        except Exception as e:
            logger.warning(f"Error fetching ClinVar details: {e}")
        
        return variants
    
    def _parse_clinvar_xml(self, xml_content: str) -> List[Dict[str, Any]]:
        """Parse ClinVar XML response"""
        
        variants = []
        
        try:
            root = ET.fromstring(xml_content)
            
            for variant_elem in root.findall('.//ClinVarSet'):
                variant = {
                    'id': variant_elem.get('ID', ''),
                    'accession': '',
                    'gene': '',
                    'condition': '',
                    'clinical_significance': '',
                    'review_status': '',
                    'submission_date': ''
                }
                
                # Extract accession
                accession_elem = variant_elem.find('.//Accession')
                if accession_elem is not None:
                    variant['accession'] = accession_elem.get('Acc', '')
                
                # Extract gene information
                gene_elem = variant_elem.find('.//Gene')
                if gene_elem is not None:
                    variant['gene'] = gene_elem.get('Symbol', '')
                
                # Extract clinical significance
                sig_elem = variant_elem.find('.//ClinicalSignificance/Description')
                if sig_elem is not None:
                    variant['clinical_significance'] = sig_elem.text or ''
                
                variants.append(variant)
        
        except Exception as e:
            logger.warning(f"Error parsing ClinVar XML: {e}")
        
        return variants
    
    async def _extract_mesh_terminology(self) -> List[Dict[str, Any]]:
        """Extract MeSH medical terminology"""
        
        mesh_terms = []
        
        # Categories of medical terms to extract
        categories = [
            'diseases', 'anatomy', 'organisms', 'chemicals', 'techniques',
            'equipment', 'psychiatry', 'biological-sciences', 'health-care'
        ]
        
        for category in categories:
            try:
                category_terms = await self._extract_mesh_category(category)
                mesh_terms.extend(category_terms)
            except Exception as e:
                logger.warning(f"Error extracting MeSH category {category}: {e}")
        
        return mesh_terms
    
    async def _extract_mesh_category(self, category: str) -> List[Dict[str, Any]]:
        """Extract MeSH terms for specific category"""
        
        category_url = f"{self.ncbi_endpoints['mesh']}browse/{category}/"
        
        async with aiohttp.ClientSession() as session:
            headers = await self.anti_detection.get_optimized_headers(category_url, 0)
            
            async with session.get(category_url, headers=headers, timeout=30) as response:
                if response.status == 200:
                    content = await response.text()
                    soup = BeautifulSoup(content, 'lxml')
                    
                    terms = []
                    
                    # Extract MeSH terms from page
                    term_elements = soup.find_all(['a', 'span'], class_=re.compile('mesh|term'))
                    
                    for elem in term_elements:
                        term_text = elem.get_text(strip=True)
                        if len(term_text) > 2 and term_text not in [t.get('term', '') for t in terms]:
                            terms.append({
                                'term': term_text,
                                'category': category,
                                'url': urljoin(category_url, elem.get('href', '')) if elem.get('href') else '',
                                'extracted_at': datetime.utcnow().isoformat()
                            })
                    
                    return terms
        
        return []
    
    async def _deduplicate_and_filter_quality(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Advanced deduplication and quality filtering"""
        
        # Remove duplicates based on title and DOI
        seen_titles = set()
        seen_dois = set()
        unique_articles = []
        
        for article in articles:
            title = article.get('title', '').strip().lower()
            doi = article.get('doi', '').strip()
            
            # Check for duplicates
            is_duplicate = False
            
            if title and title in seen_titles:
                is_duplicate = True
            
            if doi and doi in seen_dois:
                is_duplicate = True
            
            if not is_duplicate:
                # Quality filtering
                quality_score = await self.content_quality.assess_content_quality(
                    article.get('abstract', '') + ' ' + title, 
                    article.get('url', '')
                )
                
                if quality_score >= 0.5:  # Minimum quality threshold
                    article['quality_score'] = quality_score
                    unique_articles.append(article)
                    
                    if title:
                        seen_titles.add(title)
                    if doi:
                        seen_dois.add(doi)
        
        return unique_articles
    
    async def _consolidate_ncbi_results(self, results: List[Any]) -> Dict[str, Any]:
        """Consolidate results from all NCBI databases"""
        
        consolidated = {}
        
        for result in results:
            if isinstance(result, dict) and 'database' in result:
                db_name = result['database']
                consolidated[db_name] = result
        
        return consolidated


# Helper classes for NCBI operations
class EUtilsAdvancedClient:
    """Advanced client for NCBI E-utilities"""
    
    def __init__(self):
        self.base_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/'
        self.email = "medical.scraper@research.ai"
        self.tool = "MedicalScraperPhase2"
        
    async def search_pubmed_comprehensive(self, query: str, max_results: int = 10000, 
                                        batch_size: int = 1000) -> List[str]:
        """Comprehensive PubMed search with pagination"""
        
        search_url = f"{self.base_url}esearch.fcgi"
        all_ids = []
        
        for start in range(0, max_results, batch_size):
            params = {
                'db': 'pubmed',
                'term': query,
                'retmax': min(batch_size, max_results - start),
                'retstart': start,
                'retmode': 'json',
                'email': self.email,
                'tool': self.tool,
                'sort': 'relevance'
            }
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(search_url, params=params, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            ids = data.get('esearchresult', {}).get('idlist', [])
                            all_ids.extend(ids)
                            
                            # Break if no more results
                            if len(ids) < batch_size:
                                break
                        else:
                            break
            except Exception as e:
                logger.warning(f"Error in PubMed search: {e}")
                break
        
        return all_ids
    
    async def search_pmc(self, query: str, max_results: int = 1000) -> List[str]:
        """Search PMC database"""
        
        search_url = f"{self.base_url}esearch.fcgi"
        params = {
            'db': 'pmc',
            'term': query,
            'retmax': max_results,
            'retmode': 'json',
            'email': self.email,
            'tool': self.tool
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(search_url, params=params, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get('esearchresult', {}).get('idlist', [])
        except Exception as e:
            logger.warning(f"Error in PMC search: {e}")
        
        return []
    
    async def fetch_article_details_batch(self, article_ids: List[str]) -> List[Dict[str, Any]]:
        """Fetch detailed information for batch of articles"""
        
        fetch_url = f"{self.base_url}efetch.fcgi"
        articles = []
        
        # Process in batches of 200 (NCBI limit)
        batch_size = 200
        for i in range(0, len(article_ids), batch_size):
            batch_ids = article_ids[i:i + batch_size]
            
            params = {
                'db': 'pubmed',
                'id': ','.join(batch_ids),
                'retmode': 'xml',
                'email': self.email,
                'tool': self.tool
            }
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(fetch_url, params=params, timeout=60) as response:
                        if response.status == 200:
                            xml_content = await response.text()
                            batch_articles = self._parse_pubmed_xml(xml_content)
                            articles.extend(batch_articles)
            except Exception as e:
                logger.warning(f"Error fetching article details: {e}")
        
        return articles
    
    def _parse_pubmed_xml(self, xml_content: str) -> List[Dict[str, Any]]:
        """Parse PubMed XML response"""
        
        articles = []
        
        try:
            root = ET.fromstring(xml_content)
            
            for article_elem in root.findall('.//PubmedArticle'):
                article = {
                    'pmid': '',
                    'title': '',
                    'abstract': '',
                    'authors': [],
                    'journal': '',
                    'publication_date': '',
                    'doi': '',
                    'keywords': [],
                    'mesh_terms': []
                }
                
                # Extract PMID
                pmid_elem = article_elem.find('.//PMID')
                if pmid_elem is not None:
                    article['pmid'] = pmid_elem.text
                
                # Extract title
                title_elem = article_elem.find('.//ArticleTitle')
                if title_elem is not None:
                    article['title'] = title_elem.text or ''
                
                # Extract abstract
                abstract_elem = article_elem.find('.//AbstractText')
                if abstract_elem is not None:
                    article['abstract'] = abstract_elem.text or ''
                
                # Extract authors
                author_elems = article_elem.findall('.//Author')
                for author_elem in author_elems:
                    last_name = author_elem.find('.//LastName')
                    fore_name = author_elem.find('.//ForeName')
                    if last_name is not None:
                        author_name = last_name.text or ''
                        if fore_name is not None:
                            author_name = f"{fore_name.text} {author_name}"
                        article['authors'].append(author_name)
                
                # Extract journal
                journal_elem = article_elem.find('.//Journal/Title')
                if journal_elem is not None:
                    article['journal'] = journal_elem.text or ''
                
                # Extract publication date
                pub_date = article_elem.find('.//PubDate')
                if pub_date is not None:
                    year = pub_date.find('.//Year')
                    month = pub_date.find('.//Month')
                    if year is not None:
                        pub_date_str = year.text or ''
                        if month is not None:
                            pub_date_str += f"-{month.text}"
                        article['publication_date'] = pub_date_str
                
                # Extract DOI
                doi_elems = article_elem.findall('.//ArticleId[@IdType="doi"]')
                if doi_elems:
                    article['doi'] = doi_elems[0].text or ''
                
                # Extract MeSH terms
                mesh_elems = article_elem.findall('.//MeshHeading/DescriptorName')
                article['mesh_terms'] = [elem.text for elem in mesh_elems if elem.text]
                
                articles.append(article)
        
        except Exception as e:
            logger.warning(f"Error parsing PubMed XML: {e}")
        
        return articles
    
    async def fetch_pmc_full_text(self, article_id: str) -> Dict[str, Any]:
        """Fetch full text from PMC"""
        
        fetch_url = f"{self.base_url}efetch.fcgi"
        params = {
            'db': 'pmc',
            'id': article_id,
            'retmode': 'xml',
            'email': self.email,
            'tool': self.tool
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(fetch_url, params=params, timeout=60) as response:
                    if response.status == 200:
                        xml_content = await response.text()
                        return self._parse_pmc_xml(xml_content, article_id)
        except Exception as e:
            logger.warning(f"Error fetching PMC full text for {article_id}: {e}")
        
        return None
    
    def _parse_pmc_xml(self, xml_content: str, article_id: str) -> Dict[str, Any]:
        """Parse PMC XML full text"""
        
        try:
            root = ET.fromstring(xml_content)
            
            article = {
                'pmc_id': article_id,
                'title': '',
                'abstract': '',
                'full_text': '',
                'sections': [],
                'references': [],
                'figures': [],
                'tables': []
            }
            
            # Extract title
            title_elem = root.find('.//article-title')
            if title_elem is not None:
                article['title'] = ''.join(title_elem.itertext()).strip()
            
            # Extract abstract
            abstract_elem = root.find('.//abstract')
            if abstract_elem is not None:
                article['abstract'] = ''.join(abstract_elem.itertext()).strip()
            
            # Extract full text sections
            body_elem = root.find('.//body')
            if body_elem is not None:
                sections = []
                for sec_elem in body_elem.findall('.//sec'):
                    title_elem = sec_elem.find('.//title')
                    section_title = ''.join(title_elem.itertext()).strip() if title_elem is not None else 'Untitled'
                    section_text = ''.join(sec_elem.itertext()).strip()
                    
                    sections.append({
                        'title': section_title,
                        'content': section_text
                    })
                
                article['sections'] = sections
                article['full_text'] = '\n\n'.join([sec['content'] for sec in sections])
            
            # Extract references
            ref_elems = root.findall('.//ref')
            references = []
            for ref_elem in ref_elems:
                ref_text = ''.join(ref_elem.itertext()).strip()
                if ref_text:
                    references.append(ref_text)
            article['references'] = references[:50]  # Limit references
            
            return article
        
        except Exception as e:
            logger.warning(f"Error parsing PMC XML: {e}")
            return None


class NCBIRateManager:
    """Manage NCBI API rate limits"""
    
    def __init__(self):
        self.last_request = 0
        self.requests_per_second = 3  # NCBI limit without API key
        self.requests_made = 0
        
    async def wait_for_rate_limit(self):
        """Wait if necessary to respect rate limits"""
        
        current_time = time.time()
        time_since_last = current_time - self.last_request
        
        min_interval = 1.0 / self.requests_per_second
        
        if time_since_last < min_interval:
            wait_time = min_interval - time_since_last
            await asyncio.sleep(wait_time)
        
        self.last_request = time.time()
        self.requests_made += 1


class MedicalQueryGenerator:
    """Generate comprehensive medical queries for NCBI searches"""
    
    async def generate_comprehensive_medical_queries(self) -> List[str]:
        """Generate comprehensive medical queries for maximum coverage"""
        
        # Disease categories
        disease_queries = [
            'cardiovascular disease[MeSH] AND treatment',
            'cancer[MeSH] AND therapy',
            'diabetes mellitus[MeSH] AND management',
            'neurological disorders[MeSH] AND diagnosis',
            'infectious diseases[MeSH] AND prevention',
            'mental health[MeSH] AND intervention',
            'respiratory disease[MeSH] AND pathophysiology',
            'autoimmune diseases[MeSH] AND immunotherapy',
            'genetic disorders[MeSH] AND genetic testing',
            'pediatric diseases[MeSH] AND pediatrics'
        ]
        
        # Treatment modalities
        treatment_queries = [
            'pharmacotherapy[MeSH] AND drug efficacy',
            'surgical procedures[MeSH] AND outcomes',
            'radiotherapy[MeSH] AND cancer treatment',
            'immunotherapy[MeSH] AND clinical trials',
            'gene therapy[MeSH] AND genetic medicine',
            'stem cell therapy[MeSH] AND regenerative medicine',
            'precision medicine[MeSH] AND personalized treatment',
            'combination therapy[MeSH] AND synergistic effects'
        ]
        
        # Research methodologies
        research_queries = [
            'randomized controlled trial[Publication Type]',
            'systematic review[Publication Type]',
            'meta-analysis[Publication Type]',
            'clinical trial[Publication Type] AND phase III',
            'cohort studies[MeSH] AND epidemiology',
            'case-control studies[MeSH] AND risk factors',
            'cross-sectional studies[MeSH] AND prevalence'
        ]
        
        # Recent high-impact research
        current_year = datetime.now().year
        recent_queries = [
            f'"high impact"[All Fields] AND {current_year}[Publication Date]',
            f'"breakthrough"[All Fields] AND {current_year-1}[Publication Date]',
            f'"novel therapy"[All Fields] AND {current_year}[Publication Date]',
            f'"clinical significance"[All Fields] AND {current_year-1}[Publication Date]'
        ]
        
        # Combine all queries
        all_queries = disease_queries + treatment_queries + research_queries + recent_queries
        
        return all_queries
    
    async def generate_genetic_queries(self) -> List[str]:
        """Generate genetic variation queries for ClinVar"""
        
        return [
            'pathogenic variant',
            'likely pathogenic',
            'BRCA1 mutation',
            'BRCA2 mutation',
            'Lynch syndrome',
            'cystic fibrosis',
            'sickle cell disease',
            'Huntington disease',
            'muscular dystrophy',
            'hereditary cancer'
        ]


class CitationParser:
    """Parse and analyze medical citations"""
    
    def parse_citation(self, citation_text: str) -> Dict[str, str]:
        """Parse medical citation text"""
        
        # Simplified citation parsing
        citation = {
            'authors': '',
            'title': '',
            'journal': '',
            'year': '',
            'volume': '',
            'pages': ''
        }
        
        # Extract year
        year_match = re.search(r'(\d{4})', citation_text)
        if year_match:
            citation['year'] = year_match.group(1)
        
        # Extract journal (simplified)
        # This would need more sophisticated parsing in production
        
        return citation

# Export main class
__all__ = ['NCBIAdvancedScraper']