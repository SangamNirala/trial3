"""
Super-Parallel Processing Engine
Advanced parallel processing system for massive medical data extraction
"""

import asyncio
import aiohttp
import logging
from typing import List, Dict, Optional, Any, Union, Callable
from datetime import datetime, timedelta
import json
from collections import defaultdict, deque
import statistics
import random
import time
import resource
import psutil
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
import threading
from queue import PriorityQueue, Empty
import heapq

from ai_scraper_core import (
    ScrapingTask, ScrapingResult, ScrapingPriority, ContentType, ScrapingTier,
    ContentDiscoveryAI, ScraperOptimizationAI, AntiDetectionAI, ContentQualityAI,
    IntelligentTaskScheduler, AdaptiveRateLimiter, IntelligentProxyRotator, AdvancedDeduplicator
)

logger = logging.getLogger(__name__)

@dataclass
class ProcessingMetrics:
    """Real-time processing metrics"""
    tasks_queued: int = 0
    tasks_processing: int = 0
    tasks_completed: int = 0
    tasks_failed: int = 0
    avg_processing_time: float = 0.0
    peak_concurrent_tasks: int = 0
    total_content_extracted_mb: float = 0.0
    current_processing_rate: float = 0.0  # tasks per second
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    network_bandwidth_mbps: float = 0.0
    
    @property
    def success_rate(self) -> float:
        total = self.tasks_completed + self.tasks_failed
        return self.tasks_completed / total if total > 0 else 0.0

class DynamicLoadBalancer:
    """Dynamic load balancer for optimal resource distribution"""
    
    def __init__(self):
        self.tier_performance = defaultdict(lambda: {
            'avg_response_time': 5.0,
            'success_rate': 0.8,
            'current_load': 0,
            'optimal_concurrency': 50,
            'last_adjustment': time.time()
        })
        
    async def calculate_optimal_concurrency(self, tier: ScrapingTier, 
                                          current_metrics: ProcessingMetrics) -> int:
        """Calculate optimal concurrency for a tier based on performance"""
        
        tier_stats = self.tier_performance[tier.value]
        
        # Base concurrency levels
        base_concurrency = {
            ScrapingTier.TIER_1_GOVERNMENT.value: 100,
            ScrapingTier.TIER_2_INTERNATIONAL.value: 80,
            ScrapingTier.TIER_3_ACADEMIC.value: 120,
        }
        
        base_level = base_concurrency.get(tier.value, 50)
        
        # Adjust based on success rate
        success_multiplier = min(1.5, max(0.3, tier_stats['success_rate']))
        
        # Adjust based on response time
        response_time = tier_stats['avg_response_time']
        if response_time < 2.0:
            time_multiplier = 1.2
        elif response_time < 5.0:
            time_multiplier = 1.0
        elif response_time < 10.0:
            time_multiplier = 0.8
        else:
            time_multiplier = 0.5
            
        # Adjust based on system resources
        system_multiplier = await self._calculate_system_resource_multiplier(current_metrics)
        
        # Calculate optimal concurrency
        optimal = int(base_level * success_multiplier * time_multiplier * system_multiplier)
        
        # Apply bounds
        tier_stats['optimal_concurrency'] = max(10, min(optimal, 500))
        tier_stats['last_adjustment'] = time.time()
        
        return tier_stats['optimal_concurrency']
    
    async def _calculate_system_resource_multiplier(self, metrics: ProcessingMetrics) -> float:
        """Calculate system resource multiplier"""
        
        # CPU usage factor
        cpu_factor = 1.0
        if metrics.cpu_usage_percent > 90:
            cpu_factor = 0.5
        elif metrics.cpu_usage_percent > 80:
            cpu_factor = 0.7
        elif metrics.cpu_usage_percent > 70:
            cpu_factor = 0.9
        
        # Memory usage factor
        memory_factor = 1.0
        if metrics.memory_usage_mb > 8000:  # > 8GB
            memory_factor = 0.6
        elif metrics.memory_usage_mb > 6000:  # > 6GB
            memory_factor = 0.8
        elif metrics.memory_usage_mb > 4000:  # > 4GB
            memory_factor = 0.9
            
        return min(cpu_factor, memory_factor)
    
    def update_tier_performance(self, tier: ScrapingTier, response_time: float, 
                               success: bool, current_load: int):
        """Update tier performance statistics"""
        
        tier_stats = self.tier_performance[tier.value]
        
        # Update average response time (exponential moving average)
        alpha = 0.1
        tier_stats['avg_response_time'] = (
            alpha * response_time + (1 - alpha) * tier_stats['avg_response_time']
        )
        
        # Update success rate (exponential moving average)
        tier_stats['success_rate'] = (
            alpha * (1.0 if success else 0.0) + (1 - alpha) * tier_stats['success_rate']
        )
        
        # Update current load
        tier_stats['current_load'] = current_load

class PerformanceMonitoringAI:
    """AI system for monitoring and optimizing performance in real-time"""
    
    def __init__(self):
        self.metrics_history = deque(maxlen=1000)
        self.performance_alerts = []
        self.optimization_suggestions = []
        
    async def monitor_real_time_performance(self) -> ProcessingMetrics:
        """Monitor real-time system performance"""
        
        # Get system metrics
        process = psutil.Process()
        memory_info = process.memory_info()
        cpu_percent = process.cpu_percent()
        
        # Calculate network bandwidth (simplified)
        network_bandwidth = await self._estimate_network_bandwidth()
        
        metrics = ProcessingMetrics(
            memory_usage_mb=memory_info.rss / (1024 * 1024),
            cpu_usage_percent=cpu_percent,
            network_bandwidth_mbps=network_bandwidth
        )
        
        # Add to history
        self.metrics_history.append(metrics)
        
        # Generate alerts and suggestions
        await self._analyze_performance_trends(metrics)
        
        return metrics
    
    async def _estimate_network_bandwidth(self) -> float:
        """Estimate current network bandwidth usage"""
        
        try:
            # Get network I/O statistics
            net_io = psutil.net_io_counters()
            
            # This is a simplified estimation
            # In production, you'd want more sophisticated bandwidth monitoring
            bytes_per_second = net_io.bytes_sent + net_io.bytes_recv
            mbps = (bytes_per_second * 8) / (1024 * 1024)  # Convert to Mbps
            
            return mbps
        except:
            return 0.0
    
    async def _analyze_performance_trends(self, current_metrics: ProcessingMetrics):
        """Analyze performance trends and generate alerts"""
        
        if len(self.metrics_history) < 10:
            return
            
        # Check for performance degradation
        recent_metrics = list(self.metrics_history)[-10:]
        
        # CPU usage trend
        cpu_trend = [m.cpu_usage_percent for m in recent_metrics]
        if statistics.mean(cpu_trend) > 85:
            self.performance_alerts.append({
                'type': 'high_cpu',
                'message': 'High CPU usage detected - consider reducing concurrency',
                'timestamp': datetime.utcnow()
            })
        
        # Memory usage trend
        memory_trend = [m.memory_usage_mb for m in recent_metrics]
        if statistics.mean(memory_trend) > 7000:  # > 7GB
            self.performance_alerts.append({
                'type': 'high_memory',
                'message': 'High memory usage detected - consider batch processing',
                'timestamp': datetime.utcnow()
            })
    
    def get_optimization_suggestions(self) -> List[Dict[str, Any]]:
        """Get current optimization suggestions"""
        return self.optimization_suggestions.copy()
    
    def get_performance_alerts(self) -> List[Dict[str, Any]]:
        """Get current performance alerts"""
        return self.performance_alerts.copy()

class BandwidthOptimizationAI:
    """AI system for optimizing network bandwidth usage"""
    
    def __init__(self):
        self.bandwidth_history = deque(maxlen=100)
        self.optimal_batch_sizes = {}
        self.compression_settings = {}
        
    async def optimize_request_batching(self, tier: ScrapingTier, 
                                       current_bandwidth: float) -> Dict[str, Any]:
        """Optimize request batching based on available bandwidth"""
        
        # Base batch sizes
        base_batch_sizes = {
            ScrapingTier.TIER_1_GOVERNMENT.value: 100,
            ScrapingTier.TIER_2_INTERNATIONAL.value: 80,
            ScrapingTier.TIER_3_ACADEMIC.value: 120,
        }
        
        base_batch = base_batch_sizes.get(tier.value, 50)
        
        # Adjust based on available bandwidth
        if current_bandwidth > 100:  # High bandwidth
            multiplier = 1.5
        elif current_bandwidth > 50:  # Medium bandwidth
            multiplier = 1.0
        elif current_bandwidth > 20:  # Low bandwidth
            multiplier = 0.7
        else:  # Very low bandwidth
            multiplier = 0.4
            
        optimal_batch_size = int(base_batch * multiplier)
        
        # Calculate optimal delay between batches
        if current_bandwidth > 100:
            batch_delay = 0.5
        elif current_bandwidth > 50:
            batch_delay = 1.0
        else:
            batch_delay = 2.0
            
        return {
            'optimal_batch_size': optimal_batch_size,
            'batch_delay': batch_delay,
            'concurrent_batches': min(5, max(1, int(current_bandwidth / 20)))
        }
    
    async def suggest_compression_settings(self, tier: ScrapingTier) -> Dict[str, Any]:
        """Suggest optimal compression settings"""
        
        return {
            'enable_gzip': True,
            'enable_deflate': True,
            'enable_brotli': True,
            'content_encoding_preference': ['br', 'gzip', 'deflate']
        }

class IntelligentRetrySystem:
    """Intelligent retry system with adaptive backoff"""
    
    def __init__(self):
        self.retry_statistics = defaultdict(lambda: {
            'total_attempts': 0,
            'success_count': 0,
            'avg_retry_count': 0.0,
            'optimal_delay': 1.0
        })
        
    async def should_retry(self, task: ScrapingTask, error: Exception, 
                          attempt_number: int) -> bool:
        """Determine if task should be retried"""
        
        # Max retry attempts based on tier
        max_retries = {
            ScrapingTier.TIER_1_GOVERNMENT: 5,  # Government sites - more retries
            ScrapingTier.TIER_2_INTERNATIONAL: 4,
            ScrapingTier.TIER_3_ACADEMIC: 3,
        }
        
        tier_max_retries = max_retries.get(task.tier, 3)
        
        if attempt_number >= tier_max_retries:
            return False
            
        # Check error type for retry decision
        retry_on_errors = [
            'timeout', 'connection', 'network', '502', '503', '504', '429'
        ]
        
        error_str = str(error).lower()
        should_retry = any(retry_error in error_str for retry_error in retry_on_errors)
        
        return should_retry
    
    async def calculate_retry_delay(self, task: ScrapingTask, attempt_number: int, 
                                   error: Exception) -> float:
        """Calculate intelligent retry delay"""
        
        tier_stats = self.retry_statistics[task.tier.value]
        
        # Base delays by tier
        base_delays = {
            ScrapingTier.TIER_1_GOVERNMENT: 2.0,
            ScrapingTier.TIER_2_INTERNATIONAL: 3.0,
            ScrapingTier.TIER_3_ACADEMIC: 1.5,
        }
        
        base_delay = base_delays.get(task.tier, 2.0)
        
        # Exponential backoff with jitter
        exponential_delay = base_delay * (2 ** attempt_number)
        
        # Add jitter (±20%)
        jitter = random.uniform(-0.2, 0.2) * exponential_delay
        
        # Apply adaptive adjustment
        adaptive_delay = exponential_delay + jitter
        
        # Rate limiting detection
        error_str = str(error).lower()
        if '429' in error_str or 'rate limit' in error_str:
            adaptive_delay *= 5  # Significantly increase delay for rate limiting
            
        return min(adaptive_delay, 60.0)  # Cap at 60 seconds
    
    def record_retry_result(self, task: ScrapingTask, attempt_number: int, 
                          success: bool):
        """Record retry attempt result"""
        
        tier_stats = self.retry_statistics[task.tier.value]
        tier_stats['total_attempts'] += 1
        
        if success:
            tier_stats['success_count'] += 1
            
        # Update average retry count
        alpha = 0.1
        tier_stats['avg_retry_count'] = (
            alpha * attempt_number + (1 - alpha) * tier_stats['avg_retry_count']
        )

class SuperParallelScrapingEngine:
    """Super-parallel processing engine with massive concurrency"""
    
    def __init__(self):
        # Core configuration
        self.max_total_workers = 1000
        self.max_concurrent_sessions = 200
        self.worker_pool = None
        
        # AI and optimization systems
        self.load_balancer = DynamicLoadBalancer()
        self.performance_monitor = PerformanceMonitoringAI()
        self.bandwidth_optimizer = BandwidthOptimizationAI()
        self.retry_system = IntelligentRetrySystem()
        
        # Processing queues and tracking
        self.processing_queue = PriorityQueue()
        self.active_tasks = {}
        self.completed_tasks = []
        self.failed_tasks = []
        
        # Real-time metrics
        self.metrics = ProcessingMetrics()
        self.metrics_lock = threading.Lock()
        
        # Performance tracking
        self.start_time = None
        self.processing_rates = deque(maxlen=60)  # Last 60 seconds
        
    async def launch_super_parallel_extraction(self, tier_scrapers: Dict[ScrapingTier, Any], 
                                             target_documents: int = 100000) -> Dict[str, Any]:
        """Launch super-parallel extraction across all tiers"""
        
        self.start_time = time.time()
        logger.info(f"🚀 Launching Super-Parallel Extraction - Target: {target_documents:,} documents")
        
        # Initialize worker pool
        self.worker_pool = ThreadPoolExecutor(max_workers=self.max_total_workers)
        
        # Start performance monitoring
        monitoring_task = asyncio.create_task(self._continuous_performance_monitoring())
        
        # Launch tier processors
        tier_tasks = []
        for tier, scraper in tier_scrapers.items():
            tier_task = asyncio.create_task(
                self._process_tier_super_parallel(tier, scraper, target_documents // len(tier_scrapers))
            )
            tier_tasks.append(tier_task)
        
        # Wait for all tier processing to complete
        try:
            tier_results = await asyncio.gather(*tier_tasks, return_exceptions=True)
        finally:
            # Stop monitoring
            monitoring_task.cancel()
            try:
                await monitoring_task
            except asyncio.CancelledError:
                pass
        
        # Process final results
        final_results = await self._compile_super_parallel_results(tier_results)
        
        # Cleanup
        if self.worker_pool:
            self.worker_pool.shutdown(wait=True)
        
        return final_results
    
    async def _process_tier_super_parallel(self, tier: ScrapingTier, scraper: Any, 
                                         target_documents: int) -> Dict[str, Any]:
        """Process a tier with super-parallel optimization"""
        
        logger.info(f"🎯 Processing {tier.value} with super-parallel engine")
        
        # Get optimal concurrency for this tier
        optimal_concurrency = await self.load_balancer.calculate_optimal_concurrency(tier, self.metrics)
        logger.info(f"📊 {tier.value} optimal concurrency: {optimal_concurrency}")
        
        # Create semaphore for this tier
        tier_semaphore = asyncio.Semaphore(optimal_concurrency)
        
        # Generate URLs for processing
        target_urls = await self._generate_tier_urls(tier, target_documents)
        
        # Process URLs in optimized batches
        tier_results = await self._process_urls_in_batches(
            tier, target_urls, tier_semaphore, scraper
        )
        
        return {
            'tier': tier.value,
            'processed_count': len(tier_results),
            'success_count': sum(1 for r in tier_results if r.success),
            'results': tier_results
        }
    
    async def _generate_tier_urls(self, tier: ScrapingTier, target_count: int) -> List[str]:
        """Generate URLs for a tier (simplified for Phase 1)"""
        
        # This is a simplified URL generation
        # In full implementation, this would use AI discovery
        
        base_urls = {
            ScrapingTier.TIER_1_GOVERNMENT: [
                'https://medlineplus.gov/encyclopedia/{}.html',
                'https://www.cdc.gov/condition/{}.html',
                'https://www.fda.gov/drug/{}.html'
            ],
            ScrapingTier.TIER_2_INTERNATIONAL: [
                'https://www.who.int/health-topics/{}.html',
                'https://www.nhs.uk/conditions/{}.html'
            ],
            ScrapingTier.TIER_3_ACADEMIC: [
                'https://www.mayoclinic.org/diseases-conditions/{}.html',
                'https://my.clevelandclinic.org/health/diseases/{}.html'
            ]
        }
        
        patterns = base_urls.get(tier, [])
        generated_urls = []
        
        # Generate URLs with common medical terms
        medical_terms = [
            'diabetes', 'hypertension', 'cancer', 'heart-disease', 'stroke',
            'pneumonia', 'arthritis', 'depression', 'anxiety', 'asthma',
            'obesity', 'osteoporosis', 'alzheimers', 'parkinsons', 'epilepsy'
        ]
        
        for pattern in patterns:
            for term in medical_terms[:min(len(medical_terms), target_count // len(patterns))]:
                if '{}' in pattern:
                    url = pattern.format(term)
                else:
                    url = pattern + term
                generated_urls.append(url)
        
        return generated_urls[:target_count]
    
    async def _process_urls_in_batches(self, tier: ScrapingTier, urls: List[str], 
                                     semaphore: asyncio.Semaphore, scraper: Any) -> List[ScrapingResult]:
        """Process URLs in optimized batches"""
        
        # Get bandwidth optimization settings
        current_bandwidth = await self._estimate_current_bandwidth()
        batch_config = await self.bandwidth_optimizer.optimize_request_batching(tier, current_bandwidth)
        
        batch_size = batch_config['optimal_batch_size']
        batch_delay = batch_config['batch_delay']
        
        logger.info(f"📦 {tier.value} batch config: size={batch_size}, delay={batch_delay}s")
        
        all_results = []
        
        # Process URLs in batches
        for i in range(0, len(urls), batch_size):
            batch_urls = urls[i:i + batch_size]
            
            # Create session for this batch
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(limit=200, limit_per_host=50)
            ) as session:
                
                # Process batch concurrently
                batch_tasks = [
                    self._process_single_url_with_retry(tier, url, session, semaphore, scraper)
                    for url in batch_urls
                ]
                
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                # Filter valid results
                valid_results = [
                    r for r in batch_results 
                    if isinstance(r, ScrapingResult)
                ]
                
                all_results.extend(valid_results)
                
                # Update metrics
                await self._update_batch_metrics(tier, len(batch_urls), len(valid_results))
                
                logger.info(f"📊 {tier.value} batch {i//batch_size + 1}: {len(valid_results)}/{len(batch_urls)} successful")
                
                # Adaptive delay between batches
                if i + batch_size < len(urls):
                    await asyncio.sleep(batch_delay)
        
        return all_results
    
    async def _process_single_url_with_retry(self, tier: ScrapingTier, url: str, 
                                           session: aiohttp.ClientSession, 
                                           semaphore: asyncio.Semaphore, 
                                           scraper: Any) -> ScrapingResult:
        """Process single URL with intelligent retry"""
        
        task = ScrapingTask(
            url=url,
            tier=tier,
            source_name=f"{tier.value}_source"
        )
        
        async with semaphore:
            attempt = 0
            last_error = None
            
            while attempt < 5:  # Max 5 attempts
                try:
                    # Use the scraper's extraction method
                    result = await scraper.extract_content_from_url(url, session, attempt)
                    
                    # Record success
                    self.retry_system.record_retry_result(task, attempt, result.success)
                    
                    if result.success:
                        # Update load balancer performance
                        self.load_balancer.update_tier_performance(
                            tier, result.processing_time, True, len(self.active_tasks)
                        )
                        return result
                    else:
                        last_error = Exception(result.error_details or "Unknown error")
                        
                except Exception as e:
                    last_error = e
                
                # Check if should retry
                if not await self.retry_system.should_retry(task, last_error, attempt):
                    break
                
                # Calculate retry delay
                retry_delay = await self.retry_system.calculate_retry_delay(task, attempt, last_error)
                await asyncio.sleep(retry_delay)
                
                attempt += 1
            
            # All retries failed
            self.retry_system.record_retry_result(task, attempt, False)
            self.load_balancer.update_tier_performance(tier, 0, False, len(self.active_tasks))
            
            return ScrapingResult(
                task_id=task.id,
                url=url,
                success=False,
                error_details=str(last_error),
                timestamp=datetime.utcnow()
            )
    
    async def _continuous_performance_monitoring(self):
        """Continuously monitor system performance"""
        
        while True:
            try:
                # Update metrics
                current_metrics = await self.performance_monitor.monitor_real_time_performance()
                
                with self.metrics_lock:
                    # Update processing metrics
                    self.metrics.memory_usage_mb = current_metrics.memory_usage_mb
                    self.metrics.cpu_usage_percent = current_metrics.cpu_usage_percent
                    self.metrics.network_bandwidth_mbps = current_metrics.network_bandwidth_mbps
                    
                    # Calculate processing rate
                    if self.start_time:
                        elapsed = time.time() - self.start_time
                        if elapsed > 0:
                            self.metrics.current_processing_rate = self.metrics.tasks_completed / elapsed
                
                # Check for alerts
                alerts = self.performance_monitor.get_performance_alerts()
                if alerts:
                    for alert in alerts[-5:]:  # Show last 5 alerts
                        logger.warning(f"⚠️ Performance Alert: {alert['message']}")
                
                await asyncio.sleep(5)  # Monitor every 5 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Performance monitoring error: {e}")
                await asyncio.sleep(5)
    
    async def _estimate_current_bandwidth(self) -> float:
        """Estimate current network bandwidth"""
        
        # Simplified bandwidth estimation
        # In production, use more sophisticated network monitoring
        return random.uniform(50, 150)  # Mock bandwidth 50-150 Mbps
    
    async def _update_batch_metrics(self, tier: ScrapingTier, total_attempted: int, 
                                  successful: int):
        """Update batch processing metrics"""
        
        with self.metrics_lock:
            self.metrics.tasks_completed += successful
            self.metrics.tasks_failed += (total_attempted - successful)
            
            # Update processing rate
            if self.start_time:
                elapsed = time.time() - self.start_time
                if elapsed > 0:
                    self.metrics.current_processing_rate = self.metrics.tasks_completed / elapsed
    
    async def _compile_super_parallel_results(self, tier_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Compile final results from super-parallel processing"""
        
        total_execution_time = time.time() - self.start_time if self.start_time else 0
        
        # Aggregate results
        total_processed = 0
        total_success = 0
        all_results = []
        tier_summaries = {}
        
        for tier_result in tier_results:
            if isinstance(tier_result, dict) and 'tier' in tier_result:
                tier_name = tier_result['tier']
                tier_summaries[tier_name] = tier_result
                
                total_processed += tier_result.get('processed_count', 0)
                total_success += tier_result.get('success_count', 0)
                
                if 'results' in tier_result:
                    all_results.extend(tier_result['results'])
        
        # Calculate performance metrics
        processing_rate = total_processed / total_execution_time if total_execution_time > 0 else 0
        success_rate = total_success / total_processed if total_processed > 0 else 0
        
        # Get final system metrics
        final_metrics = await self.performance_monitor.monitor_real_time_performance()
        
        return {
            'super_parallel_summary': {
                'total_processed': total_processed,
                'total_success': total_success,
                'success_rate': success_rate,
                'execution_time': total_execution_time,
                'processing_rate': processing_rate,
                'peak_concurrent_workers': self.max_total_workers,
                'max_concurrent_sessions': self.max_concurrent_sessions
            },
            'tier_results': tier_summaries,
            'system_performance': {
                'peak_memory_usage_mb': final_metrics.memory_usage_mb,
                'avg_cpu_usage': final_metrics.cpu_usage_percent,
                'network_bandwidth_used_mbps': final_metrics.network_bandwidth_mbps,
                'optimization_suggestions': self.performance_monitor.get_optimization_suggestions(),
                'performance_alerts': self.performance_monitor.get_performance_alerts()
            },
            'extracted_data': all_results
        }

# Export main classes
__all__ = [
    'SuperParallelScrapingEngine', 'DynamicLoadBalancer', 'PerformanceMonitoringAI',
    'BandwidthOptimizationAI', 'IntelligentRetrySystem', 'ProcessingMetrics'
]