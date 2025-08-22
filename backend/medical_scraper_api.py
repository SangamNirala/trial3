"""
Medical Scraper API Integration
Integrates the Phase 1 World-Class Medical Scraper with the existing FastAPI system
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import asyncio
import logging
import json
from datetime import datetime

from phase1_implementation import Phase1MedicalScraperSystem

logger = logging.getLogger(__name__)

# Create API router
router = APIRouter(prefix="/api/medical-scraper", tags=["Medical Scraper"])

# Global system instance
phase1_system = None
current_operation = None

class ScrapingRequest(BaseModel):
    """Request model for scraping operations"""
    target_documents: Optional[int] = 1000
    max_concurrent_workers: Optional[int] = 100
    tiers: Optional[List[str]] = None
    quality_threshold: Optional[float] = 0.6

class ScrapingStatus(BaseModel):
    """Status model for scraping operations"""
    operation_id: str
    status: str  # "idle", "running", "completed", "failed"
    progress: Dict[str, Any]
    results_summary: Optional[Dict[str, Any]] = None

@router.post("/start-extraction", response_model=Dict[str, Any])
async def start_medical_extraction(request: ScrapingRequest, background_tasks: BackgroundTasks):
    """Start medical data extraction operation"""
    
    global phase1_system, current_operation
    
    if current_operation and current_operation.get('status') == 'running':
        raise HTTPException(
            status_code=409, 
            detail="Scraping operation already in progress"
        )

@router.post("/start-comprehensive-scraping", response_model=Dict[str, Any])
async def start_comprehensive_scraping(request: ScrapingRequest, background_tasks: BackgroundTasks):
    """Start Phase 2 comprehensive government sources scraping"""
    
    global phase1_system, current_operation
    
    # Validate request parameters
    if request.target_documents and (request.target_documents < 1 or request.target_documents > 1000000):
        raise HTTPException(
            status_code=422,
            detail="target_documents must be between 1 and 1,000,000"
        )
    
    if request.quality_threshold and (request.quality_threshold < 0.0 or request.quality_threshold > 1.0):
        raise HTTPException(
            status_code=422,
            detail="quality_threshold must be between 0.0 and 1.0"
        )
    
    if current_operation and current_operation.get('status') == 'running':
        raise HTTPException(
            status_code=409, 
            detail="Scraping operation already in progress"
        )
    
    try:
        # Initialize Phase 1 system if not already done
        if not phase1_system:
            phase1_system = Phase1MedicalScraperSystem()
        
        # Configure system based on request
        phase1_system.phase1_config.update({
            'target_documents': request.target_documents,
            'max_concurrent_workers': request.max_concurrent_workers,
            'quality_threshold': request.quality_threshold
        })
        
        # Create operation tracking
        operation_id = f"scraping_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        current_operation = {
            'operation_id': operation_id,
            'status': 'running',
            'started_at': datetime.utcnow(),
            'config': request.dict(),
            'progress': {
                'total_processed': 0,
                'successful': 0,
                'failed': 0,
                'current_tier': 'initializing'
            }
        }
        
        # Start extraction in background
        background_tasks.add_task(run_extraction_background, operation_id)
        
        return {
            'operation_id': operation_id,
            'status': 'started',
            'message': 'Medical data extraction started successfully',
            'config': request.dict(),
            'estimated_duration': 'Variable based on target size'
        }
        
    except Exception as e:
        logger.error(f"Failed to start extraction: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/status", response_model=ScrapingStatus)
async def get_scraping_status():
    """Get current scraping operation status"""
    
    global current_operation
    
    if not current_operation:
        return ScrapingStatus(
            operation_id="none",
            status="idle",
            progress={'message': 'No active operation'}
        )
    
    return ScrapingStatus(
        operation_id=current_operation['operation_id'],
        status=current_operation['status'],
        progress=current_operation['progress'],
        results_summary=current_operation.get('results_summary')
    )

@router.get("/capabilities", response_model=Dict[str, Any])
async def get_scraper_capabilities():
    """Get scraper system capabilities and configuration"""
    
    return {
        'system_name': 'World-Class Medical Scraper - Phase 1',
        'version': '1.0.0',
        'capabilities': {
            'max_concurrent_workers': 1000,
            'supported_tiers': [
                'government_sources',
                'international_organizations', 
                'academic_medical_centers'
            ],
            'ai_systems': [
                'Content Discovery AI',
                'Scraper Optimization AI',
                'Anti-Detection AI',
                'Content Quality AI',
                'Intelligent Task Scheduler',
                'Adaptive Rate Limiter',
                'Dynamic Load Balancer',
                'Performance Monitoring AI',
                'Bandwidth Optimization AI',
                'Intelligent Retry System'
            ],
            'target_sources': {
                'government': ['NIH', 'CDC', 'FDA', 'MedlinePlus'],
                'international': ['WHO', 'NHS', 'EMA'],
                'academic': ['Mayo Clinic', 'Cleveland Clinic', 'Johns Hopkins', 'Harvard Health']
            }
        },
        'performance_specs': {
            'target_processing_rate': '100+ documents/second',
            'target_success_rate': '95%+',
            'quality_assessment': 'Real-time AI scoring',
            'scalability': 'Up to 500,000+ documents'
        }
    }

@router.get("/results/{operation_id}", response_model=Dict[str, Any])
async def get_extraction_results(operation_id: str):
    """Get results from a completed extraction operation"""
    
    global current_operation
    
    if not current_operation or current_operation['operation_id'] != operation_id:
        raise HTTPException(status_code=404, detail="Operation not found")
    
    if current_operation['status'] != 'completed':
        raise HTTPException(
            status_code=400, 
            detail=f"Operation status: {current_operation['status']}"
        )
    
    return current_operation.get('results_summary', {})

@router.post("/stop-extraction", response_model=Dict[str, Any])
async def stop_extraction():
    """Stop current extraction operation"""
    
    global current_operation
    
    if not current_operation or current_operation['status'] != 'running':
        raise HTTPException(status_code=400, detail="No active operation to stop")
    
    current_operation['status'] = 'stopped'
    current_operation['stopped_at'] = datetime.utcnow()
    
    return {
        'message': 'Extraction operation stopped',
        'operation_id': current_operation['operation_id']
    }

@router.get("/health", response_model=Dict[str, Any])
async def health_check():
    """Health check endpoint for the medical scraper system"""
    
    try:
        # Test system components
        from ai_scraper_core import ContentDiscoveryAI
        from master_scraper_controller import WorldClassMedicalScraper
        from super_parallel_engine import SuperParallelScrapingEngine
        
        # Quick component test
        content_ai = ContentDiscoveryAI()
        master_scraper = WorldClassMedicalScraper()
        parallel_engine = SuperParallelScrapingEngine()
        
        return {
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat(),
            'components': {
                'ai_scraper_core': 'operational',
                'master_scraper_controller': 'operational',
                'super_parallel_engine': 'operational',
                'phase1_implementation': 'operational'
            },
            'system_ready': True
        }
        
    except Exception as e:
        return {
            'status': 'unhealthy',
            'timestamp': datetime.utcnow().isoformat(),
            'error': str(e),
            'system_ready': False
        }

async def run_extraction_background(operation_id: str):
    """Run extraction operation in background"""
    
    global phase1_system, current_operation
    
    try:
        logger.info(f"Starting background extraction: {operation_id}")
        
        # Update progress
        current_operation['progress']['current_tier'] = 'executing'
        
        # Execute Phase 1 extraction
        results = await phase1_system.execute_phase1_complete()
        
        # Update operation with results
        current_operation['status'] = 'completed'
        current_operation['completed_at'] = datetime.utcnow()
        current_operation['results_summary'] = results
        
        # Update final progress
        scraping_performance = results.get('scraping_performance', {})
        current_operation['progress'].update({
            'total_processed': scraping_performance.get('total_processed', 0),
            'successful': scraping_performance.get('total_success', 0),
            'failed': scraping_performance.get('total_processed', 0) - scraping_performance.get('total_success', 0),
            'current_tier': 'completed'
        })
        
        logger.info(f"Background extraction completed: {operation_id}")
        
    except Exception as e:
        logger.error(f"Background extraction failed: {e}")
        
        current_operation['status'] = 'failed'
        current_operation['error'] = str(e)
        current_operation['failed_at'] = datetime.utcnow()

async def run_phase2_comprehensive_scraping(operation_id: str):
    """Run Phase 2 comprehensive government sources scraping in background"""
    
    global phase1_system, current_operation
    
    try:
        logger.info(f"Starting Phase 2 comprehensive scraping: {operation_id}")
        
        # Update progress
        current_operation['progress']['current_source'] = 'executing'
        
        # Execute Phase 2 comprehensive scraping
        results = await phase1_system.execute_phase2_comprehensive()
        
        # Update operation with results
        current_operation['status'] = 'completed'
        current_operation['completed_at'] = datetime.utcnow()
        current_operation['results_summary'] = results
        
        # Update final progress
        scraping_performance = results.get('scraping_performance', {})
        current_operation['progress'].update({
            'total_processed': scraping_performance.get('total_processed', 0),
            'successful': scraping_performance.get('total_success', 0),
            'failed': scraping_performance.get('total_processed', 0) - scraping_performance.get('total_success', 0),
            'current_source': 'completed'
        })
        
        logger.info(f"Phase 2 comprehensive scraping completed: {operation_id}")
        
    except Exception as e:
        logger.error(f"Phase 2 comprehensive scraping failed: {e}")
        
        current_operation['status'] = 'failed'
        current_operation['error'] = str(e)
        current_operation['failed_at'] = datetime.utcnow()

# Export router
__all__ = ['router']