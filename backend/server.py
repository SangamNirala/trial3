from fastapi import FastAPI, APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional
import uuid
from datetime import datetime
import asyncio

from models import (
    Question, QuestionCreate, QuestionUpdate, QuestionFilter, QuestionResponse,
    Category, CategoryCreate, ScrapingJob, ScrapingJobCreate, ScrapingJobUpdate,
    DashboardStats, SystemHealth, ScrapingStatus, QuestionStatus, DifficultyLevel
)
from database_service import DatabaseService
from scraper_engine import IndiaBixScraper
from scraper_config import INDIABIX_CONFIG

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# Initialize database service
db_service = DatabaseService(db)

# Create the main app without a prefix
app = FastAPI(
    title="Aptitude Question Bank API",
    description="Comprehensive API for managing aptitude questions with web scraping capabilities",
    version="1.0.0"
)

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# Define Models
class StatusCheck(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    client_name: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class StatusCheckCreate(BaseModel):
    client_name: str

class ScrapingJobRequest(BaseModel):
    job_name: str = Field(..., description="Name for the scraping job")
    categories: List[str] = Field(default_factory=list, description="Categories to scrape (empty for all)")
    target_count: int = Field(default=1000, description="Target number of questions")
    
class ScrapingStartResponse(BaseModel):
    job_id: str
    message: str
    estimated_duration: str

# Storage for active scraping jobs
active_scraping_jobs = {}

# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    try:
        await db_service.initialize_database()
        logging.info("Database service initialized successfully")
    except Exception as e:
        logging.error(f"Failed to initialize database service: {e}")

# Basic Routes
@api_router.get("/")
async def root():
    """Health check endpoint"""
    return {
        "message": "Aptitude Question Bank API is running",
        "version": "1.0.0",
        "status": "healthy"
    }

@api_router.post("/status", response_model=StatusCheck)
async def create_status_check(input: StatusCheckCreate):
    """Create a status check entry"""
    status_dict = input.dict()
    status_obj = StatusCheck(**status_dict)
    _ = await db.status_checks.insert_one(status_obj.dict())
    return status_obj

@api_router.get("/status", response_model=List[StatusCheck])
async def get_status_checks():
    """Get all status checks"""
    status_checks = await db.status_checks.find().to_list(1000)
    return [StatusCheck(**status_check) for status_check in status_checks]

# Dashboard Routes
@api_router.get("/dashboard/stats", response_model=DashboardStats)
async def get_dashboard_stats():
    """Get comprehensive dashboard statistics"""
    try:
        stats = await db_service.get_dashboard_stats()
        return stats
    except Exception as e:
        logging.error(f"Error getting dashboard stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve dashboard stats")

@api_router.get("/dashboard/health", response_model=SystemHealth)
async def get_system_health():
    """Get system health status"""
    try:
        # Basic health checks
        health = SystemHealth()
        
        # Check database connectivity
        try:
            await db.command("ping")
            health.database_status = "healthy"
        except Exception:
            health.database_status = "unhealthy"
            health.errors.append("Database connection failed")
        
        # Check if Chrome driver is available
        try:
            import subprocess
            result = subprocess.run(['chromedriver', '--version'], capture_output=True, timeout=5)
            if result.returncode == 0:
                health.chrome_driver_status = "healthy"
            else:
                health.chrome_driver_status = "unhealthy"
                health.warnings.append("ChromeDriver version check failed")
        except Exception:
            health.chrome_driver_status = "unhealthy"
            health.errors.append("ChromeDriver not accessible")
        
        # Check scraping service status
        if active_scraping_jobs:
            health.scraping_service_status = "active"
            health.active_connections = len(active_scraping_jobs)
        else:
            health.scraping_service_status = "idle"
        
        return health
        
    except Exception as e:
        logging.error(f"Error getting system health: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve system health")

# Question Management Routes
@api_router.get("/questions", response_model=QuestionResponse)
async def get_questions(
    page: int = 1,
    per_page: int = 20,
    category: Optional[str] = None,
    subcategory: Optional[str] = None,
    difficulty: Optional[DifficultyLevel] = None,
    status: Optional[QuestionStatus] = None,
    min_quality_score: Optional[int] = None,
    search: Optional[str] = None,
    source: Optional[str] = None
):
    """Get questions with filtering and pagination"""
    try:
        filter_params = QuestionFilter(
            category=category,
            subcategory=subcategory,
            difficulty=difficulty,
            status=status,
            min_quality_score=min_quality_score,
            search_text=search,
            source=source
        )
        
        response = await db_service.get_questions(filter_params, page, per_page)
        return response
        
    except Exception as e:
        logging.error(f"Error getting questions: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve questions")

@api_router.post("/questions", response_model=Question)
async def create_question(question_data: QuestionCreate):
    """Create a new question"""
    try:
        question = await db_service.create_question(question_data)
        return question
    except Exception as e:
        logging.error(f"Error creating question: {e}")
        raise HTTPException(status_code=500, detail="Failed to create question")

@api_router.put("/questions/{question_id}", response_model=Question)
async def update_question(question_id: str, question_data: QuestionUpdate):
    """Update an existing question"""
    try:
        question = await db_service.update_question(question_id, question_data)
        if not question:
            raise HTTPException(status_code=404, detail="Question not found")
        return question
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error updating question {question_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to update question")

@api_router.delete("/questions/{question_id}")
async def delete_question(question_id: str):
    """Delete a question (soft delete)"""
    try:
        success = await db_service.delete_question(question_id)
        if not success:
            raise HTTPException(status_code=404, detail="Question not found")
        return {"message": "Question deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error deleting question {question_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete question")

# Category Management Routes
@api_router.get("/categories", response_model=List[Category])
async def get_categories():
    """Get all categories"""
    try:
        categories = await db_service.get_categories()
        return categories
    except Exception as e:
        logging.error(f"Error getting categories: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve categories")

@api_router.post("/categories", response_model=Category)
async def create_category(category_data: CategoryCreate):
    """Create a new category"""
    try:
        category = await db_service.create_category(category_data)
        return category
    except Exception as e:
        logging.error(f"Error creating category: {e}")
        raise HTTPException(status_code=500, detail="Failed to create category")

# Scraping Management Routes
@api_router.get("/scraping/config")
async def get_scraping_config():
    """Get available scraping configuration"""
    return {
        "available_categories": list(INDIABIX_CONFIG["categories"].keys()),
        "category_details": {
            name: {
                "display_name": config["display_name"],
                "subcategories": list(config["subcategories"].keys()),
                "total_target": sum(sub["target_questions"] for sub in config["subcategories"].values())
            }
            for name, config in INDIABIX_CONFIG["categories"].items()
        }
    }

@api_router.get("/scraping/jobs", response_model=List[ScrapingJob])
async def get_scraping_jobs(status: Optional[ScrapingStatus] = None):
    """Get scraping jobs with optional status filter"""
    try:
        jobs = await db_service.get_scraping_jobs(status)
        return jobs
    except Exception as e:
        logging.error(f"Error getting scraping jobs: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve scraping jobs")

@api_router.post("/scraping/start", response_model=ScrapingStartResponse)
async def start_scraping(request: ScrapingJobRequest, background_tasks: BackgroundTasks):
    """Start a new scraping job"""
    try:
        # Validate categories
        available_categories = list(INDIABIX_CONFIG["categories"].keys())
        if request.categories:
            invalid_categories = [cat for cat in request.categories if cat not in available_categories]
            if invalid_categories:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Invalid categories: {invalid_categories}. Available: {available_categories}"
                )
        
        # Create scraping job in database
        job_data = ScrapingJobCreate(
            job_name=request.job_name,
            target_categories=request.categories or available_categories,
            target_count=request.target_count,
            source_urls=[INDIABIX_CONFIG["base_url"]]
        )
        
        job = await db_service.create_scraping_job(job_data)
        
        # Start scraping in background
        background_tasks.add_task(run_scraping_job, job.id, job_data)
        
        # Estimate duration (rough calculation)
        estimated_minutes = (request.target_count * 0.1)  # ~0.1 minute per question
        estimated_duration = f"{int(estimated_minutes)} minutes"
        
        return ScrapingStartResponse(
            job_id=job.id,
            message="Scraping job started successfully",
            estimated_duration=estimated_duration
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error starting scraping job: {e}")
        raise HTTPException(status_code=500, detail="Failed to start scraping job")

async def run_scraping_job(job_id: str, job_data: ScrapingJobCreate):
    """Run the actual scraping job in background"""
    try:
        # Update job status to in_progress
        await db_service.update_scraping_job(
            job_id, 
            ScrapingJobUpdate(
                status=ScrapingStatus.IN_PROGRESS,
                started_at=datetime.utcnow()
            )
        )
        
        # Track active job
        active_scraping_jobs[job_id] = datetime.utcnow()
        
        # Initialize scraper
        scraper = IndiaBixScraper()
        
        # Run scraping
        result = await scraper.start_scraping(
            target_categories=job_data.target_categories,
            target_total=job_data.target_count
        )
        
        questions_data = result['questions']
        stats = result['stats']
        
        # Save questions to database
        if questions_data:
            question_ids = await db_service.create_questions_bulk(questions_data)
            
            # Update job completion
            await db_service.update_scraping_job(
                job_id,
                ScrapingJobUpdate(
                    status=ScrapingStatus.COMPLETED,
                    questions_scraped=stats['total_questions'],
                    questions_saved=len(question_ids),
                    success_rate=round((stats['success_count'] / max(stats['total_questions'], 1)) * 100, 2),
                    error_count=stats['error_count'],
                    completed_at=datetime.utcnow()
                )
            )
            
            logging.info(f"Scraping job {job_id} completed: {len(question_ids)} questions saved")
        else:
            # Update job as failed
            await db_service.update_scraping_job(
                job_id,
                ScrapingJobUpdate(
                    status=ScrapingStatus.FAILED,
                    error_count=stats.get('error_count', 1),
                    completed_at=datetime.utcnow()
                )
            )
            
            logging.error(f"Scraping job {job_id} failed: No questions extracted")
        
    except Exception as e:
        logging.error(f"Error running scraping job {job_id}: {e}")
        
        # Update job as failed
        try:
            await db_service.update_scraping_job(
                job_id,
                ScrapingJobUpdate(
                    status=ScrapingStatus.FAILED,
                    error_count=1,
                    completed_at=datetime.utcnow()
                )
            )
        except Exception as update_error:
            logging.error(f"Failed to update job status: {update_error}")
    
    finally:
        # Remove from active jobs
        active_scraping_jobs.pop(job_id, None)

@api_router.delete("/scraping/jobs/{job_id}")
async def cancel_scraping_job(job_id: str):
    """Cancel an active scraping job"""
    try:
        if job_id in active_scraping_jobs:
            # Update job status to paused/cancelled
            await db_service.update_scraping_job(
                job_id,
                ScrapingJobUpdate(
                    status=ScrapingStatus.PAUSED,
                    completed_at=datetime.utcnow()
                )
            )
            
            # Remove from active jobs
            active_scraping_jobs.pop(job_id, None)
            
            return {"message": "Scraping job cancelled successfully"}
        else:
            raise HTTPException(status_code=404, detail="Scraping job not found or not active")
    
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error cancelling scraping job {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to cancel scraping job")

# Include the router in the main app
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
