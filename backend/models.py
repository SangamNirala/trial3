from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum
import uuid

class DifficultyLevel(str, Enum):
    EASY = "easy"
    MEDIUM = "medium" 
    HARD = "hard"

class QuestionStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING_REVIEW = "pending_review"
    DUPLICATE = "duplicate"

class ScrapingStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"

# Question Models
class QuestionBase(BaseModel):
    question_text: str = Field(..., description="Complete question with proper formatting")
    options: List[str] = Field(..., min_items=4, max_items=4, description="Exactly 4 multiple choice options")
    correct_answer: str = Field(..., description="The correct answer from options")
    category: str = Field(..., description="Main category (e.g., quantitative_aptitude)")
    subcategory: str = Field(..., description="Specific topic (e.g., profit_and_loss)")
    difficulty: DifficultyLevel = Field(default=DifficultyLevel.MEDIUM)
    explanation: Optional[str] = Field(None, description="Detailed solution with steps")
    concepts: List[str] = Field(default_factory=list, description="Related concepts/topics")
    tags: List[str] = Field(default_factory=list, description="Additional classification tags")
    time_estimate: int = Field(default=120, description="Estimated solving time in seconds")

class Question(QuestionBase):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source: str = Field(default="indiabix", description="Source website")
    source_url: Optional[str] = Field(None, description="Original page URL")
    quality_score: int = Field(default=0, description="Quality score 0-100")
    status: QuestionStatus = Field(default=QuestionStatus.ACTIVE)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)

class QuestionCreate(QuestionBase):
    pass

class QuestionUpdate(BaseModel):
    question_text: Optional[str] = None
    options: Optional[List[str]] = None
    correct_answer: Optional[str] = None
    explanation: Optional[str] = None
    difficulty: Optional[DifficultyLevel] = None
    quality_score: Optional[int] = None
    status: Optional[QuestionStatus] = None
    updated_at: datetime = Field(default_factory=datetime.utcnow)

# Category Models
class CategoryBase(BaseModel):
    name: str = Field(..., description="Category name")
    display_name: str = Field(..., description="Human-readable name")
    description: Optional[str] = Field(None, description="Category description")
    parent_category: Optional[str] = Field(None, description="Parent category for hierarchy")

class Category(CategoryBase):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    question_count: int = Field(default=0, description="Number of questions in category")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = Field(default=True)

class CategoryCreate(CategoryBase):
    pass

# Scraping Job Models
class ScrapingJobBase(BaseModel):
    job_name: str = Field(..., description="Descriptive name for the job")
    target_categories: List[str] = Field(..., description="Categories to scrape")
    target_count: int = Field(default=1000, description="Target number of questions")
    source_urls: List[str] = Field(default_factory=list, description="URLs to scrape")

class ScrapingJob(ScrapingJobBase):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    status: ScrapingStatus = Field(default=ScrapingStatus.PENDING)
    questions_scraped: int = Field(default=0)
    questions_saved: int = Field(default=0)
    success_rate: float = Field(default=0.0, description="Success rate percentage")
    error_count: int = Field(default=0)
    started_at: Optional[datetime] = Field(None)
    completed_at: Optional[datetime] = Field(None)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    progress_log: List[Dict[str, Any]] = Field(default_factory=list)
    error_details: List[Dict[str, Any]] = Field(default_factory=list)
    estimated_completion: Optional[datetime] = Field(None)

class ScrapingJobCreate(ScrapingJobBase):
    pass

class ScrapingJobUpdate(BaseModel):
    status: Optional[ScrapingStatus] = None
    questions_scraped: Optional[int] = None
    questions_saved: Optional[int] = None
    error_count: Optional[int] = None
    last_updated: datetime = Field(default_factory=datetime.utcnow)

# Analytics Models
class ScrapingAnalytics(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    job_id: str = Field(..., description="Reference to scraping job")
    category: str = Field(..., description="Category being analyzed")
    total_questions: int = Field(default=0)
    success_count: int = Field(default=0)
    failure_count: int = Field(default=0)
    duplicate_count: int = Field(default=0)
    avg_quality_score: float = Field(default=0.0)
    processing_time: float = Field(default=0.0, description="Time taken in seconds")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    metrics: Dict[str, Any] = Field(default_factory=dict)

# Progress Tracking Models
class ScrapingProgress(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    job_id: str = Field(..., description="Reference to scraping job")
    category: str = Field(..., description="Current category")
    current_page: int = Field(default=1)
    total_pages: int = Field(default=0)
    questions_processed: int = Field(default=0)
    questions_saved: int = Field(default=0)
    current_url: str = Field(..., description="Currently processing URL")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    status: str = Field(default="processing")
    message: Optional[str] = Field(None, description="Status message")

# Quality Metrics Models  
class QuestionQuality(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    question_id: str = Field(..., description="Reference to question")
    completeness_score: int = Field(default=0, description="0-100 based on field completion")
    clarity_score: int = Field(default=0, description="0-100 based on text clarity")
    uniqueness_score: int = Field(default=0, description="0-100 based on similarity check")
    overall_score: int = Field(default=0, description="Combined quality score")
    validation_results: Dict[str, Any] = Field(default_factory=dict)
    reviewed_at: datetime = Field(default_factory=datetime.utcnow)
    reviewer: Optional[str] = Field(None, description="Who reviewed the question")

# Search and Filter Models
class QuestionFilter(BaseModel):
    category: Optional[str] = None
    subcategory: Optional[str] = None
    difficulty: Optional[DifficultyLevel] = None
    status: Optional[QuestionStatus] = None
    min_quality_score: Optional[int] = None
    search_text: Optional[str] = None
    tags: Optional[List[str]] = None
    source: Optional[str] = None
    
class QuestionResponse(BaseModel):
    questions: List[Question]
    total_count: int
    page: int
    per_page: int
    total_pages: int
    filters_applied: Dict[str, Any]

# Dashboard Models
class DashboardStats(BaseModel):
    total_questions: int = Field(default=0)
    active_jobs: int = Field(default=0) 
    completed_jobs: int = Field(default=0)
    categories_covered: int = Field(default=0)
    avg_quality_score: float = Field(default=0.0)
    last_scraping_date: Optional[datetime] = Field(None)
    daily_scraping_count: int = Field(default=0)
    category_distribution: Dict[str, int] = Field(default_factory=dict)
    difficulty_distribution: Dict[str, int] = Field(default_factory=dict)
    source_distribution: Dict[str, int] = Field(default_factory=dict)
    
class SystemHealth(BaseModel):
    database_status: str = Field(default="unknown")
    scraping_service_status: str = Field(default="unknown")
    chrome_driver_status: str = Field(default="unknown")
    total_memory_usage: float = Field(default=0.0)
    active_connections: int = Field(default=0)
    last_health_check: datetime = Field(default_factory=datetime.utcnow)
    warnings: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)