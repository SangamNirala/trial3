"""
Database service for managing questions and scraping data
Handles MongoDB operations with proper error handling and optimization
"""

import logging
from typing import List, Dict, Optional, Any
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
import asyncio
from bson import ObjectId
import json

from models import (
    Question, QuestionCreate, QuestionUpdate, QuestionFilter, QuestionResponse,
    Category, CategoryCreate, ScrapingJob, ScrapingJobCreate, ScrapingJobUpdate,
    ScrapingProgress, QuestionQuality, DashboardStats, SystemHealth,
    ScrapingStatus, QuestionStatus, DifficultyLevel
)

logger = logging.getLogger(__name__)

class DatabaseService:
    """
    Comprehensive database service for aptitude question management
    """
    
    def __init__(self, database: AsyncIOMotorDatabase):
        self.db = database
        self.questions_collection = self.db.questions
        self.categories_collection = self.db.categories
        self.scraping_jobs_collection = self.db.scraping_jobs
        self.scraping_progress_collection = self.db.scraping_progress
        self.question_quality_collection = self.db.question_quality
        
    async def initialize_database(self):
        """Initialize database with indexes and default data"""
        try:
            # Create indexes for better performance
            await self.create_indexes()
            
            # Initialize default categories
            await self.initialize_categories()
            
            logger.info("Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
    
    async def create_indexes(self):
        """Create database indexes for optimal query performance"""
        try:
            # Questions collection indexes
            await self.questions_collection.create_index([("category", 1), ("subcategory", 1)])
            await self.questions_collection.create_index([("status", 1)])
            await self.questions_collection.create_index([("source", 1)])
            await self.questions_collection.create_index([("difficulty", 1)])
            await self.questions_collection.create_index([("quality_score", -1)])
            await self.questions_collection.create_index([("created_at", -1)])
            await self.questions_collection.create_index([("question_text", "text")])  # Text search
            
            # Categories collection indexes
            await self.categories_collection.create_index([("name", 1)], unique=True)
            await self.categories_collection.create_index([("parent_category", 1)])
            
            # Scraping jobs collection indexes
            await self.scraping_jobs_collection.create_index([("status", 1)])
            await self.scraping_jobs_collection.create_index([("created_at", -1)])
            
            # Compound indexes for common queries
            await self.questions_collection.create_index([
                ("category", 1), ("status", 1), ("quality_score", -1)
            ])
            
            logger.info("Database indexes created successfully")
            
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
            raise
    
    async def initialize_categories(self):
        """Initialize default categories in database"""
        try:
            default_categories = [
                {
                    "name": "quantitative_aptitude",
                    "display_name": "Quantitative Aptitude",
                    "description": "Mathematical and numerical reasoning questions",
                    "parent_category": None
                },
                {
                    "name": "logical_reasoning", 
                    "display_name": "Logical Reasoning",
                    "description": "Pattern recognition and logical thinking questions",
                    "parent_category": None
                },
                {
                    "name": "verbal_ability",
                    "display_name": "Verbal Ability",
                    "description": "Language skills and comprehension questions",
                    "parent_category": None
                },
                {
                    "name": "general_knowledge",
                    "display_name": "General Knowledge", 
                    "description": "Current affairs and general awareness questions",
                    "parent_category": None
                }
            ]
            
            for cat_data in default_categories:
                existing = await self.categories_collection.find_one({"name": cat_data["name"]})
                if not existing:
                    category = Category(**cat_data)
                    await self.categories_collection.insert_one(category.dict())
                    logger.info(f"Created category: {cat_data['display_name']}")
            
        except Exception as e:
            logger.error(f"Error initializing categories: {e}")
    
    # Question Management Methods
    async def create_question(self, question_data: QuestionCreate) -> Question:
        """Create a new question in the database"""
        try:
            question = Question(**question_data.dict())
            
            # Calculate initial quality score
            quality_score = await self.calculate_quality_score(question)
            question.quality_score = quality_score
            
            result = await self.questions_collection.insert_one(question.dict())
            
            # Update category question count
            await self.increment_category_count(question.category)
            
            logger.info(f"Created question: {question.id}")
            return question
            
        except Exception as e:
            logger.error(f"Error creating question: {e}")
            raise
    
    async def create_questions_bulk(self, questions_data: List[Dict[str, Any]]) -> List[str]:
        """Create multiple questions in bulk for better performance"""
        try:
            questions = []
            question_ids = []
            
            for q_data in questions_data:
                # Create question object
                question = Question(
                    question_text=q_data.get('question_text', ''),
                    options=q_data.get('options', []),
                    correct_answer=q_data.get('correct_answer', ''),
                    category=q_data.get('category', ''),
                    subcategory=q_data.get('subcategory', ''),
                    difficulty=q_data.get('difficulty', DifficultyLevel.MEDIUM),
                    explanation=q_data.get('explanation', ''),
                    concepts=q_data.get('concepts', []),
                    tags=q_data.get('tags', []),
                    source=q_data.get('source', 'indiabix'),
                    source_url=q_data.get('source_url', ''),
                    time_estimate=q_data.get('time_estimate', 120)
                )
                
                # Calculate quality score
                quality_score = await self.calculate_quality_score(question)
                question.quality_score = quality_score
                
                questions.append(question.dict())
                question_ids.append(question.id)
            
            # Bulk insert
            if questions:
                await self.questions_collection.insert_many(questions)
                
                # Update category counts
                category_counts = {}
                for q in questions:
                    category = q['category']
                    category_counts[category] = category_counts.get(category, 0) + 1
                
                for category, count in category_counts.items():
                    await self.increment_category_count(category, count)
                
                logger.info(f"Created {len(questions)} questions in bulk")
            
            return question_ids
            
        except Exception as e:
            logger.error(f"Error creating questions in bulk: {e}")
            raise
    
    async def get_questions(
        self, 
        filter_params: QuestionFilter, 
        page: int = 1, 
        per_page: int = 20
    ) -> QuestionResponse:
        """Get questions with filtering and pagination"""
        try:
            # Build query
            query = {"status": {"$ne": QuestionStatus.INACTIVE}}
            
            if filter_params.category:
                query["category"] = filter_params.category
            
            if filter_params.subcategory:
                query["subcategory"] = filter_params.subcategory
                
            if filter_params.difficulty:
                query["difficulty"] = filter_params.difficulty
                
            if filter_params.status:
                query["status"] = filter_params.status
                
            if filter_params.min_quality_score:
                query["quality_score"] = {"$gte": filter_params.min_quality_score}
                
            if filter_params.source:
                query["source"] = filter_params.source
                
            if filter_params.tags:
                query["tags"] = {"$in": filter_params.tags}
                
            if filter_params.search_text:
                query["$text"] = {"$search": filter_params.search_text}
            
            # Count total
            total_count = await self.questions_collection.count_documents(query)
            total_pages = (total_count + per_page - 1) // per_page
            
            # Get questions with pagination
            skip = (page - 1) * per_page
            cursor = self.questions_collection.find(query).skip(skip).limit(per_page)
            cursor.sort("quality_score", -1)  # Sort by quality
            
            questions_data = await cursor.to_list(length=per_page)
            questions = [Question(**q) for q in questions_data]
            
            return QuestionResponse(
                questions=questions,
                total_count=total_count,
                page=page,
                per_page=per_page,
                total_pages=total_pages,
                filters_applied=filter_params.dict(exclude_none=True)
            )
            
        except Exception as e:
            logger.error(f"Error getting questions: {e}")
            raise
    
    async def update_question(self, question_id: str, update_data: QuestionUpdate) -> Optional[Question]:
        """Update an existing question"""
        try:
            update_dict = update_data.dict(exclude_none=True)
            
            if update_dict:
                update_dict["updated_at"] = datetime.utcnow()
                
                result = await self.questions_collection.update_one(
                    {"id": question_id},
                    {"$set": update_dict}
                )
                
                if result.modified_count > 0:
                    updated_doc = await self.questions_collection.find_one({"id": question_id})
                    if updated_doc:
                        return Question(**updated_doc)
            
            return None
            
        except Exception as e:
            logger.error(f"Error updating question {question_id}: {e}")
            raise
    
    async def delete_question(self, question_id: str) -> bool:
        """Delete a question (soft delete by updating status)"""
        try:
            result = await self.questions_collection.update_one(
                {"id": question_id},
                {"$set": {"status": QuestionStatus.INACTIVE, "updated_at": datetime.utcnow()}}
            )
            
            success = result.modified_count > 0
            if success:
                logger.info(f"Deleted question: {question_id}")
                
            return success
            
        except Exception as e:
            logger.error(f"Error deleting question {question_id}: {e}")
            raise
    
    # Quality and Analytics Methods
    async def calculate_quality_score(self, question: Question) -> int:
        """Calculate quality score for a question"""
        try:
            score = 0
            
            # Completeness (40 points)
            if question.question_text and len(question.question_text) >= 10:
                score += 10
            if len(question.options) == 4 and all(opt for opt in question.options):
                score += 10
            if question.correct_answer in question.options:
                score += 10
            if question.explanation and len(question.explanation) >= 20:
                score += 10
            
            # Content quality (30 points)
            if len(question.question_text.split()) >= 5:
                score += 10
            if question.concepts and len(question.concepts) > 0:
                score += 10
            if question.tags and len(question.tags) > 0:
                score += 10
            
            # Metadata completeness (30 points)
            if question.category and question.subcategory:
                score += 15
            if question.difficulty != DifficultyLevel.MEDIUM:  # Explicitly set
                score += 5
            if question.source_url:
                score += 5
            if question.time_estimate > 0:
                score += 5
            
            return min(score, 100)
            
        except Exception as e:
            logger.error(f"Error calculating quality score: {e}")
            return 0
    
    async def get_dashboard_stats(self) -> DashboardStats:
        """Get comprehensive dashboard statistics"""
        try:
            # Get total questions by status
            total_questions = await self.questions_collection.count_documents({
                "status": {"$ne": QuestionStatus.INACTIVE}
            })
            
            # Get active jobs count
            active_jobs = await self.scraping_jobs_collection.count_documents({
                "status": {"$in": [ScrapingStatus.PENDING, ScrapingStatus.IN_PROGRESS]}
            })
            
            # Get completed jobs count  
            completed_jobs = await self.scraping_jobs_collection.count_documents({
                "status": ScrapingStatus.COMPLETED
            })
            
            # Get categories count
            categories_covered = await self.categories_collection.count_documents({
                "is_active": True
            })
            
            # Get average quality score
            pipeline = [
                {"$match": {"status": {"$ne": QuestionStatus.INACTIVE}}},
                {"$group": {"_id": None, "avg_quality": {"$avg": "$quality_score"}}}
            ]
            avg_result = await self.questions_collection.aggregate(pipeline).to_list(1)
            avg_quality_score = round(avg_result[0]["avg_quality"], 2) if avg_result else 0
            
            # Get category distribution
            cat_pipeline = [
                {"$match": {"status": {"$ne": QuestionStatus.INACTIVE}}},
                {"$group": {"_id": "$category", "count": {"$sum": 1}}}
            ]
            cat_results = await self.questions_collection.aggregate(cat_pipeline).to_list(None)
            category_distribution = {item["_id"]: item["count"] for item in cat_results}
            
            # Get difficulty distribution
            diff_pipeline = [
                {"$match": {"status": {"$ne": QuestionStatus.INACTIVE}}},
                {"$group": {"_id": "$difficulty", "count": {"$sum": 1}}}
            ]
            diff_results = await self.questions_collection.aggregate(diff_pipeline).to_list(None)
            difficulty_distribution = {item["_id"]: item["count"] for item in diff_results}
            
            # Get source distribution
            source_pipeline = [
                {"$match": {"status": {"$ne": QuestionStatus.INACTIVE}}},
                {"$group": {"_id": "$source", "count": {"$sum": 1}}}
            ]
            source_results = await self.questions_collection.aggregate(source_pipeline).to_list(None)
            source_distribution = {item["_id"]: item["count"] for item in source_results}
            
            # Get last scraping date
            last_job = await self.scraping_jobs_collection.find_one(
                {"status": ScrapingStatus.COMPLETED},
                sort=[("completed_at", -1)]
            )
            last_scraping_date = last_job["completed_at"] if last_job else None
            
            # Get today's scraping count
            today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            daily_count = await self.questions_collection.count_documents({
                "created_at": {"$gte": today},
                "status": {"$ne": QuestionStatus.INACTIVE}
            })
            
            return DashboardStats(
                total_questions=total_questions,
                active_jobs=active_jobs,
                completed_jobs=completed_jobs,
                categories_covered=categories_covered,
                avg_quality_score=avg_quality_score,
                last_scraping_date=last_scraping_date,
                daily_scraping_count=daily_count,
                category_distribution=category_distribution,
                difficulty_distribution=difficulty_distribution,
                source_distribution=source_distribution
            )
            
        except Exception as e:
            logger.error(f"Error getting dashboard stats: {e}")
            raise
    
    # Category Management Methods
    async def create_category(self, category_data: CategoryCreate) -> Category:
        """Create a new category"""
        try:
            category = Category(**category_data.dict())
            await self.categories_collection.insert_one(category.dict())
            logger.info(f"Created category: {category.name}")
            return category
            
        except Exception as e:
            logger.error(f"Error creating category: {e}")
            raise
    
    async def get_categories(self) -> List[Category]:
        """Get all active categories"""
        try:
            cursor = self.categories_collection.find({"is_active": True})
            categories_data = await cursor.to_list(None)
            return [Category(**cat) for cat in categories_data]
            
        except Exception as e:
            logger.error(f"Error getting categories: {e}")
            raise
    
    async def increment_category_count(self, category_name: str, increment: int = 1):
        """Increment question count for a category"""
        try:
            await self.categories_collection.update_one(
                {"name": category_name},
                {"$inc": {"question_count": increment}}
            )
        except Exception as e:
            logger.error(f"Error incrementing category count: {e}")
    
    # Scraping Job Management
    async def create_scraping_job(self, job_data: ScrapingJobCreate) -> ScrapingJob:
        """Create a new scraping job"""
        try:
            job = ScrapingJob(**job_data.dict())
            await self.scraping_jobs_collection.insert_one(job.dict())
            logger.info(f"Created scraping job: {job.id}")
            return job
            
        except Exception as e:
            logger.error(f"Error creating scraping job: {e}")
            raise
    
    async def update_scraping_job(self, job_id: str, update_data: ScrapingJobUpdate) -> Optional[ScrapingJob]:
        """Update a scraping job"""
        try:
            update_dict = update_data.dict(exclude_none=True)
            
            if update_dict:
                result = await self.scraping_jobs_collection.update_one(
                    {"id": job_id},
                    {"$set": update_dict}
                )
                
                if result.modified_count > 0:
                    updated_doc = await self.scraping_jobs_collection.find_one({"id": job_id})
                    if updated_doc:
                        return ScrapingJob(**updated_doc)
            
            return None
            
        except Exception as e:
            logger.error(f"Error updating scraping job {job_id}: {e}")
            raise
    
    async def get_scraping_jobs(self, status: Optional[ScrapingStatus] = None) -> List[ScrapingJob]:
        """Get scraping jobs with optional status filter"""
        try:
            query = {}
            if status:
                query["status"] = status
                
            cursor = self.scraping_jobs_collection.find(query).sort("created_at", -1)
            jobs_data = await cursor.to_list(None)
            return [ScrapingJob(**job) for job in jobs_data]
            
        except Exception as e:
            logger.error(f"Error getting scraping jobs: {e}")
            raise