#!/usr/bin/env python3
"""
Comprehensive Backend API Testing for Aptitude Question Bank System
Tests all endpoints and validates the 10,776+ question database achievement
"""

import requests
import sys
import json
from datetime import datetime
from typing import Dict, Any, List

class AptitudeQuestionBankTester:
    def __init__(self, base_url="https://qbank-creator-1.preview.emergentagent.com"):
        self.base_url = base_url
        self.api_url = f"{base_url}/api"
        self.tests_run = 0
        self.tests_passed = 0
        self.test_results = []
        
        print(f"🎯 Aptitude Question Bank API Tester")
        print(f"📡 Testing API at: {self.api_url}")
        print("=" * 60)

    def log_test(self, name: str, success: bool, details: str = "", response_data: Any = None):
        """Log test results"""
        self.tests_run += 1
        if success:
            self.tests_passed += 1
            
        result = {
            "test": name,
            "success": success,
            "details": details,
            "timestamp": datetime.now().isoformat()
        }
        
        if response_data:
            result["response_sample"] = response_data
            
        self.test_results.append(result)
        
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {name}")
        if details:
            print(f"    📝 {details}")
        if not success and response_data:
            print(f"    🔍 Response: {response_data}")
        print()

    def test_health_check(self):
        """Test basic health check endpoint"""
        try:
            response = requests.get(f"{self.api_url}/", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                expected_fields = ["message", "version", "status"]
                
                if all(field in data for field in expected_fields):
                    self.log_test(
                        "Health Check", 
                        True, 
                        f"API is running - Version: {data.get('version', 'N/A')}, Status: {data.get('status', 'N/A')}"
                    )
                    return True
                else:
                    self.log_test("Health Check", False, "Missing expected fields in response", data)
            else:
                self.log_test("Health Check", False, f"HTTP {response.status_code}", response.text[:200])
                
        except Exception as e:
            self.log_test("Health Check", False, f"Connection error: {str(e)}")
            
        return False

    def test_dashboard_stats(self):
        """Test dashboard statistics - should show 10,776+ questions"""
        try:
            response = requests.get(f"{self.api_url}/dashboard/stats", timeout=15)
            
            if response.status_code == 200:
                stats = response.json()
                
                # Check for required fields
                required_fields = ["total_questions", "categories_covered", "avg_quality_score"]
                missing_fields = [field for field in required_fields if field not in stats]
                
                if missing_fields:
                    self.log_test("Dashboard Stats", False, f"Missing fields: {missing_fields}", stats)
                    return False
                
                total_questions = stats.get("total_questions", 0)
                categories = stats.get("categories_covered", 0)
                avg_quality = stats.get("avg_quality_score", 0)
                
                # Validate the 10,776+ questions achievement
                if total_questions >= 10000:
                    details = f"🎉 SUCCESS! {total_questions:,} questions (Target: 10,000+), {categories} categories, {avg_quality}% avg quality"
                    self.log_test("Dashboard Stats", True, details, {
                        "total_questions": total_questions,
                        "categories_covered": categories,
                        "avg_quality_score": avg_quality
                    })
                    return True
                else:
                    self.log_test("Dashboard Stats", False, f"Only {total_questions:,} questions (Expected: 10,000+)", stats)
            else:
                self.log_test("Dashboard Stats", False, f"HTTP {response.status_code}", response.text[:200])
                
        except Exception as e:
            self.log_test("Dashboard Stats", False, f"Error: {str(e)}")
            
        return False

    def test_system_health(self):
        """Test system health status"""
        try:
            response = requests.get(f"{self.api_url}/dashboard/health", timeout=10)
            
            if response.status_code == 200:
                health = response.json()
                
                db_status = health.get("database_status", "unknown")
                chrome_status = health.get("chrome_driver_status", "unknown")
                scraping_status = health.get("scraping_service_status", "unknown")
                
                all_healthy = (
                    db_status == "healthy" and 
                    chrome_status == "healthy" and 
                    scraping_status in ["idle", "active"]
                )
                
                details = f"DB: {db_status}, Chrome: {chrome_status}, Scraping: {scraping_status}"
                
                if all_healthy:
                    self.log_test("System Health", True, f"All systems healthy - {details}")
                else:
                    self.log_test("System Health", False, f"Some systems unhealthy - {details}", health)
                    
                return all_healthy
            else:
                self.log_test("System Health", False, f"HTTP {response.status_code}", response.text[:200])
                
        except Exception as e:
            self.log_test("System Health", False, f"Error: {str(e)}")
            
        return False

    def test_questions_endpoint(self):
        """Test questions retrieval with pagination and filtering"""
        try:
            # Test basic questions retrieval
            response = requests.get(f"{self.api_url}/questions?page=1&per_page=5", timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                
                if "questions" in data and "pagination" in data:
                    questions = data["questions"]
                    pagination = data["pagination"]
                    
                    if len(questions) > 0:
                        # Validate question structure
                        sample_question = questions[0]
                        required_fields = ["id", "question_text", "options", "correct_answer"]
                        
                        if all(field in sample_question for field in required_fields):
                            details = f"Retrieved {len(questions)} questions, Total: {pagination.get('total', 'N/A')}"
                            self.log_test("Questions Retrieval", True, details, {
                                "sample_question_id": sample_question.get("id"),
                                "has_options": len(sample_question.get("options", [])),
                                "pagination": pagination
                            })
                            return True
                        else:
                            missing = [f for f in required_fields if f not in sample_question]
                            self.log_test("Questions Retrieval", False, f"Question missing fields: {missing}", sample_question)
                    else:
                        self.log_test("Questions Retrieval", False, "No questions returned", data)
                else:
                    self.log_test("Questions Retrieval", False, "Invalid response structure", data)
            else:
                self.log_test("Questions Retrieval", False, f"HTTP {response.status_code}", response.text[:200])
                
        except Exception as e:
            self.log_test("Questions Retrieval", False, f"Error: {str(e)}")
            
        return False

    def test_categories_endpoint(self):
        """Test categories endpoint"""
        try:
            response = requests.get(f"{self.api_url}/categories", timeout=10)
            
            if response.status_code == 200:
                categories = response.json()
                
                if isinstance(categories, list) and len(categories) > 0:
                    category_names = [cat.get("name", "Unknown") for cat in categories if isinstance(cat, dict)]
                    details = f"Found {len(categories)} categories: {', '.join(category_names[:4])}"
                    self.log_test("Categories", True, details, {"count": len(categories), "names": category_names})
                    return True
                else:
                    self.log_test("Categories", False, "No categories found or invalid format", categories)
            else:
                self.log_test("Categories", False, f"HTTP {response.status_code}", response.text[:200])
                
        except Exception as e:
            self.log_test("Categories", False, f"Error: {str(e)}")
            
        return False

    def test_scraping_config(self):
        """Test scraping configuration endpoint"""
        try:
            response = requests.get(f"{self.api_url}/scraping/config", timeout=10)
            
            if response.status_code == 200:
                config = response.json()
                
                if "available_categories" in config and "category_details" in config:
                    available_cats = config["available_categories"]
                    details = f"Scraping config loaded - {len(available_cats)} categories available"
                    self.log_test("Scraping Config", True, details, {
                        "categories_count": len(available_cats),
                        "categories": available_cats
                    })
                    return True
                else:
                    self.log_test("Scraping Config", False, "Invalid config structure", config)
            else:
                self.log_test("Scraping Config", False, f"HTTP {response.status_code}", response.text[:200])
                
        except Exception as e:
            self.log_test("Scraping Config", False, f"Error: {str(e)}")
            
        return False

    def test_scraping_jobs(self):
        """Test scraping jobs endpoint"""
        try:
            response = requests.get(f"{self.api_url}/scraping/jobs", timeout=10)
            
            if response.status_code == 200:
                jobs = response.json()
                
                if isinstance(jobs, list):
                    details = f"Retrieved {len(jobs)} scraping jobs"
                    if len(jobs) > 0:
                        completed_jobs = [job for job in jobs if job.get("status") == "completed"]
                        details += f" ({len(completed_jobs)} completed)"
                    
                    self.log_test("Scraping Jobs", True, details, {"jobs_count": len(jobs)})
                    return True
                else:
                    self.log_test("Scraping Jobs", False, "Invalid jobs format", jobs)
            else:
                self.log_test("Scraping Jobs", False, f"HTTP {response.status_code}", response.text[:200])
                
        except Exception as e:
            self.log_test("Scraping Jobs", False, f"Error: {str(e)}")
            
        return False

    def test_question_creation(self):
        """Test creating a new question"""
        try:
            test_question = {
                "question_text": "What is 2 + 2?",
                "options": ["3", "4", "5", "6"],
                "correct_answer": "4",
                "explanation": "Basic arithmetic: 2 + 2 = 4",
                "category": "quantitative_aptitude",
                "subcategory": "arithmetic",
                "difficulty": "easy",
                "source": "test_suite",
                "quality_score": 95
            }
            
            response = requests.post(
                f"{self.api_url}/questions", 
                json=test_question,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            if response.status_code in [200, 201]:
                created_question = response.json()
                if "id" in created_question:
                    self.log_test("Question Creation", True, f"Created question with ID: {created_question['id']}")
                    return True
                else:
                    self.log_test("Question Creation", False, "No ID in response", created_question)
            else:
                self.log_test("Question Creation", False, f"HTTP {response.status_code}", response.text[:200])
                
        except Exception as e:
            self.log_test("Question Creation", False, f"Error: {str(e)}")
            
        return False

    def test_filtered_questions(self):
        """Test question filtering by category"""
        try:
            response = requests.get(
                f"{self.api_url}/questions?category=quantitative_aptitude&per_page=3", 
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                questions = data.get("questions", [])
                
                if len(questions) > 0:
                    # Check if questions are actually from the requested category
                    category_match = all(
                        q.get("category") == "quantitative_aptitude" 
                        for q in questions 
                        if "category" in q
                    )
                    
                    if category_match:
                        self.log_test("Filtered Questions", True, f"Retrieved {len(questions)} quantitative aptitude questions")
                        return True
                    else:
                        self.log_test("Filtered Questions", False, "Category filter not working properly", questions[0])
                else:
                    self.log_test("Filtered Questions", False, "No filtered questions returned")
            else:
                self.log_test("Filtered Questions", False, f"HTTP {response.status_code}", response.text[:200])
                
        except Exception as e:
            self.log_test("Filtered Questions", False, f"Error: {str(e)}")
            
        return False

    def run_all_tests(self):
        """Run comprehensive test suite"""
        print("🚀 Starting Comprehensive API Testing...")
        print()
        
        # Core functionality tests
        tests = [
            ("Health Check", self.test_health_check),
            ("Dashboard Stats (10K+ Questions)", self.test_dashboard_stats),
            ("System Health", self.test_system_health),
            ("Questions Endpoint", self.test_questions_endpoint),
            ("Categories", self.test_categories_endpoint),
            ("Scraping Config", self.test_scraping_config),
            ("Scraping Jobs", self.test_scraping_jobs),
            ("Question Creation", self.test_question_creation),
            ("Filtered Questions", self.test_filtered_questions),
        ]
        
        for test_name, test_func in tests:
            try:
                test_func()
            except Exception as e:
                self.log_test(test_name, False, f"Test execution error: {str(e)}")
        
        self.print_summary()
        return self.tests_passed == self.tests_run

    def print_summary(self):
        """Print test summary"""
        print("=" * 60)
        print("📊 TEST SUMMARY")
        print("=" * 60)
        
        success_rate = (self.tests_passed / self.tests_run * 100) if self.tests_run > 0 else 0
        
        print(f"✅ Tests Passed: {self.tests_passed}")
        print(f"❌ Tests Failed: {self.tests_run - self.tests_passed}")
        print(f"📈 Success Rate: {success_rate:.1f}%")
        print()
        
        # Show failed tests
        failed_tests = [r for r in self.test_results if not r["success"]]
        if failed_tests:
            print("❌ FAILED TESTS:")
            for test in failed_tests:
                print(f"   • {test['test']}: {test['details']}")
            print()
        
        # Show key achievements
        stats_test = next((r for r in self.test_results if r["test"] == "Dashboard Stats (10K+ Questions)"), None)
        if stats_test and stats_test["success"]:
            print("🎉 KEY ACHIEVEMENTS:")
            if "response_sample" in stats_test:
                sample = stats_test["response_sample"]
                print(f"   • Total Questions: {sample.get('total_questions', 'N/A'):,}")
                print(f"   • Categories Covered: {sample.get('categories_covered', 'N/A')}")
                print(f"   • Average Quality Score: {sample.get('avg_quality_score', 'N/A')}%")
        
        print("=" * 60)

def main():
    """Main test execution"""
    tester = AptitudeQuestionBankTester()
    
    success = tester.run_all_tests()
    
    if success:
        print("🎯 ALL TESTS PASSED! System is ready for production.")
        return 0
    else:
        print("⚠️  Some tests failed. Please review the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())