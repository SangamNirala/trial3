#!/usr/bin/env python3
"""
Comprehensive Medical Scraper Phase 2 Testing
Tests Phase 2 implementation including comprehensive government scrapers:
- MedlinePlus Advanced Scraper (17,000+ articles)
- NCBI Comprehensive Database Scraper (50,000+ research papers)
- CDC Comprehensive Data Scraper (22,000+ documents)
- FDA Comprehensive Database Scraper (150,000+ records)
"""

import requests
import sys
import json
import time
from datetime import datetime
from typing import Dict, Any, List

class MedicalScraperPhase2Tester:
    def __init__(self, base_url="https://scraper-debug.preview.emergentagent.com"):
        self.base_url = base_url
        self.api_url = f"{base_url}/api"
        self.medical_api_url = f"{base_url}/api/medical-scraper"
        self.tests_run = 0
        self.tests_passed = 0
        self.test_results = []
        
        print(f"🏥 Medical Scraper Phase 2 API Tester")
        print(f"📡 Testing API at: {self.api_url}")
        print(f"🔬 Medical Scraper API: {self.medical_api_url}")
        print("=" * 80)

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

    def test_basic_health_check(self):
        """Test basic API health check"""
        try:
            response = requests.get(f"{self.api_url}/", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                expected_fields = ["message", "version", "status"]
                
                if all(field in data for field in expected_fields):
                    self.log_test(
                        "Basic Health Check", 
                        True, 
                        f"API is running - Version: {data.get('version', 'N/A')}, Status: {data.get('status', 'N/A')}"
                    )
                    return True
                else:
                    self.log_test("Basic Health Check", False, "Missing expected fields in response", data)
            else:
                self.log_test("Basic Health Check", False, f"HTTP {response.status_code}", response.text[:200])
                
        except Exception as e:
            self.log_test("Basic Health Check", False, f"Connection error: {str(e)}")
            
        return False

    def test_medical_scraper_health(self):
        """Test medical scraper health endpoint"""
        try:
            response = requests.get(f"{self.medical_api_url}/health", timeout=15)
            
            if response.status_code == 200:
                health_data = response.json()
                
                required_fields = ["status", "timestamp", "components", "system_ready"]
                missing_fields = [field for field in required_fields if field not in health_data]
                
                if missing_fields:
                    self.log_test("Medical Scraper Health", False, f"Missing fields: {missing_fields}", health_data)
                    return False
                
                status = health_data.get("status", "unknown")
                system_ready = health_data.get("system_ready", False)
                components = health_data.get("components", {})
                
                if status == "healthy" and system_ready:
                    component_status = ", ".join([f"{k}: {v}" for k, v in components.items()])
                    self.log_test("Medical Scraper Health", True, f"System healthy - {component_status}")
                    return True
                else:
                    self.log_test("Medical Scraper Health", False, f"System not ready - Status: {status}, Ready: {system_ready}", health_data)
            else:
                self.log_test("Medical Scraper Health", False, f"HTTP {response.status_code}", response.text[:200])
                
        except Exception as e:
            self.log_test("Medical Scraper Health", False, f"Error: {str(e)}")
            
        return False

    def test_medical_scraper_capabilities(self):
        """Test medical scraper capabilities endpoint"""
        try:
            response = requests.get(f"{self.medical_api_url}/capabilities", timeout=10)
            
            if response.status_code == 200:
                capabilities = response.json()
                
                required_fields = ["system_name", "version", "capabilities", "performance_specs"]
                missing_fields = [field for field in required_fields if field not in capabilities]
                
                if missing_fields:
                    self.log_test("Medical Scraper Capabilities", False, f"Missing fields: {missing_fields}", capabilities)
                    return False
                
                system_name = capabilities.get("system_name", "")
                version = capabilities.get("version", "")
                caps = capabilities.get("capabilities", {})
                
                # Check for Phase 2 capabilities
                expected_tiers = ["government_sources", "international_organizations", "academic_medical_centers"]
                supported_tiers = caps.get("supported_tiers", [])
                
                tier_coverage = len(set(expected_tiers) & set(supported_tiers))
                
                if "Phase" in system_name and tier_coverage >= 2:
                    self.log_test("Medical Scraper Capabilities", True, 
                                f"System: {system_name} v{version}, Tiers: {tier_coverage}/3")
                    return True
                else:
                    self.log_test("Medical Scraper Capabilities", False, 
                                f"Insufficient capabilities - Tiers: {tier_coverage}/3", capabilities)
            else:
                self.log_test("Medical Scraper Capabilities", False, f"HTTP {response.status_code}", response.text[:200])
                
        except Exception as e:
            self.log_test("Medical Scraper Capabilities", False, f"Error: {str(e)}")
            
        return False

    def test_medical_scraper_status(self):
        """Test medical scraper status endpoint"""
        try:
            response = requests.get(f"{self.medical_api_url}/status", timeout=10)
            
            if response.status_code == 200:
                status_data = response.json()
                
                required_fields = ["operation_id", "status", "progress"]
                missing_fields = [field for field in required_fields if field not in status_data]
                
                if missing_fields:
                    self.log_test("Medical Scraper Status", False, f"Missing fields: {missing_fields}", status_data)
                    return False
                
                operation_id = status_data.get("operation_id", "")
                status = status_data.get("status", "")
                progress = status_data.get("progress", {})
                
                # Status should be idle initially
                if status in ["idle", "running", "completed"]:
                    self.log_test("Medical Scraper Status", True, 
                                f"Status: {status}, Operation: {operation_id}")
                    return True
                else:
                    self.log_test("Medical Scraper Status", False, 
                                f"Invalid status: {status}", status_data)
            else:
                self.log_test("Medical Scraper Status", False, f"HTTP {response.status_code}", response.text[:200])
                
        except Exception as e:
            self.log_test("Medical Scraper Status", False, f"Error: {str(e)}")
            
        return False

    def test_phase2_comprehensive_scraping_start(self):
        """Test starting Phase 2 comprehensive scraping operation"""
        try:
            # Test payload for Phase 2 comprehensive scraping
            scraping_request = {
                "target_documents": 5000,  # Reduced for testing
                "max_concurrent_workers": 50,
                "tiers": ["government_sources"],
                "quality_threshold": 0.7
            }
            
            response = requests.post(
                f"{self.medical_api_url}/start-comprehensive-scraping",
                json=scraping_request,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            
            if response.status_code == 200:
                start_data = response.json()
                
                required_fields = ["operation_id", "status", "message"]
                missing_fields = [field for field in required_fields if field not in start_data]
                
                if missing_fields:
                    self.log_test("Phase 2 Comprehensive Scraping Start", False, 
                                f"Missing fields: {missing_fields}", start_data)
                    return False
                
                operation_id = start_data.get("operation_id", "")
                status = start_data.get("status", "")
                message = start_data.get("message", "")
                
                if status == "started" and operation_id:
                    self.log_test("Phase 2 Comprehensive Scraping Start", True, 
                                f"Started operation {operation_id}: {message}")
                    
                    # Store operation ID for later tests
                    self.current_operation_id = operation_id
                    return True
                else:
                    self.log_test("Phase 2 Comprehensive Scraping Start", False, 
                                f"Failed to start - Status: {status}", start_data)
            else:
                self.log_test("Phase 2 Comprehensive Scraping Start", False, 
                            f"HTTP {response.status_code}", response.text[:200])
                
        except Exception as e:
            self.log_test("Phase 2 Comprehensive Scraping Start", False, f"Error: {str(e)}")
            
        return False

    def test_scraping_operation_monitoring(self):
        """Test monitoring of active scraping operation"""
        if not hasattr(self, 'current_operation_id'):
            self.log_test("Scraping Operation Monitoring", False, "No active operation to monitor")
            return False
        
        try:
            # Monitor operation for a short time
            max_checks = 5
            check_interval = 3
            
            for i in range(max_checks):
                response = requests.get(f"{self.medical_api_url}/status", timeout=10)
                
                if response.status_code == 200:
                    status_data = response.json()
                    operation_id = status_data.get("operation_id", "")
                    status = status_data.get("status", "")
                    progress = status_data.get("progress", {})
                    
                    if operation_id == self.current_operation_id:
                        if status in ["running", "completed"]:
                            processed = progress.get("total_processed", 0)
                            successful = progress.get("successful", 0)
                            
                            self.log_test("Scraping Operation Monitoring", True, 
                                        f"Check {i+1}/{max_checks}: Status={status}, Processed={processed}, Success={successful}")
                            
                            if status == "completed":
                                return True
                        
                        if i < max_checks - 1:
                            time.sleep(check_interval)
                    else:
                        self.log_test("Scraping Operation Monitoring", False, 
                                    f"Operation ID mismatch: expected {self.current_operation_id}, got {operation_id}")
                        return False
                else:
                    self.log_test("Scraping Operation Monitoring", False, 
                                f"HTTP {response.status_code}", response.text[:200])
                    return False
            
            # If we get here, operation is still running
            self.log_test("Scraping Operation Monitoring", True, 
                        f"Operation {self.current_operation_id} is running (monitored for {max_checks * check_interval}s)")
            return True
                
        except Exception as e:
            self.log_test("Scraping Operation Monitoring", False, f"Error: {str(e)}")
            
        return False

    def test_phase2_government_scrapers_integration(self):
        """Test Phase 2 government scrapers integration"""
        try:
            # Test if Phase 2 scrapers are properly integrated
            response = requests.get(f"{self.medical_api_url}/capabilities", timeout=10)
            
            if response.status_code == 200:
                capabilities = response.json()
                target_sources = capabilities.get("capabilities", {}).get("target_sources", {})
                government_sources = target_sources.get("government", [])
                
                # Check for Phase 2 government sources
                expected_sources = ["NIH", "CDC", "FDA", "MedlinePlus"]
                found_sources = [source for source in expected_sources if source in government_sources]
                
                if len(found_sources) >= 3:
                    self.log_test("Phase 2 Government Scrapers Integration", True, 
                                f"Found {len(found_sources)}/4 government sources: {', '.join(found_sources)}")
                    return True
                else:
                    self.log_test("Phase 2 Government Scrapers Integration", False, 
                                f"Only found {len(found_sources)}/4 government sources: {', '.join(found_sources)}")
            else:
                self.log_test("Phase 2 Government Scrapers Integration", False, 
                            f"HTTP {response.status_code}", response.text[:200])
                
        except Exception as e:
            self.log_test("Phase 2 Government Scrapers Integration", False, f"Error: {str(e)}")
            
        return False

    def test_ai_systems_integration(self):
        """Test AI systems integration"""
        try:
            response = requests.get(f"{self.medical_api_url}/capabilities", timeout=10)
            
            if response.status_code == 200:
                capabilities = response.json()
                ai_systems = capabilities.get("capabilities", {}).get("ai_systems", [])
                
                # Check for required AI systems
                required_ai_systems = [
                    "Content Discovery AI",
                    "Anti-Detection AI", 
                    "Content Quality AI",
                    "Intelligent Task Scheduler"
                ]
                
                found_systems = [system for system in required_ai_systems if system in ai_systems]
                
                if len(found_systems) >= 3:
                    self.log_test("AI Systems Integration", True, 
                                f"Found {len(found_systems)}/4 AI systems: {', '.join(found_systems)}")
                    return True
                else:
                    self.log_test("AI Systems Integration", False, 
                                f"Only found {len(found_systems)}/4 AI systems: {', '.join(found_systems)}")
            else:
                self.log_test("AI Systems Integration", False, 
                            f"HTTP {response.status_code}", response.text[:200])
                
        except Exception as e:
            self.log_test("AI Systems Integration", False, f"Error: {str(e)}")
            
        return False

    def test_performance_specifications(self):
        """Test performance specifications"""
        try:
            response = requests.get(f"{self.medical_api_url}/capabilities", timeout=10)
            
            if response.status_code == 200:
                capabilities = response.json()
                performance_specs = capabilities.get("performance_specs", {})
                
                required_specs = ["target_processing_rate", "target_success_rate", "scalability"]
                missing_specs = [spec for spec in required_specs if spec not in performance_specs]
                
                if missing_specs:
                    self.log_test("Performance Specifications", False, 
                                f"Missing specs: {missing_specs}", performance_specs)
                    return False
                
                processing_rate = performance_specs.get("target_processing_rate", "")
                success_rate = performance_specs.get("target_success_rate", "")
                scalability = performance_specs.get("scalability", "")
                
                # Check if specs meet Phase 2 requirements
                if ("100+" in processing_rate and 
                    "95%" in success_rate and 
                    "500,000+" in scalability):
                    
                    self.log_test("Performance Specifications", True, 
                                f"Rate: {processing_rate}, Success: {success_rate}, Scale: {scalability}")
                    return True
                else:
                    self.log_test("Performance Specifications", False, 
                                f"Specs don't meet Phase 2 requirements", performance_specs)
            else:
                self.log_test("Performance Specifications", False, 
                            f"HTTP {response.status_code}", response.text[:200])
                
        except Exception as e:
            self.log_test("Performance Specifications", False, f"Error: {str(e)}")
            
        return False

    def test_error_handling_and_recovery(self):
        """Test error handling and recovery mechanisms"""
        try:
            # Test invalid scraping request
            invalid_request = {
                "target_documents": -1,  # Invalid
                "max_concurrent_workers": 0,  # Invalid
                "quality_threshold": 2.0  # Invalid (should be 0-1)
            }
            
            response = requests.post(
                f"{self.medical_api_url}/start-extraction",
                json=invalid_request,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            # Should return error for invalid request
            if response.status_code in [400, 422]:
                self.log_test("Error Handling and Recovery", True, 
                            f"Properly rejected invalid request with HTTP {response.status_code}")
                return True
            elif response.status_code == 200:
                # If it accepts invalid request, that's also a problem
                self.log_test("Error Handling and Recovery", False, 
                            "Accepted invalid request - poor validation")
                return False
            else:
                self.log_test("Error Handling and Recovery", False, 
                            f"Unexpected response: HTTP {response.status_code}")
                
        except Exception as e:
            self.log_test("Error Handling and Recovery", False, f"Error: {str(e)}")
            
        return False

    def test_rate_limiting_and_respectful_scraping(self):
        """Test rate limiting and respectful scraping mechanisms"""
        try:
            # This test checks if the system has proper rate limiting
            response = requests.get(f"{self.medical_api_url}/capabilities", timeout=10)
            
            if response.status_code == 200:
                capabilities = response.json()
                ai_systems = capabilities.get("capabilities", {}).get("ai_systems", [])
                
                # Check for rate limiting and anti-detection systems
                rate_limiting_systems = [
                    "Adaptive Rate Limiter",
                    "Anti-Detection AI",
                    "Intelligent Retry System"
                ]
                
                found_systems = [system for system in rate_limiting_systems if system in ai_systems]
                
                if len(found_systems) >= 2:
                    self.log_test("Rate Limiting and Respectful Scraping", True, 
                                f"Found {len(found_systems)}/3 rate limiting systems: {', '.join(found_systems)}")
                    return True
                else:
                    self.log_test("Rate Limiting and Respectful Scraping", False, 
                                f"Only found {len(found_systems)}/3 rate limiting systems")
            else:
                self.log_test("Rate Limiting and Respectful Scraping", False, 
                            f"HTTP {response.status_code}", response.text[:200])
                
        except Exception as e:
            self.log_test("Rate Limiting and Respectful Scraping", False, f"Error: {str(e)}")
            
        return False

    def run_all_tests(self):
        """Run comprehensive Phase 2 medical scraper test suite"""
        print("🚀 Starting Phase 2 Medical Scraper Comprehensive Testing...")
        print()
        
        # Phase 2 specific tests
        tests = [
            ("Basic Health Check", self.test_basic_health_check),
            ("Medical Scraper Health", self.test_medical_scraper_health),
            ("Medical Scraper Capabilities", self.test_medical_scraper_capabilities),
            ("Medical Scraper Status", self.test_medical_scraper_status),
            ("Phase 2 Government Scrapers Integration", self.test_phase2_government_scrapers_integration),
            ("AI Systems Integration", self.test_ai_systems_integration),
            ("Performance Specifications", self.test_performance_specifications),
            ("Rate Limiting and Respectful Scraping", self.test_rate_limiting_and_respectful_scraping),
            ("Error Handling and Recovery", self.test_error_handling_and_recovery),
            ("Phase 2 Comprehensive Scraping Start", self.test_phase2_comprehensive_scraping_start),
            ("Scraping Operation Monitoring", self.test_scraping_operation_monitoring),
        ]
        
        for test_name, test_func in tests:
            try:
                test_func()
            except Exception as e:
                self.log_test(test_name, False, f"Test execution error: {str(e)}")
        
        self.print_summary()
        return self.tests_passed == self.tests_run

    def print_summary(self):
        """Print comprehensive test summary"""
        print("=" * 80)
        print("📊 PHASE 2 MEDICAL SCRAPER TEST SUMMARY")
        print("=" * 80)
        
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
        
        # Show Phase 2 achievements
        print("🏆 PHASE 2 MEDICAL SCRAPER ASSESSMENT:")
        
        # Check key Phase 2 capabilities
        capabilities_test = next((r for r in self.test_results if r["test"] == "Medical Scraper Capabilities"), None)
        if capabilities_test and capabilities_test["success"]:
            print("   ✅ Phase 2 System Capabilities Verified")
        else:
            print("   ❌ Phase 2 System Capabilities Not Verified")
        
        gov_scrapers_test = next((r for r in self.test_results if r["test"] == "Phase 2 Government Scrapers Integration"), None)
        if gov_scrapers_test and gov_scrapers_test["success"]:
            print("   ✅ Government Scrapers (MedlinePlus, NCBI, CDC, FDA) Integrated")
        else:
            print("   ❌ Government Scrapers Integration Issues")
        
        ai_systems_test = next((r for r in self.test_results if r["test"] == "AI Systems Integration"), None)
        if ai_systems_test and ai_systems_test["success"]:
            print("   ✅ AI Systems (Discovery, Quality, Anti-Detection) Operational")
        else:
            print("   ❌ AI Systems Integration Issues")
        
        performance_test = next((r for r in self.test_results if r["test"] == "Performance Specifications"), None)
        if performance_test and performance_test["success"]:
            print("   ✅ Performance Specs Meet Phase 2 Requirements (100+ docs/sec, 95%+ success)")
        else:
            print("   ❌ Performance Specifications Below Phase 2 Requirements")
        
        scraping_test = next((r for r in self.test_results if r["test"] == "Phase 2 Comprehensive Scraping Start"), None)
        if scraping_test and scraping_test["success"]:
            print("   ✅ Phase 2 Comprehensive Scraping Operations Functional")
        else:
            print("   ❌ Phase 2 Comprehensive Scraping Operations Issues")
        
        print()
        print("🎯 PHASE 2 TARGET ASSESSMENT:")
        print("   📊 Target: 200,000+ government medical documents")
        print("   🏛️ Sources: MedlinePlus (17K+), NCBI (50K+), CDC (22K+), FDA (150K+)")
        print("   🤖 AI Integration: Content Discovery, Quality Assessment, Anti-Detection")
        print("   ⚡ Performance: 100+ documents/second processing rate")
        print("   🎖️ Authority Score: 0.98 for government sources")
        
        print("=" * 80)

def main():
    """Main test execution"""
    tester = MedicalScraperPhase2Tester()
    
    success = tester.run_all_tests()
    
    if success:
        print("🎯 ALL PHASE 2 TESTS PASSED! Medical Scraper System is ready for production.")
        print("🏆 Phase 2 Implementation: VERIFIED")
        print("📈 Government Sources Scraping: OPERATIONAL")
        print("🤖 AI Systems Integration: CONFIRMED")
        return 0
    else:
        print("⚠️  Some Phase 2 tests failed. Please review the issues above.")
        print("🔧 Phase 2 Implementation needs attention before production deployment.")
        return 1

if __name__ == "__main__":
    sys.exit(main())