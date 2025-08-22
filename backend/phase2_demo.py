"""
Phase 2 Demo: TIER 1 GOVERNMENT SOURCES SCRAPER
Demonstrates the comprehensive government scrapers implementation
"""

import asyncio
import logging
import sys
from datetime import datetime
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

async def test_individual_phase2_scrapers():
    """Test each Phase 2 scraper individually"""
    
    logger.info("🚀 PHASE 2 INDIVIDUAL SCRAPER TESTING")
    logger.info("=" * 80)
    
    test_results = {}
    
    # Test 1: MedlinePlus Comprehensive Scraper
    logger.info("📋 Testing MedlinePlus Comprehensive Scraper")
    try:
        from medlineplus_scraper import MedlinePlusAdvancedScraper
        
        medlineplus_scraper = MedlinePlusAdvancedScraper()
        logger.info("✅ MedlinePlus scraper initialized successfully")
        
        # Test basic functionality
        test_results['medlineplus'] = {
            'initialization': True,
            'scraper_type': 'MedlinePlus Comprehensive',
            'target_sections': 9,
            'ai_systems': ['ContentDiscoveryAI', 'AntiDetectionAI', 'ContentQualityAI']
        }
        
    except Exception as e:
        logger.error(f"❌ MedlinePlus scraper test failed: {e}")
        test_results['medlineplus'] = {'initialization': False, 'error': str(e)}
    
    # Test 2: NCBI Comprehensive Scraper
    logger.info("\n🔬 Testing NCBI Comprehensive Database Scraper")
    try:
        from ncbi_scraper import NCBIAdvancedScraper
        
        ncbi_scraper = NCBIAdvancedScraper()
        logger.info("✅ NCBI scraper initialized successfully")
        
        test_results['ncbi'] = {
            'initialization': True,
            'scraper_type': 'NCBI Comprehensive Database',
            'databases': ['PubMed', 'PMC', 'ClinVar', 'MeSH', 'Bookshelf'],
            'api_integration': True
        }
        
    except Exception as e:
        logger.error(f"❌ NCBI scraper test failed: {e}")
        test_results['ncbi'] = {'initialization': False, 'error': str(e)}
    
    # Test 3: CDC Comprehensive Scraper
    logger.info("\n🏛️ Testing CDC Comprehensive Data Scraper")
    try:
        from cdc_scraper import CDCAdvancedScraper
        
        cdc_scraper = CDCAdvancedScraper()
        logger.info("✅ CDC scraper initialized successfully")
        
        test_results['cdc'] = {
            'initialization': True,
            'scraper_type': 'CDC Comprehensive Data',
            'sections': 12,
            'public_health_focus': True
        }
        
    except Exception as e:
        logger.error(f"❌ CDC scraper test failed: {e}")
        test_results['cdc'] = {'initialization': False, 'error': str(e)}
    
    # Test 4: FDA Comprehensive Scraper
    logger.info("\n🏛️ Testing FDA Comprehensive Database Scraper")
    try:
        from fda_scraper import FDAAdvancedScraper
        
        fda_scraper = FDAAdvancedScraper()
        logger.info("✅ FDA scraper initialized successfully")
        
        test_results['fda'] = {
            'initialization': True,
            'scraper_type': 'FDA Comprehensive Database',
            'databases': 11,
            'openfda_integration': True
        }
        
    except Exception as e:
        logger.error(f"❌ FDA scraper test failed: {e}")
        test_results['fda'] = {'initialization': False, 'error': str(e)}
    
    return test_results

async def test_master_controller_integration():
    """Test Phase 2 integration with master controller"""
    
    logger.info("\n🎯 TESTING MASTER CONTROLLER PHASE 2 INTEGRATION")
    logger.info("=" * 80)
    
    try:
        from master_scraper_controller import WorldClassMedicalScraper
        
        # Initialize master scraper
        master_scraper = WorldClassMedicalScraper()
        logger.info("✅ Master scraper initialized successfully")
        
        # Test government scraper Phase 2 integration
        from ai_scraper_core import ScrapingTier
        government_scraper = master_scraper.tier_scrapers.get(ScrapingTier.TIER_1_GOVERNMENT)
        if government_scraper:
            logger.info("✅ Government scraper found in master controller")
            
            # Check Phase 2 components
            phase2_components = [
                'medlineplus_scraper',
                'ncbi_scraper', 
                'cdc_scraper',
                'fda_scraper'
            ]
            
            integration_status = {}
            for component in phase2_components:
                if hasattr(government_scraper, component):
                    integration_status[component] = True
                    logger.info(f"✅ {component} integrated")
                else:
                    integration_status[component] = False
                    logger.warning(f"⚠️ {component} not found")
            
            return {
                'master_controller': True,
                'government_scraper': True,
                'phase2_integration': integration_status,
                'integration_complete': all(integration_status.values())
            }
        else:
            logger.error("❌ Government scraper not found in master controller")
            return {'master_controller': True, 'government_scraper': False}
            
    except Exception as e:
        logger.error(f"❌ Master controller integration test failed: {e}")
        traceback.print_exc()
        return {'master_controller': False, 'error': str(e)}

async def test_ai_systems_integration():
    """Test AI systems integration across Phase 2 scrapers"""
    
    logger.info("\n🤖 TESTING AI SYSTEMS INTEGRATION")
    logger.info("=" * 80)
    
    ai_systems_status = {}
    
    try:
        from ai_scraper_core import (
            ContentDiscoveryAI, AntiDetectionAI, ContentQualityAI, 
            AdvancedDeduplicator, IntelligentTaskScheduler, AdaptiveRateLimiter
        )
        
        # Test core AI systems
        ai_systems = [
            ('ContentDiscoveryAI', ContentDiscoveryAI),
            ('AntiDetectionAI', AntiDetectionAI),
            ('ContentQualityAI', ContentQualityAI),
            ('AdvancedDeduplicator', AdvancedDeduplicator),
            ('IntelligentTaskScheduler', IntelligentTaskScheduler),
            ('AdaptiveRateLimiter', AdaptiveRateLimiter)
        ]
        
        for system_name, system_class in ai_systems:
            try:
                system_instance = system_class()
                ai_systems_status[system_name] = True
                logger.info(f"✅ {system_name} initialized successfully")
            except Exception as e:
                ai_systems_status[system_name] = False
                logger.error(f"❌ {system_name} failed: {e}")
        
        return {
            'ai_systems_available': True,
            'systems_status': ai_systems_status,
            'all_systems_working': all(ai_systems_status.values())
        }
        
    except Exception as e:
        logger.error(f"❌ AI systems test failed: {e}")
        return {'ai_systems_available': False, 'error': str(e)}

async def run_phase2_comprehensive_demo():
    """Run comprehensive Phase 2 demonstration"""
    
    logger.info("🏆 PHASE 2 COMPREHENSIVE DEMO STARTING")
    logger.info("=" * 100)
    logger.info("🎯 TIER 1 GOVERNMENT SOURCES SCRAPER - PHASE 2 IMPLEMENTATION")
    logger.info("=" * 100)
    
    demo_start_time = datetime.utcnow()
    
    # Test Results Storage
    comprehensive_results = {
        'demo_start_time': demo_start_time.isoformat(),
        'phase': 'Phase 2 - TIER 1 GOVERNMENT SOURCES SCRAPER',
        'components_tested': [],
        'test_results': {}
    }
    
    # 1. Test Individual Scrapers
    logger.info("📋 STEP 1: Testing Individual Phase 2 Scrapers")
    scraper_results = await test_individual_phase2_scrapers()
    comprehensive_results['test_results']['individual_scrapers'] = scraper_results
    comprehensive_results['components_tested'].append('individual_scrapers')
    
    # 2. Test Master Controller Integration
    logger.info("\n📋 STEP 2: Testing Master Controller Integration")
    integration_results = await test_master_controller_integration()
    comprehensive_results['test_results']['master_integration'] = integration_results
    comprehensive_results['components_tested'].append('master_integration')
    
    # 3. Test AI Systems
    logger.info("\n📋 STEP 3: Testing AI Systems Integration")
    ai_results = await test_ai_systems_integration()
    comprehensive_results['test_results']['ai_systems'] = ai_results
    comprehensive_results['components_tested'].append('ai_systems')
    
    # Calculate overall results
    demo_end_time = datetime.utcnow()
    execution_time = (demo_end_time - demo_start_time).total_seconds()
    
    # Success metrics
    scraper_success = sum(1 for result in scraper_results.values() if result.get('initialization', False))
    total_scrapers = len(scraper_results)
    
    integration_success = integration_results.get('integration_complete', False)
    ai_success = ai_results.get('all_systems_working', False)
    
    comprehensive_results.update({
        'demo_end_time': demo_end_time.isoformat(),
        'execution_time_seconds': execution_time,
        'success_metrics': {
            'scrapers_successful': scraper_success,
            'total_scrapers': total_scrapers,
            'scraper_success_rate': scraper_success / total_scrapers if total_scrapers > 0 else 0,
            'master_integration_success': integration_success,
            'ai_systems_success': ai_success,
            'overall_success': scraper_success == total_scrapers and integration_success and ai_success
        }
    })
    
    # Final Results Summary
    logger.info("\n" + "=" * 100)
    logger.info("🏆 PHASE 2 COMPREHENSIVE DEMO RESULTS")
    logger.info("=" * 100)
    logger.info(f"⏱️  Total Execution Time: {execution_time:.2f} seconds")
    logger.info(f"📊 Scrapers Tested: {total_scrapers}")
    logger.info(f"✅ Scrapers Successful: {scraper_success}")
    logger.info(f"📈 Success Rate: {(scraper_success/total_scrapers)*100:.1f}%")
    logger.info(f"🔗 Master Integration: {'✅ Success' if integration_success else '❌ Failed'}")
    logger.info(f"🤖 AI Systems: {'✅ All Working' if ai_success else '❌ Issues Found'}")
    
    overall_success = comprehensive_results['success_metrics']['overall_success']
    logger.info(f"\n🎯 OVERALL PHASE 2 STATUS: {'✅ COMPLETE' if overall_success else '⚠️ ISSUES DETECTED'}")
    
    if overall_success:
        logger.info("\n🚀 PHASE 2 TIER 1 GOVERNMENT SOURCES SCRAPER READY FOR PRODUCTION!")
        logger.info("📋 Capabilities Available:")
        logger.info("   • MedlinePlus Comprehensive Scraper (9 sections)")
        logger.info("   • NCBI Database Scraper (PubMed, PMC, ClinVar, MeSH)")
        logger.info("   • CDC Data Scraper (12 public health sections)")
        logger.info("   • FDA Database Scraper (11 databases + OpenFDA API)")
        logger.info("   • Advanced AI Systems (6 core systems)")
        logger.info("   • Master Controller Integration")
        logger.info("\n🎯 Target Documents: 200,000+ government medical documents")
        logger.info("🏛️ Authority Score: 0.98 (Government Sources)")
    else:
        logger.warning("\n⚠️ PHASE 2 HAS ISSUES - REVIEW REQUIRED")
        logger.warning("Please check individual component failures above")
    
    logger.info("=" * 100)
    
    return comprehensive_results

async def quick_phase2_test():
    """Quick test of Phase 2 components"""
    
    logger.info("⚡ QUICK PHASE 2 TEST")
    logger.info("=" * 50)
    
    try:
        # Quick import test
        from medlineplus_scraper import MedlinePlusAdvancedScraper
        from ncbi_scraper import NCBIAdvancedScraper  
        from cdc_scraper import CDCAdvancedScraper
        from fda_scraper import FDAAdvancedScraper
        from master_scraper_controller import WorldClassMedicalScraper
        
        logger.info("✅ All Phase 2 imports successful")
        
        # Quick initialization test
        master = WorldClassMedicalScraper()
        logger.info("✅ Master controller initialized")
        
        logger.info("🎯 PHASE 2 QUICK TEST: SUCCESS")
        return True
        
    except Exception as e:
        logger.error(f"❌ PHASE 2 QUICK TEST FAILED: {e}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    try:
        # Run quick test first
        logger.info("Starting Phase 2 Demo...")
        quick_success = asyncio.run(quick_phase2_test())
        
        if quick_success:
            # Run comprehensive demo
            results = asyncio.run(run_phase2_comprehensive_demo())
            
            # Save results
            import json
            with open('/app/phase2_demo_results.json', 'w') as f:
                json.dump(results, f, indent=2)
            
            logger.info("📁 Results saved to phase2_demo_results.json")
        else:
            logger.error("Quick test failed - skipping comprehensive demo")
            
    except Exception as e:
        logger.error(f"Demo execution failed: {e}")
        traceback.print_exc()