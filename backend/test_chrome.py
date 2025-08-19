"""
Test Chrome/ChromeDriver setup to ensure scraping functionality works
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_chrome_setup():
    """Test Chrome and ChromeDriver functionality"""
    driver = None
    try:
        # Configure Chrome options
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # Run in headless mode
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Create driver
        service = Service('/usr/bin/chromedriver')
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        logger.info("✅ Chrome WebDriver created successfully")
        
        # Test basic navigation
        driver.get("https://www.google.com")
        logger.info("✅ Successfully navigated to Google")
        
        # Test element finding
        search_box = driver.find_element(By.NAME, "q")
        search_box.send_keys("IndiaBix aptitude questions")
        logger.info("✅ Successfully interacted with page elements")
        
        # Test IndiaBix access
        driver.get("https://www.indiabix.com")
        time.sleep(3)
        
        title = driver.title
        logger.info(f"✅ Successfully accessed IndiaBix. Title: {title}")
        
        # Test specific page
        driver.get("https://www.indiabix.com/aptitude/percentage/")
        time.sleep(3)
        
        page_title = driver.title
        logger.info(f"✅ Successfully accessed IndiaBix percentage page. Title: {page_title}")
        
        logger.info("🎉 All Chrome/ChromeDriver tests passed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Chrome setup test failed: {e}")
        return False
    finally:
        if driver:
            driver.quit()
            logger.info("🔒 Chrome driver closed")

if __name__ == "__main__":
    print("Testing Chrome/ChromeDriver setup...")
    success = test_chrome_setup()
    if success:
        print("✅ Setup test completed successfully!")
    else:
        print("❌ Setup test failed!")