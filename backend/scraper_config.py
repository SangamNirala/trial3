"""
Configuration file for IndiaBix scraping operations
Contains all URLs, selectors, and settings needed for scraping
"""

from dataclasses import dataclass
from typing import List, Dict, Optional
from fake_useragent import UserAgent

@dataclass
class ScrapingConfig:
    # Anti-detection settings
    USER_AGENTS: List[str] = None
    MIN_DELAY: float = 2.0
    MAX_DELAY: float = 8.0
    REQUEST_TIMEOUT: int = 30
    MAX_RETRIES: int = 3
    
    # Chrome options
    HEADLESS: bool = True
    WINDOW_SIZE: str = "1920,1080"
    DISABLE_IMAGES: bool = True
    DISABLE_CSS: bool = False
    
    def __post_init__(self):
        if self.USER_AGENTS is None:
            ua = UserAgent()
            self.USER_AGENTS = [
                ua.chrome,
                ua.firefox, 
                ua.safari,
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
            ]

# IndiaBix specific configuration
INDIABIX_CONFIG = {
    "base_url": "https://www.indiabix.com",
    "categories": {
        "quantitative_aptitude": {
            "display_name": "Quantitative Aptitude",
            "subcategories": {
                "percentage": {
                    "url": "/aptitude/percentage/",
                    "display_name": "Percentage",
                    "target_questions": 400
                },
                "profit_and_loss": {
                    "url": "/aptitude/profit-and-loss/",
                    "display_name": "Profit and Loss", 
                    "target_questions": 400
                },
                "simple_interest": {
                    "url": "/aptitude/simple-interest/",
                    "display_name": "Simple Interest",
                    "target_questions": 300
                },
                "compound_interest": {
                    "url": "/aptitude/compound-interest/",
                    "display_name": "Compound Interest",
                    "target_questions": 300
                },
                "time_and_work": {
                    "url": "/aptitude/time-and-work/",
                    "display_name": "Time and Work",
                    "target_questions": 400
                },
                "time_speed_distance": {
                    "url": "/aptitude/time-and-distance/",
                    "display_name": "Time, Speed and Distance",
                    "target_questions": 400
                },
                "algebra": {
                    "url": "/aptitude/algebra/",
                    "display_name": "Algebra",
                    "target_questions": 300
                },
                "geometry": {
                    "url": "/aptitude/geometry/",
                    "display_name": "Geometry",
                    "target_questions": 300
                },
                "area": {
                    "url": "/aptitude/area/",
                    "display_name": "Area",
                    "target_questions": 300
                },
                "volume_surface_area": {
                    "url": "/aptitude/volume-and-surface-area/",
                    "display_name": "Volume and Surface Area",
                    "target_questions": 300
                },
                "permutation_combination": {
                    "url": "/aptitude/permutation-and-combination/",
                    "display_name": "Permutation and Combination",
                    "target_questions": 300
                },
                "probability": {
                    "url": "/aptitude/probability/",
                    "display_name": "Probability",
                    "target_questions": 300
                },
                "pipes_cisterns": {
                    "url": "/aptitude/pipes-and-cisterns/",
                    "display_name": "Pipes and Cisterns",
                    "target_questions": 200
                },
                "boats_streams": {
                    "url": "/aptitude/boats-and-streams/",
                    "display_name": "Boats and Streams",
                    "target_questions": 200
                },
                "alligation": {
                    "url": "/aptitude/alligation-or-mixture/",
                    "display_name": "Alligation or Mixture",
                    "target_questions": 200
                }
            }
        },
        "logical_reasoning": {
            "display_name": "Logical Reasoning",
            "subcategories": {
                "series": {
                    "url": "/logical-reasoning/series-completion/",
                    "display_name": "Series Completion",
                    "target_questions": 400
                },
                "analogies": {
                    "url": "/logical-reasoning/analogies/",
                    "display_name": "Analogies",
                    "target_questions": 300
                },
                "coding_decoding": {
                    "url": "/logical-reasoning/coding-decoding/",
                    "display_name": "Coding Decoding",
                    "target_questions": 300
                },
                "blood_relations": {
                    "url": "/logical-reasoning/blood-relation-test/",
                    "display_name": "Blood Relations",
                    "target_questions": 300
                },
                "direction_sense": {
                    "url": "/logical-reasoning/direction-sense-test/",
                    "display_name": "Direction Sense Test",
                    "target_questions": 200
                },
                "logical_sequence": {
                    "url": "/logical-reasoning/logical-sequence-of-words/",
                    "display_name": "Logical Sequence of Words",
                    "target_questions": 200
                },
                "puzzles": {
                    "url": "/logical-reasoning/logical-problems/",
                    "display_name": "Logical Problems",
                    "target_questions": 400
                },
                "seating_arrangement": {
                    "url": "/logical-reasoning/seating-arrangement/",
                    "display_name": "Seating Arrangement",
                    "target_questions": 300
                },
                "syllogism": {
                    "url": "/logical-reasoning/syllogism/",
                    "display_name": "Syllogism",
                    "target_questions": 300
                }
            }
        },
        "verbal_ability": {
            "display_name": "Verbal Ability",
            "subcategories": {
                "synonyms": {
                    "url": "/verbal-ability/synonyms/",
                    "display_name": "Synonyms",
                    "target_questions": 400
                },
                "antonyms": {
                    "url": "/verbal-ability/antonyms/",
                    "display_name": "Antonyms",
                    "target_questions": 400
                },
                "sentence_completion": {
                    "url": "/verbal-ability/sentence-completion/",
                    "display_name": "Sentence Completion",
                    "target_questions": 300
                },
                "sentence_improvement": {
                    "url": "/verbal-ability/sentence-improvement/",
                    "display_name": "Sentence Improvement",
                    "target_questions": 300
                },
                "comprehension": {
                    "url": "/verbal-ability/comprehension/",
                    "display_name": "Reading Comprehension",
                    "target_questions": 300
                },
                "error_detection": {
                    "url": "/verbal-ability/spotting-errors/",
                    "display_name": "Spotting Errors",
                    "target_questions": 300
                },
                "fill_blanks": {
                    "url": "/verbal-ability/fill-in-the-blanks/",
                    "display_name": "Fill in the Blanks",
                    "target_questions": 200
                },
                "para_jumbles": {
                    "url": "/verbal-ability/para-jumbles/",
                    "display_name": "Para Jumbles",
                    "target_questions": 200
                }
            }
        },
        "general_knowledge": {
            "display_name": "General Knowledge",
            "subcategories": {
                "history": {
                    "url": "/general-knowledge/history/",
                    "display_name": "History",
                    "target_questions": 300
                },
                "geography": {
                    "url": "/general-knowledge/geography/",
                    "display_name": "Geography", 
                    "target_questions": 300
                },
                "current_affairs": {
                    "url": "/current-affairs/",
                    "display_name": "Current Affairs",
                    "target_questions": 200
                },
                "politics": {
                    "url": "/general-knowledge/politics/",
                    "display_name": "Politics",
                    "target_questions": 200
                }
            }
        }
    }
}

# CSS selectors for IndiaBix (updated for current website structure)
INDIABIX_SELECTORS = {
    "question_container": ".question-section, .aptitude-question, .qa-question",
    "question_text": ".question-text, .question p, h4, .question-title",
    "options_container": ".options, .answer-options, .choices",
    "option": ".option, .choice, li",
    "answer_container": ".answer, .correct-answer, .solution",
    "explanation": ".explanation, .answer-description, .solution-text", 
    "next_button": ".next-question, .next, a[href*='next']",
    "pagination": ".pagination, .page-nav",
    "category_links": ".nav-item a, .category-link",
    "question_number": ".question-no, .q-number, .question-count",
    
    # Alternative selectors for fallback
    "fallback_question": "h1, h2, h3, h4, h5, h6, p",
    "fallback_options": "ul li, ol li, .list-group-item",
    "fallback_links": "a[href*='percentage'], a[href*='aptitude'], a[href*='question']"
}

# Quality thresholds
QUALITY_THRESHOLDS = {
    "min_question_length": 10,
    "max_question_length": 2000,
    "min_option_length": 1,
    "max_option_length": 200,
    "required_options": 4,
    "similarity_threshold": 0.85  # For duplicate detection
}

# Default scraping configuration
DEFAULT_SCRAPING_CONFIG = ScrapingConfig()