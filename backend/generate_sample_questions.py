"""
Generate high-quality sample aptitude questions to demonstrate the system
This creates a comprehensive question database following the exact requirements
"""

import asyncio
import json
from datetime import datetime
from typing import List, Dict
from database_service import DatabaseService
from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment
ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# Sample questions data following the exact structure requirements
SAMPLE_QUESTIONS = {
    "quantitative_aptitude": [
        {
            "subcategory": "percentage",
            "questions": [
                {
                    "question_text": "If 20% of a number is 80, what is 50% of that number?",
                    "options": ["200", "100", "150", "400"],
                    "correct_answer": "200",
                    "explanation": "If 20% of x = 80, then x = 80 × 100/20 = 400. Therefore, 50% of 400 = 400 × 50/100 = 200.",
                    "concepts": ["percentage", "basic_calculation", "proportion"],
                    "difficulty": "easy",
                    "time_estimate": 90
                },
                {
                    "question_text": "A shopkeeper marks his goods 40% above cost price and gives a discount of 15%. Find his profit percentage.",
                    "options": ["19%", "25%", "21%", "18%"],
                    "correct_answer": "19%",
                    "explanation": "Let CP = 100. MP = 140. SP = 140 - 15% of 140 = 140 - 21 = 119. Profit = 119 - 100 = 19%",
                    "concepts": ["percentage", "profit_loss", "discount", "markup"],
                    "difficulty": "medium", 
                    "time_estimate": 120
                },
                {
                    "question_text": "In an election, candidate A got 55% votes and won by 2400 votes. Find the total number of votes polled.",
                    "options": ["24000", "12000", "20000", "18000"],
                    "correct_answer": "24000",
                    "explanation": "A got 55%, B got 45%. Difference = 10% = 2400 votes. So 100% = 24000 votes",
                    "concepts": ["percentage", "election_problems", "proportion"],
                    "difficulty": "medium",
                    "time_estimate": 150
                },
                {
                    "question_text": "What percentage of 1 hour is 15 minutes?",
                    "options": ["25%", "15%", "20%", "30%"],
                    "correct_answer": "25%",
                    "explanation": "15 minutes out of 60 minutes = 15/60 × 100% = 25%",
                    "concepts": ["percentage", "time_calculation", "basic_percentage"],
                    "difficulty": "easy",
                    "time_estimate": 60
                }
            ]
        },
        {
            "subcategory": "profit_and_loss",
            "questions": [
                {
                    "question_text": "A man buys an article for Rs. 800 and sells it for Rs. 920. Find his profit percentage.",
                    "options": ["15%", "12%", "18%", "20%"],
                    "correct_answer": "15%",
                    "explanation": "Profit = 920 - 800 = 120. Profit% = (120/800) × 100 = 15%",
                    "concepts": ["profit_loss", "basic_calculation", "percentage"],
                    "difficulty": "easy",
                    "time_estimate": 90
                },
                {
                    "question_text": "If selling price is Rs. 1200 and loss is 20%, find the cost price.",
                    "options": ["Rs. 1500", "Rs. 1440", "Rs. 1600", "Rs. 1350"],
                    "correct_answer": "Rs. 1500",
                    "explanation": "If loss is 20%, then SP = 80% of CP. So 1200 = 80% of CP, CP = 1200 × 100/80 = 1500",
                    "concepts": ["profit_loss", "percentage", "reverse_calculation"],
                    "difficulty": "medium",
                    "time_estimate": 120
                }
            ]
        },
        {
            "subcategory": "simple_interest",
            "questions": [
                {
                    "question_text": "Find the simple interest on Rs. 5000 at 12% per annum for 3 years.",
                    "options": ["Rs. 1800", "Rs. 1500", "Rs. 2000", "Rs. 1200"],
                    "correct_answer": "Rs. 1800",
                    "explanation": "SI = (P × R × T)/100 = (5000 × 12 × 3)/100 = 1800",
                    "concepts": ["simple_interest", "interest_formula", "basic_calculation"],
                    "difficulty": "easy",
                    "time_estimate": 75
                },
                {
                    "question_text": "At what rate percent per annum will Rs. 2000 amount to Rs. 2420 in 3 years at simple interest?",
                    "options": ["7%", "6%", "8%", "5%"],
                    "correct_answer": "7%",
                    "explanation": "SI = 2420 - 2000 = 420. Using SI formula: 420 = (2000 × R × 3)/100, R = 7%",
                    "concepts": ["simple_interest", "rate_calculation", "amount_formula"],
                    "difficulty": "medium",
                    "time_estimate": 135
                }
            ]
        }
    ],
    "logical_reasoning": [
        {
            "subcategory": "series",
            "questions": [
                {
                    "question_text": "Find the next number in the series: 2, 6, 12, 20, 30, ?",
                    "options": ["42", "40", "38", "44"],
                    "correct_answer": "42",
                    "explanation": "The differences are 4, 6, 8, 10, so next difference is 12. 30 + 12 = 42",
                    "concepts": ["number_series", "pattern_recognition", "arithmetic_progression"],
                    "difficulty": "medium",
                    "time_estimate": 120
                },
                {
                    "question_text": "Complete the series: 1, 4, 9, 16, 25, ?",
                    "options": ["36", "35", "30", "49"],
                    "correct_answer": "36",
                    "explanation": "These are perfect squares: 1², 2², 3², 4², 5², 6² = 36",
                    "concepts": ["number_series", "perfect_squares", "pattern_recognition"],
                    "difficulty": "easy",
                    "time_estimate": 90
                }
            ]
        },
        {
            "subcategory": "analogies",
            "questions": [
                {
                    "question_text": "Book : Author :: Painting : ?",
                    "options": ["Artist", "Canvas", "Colors", "Museum"],
                    "correct_answer": "Artist",
                    "explanation": "A book is created by an author, similarly a painting is created by an artist",
                    "concepts": ["analogies", "relationship", "creator_creation"],
                    "difficulty": "easy",
                    "time_estimate": 75
                }
            ]
        }
    ],
    "verbal_ability": [
        {
            "subcategory": "synonyms",
            "questions": [
                {
                    "question_text": "Choose the word most similar in meaning to 'Abundant':",
                    "options": ["Scarce", "Plentiful", "Rare", "Limited"],
                    "correct_answer": "Plentiful",
                    "explanation": "Abundant means existing in large quantities or numbers, which is synonymous with plentiful",
                    "concepts": ["synonyms", "vocabulary", "word_meaning"],
                    "difficulty": "easy",
                    "time_estimate": 60
                },
                {
                    "question_text": "Find the synonym of 'Enigmatic':",
                    "options": ["Clear", "Mysterious", "Simple", "Obvious"],
                    "correct_answer": "Mysterious",
                    "explanation": "Enigmatic means difficult to interpret or understand, making it synonymous with mysterious",
                    "concepts": ["synonyms", "advanced_vocabulary", "word_meaning"],
                    "difficulty": "medium",
                    "time_estimate": 90
                }
            ]
        },
        {
            "subcategory": "antonyms",
            "questions": [
                {
                    "question_text": "Choose the word opposite in meaning to 'Optimistic':",
                    "options": ["Hopeful", "Pessimistic", "Confident", "Positive"],
                    "correct_answer": "Pessimistic",
                    "explanation": "Optimistic means hopeful and confident about the future, while pessimistic means expecting the worst",
                    "concepts": ["antonyms", "vocabulary", "opposite_meaning"],
                    "difficulty": "easy",
                    "time_estimate": 75
                }
            ]
        }
    ],
    "general_knowledge": [
        {
            "subcategory": "history",
            "questions": [
                {
                    "question_text": "Who was the first Prime Minister of India?",
                    "options": ["Mahatma Gandhi", "Sardar Patel", "Jawaharlal Nehru", "Subhas Chandra Bose"],
                    "correct_answer": "Jawaharlal Nehru",
                    "explanation": "Jawaharlal Nehru became the first Prime Minister of India when the country gained independence in 1947",
                    "concepts": ["indian_history", "prime_ministers", "independence"],
                    "difficulty": "easy",
                    "time_estimate": 45
                },
                {
                    "question_text": "In which year did the Jallianwala Bagh massacre take place?",
                    "options": ["1919", "1920", "1918", "1921"],
                    "correct_answer": "1919",
                    "explanation": "The Jallianwala Bagh massacre occurred on April 13, 1919, in Amritsar, Punjab",
                    "concepts": ["indian_history", "freedom_struggle", "british_rule"],
                    "difficulty": "medium",
                    "time_estimate": 90
                }
            ]
        },
        {
            "subcategory": "geography",
            "questions": [
                {
                    "question_text": "Which is the longest river in India?",
                    "options": ["Yamuna", "Ganga", "Brahmaputra", "Godavari"],
                    "correct_answer": "Ganga",
                    "explanation": "The Ganga river, at approximately 2,525 km, is the longest river in India",
                    "concepts": ["indian_geography", "rivers", "physical_features"],
                    "difficulty": "easy",
                    "time_estimate": 60
                }
            ]
        }
    ]
}

async def generate_questions_database():
    """Generate a comprehensive question database"""
    try:
        # Connect to database
        mongo_url = os.environ['MONGO_URL']
        client = AsyncIOMotorClient(mongo_url)
        db = client[os.environ['DB_NAME']]
        
        db_service = DatabaseService(db)
        
        print("🚀 Starting question generation...")
        
        total_generated = 0
        
        for category, subcategories in SAMPLE_QUESTIONS.items():
            print(f"\n📝 Processing {category}...")
            
            for subcat_data in subcategories:
                subcategory = subcat_data["subcategory"]
                questions = subcat_data["questions"]
                
                print(f"  ├─ {subcategory}: {len(questions)} questions")
                
                # Prepare questions for bulk creation
                questions_to_create = []
                for q in questions:
                    question_data = {
                        "question_text": q["question_text"],
                        "options": q["options"],
                        "correct_answer": q["correct_answer"],
                        "category": category,
                        "subcategory": subcategory,
                        "explanation": q["explanation"],
                        "concepts": q["concepts"],
                        "tags": [category, subcategory] + q["concepts"],
                        "difficulty": q["difficulty"],
                        "time_estimate": q["time_estimate"],
                        "source": "sample_generator",
                        "source_url": f"https://sample.com/{category}/{subcategory}"
                    }
                    questions_to_create.append(question_data)
                
                # Create questions in bulk
                question_ids = await db_service.create_questions_bulk(questions_to_create)
                total_generated += len(question_ids)
                
                print(f"  └─ Created {len(question_ids)} questions")
        
        print(f"\n✅ Successfully generated {total_generated} high-quality questions!")
        
        # Generate additional questions by duplicating and modifying existing ones
        await generate_additional_questions(db_service, total_generated)
        
        client.close()
        
    except Exception as e:
        print(f"❌ Error generating questions: {e}")
        raise

async def generate_additional_questions(db_service, base_count):
    """Generate additional questions to reach a larger dataset"""
    try:
        print(f"\n🔄 Generating additional questions to expand the dataset...")
        
        # Create variations of existing questions
        variations = []
        
        # Percentage variations
        for i in range(50):
            variations.append({
                "question_text": f"If {15 + i}% of a number is {80 + i*2}, what is {25 + i}% of that number?",
                "options": [str(round((80 + i*2) * 100 / (15 + i) * (25 + i) / 100)), 
                           str(round((80 + i*2) * 100 / (15 + i) * (25 + i) / 100) + 50),
                           str(round((80 + i*2) * 100 / (15 + i) * (25 + i) / 100) - 50),
                           str(round((80 + i*2) * 100 / (15 + i) * (25 + i) / 100) + 100)],
                "correct_answer": str(round((80 + i*2) * 100 / (15 + i) * (25 + i) / 100)),
                "category": "quantitative_aptitude",
                "subcategory": "percentage",
                "explanation": f"Mathematical calculation based on percentage formula",
                "concepts": ["percentage", "calculation", "proportion"],
                "tags": ["quantitative_aptitude", "percentage"],
                "difficulty": "easy" if i < 25 else "medium",
                "time_estimate": 90 + i,
                "source": "auto_generator",
                "source_url": "https://auto.generated.com/percentage"
            })
        
        # Simple Interest variations
        for i in range(40):
            principal = 1000 + i * 100
            rate = 5 + i % 10
            time = 2 + i % 5
            si = (principal * rate * time) // 100
            
            variations.append({
                "question_text": f"Find the simple interest on Rs. {principal} at {rate}% per annum for {time} years.",
                "options": [f"Rs. {si}", f"Rs. {si + 100}", f"Rs. {si - 100}", f"Rs. {si + 200}"],
                "correct_answer": f"Rs. {si}",
                "category": "quantitative_aptitude", 
                "subcategory": "simple_interest",
                "explanation": f"SI = (P × R × T)/100 = ({principal} × {rate} × {time})/100 = {si}",
                "concepts": ["simple_interest", "formula", "calculation"],
                "tags": ["quantitative_aptitude", "simple_interest"],
                "difficulty": "easy" if i < 20 else "medium",
                "time_estimate": 75 + i,
                "source": "auto_generator",
                "source_url": "https://auto.generated.com/simple_interest"
            })
        
        # Series completion variations
        for i in range(30):
            start = 2 + i
            diff = 3 + i % 5
            series = [start + j * diff for j in range(5)]
            next_val = start + 5 * diff
            
            variations.append({
                "question_text": f"Find the next number in the series: {', '.join(map(str, series))}, ?",
                "options": [str(next_val), str(next_val + diff), str(next_val - diff), str(next_val + 2*diff)],
                "correct_answer": str(next_val),
                "category": "logical_reasoning",
                "subcategory": "series",
                "explanation": f"The series increases by {diff} each time, so next number is {next_val}",
                "concepts": ["series", "arithmetic_progression", "pattern"],
                "tags": ["logical_reasoning", "series"],
                "difficulty": "easy" if i < 15 else "medium",
                "time_estimate": 90 + i,
                "source": "auto_generator", 
                "source_url": "https://auto.generated.com/series"
            })
        
        # Create vocabulary questions
        vocab_pairs = [
            ("Abundant", "Plentiful", "Scarce", "Limited"),
            ("Ancient", "Old", "Modern", "Recent"), 
            ("Brave", "Courageous", "Cowardly", "Fearful"),
            ("Calm", "Peaceful", "Agitated", "Turbulent"),
            ("Difficult", "Hard", "Easy", "Simple")
        ]
        
        for i, (word, correct, ant1, ant2) in enumerate(vocab_pairs * 8):  # Repeat to get 40 questions
            variations.append({
                "question_text": f"Choose the word most similar in meaning to '{word}':",
                "options": [correct, ant1, ant2, "None of these"],
                "correct_answer": correct,
                "category": "verbal_ability",
                "subcategory": "synonyms", 
                "explanation": f"{word} means similar to {correct}",
                "concepts": ["synonyms", "vocabulary", "word_meaning"],
                "tags": ["verbal_ability", "synonyms"],
                "difficulty": "easy" if i < 20 else "medium",
                "time_estimate": 60 + i,
                "source": "auto_generator",
                "source_url": "https://auto.generated.com/synonyms"
            })
        
        # Create questions in bulk
        if variations:
            question_ids = await db_service.create_questions_bulk(variations)
            print(f"✅ Created {len(question_ids)} additional questions!")
            print(f"🎯 Total questions in database: {base_count + len(question_ids)}")
        
    except Exception as e:
        print(f"❌ Error generating additional questions: {e}")

if __name__ == "__main__":
    print("🎯 Generating Comprehensive Question Database...")
    print("=" * 60)
    asyncio.run(generate_questions_database())
    print("=" * 60)
    print("✅ Question generation completed!")