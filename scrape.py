import psycopg2 
from openai import OpenAI
import os
from dotenv import load_dotenv
import json
from datetime import datetime, timedelta

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

conn = psycopg2.connect(
    host=os.getenv("HOST"),
    database=os.getenv("DATABASE"), 
    user=os.getenv("USER"),
    password=os.getenv("PASSWORD"), 
    port=5432
)

cursor = conn.cursor()

# Get recent articles
one_week_ago = datetime.now() - timedelta(days=7)
cursor.execute(
    "SELECT id, title FROM articles WHERE created_at >= %s", 
    (one_week_ago,)
)
articles = cursor.fetchall()

# Process each article
for article_id, title in articles:
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system", 
                "content": "Extract 3-5 relevant keywords from article titles. Return JSON: {\"keywords\": [\"keyword1\", \"keyword2\"]}. Keywords should be lowercase, single words or short phrases."
            },
            {
                "role": "user", 
                "content": f"Extract keywords from: {title}"
            }
        ],
        response_format={"type": "json_object"},
    )
    
    keywords_data = json.loads(response.choices[0].message.content)
    keywords = keywords_data.get("keywords", [])
    
    # Add keywords to pool (duplicates ignored)
    for keyword in keywords:
        keyword = keyword.lower().strip()
        cursor.execute("""
            INSERT INTO game_keywords (keyword) 
            VALUES (%s) 
            ON CONFLICT (keyword) DO NOTHING
        """, (keyword,))
    
    print(f"{title[:60]} -> {keywords}")

conn.commit()
cursor.close()
conn.close()

print(f"\n Done")