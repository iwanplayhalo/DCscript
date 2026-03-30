import os
import json
import psycopg2
from openai import OpenAI
from dotenv import load_dotenv
import re 
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PAsSWORD")
DB_PORT = os.getenv("DB_PORT")

if DB_HOST != "localhost" or DB_NAME != "postgres":
    raise Exception("Refusing to run outside local test database")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = "gpt-4o-mini"

BATCH_SIZE = 30

conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port=DB_PORT
)

client = OpenAI(api_key=OPENAI_API_KEY)

def fetch_titles():
    with conn.cursor() as cur:
        cur.execute("""
            SELECT title
            FROM articles
            WHERE pub_date >= NOW() - INTERVAL '7 days'
            AND title IS NOT NULL;
        """)
        return [row[0] for row in cur.fetchall()]


def extract_keywords(titles):
    prompt = f"""
Extract 5-15 high-quality keywords or short phrases from these article titles.

- Focus on meaningful topics (companies, events, concepts)
- Avoid filler words
- Keep them reusable for a game
- Return ONLY JSON:
{{"keywords": ["word1", "word2"]}}

Titles:
{titles}
"""

    response = client.responses.create(
        model=MODEL,
        input=prompt
    )

    text = response.output_text.strip()

    # Remove ```json ... ``` or ``` ... ``` wrappers if present
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    try:
        data = json.loads(text)
        return data.get("keywords", [])
    except Exception as e:
        print("Bad response:", text)
        print("Parse error:", e)
        return []


def insert_keywords(keywords):
    with conn.cursor() as cur:
        for kw in set(keywords):
            cur.execute("""
                INSERT INTO game_keywords (keyword)
                VALUES (%s)
                ON CONFLICT (keyword)
                DO UPDATE SET frequency = game_keywords.frequency + 1;
            """, (kw,))
    conn.commit()

def main():
    titles = fetch_titles()

    print(f"Found {len(titles)} titles")

    all_keywords = []

    # batch to avoid huge prompt
    for i in range(0, len(titles), BATCH_SIZE):
        batch = titles[i:i+BATCH_SIZE]
        kws = extract_keywords(batch)
        all_keywords.extend(kws)

    print(f"Extracted {len(all_keywords)} keywords")

    insert_keywords(all_keywords)

    print("Done")


if __name__ == "__main__":
    main()