import psycopg2 
import nltk
from nltk.tokenize import sent_tokenize
from openai import OpenAI
import os
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(api_key = os.getenv("OPENAI_API_KEY")) #will not be sharing my personal key here. need your own api key to run this and maybe hardcode it (not posting env)
#nltk.download('punkt_tab')
#Connect to the PostgreSQL database
conn = psycopg2.connect(
 host=os.getenv("HOST"),
 database=os.getenv("DATABASE"), 
 user=os.getenv("USER"),
 password=os.getenv("PASSWORD"), 
 port=5432
)

cursor = conn.cursor()
cursor.execute("SELECT title, content FROM articles LIMIT 5") #test with 5 articles so i don't burn a hole through my wallet
articles = cursor.fetchall()

for title, content in articles:
       content = content.strip('"“”\'')
       sentences = sent_tokenize(content)
       quotes = [s for s in sentences if '“' in s or '"' in s]
       quote_text = "\n".join(quotes)
       if not quotes:
            continue

       prompt = (
             f"""
             #Title: {title}
             #Quoted sentences:
             #{quote_text}
             Task:
             Identify who said each quote. Return JSON as a list of objects with:
             - "title": the article title
             - "speaker": the person who said it, or 'unknown'
             - "quotes": list of quoted text
             """
    )
       response = client.chat.completions.create(
             model = "gpt-4o-mini",
             messages = [
                   {"role" : "system", "content" : "You are a quote attribution and coreference resolver."},
                   {"role" : "user", "content" : prompt}],
                   response_format= {"type": "json_object"},
                   #limit = 500, this is for token limit but it didn't work :(
                   )

       result = response.choices[0].message.content
       print(result)
       