import pandas as pd
import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from openai import OpenAI
from groq import Groq

load_dotenv() # Load environment variables


# 1. Load data

df = pd.read_csv("/Users/shivanshumac/Documents/Python/Projects/cricket-pipeline/ContinousDatasetModified.csv")
#print(f"Loaded {len(df)} rows")

# 2. Clean Data
df = df.drop(columns=['Unnamed: 0']) #dropping unnamed column

# Renaming columns for consistency
df.columns = ['Scorecard', 'Team1', 'Team2', 'Margin', 
              'Ground', 'Match_Date', 'Winner', 'Host_Country',
              'Venue_Team1', 'Venue_Team2', 'Innings_Team1', 'Innings_Team2']

df['Match_Date'] = pd.to_datetime(df['Match_Date'],errors='coerce') #converting the date

df = df.dropna(subset=['Winner']) # drop rows with no winner

#print(f"After cleaning: {len(df)} rows")
#print(df.dtypes)

# 3. Filter India Matches
india = df[(df['Team1'] == 'India') | (df['Team2'] == 'India')]
#print(f"\nIndia matches: {len(india)}")

# 4. Analysis
#Q1 India win/loss record
india_wins = india[india['Winner'] == 'India']
india_loss = india[india['Winner'] != 'India']

#print(f"\nIndia Wins: {len(india_wins)}")
#print(f"\nIndia Losses: {len(india_loss)}")

#Q2 Teams that beat India Most
beat_india = india[india['Winner'] != 'India']['Winner'].value_counts().head(10)
#print(f"\nTeams that beat India most:\n{beat_india}")

# Q3: India win rate by venue (home/away)
india_home = india[india['Host_Country'] == 'India']
india_away = india[india['Host_Country'] != 'India']
#print(f"\nIndia Home wins: {len(india_home[india_home['Winner'] == 'India'])}/{len(india_home)}")
#print(f"India Away wins: {len(india_away[india_away['Winner'] == 'India'])}/{len(india_away)}")

# Q4: India best grounds
best_grounds = india_wins['Ground'].value_counts().head(10)
#print(f"\nIndia best grounds: \n{best_grounds}")


# Q5: Decade Analysis
india['Year'] = india['Match_Date'].dt.year

india['decade'] = (india['Year'] // 10) * 10

india_decade = india.groupby('decade').agg(
    Total_Matches=('Match_Date', 'count'),
    Wins=('Winner', lambda x: (x == 'India').sum())
).reset_index()
#print(india_decade)




#client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

summary = f""" 
    India ODI Cricket Statistics:
- Total matches: {len(india)}
- Wins: {len(india_wins)}, Losses: {len(india_loss)}
- Biggest rival: Australia (beat India 73 times)
- Best ground: Sharjah (35 wins)
- Home record: 179 wins from 295 matches
- Away record: 296 wins from 585 matches
- Best decade: 2010s with 65% win rate
"""

client = Groq(api_key=os.getenv("GROQ_API_KEY"))
response = client.chat.completions.create(
    model="llama-3.3-70b-versatile" ,    
    messages=[
        {"role": "system", "content": "You are a cricket analyst. Give a brief 3-4 line insight."},
        {"role": "user", "content": f"Analyze this India ODI data and give key insights:\n{summary}"}
    ])

ai_insight = response.choices[0].message.content
#print(f"\nAI Insight:\n{ai_insight}")

## saving the clean csv
india.to_csv("india_odi_cleaned.csv", index=False)

#upload to ADLS
connect_str = f"DefaultEndpointsProtocol=https;AccountName={os.getenv('STORAGE_ACCOUNT_NAME')};AccountKey={os.getenv('STORAGE_ACCOUNT_KEY')};EndpointSuffix=core.windows.net"

blob_service_client = BlobServiceClient.from_connection_string(connect_str)
blob_client = blob_service_client.get_blob_client(
    container=os.getenv("CONTAINER_NAME"),
    blob="india_odi_cleaned.csv"
)

with open("india_odi_cleaned.csv", "rb") as f:
    blob_client.upload_blob(f, overwrite=True)

print("Uploaded to ADLS Gen2!")
