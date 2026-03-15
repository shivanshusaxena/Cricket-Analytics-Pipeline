# Cricket ODI Analytics Pipeline 🏏

## Overview
End-to-end analytics pipeline analyzing India's ODI cricket performance 
across 880 matches using Python, Azure Data Lake Gen2, and AI insights.

## Pipeline Architecture
Raw CSV → Python (Pandas) → Azure ADLS Gen2 → AI Insights (Groq LLM)

## Key Insights
- India win rate: 53.8% across 880 matches
- Biggest rival: Australia (beat India 73 times)
- Best ground: Sharjah (35 wins)
- Best decade: 2010s with 65% win rate
- Home record: 179/295 | Away record: 296/585

## Business Questions Answered
1. India's overall win/loss record
2. Teams that beat India most
3. India home vs away win rate
4. India's best performing grounds
5. Decade-wise performance analysis
6. AI generated performance insights using Groq LLM

## Tech Stack
- Python (Pandas)
- Azure Data Lake Storage Gen2
- Groq LLM API (Llama 3.3)
- Azure Blob Storage

## Dataset
Source: Kaggle — Continuous ODI Dataset
Rows: 3,747 | Columns: 12

## AI Insight Generated
> "India's ODI performance is marked by a respectable 53.8% win rate 
> across 880 matches. Their strong home record is eclipsed by an 
> impressive away record, with 296 wins from 585 matches. The team's 
> dominance in the 2010s with a 65% win rate is a notable highlight, 
> while Australia remains their biggest rival."
