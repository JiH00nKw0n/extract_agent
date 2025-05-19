import os

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# How to get your Databricks token: https://docs.databricks.com/en/dev-tools/auth/pat.html
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

client = OpenAI(
  api_key=DATABRICKS_TOKEN,
  base_url="https://dbc-449ecea5-a3a3.cloud.databricks.com/serving-endpoints"
)

chat_completion = client.chat.completions.create(
  messages=[
  {
    "role": "system",
    "content": "You are an AI assistant"
  },
  {
    "role": "user",
    "content": "Tell me about Large Language Models"
  }
  ],
  model="databricks-meta-llama-3-3-70b-instruct",
  max_tokens=256
)

print(chat_completion.choices[0].message.content)