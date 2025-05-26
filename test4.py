import os

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

from utils import get_chunk

file_path = "/Users/junekwon/Desktop/Projects/extraction_agent/annotations/sbux/10-k.html"

with open(file_path, 'r', encoding='utf-8') as f:
    file_content = f.read()

chunks = get_chunk(file_content, "10-K")

print(chunks[110])
print("--------------------------------")
print(chunks[111])
print("--------------------------------")
print(chunks[112])
print("--------------------------------")
print(chunks[113])
print("--------------------------------")
print(chunks[114])
print("--------------------------------")
print(chunks[115])
print("--------------------------------")
print(chunks[116])
print("--------------------------------")
