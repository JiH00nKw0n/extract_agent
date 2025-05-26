from typing import Dict, List

_SYSTEM_CHUNK_RELEVANCE_PROMPT_FILTER = f"""
<task>
You are tasked with determining whether the provided data contains meaningful and necessary information that helps answer a given question.
You must carefully analyze the question to identify its core informational requirements, then evaluate whether the data meaningfully supports or contributes relevant context to those needs.
</task>

<input_format>
The input consists of:
1.  A "question" seeking specific information.
2.  "data" containing a chunk of text, formatted like:
    <text>
    <text>
    ...
    <text>
</input_format>

<analysis_criteria>
1.  **Understand the Question:** Break down the user's question to identify its key informational requirements.
2.  **Evaluate Supportiveness:** Review the quotes and ask:
    - Does at least one sentence support or provide meaningful context for answering the question, even if it does not fully resolve it?
    - Would the information be useful or relevant for someone constructing an accurate answer?
3.  **Focus on Informative Helpfulness:** The sentence does not need to completely answer the question, but it should be clearly related and provide **useful, insightful, or contextual** information that helps move toward the answer.
4.  **Avoid Superficial Associations:** Ignore sentences that are only weakly connected through keywords or vague topic similarity. Relevance should be **informationally meaningful**, not just thematic.
5.  **Answer Based on Utility:** If the data includes helpful or reasonably relevant content that contributes to answering the question — either directly or through supporting context — return `true`. Otherwise, return `false`.
</analysis_criteria>

<output_format>
Return a JSON object with a single key "related" whose value is either true or false.

Example Output:
```json
{{
    "reasoning": "reasoning for the relevance",
    "related": True | False,
}}
```
</output_format>
"""

_USER_CHUNK_RELEVANCE_PROMPT = """
Please identify the relevance of the data below if it helps answer the following question.

<question>
{question}
</question>

<data>
{data}
</data>
"""


def get_chunk_relevance_messages(question: str, data: str) -> List[Dict[str, str]]:
    """
    Generates messages for the LLM to find the index of the most relevant quote from the data based on the question.
    """
    messages = [
        {
            "role": "system",
            "content": _SYSTEM_CHUNK_RELEVANCE_PROMPT_FILTER
        },
        {
            "role": "user",
            "content": _USER_CHUNK_RELEVANCE_PROMPT.format(question=question, data=data)
        },
    ]
    return messages

