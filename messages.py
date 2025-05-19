from typing import Dict, List

######
# DATA EXTRACTION
######

_SYSTEM_KPI_EXTRACT_PROMPT = f"""
<task>
You are a financial analyst reviewing an 8-K report of a specific firm.
Your task is to analyze a sentence (referred to as "line") and determine whether it references Key Performance Indicators (KPIs), their specific numerical values, and corresponding units.
If the line does not mention any KPIs and their corresponding values, return empty lists for all fields.

The "quarter" variable refers to the specific fiscal period the values apply to (e.g., "Q1 2023").
</task>

<guideline>
For each given line:
    - Identify if the sentence includes KPIs (e.g., revenue, operating margin, EPS, growth rate, free cash flow, etc.).
    - For each KPI mentioned along with a clearly associated numerical value (e.g., dollars, percentages, basis points, units), extract:
        - The name or description of the KPI
        - The corresponding value (as a number or a string)
        - The unit of measurement (e.g., USD, %, bps, million units)
    - If no KPIs or values are mentioned, return empty lists for all fields.
</guideline>

<format>
Return your output in the following JSON format:
```json
{{
    "titles": ["Described KPI 1", "Described KPI 2", ...],
    "values": [Numeric value 1, Numeric value 2, ...],
    "units": ["Unit 1", "Unit 2", ...]
}}
```

If no KPIs or values are present:

```json
{{
    "titles": [],
    "values": [],
    "units": []
}}
```
</format>
"""

_USER_KPI_EXTRACT_PROMPT = """
Extract `Key Performance Indicator` of following sentence.
<company>
{company_name}
</company>

<year_and_quarter>
{quarter}
</year_and_quarter>>

<line>
{line}
</line>
"""


def get_kpi_extracting_messages(company_name: str, line: str, quarter: str) -> List[Dict[str, str]]:

    messages = [
        {
            "role": "system",
            "content": _SYSTEM_KPI_EXTRACT_PROMPT
        },
        {
            "role": "user",
            "content": _USER_KPI_EXTRACT_PROMPT.format(company_name=company_name, line=line, quarter=quarter)
        },
    ]
    return messages


_SYSTEM_KPI_AUDITING_PROMPT = f"""
<task>
You are a financial analyst reviewing a chunk of an 8-K report for a specific firm.
Your task is to analyze a specific sentence (referred to as "line") within the provided "chunk" of text.
Determine if the KPI mentioned in the "line" refers to an actual (historical) performance or an expected (forward-looking) performance.
Based on this, extract the type ("actual" or "expected"), the relevant period, and the unit of the KPI.
</task>

<context>
- The "chunk" provides the surrounding context from the 8-K report to help understand the "line".
- The "company_name" is the name of the firm.
- The "quarter" refers to the reporting period of the 8-K document itself (e.g., "2023 Q4").
</context>

<guideline>
1.  **Analyze the "line" within the context of the "chunk".**
2.  **Identify the main KPI** discussed in the "line".
3.  **Determine the Type:**
    *   If the KPI represents a past result or performance that has already occurred, classify it as "actual".
    *   If the KPI represents a future expectation, projection, guidance, forecast, or outlook, classify it as "expected". Look for keywords like "expect," "guide," "project," "forecast," "outlook," "anticipate," "will be," etc.
4.  **Determine the Period:**
    *   Extract the specific fiscal period the KPI value applies to (e.g., "2023 Q1", "2024 Full Year"). This might be mentioned directly in the "line" or inferred from the "chunk".
    *   If the type is "actual", the period should be formatted as "YYYY QN" or "YYYY Full Year".
    *   If the type is "expected", check if a specific target period for achieving the KPI is mentioned.
        *   If a specific period is mentioned, format it as "(Expected) YYYY QN" or "(Expected) YYYY Full Year".
        *   If no specific period is mentioned for the expectation, the period should be "None".
5.  **Determine the Unit:**
    *   Identify the unit of measurement for the KPI value (e.g., "$", "%", "shares", "basis points", "millions", "thousands", currency codes like "USD", "EUR").
    *   The unit might be directly attached to the number (e.g., "$100M", "50%"), mentioned nearby in the "line", or implied by the context in the "chunk".
    *   If no specific unit is found or applicable, return "None".
6.  **Ambiguity:** If the line does not clearly mention a KPI or its type/period/unit cannot be reliably determined, return "None" for type, period, and unit.
</guideline>

<format>
Return your output in the following JSON format:
```json
{{
    "type_": "actual" | "expected" | "None",
    "period": "YYYY QN | YYYY Full Year" | "(Expected) YYYY QN | (Expected) YYYY Full Year" | "None",
    "unit": "string" | "None"
}}
```
</format>
"""

_USER_KPI_AUDITING_PROMPT = """
Analyze the following line based on the provided chunk and context.

**Important:** Always analyze the entire "chunk" to infer the period if it's not explicitly mentioned in the "line".
The period information might be in surrounding sentences or context.

<company>
{company_name}
</company>

<reporting_quarter>
{quarter}
</reporting_quarter>

<chunk>
{chunk}
</chunk>

<line>
{line}
</line>
"""


def get_kpi_auditing_messages(company_name: str, chunk: str, line: str, quarter: str) -> List[Dict[str, str]]:
    """
    Generates messages for the LLM to audit a KPI as actual or expected and extract the period and unit.
    """
    messages = [
        {
            "role": "system",
            "content": _SYSTEM_KPI_AUDITING_PROMPT
        },
        {
            "role": "user",
            "content": _USER_KPI_AUDITING_PROMPT.format(
                company_name=company_name, chunk=chunk, line=line, quarter=quarter
            )
        },
    ]
    return messages


_SYSTEM_KPI_CLASSIFIER_PROMPT = f"""
<task>
You are tasked with classifying a given sentence (referred to as "line"), typically extracted from a financial report (like an 8-K), into one of three categories: "Financials", "KPI", or "Guidance" based on the definitions provided below.
</task>

<definitions>
- **Financials:**
    - **Purpose:** To officially report the company's financial health and performance according to accounting standards (e.g., GAAP, IFRS).
    - **Content:** Typically includes standard line items like Revenue, Operating Income, Operating margin, Net Income, Assets, Liabilities, Cash Flow, Earnings Per Share (EPS). 
        Changes or variations in standard financial statement items are also classified as Financials.
        **Note: All EPS (Earnings Per Share) related metrics, including variations like diluted EPS, adjusted EPS, or any EPS-related calculations, should be classified as Financials.**
        **Note: All growth rates derived from standard financial statement items (e.g., revenue, net income, EPS, Operating Margin,Sales) should be categorized as Financials.**
    - **Nature:** Represents **actual**, historical financial results.
    - **Format:** Often found in official reports like SEC filings, audited reports, etc.

- **KPI (Key Performance Indicator):**
    - **Purpose:** Used by management to measure performance and track progress towards strategic goals. Can be financial or non-financial.
    - **Content:** Examples include customer growth rate, store count, Average Revenue Per User (ARPU), user retention rate, Net Promoter Score (NPS), segment-specific revenues/margins, non-GAAP measures presented alongside GAAP results.
        - **Note:** When standard financial statement items are broken down into more granular components (e.g., revenue by channel, specific cost items), such breakdowns should be classified as KPI.
    - **Nature:** Represents **actual**, historical or current performance metrics.
    - **Format:** Often used for internal management but may be selectively shared externally, especially with investors. Defined by the company, varying by industry and strategy.

- **Guidance:**
    - **Purpose:** To provide forward-looking statements, projections, forecasts, or expectations about future performance.
    - **Content:** Can refer to future Financials (e.g., "expected revenue", "projected EPS") or KPIs (e.g., "anticipated user growth"). Often includes keywords like "expect," "guide," "project," "forecast," "outlook," "anticipate," "will be," "target."
    - **Nature:** Represents **expected** future outcomes, not actual results.
    - **Format:** Explicitly stated as expectations for upcoming periods (e.g., next quarter, full year).
</definitions>

<criteria>
- Analyze the content of the input "line".
- Determine if the line primarily discusses a standard financial statement item (Financials), a specific performance metric (KPI), or a future expectation (Guidance).
- Pay attention to keywords indicating future outlook (for Guidance) versus reporting past results (for Financials/KPI).
- If the line doesn't clearly fit into one category or lacks sufficient context, classify it as "Unclear" or choose the best fit.
</criteria>

<format>
The input is a single string provided within `<line>` tags.
Return your output as a JSON object containing a single key "category" with the classification string as its value.

Example Output:
```json
{{
    "category": "Financials" | "KPI" | "Guidance" | "Unclear"
}}
```
</format>
"""

_USER_KPI_CLASSIFIER_PROMPT = """
Please classify the following line into "Financials", "KPI", or "Guidance" based on the provided definitions and criteria.

<line>
{line}
</line>
"""


def get_kpi_classification_messages(line: str) -> List[Dict[str, str]]:
    """
    Generates messages for the LLM to classify a single line into Financials, KPI, or Guidance.
    """
    messages = [
        {
            "role": "system",
            "content": _SYSTEM_KPI_CLASSIFIER_PROMPT
        },
        {
            "role": "user",
            "content": _USER_KPI_CLASSIFIER_PROMPT.format(line=line)
        },
    ]
    return messages


######
# DATA ANNOTATION: Quote Relevance
######

_SYSTEM_QUOTE_RELEVANCE_PROMPT_FILTER = f"""
<task>
You are tasked with identifying the index of the quote from the provided data that is most relevant to answering the given question.
Analyze the question deeply to understand the specific information needed.
Then, review the numbered list of quotes in the data and select the index of the single quote that best relates to the question.
</task>

<input_format>
The input consists of:
1.  A "question" seeking specific information.
2.  "data" containing a numbered list of quotes, formatted like:
    1. <Quote 1 text>
    2. <Quote 2 text>
    ...
    N. <Quote N text>
</input_format>

<analysis_steps>
1.  **Understand the Question:** Carefully analyze the user's question to determine the core information being sought. Identify key entities, concepts, timeframes, or metrics mentioned.
2.  **Evaluate Quotes:** Read through each numbered quote provided in the data.
3.  **Assess Relevance:** For each quote, determine how directly and completely it answers the analyzed question.
4.  **Select Best Match:** Choose the index (the number preceding the quote) of the single quote that provides the most relevant information to answer the question. If multiple quotes seem relevant, select the one that is most directly and comprehensively related to the question's core inquiry.
5.  **Handle Ambiguity:** Be generous in assessing relevance - even if a quote only partially or tangentially relates to the question, consider it as a candidate. Only return an index of -1 if absolutely no quote has ANY connection or relevance to the question whatsoever. Try your best to find some related quote first before concluding there is none.
</analysis_steps>

<output_format>
Return your output as a JSON object containing a single key "index" with the integer index of the most relevant quote as its value. If no quote is relevant or no quote adequately answers the question, the value should be -1.

Example Output:
```json
{{
    "index": most relevant quote's index (or -1 if no relevant quote is found)
}}
```
</output_format>
"""

_USER_QUOTE_RELEVANCE_PROMPT = """
Please identify the index of the most relevant quote from the data below to answer the following question.

<question>
{question}
</question>

<data>
{data}
</data>
"""


def get_quote_relevance_messages(question: str, data: str) -> List[Dict[str, str]]:
    """
    Generates messages for the LLM to find the index of the most relevant quote from the data based on the question.
    """
    messages = [
        {
            "role": "system",
            "content": _SYSTEM_QUOTE_RELEVANCE_PROMPT_FILTER
        },
        {
            "role": "user",
            "content": _USER_QUOTE_RELEVANCE_PROMPT.format(question=question, data=data)
        },
    ]
    return messages


_SYSTEM_KOREAN_TRANSLATION_PROMPT = f"""
<task>
You are tasked with translating a given text into Korean.
</task>

<input_format>
The input is a single string provided within `<text>` tags.
</input_format>

<output_format>
Return your output as a JSON object containing a single key "translation" with the translated text as its value.
</output_format>
"""

_USER_KOREAN_TRANSLATION_PROMPT = """
Please translate the following text into Korean.

<text>
{text}
</text>
"""


def get_korean_translation_messages(text: str) -> List[Dict[str, str]]:
    """
    Generates messages for the LLM to translate the text into Korean.
    """
    messages = [
        {
            "role": "system",
            "content": _SYSTEM_KOREAN_TRANSLATION_PROMPT
        },
        {
            "role": "user",
            "content": _USER_KOREAN_TRANSLATION_PROMPT.format(text=text)
        },
    ]
    return messages
