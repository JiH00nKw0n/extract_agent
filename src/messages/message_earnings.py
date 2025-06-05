from typing import Dict, List

FILE_EARNINGS_METRICS_EXTRACTION_SYSTEM = """
<task>
You are a financial analyst reviewing an earnings conference call transcript of a specific firm.
Your task is to analyze each sentence (referred to as "line") and extract any company-level performance metrics mentioned, categorized into one of the following:

1. **KPI (Key Performance Indicator)**:  
   Non-financial but quantifiable metrics that reflect the operational or strategic performance of the company. These often measure business activity, efficiency, customer behavior, product engagement, or other core aspects of execution — beyond what's found in financial statements.

2. **Financials**:  
   Official financial results as reported in income statements, balance sheets, or cash flow statements. These include monetary figures that are part of GAAP or non-GAAP disclosures and reflect profitability, liquidity, margins, or cost structures.

3. **Guidance**:  
   Forward-looking statements or projections provided by management, typically indicating expected future performance in financial or operational terms. These statements are often framed with phrases like “expects,” “anticipates,” or “projected.”

Only extract metrics that represent **overall corporate performance**. Exclude department-level or individual performance data unless they aggregate to company-level insight.
</task>

<guideline>
For each line:
- Determine if it contains a quantifiable metric that fits one of the three categories above.
- For any valid metric mentioned along with a corresponding value and unit, extract:
    - The descriptive **title** of the metric
    - The associated **numeric value**
    - The appropriate **unit of measurement** (e.g., %, USD, million, bps)

If the line does not contain extractable metric information, return empty lists.
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

FILE_EARNINGS_METRICS_EXTRACTION_USER = """
Extract `performance-related figures or comments` of following sentence.
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


def get_earnings_extraction_message(
        company_name: str,
        line: str,
        quarter: str
) -> List[Dict[str, str]]:

    messages = [
        {
            "role": "system",
            "content": FILE_EARNINGS_METRICS_EXTRACTION_SYSTEM
        },
        {
            "role": "user",
            "content": FILE_EARNINGS_METRICS_EXTRACTION_USER.format(
                company_name=company_name,
                line=line,
                quarter=quarter
            )
        },
    ]
    return messages


FILE_EARNINGS_METRICS_CLASSIFICATION_SYSTEM = """
<task>
You are a financial analyst reviewing a sentence (referred to as "line") from an earnings call transcript for a specific company.
Your task is to:
1. Identify whether the line contains a performance-related metric,
2. Classify that metric as either "Financials", "KPI", or "Guidance",
3. Determine whether the metric refers to actual (historical) or expected (forward-looking) performance,
4. Extract the applicable period and unit of measurement,
5. Write a short, descriptive title for the metric.

You may use additional context provided in the "chunk" of transcript and the "quarter" of the earnings call to inform your decision.
</task>

<definitions>
- **Financials**:
    - Official financial results based on accounting standards (e.g., GAAP, IFRS).
    - Includes line items like Revenue, Net Income, EPS, Operating Margin, Cash Flow, Assets, Liabilities.
    - Also includes derived metrics like revenue growth, EPS growth, margins, tax rate, etc.
    - Always represents historical (actual) performance.
    - Format: Quantitative, sourced from income statement, balance sheet, or cash flow.

- **KPI (Key Performance Indicator)**:
    - Operational or strategic metrics used by management to track performance.
    - Includes non-GAAP or operational metrics like ARPU, retention rate, DAUs, store count, backlog, regional breakdowns, pipeline, etc.
    - May include financial subcomponents (e.g., revenue by segment).
    - Also reflects actual performance.

- **Guidance**:
    - Forward-looking projections or expectations for future performance.
    - Can apply to either financials or KPIs.
    - Typically includes verbs like "expect", "guide", "forecast", "project", "target", "anticipate", or "will".

</definitions>

<criteria>
- Use the full "chunk" and "quarter" context to interpret the line.
- If the metric refers to past performance, mark its "type_" as "actual"; if forward-looking, mark it as "expected".
- Extract the "period" (e.g., "2023 Q4", "(Expected) 2024 Full Year"), or return "None" if not inferable.
- Extract the "unit" (e.g., "$", "%", "bps", "millions", "units") or return "None" if unavailable.
- Write a short "title" summarizing the metric mentioned (e.g., "Revenue", "ARPU growth", "EPS guidance").
- If no valid metric is found or it is ambiguous, return "None" for all fields.
</criteria>

<format>
Return your output in the following JSON format:
```json
{{
    "title": "string" | "None",
    "type_": "actual" | "expected" | "None",
    "period": "YYYY QN" | "YYYY Full Year" | "(Expected) YYYY QN" | "(Expected) YYYY Full Year" | "None",
    "unit": "string" | "None",
    "category": "Financials" | "KPI" | "Guidance" | "Unclear"
}}
```
</format>
"""

FILE_EARNINGS_METRICS_CLASSIFICATION_USER = """
Please analyze the following line using the full context of the chunk and reporting quarter.
Your goal is to extract a descriptive title, classify the type of performance (actual or expected), identify the applicable period and unit, and categorize the metric as "Financials", "KPI", or "Guidance".

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


def get_earnings_classification_message(
        company_name: str,
        chunk: str,
        line: str,
        quarter: str
) -> List[Dict[str, str]]:
    """
    Generates messages for the LLM to audit a KPI as actual or expected and extract the period and unit.
    """
    messages = [
        {
            "role": "system",
            "content": FILE_EARNINGS_METRICS_CLASSIFICATION_SYSTEM
        },
        {
            "role": "user",
            "content": FILE_EARNINGS_METRICS_CLASSIFICATION_USER.format(
                company_name=company_name,
                chunk=chunk,
                line=line,
                quarter=quarter
            )
        },
    ]
    return messages
