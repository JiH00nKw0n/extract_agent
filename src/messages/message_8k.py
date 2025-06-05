from typing import Dict, List

FILE_8K_METRICS_EXTRACTION_SYSTEM = """
<task>
You are a financial analyst reviewing an 8-K report or earnings disclosure from a publicly traded company.
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
    "titles": ["Metric 1", "Metric 2", ...],
    "values": [Value 1, Value 2, ...],
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

FILE_8K_METRICS_EXTRACTION_USER = """
Extract a key business performance metric, such as KPI, Financials, Guidance.
In other words, extract a metric that reflects corporate results.

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


def get_8k_extraction_message(
        company_name: str,
        quarter: str,
        line: str
) -> List[Dict[str, str]]:
    messages = [
        {
            "role": "system",
            "content": FILE_8K_METRICS_EXTRACTION_SYSTEM
        },
        {
            "role": "user",
            "content": FILE_8K_METRICS_EXTRACTION_USER.format(
                company_name=company_name,
                quarter=quarter,
                line=line
            )
        },
    ]
    return messages


FILE_8K_METRICS_CLASSIFICATION_SYSTEM = """
<task>
You are a financial analyst reviewing a sentence (referred to as "line") from an 8-K report. Your task is to:
1. Determine whether the "line" refers to a performance metric.
2. Classify that metric into one of the following categories: "Financials", "KPI", or "Guidance".
3. If applicable, determine whether the statement refers to actual (historical) performance or expected (forward-looking) performance.
4. Extract the relevant period and unit of the metric.

You are provided with additional context ("chunk") from the same 8-K document to help determine these properties.
</task>

<definitions>
- **Financials**:
    - Report standardized accounting-based financial results (e.g., Revenue, Operating Income, Net Income, EPS, Cash Flow).
    - Includes growth rates derived from financial statement items.
    - Always refers to actual results.
- **KPI (Key Performance Indicator)**:
    - Non-standard or non-GAAP metrics used to track strategic or operational performance (e.g., customer retention rate, DAU/MAU, store count, ARPU, NPS).
    - May include financial components when broken down (e.g., revenue by segment).
    - Refers to actual historical performance.
- **Guidance**:
    - Forward-looking expectations or projections, whether financial or operational.
    - Identified via language such as “expects”, “projects”, “anticipates”, “guides”, “will be”, “outlook”.
</definitions>

<criteria>
- Analyze the content of the "line" within the full "chunk".
- Determine whether the metric is **actual** or **expected**:
    - If it reflects past performance, classify as "actual".
    - If it refers to a future projection or expectation, classify as "expected".
    - If it is ambiguous or unidentifiable, use "None".
- Determine the **period** (e.g., "2023 Q4", "(Expected) 2024 Full Year") from either the "line" or the "chunk". If unknown, use "None".
- Determine the **unit** of measurement (e.g., "$", "%", "bps", "millions"). If not clearly specified or applicable, use "None".
- Classify the **category** of the metric: "Financials", "KPI", "Guidance", or "Unclear".

</criteria>

<format>
Return your output in the following JSON format:
```json
{{
    "title": "string" # copy the title of the line
    "type_": "actual" | "expected" | "None",
    "period": "YYYY QN" | "YYYY Full Year" | "(Expected) YYYY QN" | "(Expected) YYYY Full Year" | "None",
    "unit": "string" | "None",
    "category": "Financials" | "KPI" | "Guidance" | "Unclear"
}}
```
</format>
"""

FILE_8K_METRICS_CLASSIFICATION_USER = """
Please review the following line in the context of the surrounding chunk and the reporting period.
Your task is to determine:
1. Whether the line refers to actual or expected performance,
2. The applicable period,
3. The unit of measurement,
4. And the category of the metric (Financials, KPI, or Guidance).

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


def get_8k_classification_message(
        company_name: str,
        quarter: str,
        chunk: str,
        line: str
) -> List[Dict[str, str]]:
    messages = [
        {
            "role": "system",
            "content": FILE_8K_METRICS_CLASSIFICATION_SYSTEM
        },
        {
            "role": "user",
            "content": FILE_8K_METRICS_CLASSIFICATION_USER.format(
                company_name=company_name,
                quarter=quarter,
                chunk=chunk,
                line=line
            )
        },
    ]
    return messages
