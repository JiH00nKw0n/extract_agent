from typing import Dict, List

FILE_8K_TABLE_ROW_WISE_EXTRACT_SYSTEM = """
<task>
You are a financial analyst reviewing a table from an 8-K report for a specific firm.
Your task is to analyze the table refer to the provided "context" of text.
This is the first stage of a two-stage extraction process where your goal is to identify all metrics (rows) in the table.
</task>

<context>
- The "company_name" is the name of the firm.
- The "reporting_quarter" refers to the reporting financial period of the 8-K document itself (e.g., "2023 Q4").
</context>

<guideline>
1.  **Analyze the "table" within the context.**
2.  **Identify the main KPI** discussed in the "table".
3.  **Determine the title of the KPI metrics:** 
    *   Extract the title of the KPI metrics from the row header (first cell in each row) of the table.
    *   Sometimes, the title is not directly in the row header, but column header separates similar metrics into distinct columns.
        If multiple columns are related to the same metric, extract the title from both row header and column header then combine to one representative title.
        If column header is unclear, guess by the context and unit of the metric in the cell.
        (e.g., "Total Revenue" in row header and column separated by "actual" and "percentage of total" -> two metrics: "Total Revenue" and "Total Revenue (percentage)")
    *   Clarify the hierachy among row headers then generate representative title for each row.
        (e.g., "Revenue:" in row header without values in the row and Segment names in row headers below. -> "Segment name Revenue")
    *   If the same metric is broken down into different sectors or segments, extract the title as "{{Segment name}} {{metric name}}".
        If the row header only contains name of sector, try to find the metric name (e.g., "Operating Income") in the upper row header.
        Same metric broken down into different sectors or segments should be extracted as distinct metrics.
        (e.g., "Revenue" in row header and "Food & Beverage" in lower row header -> "Food & Beverage Revenue")
4.  **Determine the Type:**
    *   If the KPI represents a past result or performance that has already occurred, classify it as "actual".
    *   If the KPI represents a future expectation, projection, guidance, forecast, or outlook, classify it as "expected". 
        Look for keywords like "estimates," "guidance," "consensus," etc.
5.  **Determine the Unit:**
    *   Identify the unit of measurement for the KPI value (e.g., "$", "%", "shares", "basis points", "millions", etc.).
    *   The unit might be directly attached to the number or implied by the context in the metric title.
    *   There might be unit expression in another row or column.
        If so, extract the unit from the expression.
    *   Same metric with different unit should be separated into distinct columns. Consider them as different metrics.
        (e.g., "Revenue item" in row header and "propotion in segment breakdown" in column header.)
    *   If no specific unit is found or applicable, return "None".
6.  **Determine the category of the KPI metrics:**
    *   Determine the category of the KPI metrics based on the context in the table.
    *   The category should be one of the following: "Financials", "KPI", "Guidance", "Unclear".
    *   Definitions of the categories are as follows:
        - **Financials:** Standard financial statement items (Revenue, Income, EPS, etc.) representing actual historical results.
        - **KPI:** Sector-specific performance metrics about (customer growth, ARPU, segment revenues) that can be financial or non-financial.
        - **Guidance:** Forward-looking statements about future performance, often using terms like "expect," "guidance," etc.
7.  **TIP:**
    *   Since the input table is created by horizontally splitting merged cells, you should examine horizontally adjacent multiple cells 
        and concatenate them to form the complete value.
    *   Same metric with different unit should be separated into distinct rows.
</guideline>

<format>
Return your output in the following JSON format:
```json
{{
    "data": [
        {{
            "title": string | "None" /** name of the KPI metrics */,
            "unit": string | "None" /** unit of the KPI metrics */,
            "type_": "actual" | "expected" | "None" /** type of the KPI metrics */,
            "category": "Financials" | "KPI" | "Guidance" | "Unclear" /** category of the KPI metrics */
        }},
        ... /** list of rows/metrics in the table */
    ]
}}
```
</format>
"""

FILE_8K_TABLE_ROW_WISE_EXTRACT_USER = """
Analyze the following table based on the provided context.

<company_name>
{company_name}
</company_name>

<reporting_quarter>
{quarter}
</reporting_quarter>

<table>
{table_data}
</table>
"""


def get_table_row_wise_messages(
        company_name: str,
        table_data: str,
        quarter: str) -> List[Dict[str, str]]:
    """
    Generates messages for the LLM to extract structured data from a given table.
    """
    messages = [
        {
            "role": "system",
            "content": FILE_8K_TABLE_ROW_WISE_EXTRACT_SYSTEM
        },
        {
            "role": "user",
            "content": FILE_8K_TABLE_ROW_WISE_EXTRACT_USER.format(
                company_name=company_name,
                table_data=table_data,
                quarter=quarter
            )
        },
    ]
    return messages


FILE_8K_TABLE_CELL_WISE_EXTRACT_SYSTEM = f"""
<task>
You are a financial analyst reviewing a table from an 8-K report for a specific firm.
Your task is to analyze the table refer to the provided "context" of text.
This is the second stage of a two-stage extraction process, where you are given a specific metric and need to extract all values and periods for this metric from the table.
</task>

<context>
- The "company_name" is the name of the firm.
- The "reporting_quarter" refers to the reporting financial period of the 8-K document itself (e.g., "2023 Q4").
</context>

<guideline>
1.  **Analyze the "table" within the context.**
2.  **Focus on the specific metric** provided in the input. The metric title, unit, type, and category are already extracted.
3.  **Extract the value of the KPI metrics:**
    *   Extract the value of the KPI from each cell in the table that corresponds to this metric.
    *   Since table parsed implementation is not perfect, some values might be separated into multiple cells.
    *   In this case, concatenate the values from multiple cells to form the complete value.
    *   Negative values are represented with parentheses. If so, return as -%d and remove the parentheses.
    *   Remove the unit of value. Unit should already be defined in the metric.
    *   If the value is not specified as numeric value in the table, return "None".
4.  **Determine the Period:**
    *   Extract the specific fiscal period the KPI value applies to (e.g., "2023 Q1", "2024 Full Year").
    *   This might be mentioned directly in the *column header* of table or inferred from the reporting_quarter.
    *   If the type is "actual", the period should be formatted as "YYYY QN" or "YYYY Full Year".
    *   If the type is "expected", the period should be formatted as "(Expected) YYYY QN" or "(Expected) YYYY Full Year".
    *   If no specific period is mentioned, the period should be "None".
    *   There might expressions like "Three months ended" or "Year ended" on the column header.
        In this case, extract the period as "YYYY QN" or "YYYY Full Year" based on the context.
5.  **TIP:**
    *   Table has period information in the column header, but not directly in the same column of cell. 
        Refer to the last non-null column header to determine the period.
    *   Since the input table is created by horizontally splitting merged cells, you should examine horizontally adjacent multiple cells 
        and concatenate them to form the complete value.
    *   Output data might contain values in duplicated (metric, period, value) tuples for multiple cells.
        Only return distinct values for (metric, period, value) tuples.
</guideline>

<format>
Return your output in the following JSON format:
```json
{{
    "data": [
        {{
            "value": string | "None" /** value in each cells of table */,
            "period": "YYYY QN | YYYY Full Year" | "(Expected) YYYY QN | (Expected) YYYY Full Year" | "None" /** period of the KPI reported in the table */
        }},
        ... /** list of values for the metric from the table */
    ]
}}
```
</format>   
"""

FILE_8K_TABLE_CELL_WISE_EXTRACT_USER = """
Analyze the following table based on the provided context. 
Extract all values and periods for a specific metric in the table.

<company_name>
{company_name}
</company_name>

<reporting_quarter>
{quarter}
</reporting_quarter>

<metric>
Title: {metric_title}
Unit: {metric_unit}
Type: {metric_type}
Category: {metric_category}
</metric>

<table>
{table_data}
</table>
"""


def get_table_cell_wise_messages(
        company_name: str,
        table_data: str,
        quarter: str,
        metric_title: str = "",
        metric_unit: str = "",
        metric_type: str = "",
        metric_category: str = ""
) -> List[Dict[str, str]]:
    """
    Generates messages for the LLM to extract cell values for a specific metric from a table.

    Args:
        company_name: Name of the company
        table_data: CSV or markdown representation of the table
        quarter: Reporting quarter
        metric_title: Title of the metric to extract values for
        metric_unit: Unit of the metric
        metric_type: Type of the metric (actual or expected)
        metric_category: Category of the metric
    """
    messages = [
        {
            "role": "system",
            "content": FILE_8K_TABLE_CELL_WISE_EXTRACT_SYSTEM
        },
        {
            "role": "user",
            "content": FILE_8K_TABLE_CELL_WISE_EXTRACT_USER.format(
                company_name=company_name,
                table_data=table_data,
                quarter=quarter,
                metric_title=metric_title,
                metric_unit=metric_unit,
                metric_type=metric_type,
                metric_category=metric_category
            )
        },
    ]
    return messages
