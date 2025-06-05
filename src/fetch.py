import logging
import traceback
from typing import Dict, List, Tuple, Type

from pydantic import BaseModel
from tqdm.asyncio import tqdm_asyncio

from src._default import DEFAULT_DATABRICKS_KWARGS, DEFAULT_OPENAI_KWARGS
from src.api_fetcher import AsyncDatabricksAPIFetcher, AsyncOpenAIAPIFetcher
from src.formats import (
    CellListOutput,
    ClassificationOutput,
    DocType,
    ExtractedOutput,
    MetricListOutput,
)
from src.messages import (
    get_8k_classification_message,
    get_8k_extraction_message,
    get_earnings_classification_message,
    get_earnings_extraction_message,
    get_table_cell_wise_messages,
    get_table_row_wise_messages,
)
from src.utils import get_sentences

BaseModelType = Type[BaseModel]

logger = logging.getLogger(__name__)
openai_async_fetcher = AsyncOpenAIAPIFetcher()
databricks_async_fetcher = AsyncDatabricksAPIFetcher()


async def fetch_parsed(
        messages: List[Dict[str, str]],
        response_format: BaseModelType,
        use_databricks: bool = False,
        **kwargs
) -> Tuple[BaseModelType | None, Dict[str, Dict]]:
    
    if use_databricks:
        db_kwargs = DEFAULT_DATABRICKS_KWARGS | {**kwargs,"messages": messages}
    
        databricks_chat_completion = await databricks_async_fetcher.fetch_parsed_completion(**db_kwargs)
        db_output = databricks_chat_completion.choices[0].message.content
        messages =[
                {
                    "role": "user",
                    "content": f"""
        Given the following data, format it with the given response format: {db_output}
        """
                }
            ]

    kwargs = DEFAULT_OPENAI_KWARGS | {**kwargs, "messages": messages, "response_format": response_format}
    openai_parsed_completion = await openai_async_fetcher.fetch_parsed_completion(**kwargs)
    extracted_output = openai_parsed_completion.choices[0].message.parsed
    usage = {"openai": openai_parsed_completion.usage.model_dump()}

    return extracted_output, usage


async def _fetch_extracted_output(
        company_name: str,
        text: str,
        quarter: str,
        doc_type: DocType,
) -> list[Dict[str, Dict]]:
    lines = get_sentences(text=text)

    async def fetch_line(line: str):

        if doc_type == DocType.FILING_8K:
            messages = get_8k_extraction_message(
                company_name=company_name,
                quarter=quarter,
                line=line
            )
        elif doc_type == DocType.EARNINGS_CALL:
            messages = get_earnings_extraction_message(
                company_name=company_name,
                quarter=quarter,
                line=line
            )
        else:
            raise NotImplementedError

        try:
            extracted_output, _ = await fetch_parsed(
                messages=messages, response_format=ExtractedOutput
            )

            return extracted_output.model_dump()

        except Exception as e:
            print(f"An Error occurred while processing extracting quote: {line}, {e}")
            traceback.print_exc()
            return {"kpis": [], "values": [], "units": []}

    results = await tqdm_asyncio.gather(*[fetch_line(line) for line in lines])

    final_result = []

    for result, line in zip(results, lines):
        for title, value, unit in zip(result['titles'], result['values'], result['units']):
            final_result.append(
                {
                    'title': title,
                    'value': value,
                    'unit': unit,
                    'reference': line
                }
            )

    return final_result


async def _fetch_classification_output(
        company_name: str,
        chunk: str,
        line_data: Dict,
        quarter: str,
        doc_type: DocType,
) -> Dict[str, str]:
    line_str = f"Title: {line_data.get('title', 'N/A')}, Value: {line_data.get('value', 'N/A')}, Unit: {line_data.get('unit', 'N/A')}"
    if 'reference' in line_data:
        line_str += f" (from sentence: {line_data['reference']})"

    if doc_type == DocType.FILING_8K:
        messages = get_8k_classification_message(
            company_name=company_name,
            quarter=quarter,
            chunk=chunk,
            line=line_str,
        )
    elif doc_type == DocType.EARNINGS_CALL:
        messages = get_earnings_classification_message(
            company_name=company_name,
            quarter=quarter,
            chunk=chunk,
            line=line_str,
        )
    else:
        raise NotImplementedError

    try:
        classification_output, _ = await fetch_parsed(
            messages=messages, response_format=ClassificationOutput
        )
        return classification_output.model_dump()

    except Exception as e:
        print(f"An Error occurred while processing auditing quote: {line_str}, {e}")
        traceback.print_exc()
        return {"title": "None", "type_": "None", "period": "None", "unit": "None", "category": "None"}


async def _fetch_table_data_row_wise_output(
        table_data: Dict,
        company_name: str,
        quarter: str,
) -> List[Dict]:
    """
    Extracts metric information from table rows.
    Identifies all metrics (rows) in the table and their metadata.
    
    Args:
        table_data (Dict): Dictionary containing table data
        
    Returns:
        List[Dict]: List of dictionaries containing extracted metric information
    """
    try:
        raw_table_data = table_data.get('reference', '')
        messages = get_table_row_wise_messages(company_name, raw_table_data, quarter)

        table_output, _ = await fetch_parsed(
            messages=messages, response_format=MetricListOutput, top_p=0.1, timeout=100
        )
        result = []
        for metric in table_output.model_dump()["data"]:
            if metric.get('title', '').strip() and metric.get('title', '').lower().strip() != 'none':
                result.append(
                    {
                        "index": table_data.get('index', ''),
                        "category": metric.get('category', '').strip(),
                        "title": metric.get('title', '').strip(),
                        "unit": metric.get('unit', '').strip(),
                        "type_": metric.get('type_', '').strip(),
                        "reference": table_data.get('reference', '')
                    }
                )
        return result
    except Exception as e:
        print(f"An Error occurred while processing table data rowwise: {e}")
        traceback.print_exc()
        return []


async def _fetch_table_data_cell_wise_output(
        metric_data: Dict,
        company_name: str,
        quarter: str,
) -> List[Dict]:
    """
    Extracts cell values and periods for a specific metric.
    Takes a single metric as input and extracts all values and periods for this metric.
    
    Args:
        metric_data (Dict): Dictionary containing information about a single metric
                           (includes title, unit, type_, category, reference)
        
    Returns:
        List[Dict]: List of dictionaries containing extracted values and periods for the metric
    """
    try:
        raw_table_data = metric_data.get('reference', '')
        metric_title = metric_data.get('title', '')
        metric_unit = metric_data.get('unit', '')
        metric_type = metric_data.get('type_', '')
        metric_category = metric_data.get('category', '')

        # Pass the specific metric information to the prompt
        messages = get_table_cell_wise_messages(
            company_name=company_name,
            table_data=raw_table_data,
            quarter=quarter,
            metric_title=metric_title,
            metric_unit=metric_unit,
            metric_type=metric_type,
            metric_category=metric_category
        )

        table_output, _ = await fetch_parsed(
            messages=messages, response_format=CellListOutput, top_p=0.1
        )
        result = []

        # Process each cell value and period extracted for this specific metric
        for cell in table_output.model_dump()["data"]:
            added_cell = set()
            if cell.get('value', '').strip() and cell.get('value', '').lower().strip() != 'none' and (
                    cell.get('value', ''), cell.get('period', '')) not in added_cell:
                result.append(
                    {
                        "index": metric_data.get('index', ''),
                        "category": metric_category,
                        "title": metric_title,
                        "value": cell.get('value', '').strip(),
                        "unit": metric_unit,
                        "type_": metric_type,
                        "period": cell.get('period', '').strip(),
                        "reference": raw_table_data
                    }
                )
                added_cell.add((cell.get('value', ''), cell.get('period', '')))
        return result
    except Exception as e:
        print(f"An Error occurred while processing table data cellwise: {e}")
        traceback.print_exc()
        return []
