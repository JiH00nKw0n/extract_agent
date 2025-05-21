import asyncio
import logging
import traceback
from typing import Dict, List, Tuple, Type

from _default import DEFAULT_DATABRICKS_KWARGS, DEFAULT_OPENAI_KWARGS
from api_fetcher import AsyncDatabricksAPIFetcher, AsyncOpenAIAPIFetcher
from messages import (
    get_korean_translation_messages,
    get_kpi_auditing_messages,
    get_kpi_classification_messages,
    get_kpi_extracting_messages,
    get_quote_relevance_messages,
    get_table_extracting_messages,
    get_table_rowwise_messages,
    get_table_cellwise_messages,
)
from pydantic import BaseModel
from tqdm.asyncio import tqdm_asyncio
from utils import get_sentences

BaseModelType = Type[BaseModel]

logger = logging.getLogger(__name__)
openai_async_fetcher = AsyncOpenAIAPIFetcher()
databricks_async_fetcher = AsyncDatabricksAPIFetcher()



class ExtractedOutput(BaseModel):
    titles: List[str]
    values: List[str]
    units: List[str]

class AuditingOutput(BaseModel):
    type_: str
    period: str
    unit: str

class CategoryOutput(BaseModel):
    category: str

class QuoteRelevanceOutput(BaseModel):
    index: int

class KoreanTranslationOutput(BaseModel):
    translation: str

class MetricOutput(BaseModel):
    title: str
    unit: str
    type_: str
    category: str

class MetricListOutput(BaseModel):
    data: List[MetricOutput]

class CellOutput(BaseModel):
    value: str
    period: str
    
class CellListOutput(BaseModel):
    data: List[CellOutput]

class TableCellOutput(BaseModel):
    title: str
    value: str
    unit: str
    period: str
    type_: str
    category: str

class TableDataOutput(BaseModel):
    data: List[TableCellOutput]

async def fetch_parsed(
        messages: List[Dict[str, str]],
        response_format: BaseModelType,
        **kwargs
) -> Tuple[BaseModelType | None, Dict[str, Dict]]:

    # db_kwargs = DEFAULT_DATABRICKS_KWARGS | {**kwargs,"messages": messages}
    
    # databricks_chat_completion = await databricks_async_fetcher.fetch_parsed_completion(**db_kwargs)
    # db_output = databricks_chat_completion.choices[0].message.content
    # parse_message =[
    #         {
    #             "role": "user",
    #             "content": f"""
    # Given the following data, format it with the given response format: {db_output}
    # """
    #         }
    #     ]
    kwargs = DEFAULT_OPENAI_KWARGS | {**kwargs,"messages": messages, "response_format": response_format}
    openai_parsed_completion = await openai_async_fetcher.fetch_parsed_completion(**kwargs)
    extracted_output = openai_parsed_completion.choices[0].message.parsed
    usage = {"openai": openai_parsed_completion.usage.model_dump()}

    return extracted_output, usage


async def _fetch_extracted_output(
        company_name: str,
        text: str,
        quarter: str,
) -> list[Dict[str, Dict]]:
    lines = get_sentences(text=text)

    async def fetch_line(line: str):

        messages = get_kpi_extracting_messages(company_name, line, quarter)

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
            final_result.append({
                'title': title,
                'value': value,
                'unit': unit,
                'reference': line
            })

    return final_result


async def _fetch_auditing_output(
        company_name: str,
        chunk: str,
        line_data: Dict,
        quarter: str,
) -> Dict[str, str]:
    line_str = f"Title: {line_data.get('title', 'N/A')}, Value: {line_data.get('value', 'N/A')}, Unit: {line_data.get('unit', 'N/A')}"
    if 'reference' in line_data:
        line_str += f" (from sentence: {line_data['reference']})"

    messages = get_kpi_auditing_messages(company_name, chunk, line_str, quarter)

    try:
        auditing_output, _ = await fetch_parsed(
            messages=messages, response_format=AuditingOutput
        )
        return auditing_output.model_dump()

    except Exception as e:
        print(f"An Error occurred while processing auditing quote: {line_str}, {e}")
        traceback.print_exc()
        return {"type_": "None", "period": "None", "unit": "None"}


async def _fetch_classification_output(
        result: Dict,
) -> Dict:
    line = result.get('reference', '')
    messages = get_kpi_classification_messages(line)

    try:
        classification_output, _ = await fetch_parsed(
            messages=messages, response_format=CategoryOutput
        )
        category = classification_output.model_dump()['category']
        return {**result, "category": category}

    except Exception as e:
        print(f"An Error occurred while processing duplicate indices: {e}")
        traceback.print_exc()
        return {**result, "category": "Unclear"}


async def _fetch_quote_relevance_output(
        question: str,
        chunk: str,
) -> Tuple[List[str], List[str]]:
    lines = get_sentences(text=chunk)
    data = "\n".join([f"**Index {i}**. {line}" for i, line in enumerate(lines)])
    messages = get_quote_relevance_messages(question, data)

    try:
        quote_relevance_output, _ = await fetch_parsed(
            messages=messages, response_format=QuoteRelevanceOutput
        )
        index = quote_relevance_output.model_dump()["index"]
        if index == -1:
            return [], []
        # 인덱스 앞뒤로 총 3개의 항목을 담는 리스트 생성
        result_lines = []
        
        # 앞의 항목 추가 (인덱스-1이 존재하는 경우)
        if index > 0:
            result_lines.append(lines[index-1])
            
        # 현재 인덱스 항목 추가
        result_lines.append(lines[index])
        
        # 뒤의 항목 추가 (인덱스+1이 존재하는 경우)
        if index < len(lines) - 1:
            result_lines.append(lines[index+1])
        
        translated_outputs = await tqdm_asyncio.gather(*[ 
            fetch_parsed(messages=get_korean_translation_messages(line), response_format=KoreanTranslationOutput) for line in result_lines
        ])
        translated_lines = [t[0].model_dump()["translation"] for t in translated_outputs]
        return result_lines, translated_lines

    except Exception as e:
        print(f"An Error occurred while processing quote relevance: {e}")
        traceback.print_exc()
        return [], []


async def _fetch_table_data_rowwise(
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
        messages = get_table_rowwise_messages(company_name, raw_table_data, quarter)
        
        table_output, _ = await fetch_parsed(
            messages=messages, response_format=MetricListOutput, top_p = 0.1, timeout = 100
        )
        result = []
        for metric in table_output.model_dump()["data"]:
            if metric.get('title', '').strip() and metric.get('title', '').lower().strip() != 'none':
                result.append({
                    "index": table_data.get('index', ''),
                    "category": metric.get('category', '').strip(),
                    "title": metric.get('title', '').strip(),
                    "unit": metric.get('unit', '').strip(),
                    "type_": metric.get('type_', '').strip(),
                    "reference": table_data.get('reference', '')
                })
        return result
    except Exception as e:
        print(f"An Error occurred while processing table data rowwise: {e}")
        traceback.print_exc()
        return []

async def _fetch_table_data_cellwise(
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
        messages = get_table_cellwise_messages(
            company_name=company_name,
            table_data=raw_table_data,
            quarter=quarter,
            metric_title=metric_title,
            metric_unit=metric_unit,
            metric_type=metric_type,
            metric_category=metric_category
        )
        
        table_output, _ = await fetch_parsed(
            messages=messages, response_format=CellListOutput, top_p = 0.1
        )   
        result = []
        
        # Process each cell value and period extracted for this specific metric
        for cell in table_output.model_dump()["data"]:
            added_cell = set()
            if cell.get('value', '').strip() and cell.get('value', '').lower().strip() != 'none' and (cell.get('value', ''), cell.get('period', '')) not in added_cell:
                result.append({
                    "index": metric_data.get('index', ''),
                    "category": metric_category,
                    "title": metric_title,
                    "value": cell.get('value', '').strip(),
                    "unit": metric_unit,
                    "period": cell.get('period', '').strip(),
                    "type_": metric_type,
                    "reference": metric_data.get('reference', '')
                })
                added_cell.add((cell.get('value', ''), cell.get('period', '')))
        return result
    except Exception as e:
        print(f"An Error occurred while processing table data cellwise for metric '{metric_data.get('title', '')}': {e}")
        traceback.print_exc()
        return []
