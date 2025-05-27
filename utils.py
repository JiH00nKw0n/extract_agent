import csv
import io
import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Set

import dotenv
import pandas as pd
import requests
from bs4 import BeautifulSoup
from nltk import sent_tokenize
from tenacity import retry, stop_after_attempt, wait_fixed

from _default import DEFAULT_EMPTY_PARSED_COMPLETION

logger = logging.getLogger(__name__)

dotenv.load_dotenv()


def handle_max_retries(retry_state):
    """Logs only the last error message after max retries are exhausted."""
    last_exception = retry_state.outcome.exception()
    if last_exception:
        logging.error(f"Task {retry_state.args[0]} failed after max retries: {str(last_exception)[:20]}")
    return DEFAULT_EMPTY_PARSED_COMPLETION


# Create a reusable decorator for retrying async functions
def retry_fetch(wait_seconds: int, max_retries: int):
    return retry(
        stop=stop_after_attempt(max_retries),  # Retry any exception up to 3 times
        wait=wait_fixed(wait_seconds),  # Wait 1 second between retries
        retry_error_callback=handle_max_retries  # Log only the last error
    )


def get_sentences(text: str):
    """
    Tokenizes the given text into sentences using NLTK's `sent_tokenize`.

    Args:
        text (`str`): The text to be tokenized into sentences.

    Returns:
        `List[str]`: A list of sentences extracted from the text.
    """
    text_sentences = text.split("\n")  # Split text into lines
    sentences = []
    for sentence in text_sentences:
        # Tokenize each line into sentences
        sentences.extend(sent_tokenize(sentence))

    return sentences


def split_transcript_into_n(text: str, n: int) -> List[str]:
    if n < 2:
        if n == 1:
            return [text]
        else:
            raise ValueError("The number of parts (n) must be 1 or greater.")

    sections = text.split("\n")
    total_length = len(sections)
    split_size = total_length // n

    parts = [sections[i * split_size:(i + 1) * split_size] for i in range(n)]
    remaining_sections = sections[n * split_size:]
    if remaining_sections:
        parts[-1].extend(remaining_sections)

    return ["\n".join(part) for part in parts]


def split_list_into_n(lst: List[Any], n: int) -> List[List[Any]]:
    """
    Splits a list into `n` roughly equal parts.

    Args:
        lst (`List[Any]`): The list to be split.
        n (`int`): The number of parts to split the list into.

    Returns:
        `List[List[Any]]`: A list of `n` sublists, each containing roughly equal elements from the input list.
    """
    len_list = len(lst)  # Total length of the input list
    return [lst[i * len_list // n: (i + 1) * len_list // n] for i in range(n) if
            lst[i * len_list // n: (i + 1) * len_list // n]]


def get_company_name(ticker):
    api_key = os.getenv("FMP_API_KEY")
    url = f'https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={api_key}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data:
            return data[0].get('companyName', 'Company name not found')
        else:
            return 'No data found for the given ticker symbol'
    else:
        logger.info(f'Error while retrieving company name for ticker {ticker}: {response.status_code}')
        return ticker


def get_ticker_set(
        start_date: datetime,
        end_date: datetime,
        filename: str = "./data/historical_component.csv"
) -> Set:
    """
    Extract the common subset of tickers that appear in all rows of a given date range.

    Parameters
    ----------
    start_date : datetime
        The start date for filtering the data.
    end_date : datetime
        The end date for filtering the data.
    filename : str, optional
        The path to the CSV file containing historical data (default is "./data/historical_component.csv").

    Returns
    -------
    set
        A set of tickers that are common across all rows within the specified date range.

    Raises
    ------
    FileNotFoundError
        If the specified file is not found.
    ValueError
        If the 'tickers' column is missing or the filtered DataFrame is empty.

    Notes
    -----
    - The CSV file should have a 'date' column as its index and a 'tickers' column containing comma-separated tickers.
    - The 'date' column in the CSV must be in a format that can be parsed as datetime.
    """
    if not os.path.isfile(filename):
        raise FileNotFoundError(f"{filename} not found.")

    # Read the CSV file
    dataframe = pd.read_csv(filename, index_col='date', parse_dates=True)

    # Filter the DataFrame by date range
    filtered_df = dataframe[(dataframe.index >= start_date) & (dataframe.index <= end_date)]

    print(filtered_df)

    if filtered_df.empty:
        raise ValueError("No data found in the specified date range.")

    if 'tickers' not in filtered_df.columns:
        raise ValueError("The required 'tickers' column is missing in the CSV file.")

    # Split tickers in each row and find the common subset
    tickers_sets = [set(row.split(',')) for row in filtered_df['tickers']]
    common_tickers = set.intersection(*tickers_sets)
    print(common_tickers)
    print(len(common_tickers))
    return {t.strip() for t in common_tickers}


def is_numeric_value(text: str) -> bool:
    """Checks if a string represents a numeric value, handling currency, commas, and parentheses."""
    if not isinstance(text, str):
        return False
    cleaned_text = text.strip()
    if not cleaned_text:
        return False

    # Handle parentheses for negative numbers: e.g., (1,234.56) -> -1234.56
    is_negative = False
    if cleaned_text.startswith('(') and cleaned_text.endswith(')'):
        cleaned_text = cleaned_text[1:-1]
        is_negative = True  # Mark as negative

    # Remove currency symbols and commas
    cleaned_text = cleaned_text.replace('$', '').replace(',', '')

    # Explicitly check for common non-numeric placeholders after cleaning
    if cleaned_text == '-' or not cleaned_text:
        return False

    # Prepend '-' back if it was in parentheses
    if is_negative:
        cleaned_text = '-' + cleaned_text

    try:
        float(cleaned_text)
        return True
    except ValueError:
        return False


def parse_html_table(raw_html: str) -> list[dict]:
    """
    Parses an HTML table string and extracts data into a list of dictionaries.

    Each dictionary represents a cell value associated with its row name.
    Excludes records where:
    - Either the row name or the cell value is empty.
    - The row name is the BOM character ('\ufeff').
    - The cell value does not represent a numeric value (handles $, ,, ()).

    Args:
        raw_html: A string containing the HTML table.

    Returns:
        A list of dictionaries, each with "row_name" and "value" keys.
    """
    # (1) Clean HTML
    clean_html = raw_html.replace("\\n", "\n")  # Escape sequence correction if needed

    # (2) BeautifulSoup로 파싱
    soup = BeautifulSoup(clean_html, "html.parser")
    table = soup.find("table")
    if not table:
        return []  # 테이블이 없으면 빈 리스트 반환
    rows = table.find_all("tr")

    # (3) 테이블 데이터 추출 및 필터링
    result_records = []
    for row in rows:
        cols = row.find_all("td")
        parsed_row_texts = [col.get_text(strip=True) for col in cols]

        if not parsed_row_texts:
            continue  # 빈 행 스킵

        row_name = parsed_row_texts[0]

        # row_name이 비어있거나 BOM 문자인 경우 이 행 전체를 건너뛰니다.
        if not row_name or row_name == '\ufeff':
            continue

        # 첫 번째 열(row_name) 이후의 셀들을 처리
        for value in parsed_row_texts[1:]:
            # value가 비어있지 않고 숫자 형식인 경우에만 레코드 생성
            if value and is_numeric_value(value):
                record = {
                    "title": row_name,
                    "value": value,  # 원본 값 저장
                    "unit": "N/A",
                    "reference": "N/A"
                }
                result_records.append(record)

    return result_records

def parse_html_table_to_csv(raw_html: str) -> str:
    """
    Converts an HTML <table> into a CSV string preserving its structure as-is.

    Args:
        raw_html (str): A string containing the HTML table.

    Returns:
        str: A CSV-formatted string preserving the original table's layout.
    """
    clean_html = raw_html.replace("\\n", "\n")
    soup = BeautifulSoup(clean_html, "html.parser")
    table = soup.find("table")
    if not table:
        return ""

    writer = []

    for row in table.find_all("tr"):
        row_data = []
        for idx, cell in enumerate(row.find_all(["td", "th"])):
            # Get text with indentation preserved
            text = cell.get_text(strip=False)
            
            # Extract indentation from style attribute if available
            indent_px = 0
            # Try to get style from <td> or from its first <p> child if not present
            style = cell.get("style", "")
            if not style:
                # Check if the cell contains a <p> tag with style
                p_tag = cell.find("p")
                if p_tag and p_tag.has_attr("style"):
                    style = p_tag["style"]
            if style:
                # Look for various padding patterns
                padding_patterns = [
                    r'padding-left:\s*(-?\d+(?:\.\d+)?)(pt|px|em|rem)',
                    r'padding:\s*(-?\d+(?:\.\d+)?)(?:pt|px|em|rem)?\s+(-?\d+(?:\.\d+)?)(?:pt|px|em|rem)?\s+(-?\d+(?:\.\d+)?)(?:pt|px|em|rem)?\s+(-?\d+(?:\.\d+)?)(?:pt|px|em|rem)?',  # padding: top right bottom left
                    r'padding:\s*(-?\d+(?:\.\d+)?)(?:pt|px|em|rem)?\s+(-?\d+(?:\.\d+)?)(?:pt|px|em|rem)?',  # padding: vertical horizontal
                ]
                
                for pattern in padding_patterns:
                    padding_match = re.search(pattern, style)
                    if padding_match:
                        value = float(padding_match.group(1))
                        unit = padding_match.group(2) if len(padding_match.groups()) > 1 else 'px'
                        
                        # Convert different units to approximate pixel values
                        if unit == 'pt':
                            value = value * 1.33  # 1pt ≈ 1.33px
                        elif unit == 'em':
                            value = value * 16    # 1em ≈ 16px (default font size)
                        elif unit == 'rem':
                            value = value * 16    # 1rem ≈ 16px (default font size)
                        
                        indent_px = int(value) if value > 0 else 0
                        break
                
                # Also check for text-indent
                if not indent_px:
                    text_indent_match = re.search(r'text-indent:\s*(-?\d+(?:\.\d+)?)(pt|px|em|rem)', style)
                    if text_indent_match:
                        value = float(text_indent_match.group(1))
                        unit = text_indent_match.group(2)
                        
                        if unit == 'pt':
                            value = value * 1.33
                        elif unit == 'em':
                            value = value * 16
                        elif unit == 'rem':
                            value = value * 16
                        
                        indent_px = int(value) if value > 0 else 0
                
                # Check for margin-left
                if not indent_px:
                    margin_match = re.search(r'margin-left:\s*(-?\d+(?:\.\d+)?)(pt|px|em|rem)', style)
                    if margin_match:
                        value = float(margin_match.group(1))
                        unit = margin_match.group(2)
                        
                        if unit == 'pt':
                            value = value * 1.33
                        elif unit == 'em':
                            value = value * 16
                        elif unit == 'rem':
                            value = value * 16
                        
                        indent_px = int(value) if value > 0 else 0
            
            # Calculate leading spaces from the text as well
            leading_spaces = len(text) - len(text.lstrip())
            
            # Count HTML non-breaking spaces
            nbsp_count = text.count('&nbsp;') + text.count('&#160;') + text.count('\u00a0')
            
            # Use the largest indentation value (convert px to approximate space count)
            space_from_px = indent_px // 8 if indent_px > 0 else 0  # Approximate: 8px ≈ 1 space
            effective_indent = max(leading_spaces, space_from_px, nbsp_count)
            
            if effective_indent > 0 and idx == 0:
                text = "&nbsp;" * effective_indent + text.lstrip()
            else:
                text = text.lstrip()
                
            # Strip trailing whitespace
            text = text.rstrip()
            # Handle colspan
            colspan = int(cell.get("colspan", 1))
            row_data.extend([text] * colspan)
        # pass if no text in the row
        if all([cell.strip() in ['', '\ufeff', '\ufffd'] for cell in row_data]):
            continue
        writer.append(row_data)


    if not writer:
        return ""
    
    # Create markdown table
    markdown_table = []
    
    # Process each row
    for i, line in enumerate(writer):
        # Split the line into cells
        cells = line
            
        # Format cells for markdown (replace empty cells with spaces and deduplicate consecutive identical values)
        formatted_cells = []
        prev_cell = None
        for cell in cells:
            if cell == prev_cell:
                formatted_cells.append(" ")
            else:
                formatted_cells.append(cell if cell else " ")
                prev_cell = cell
        row = "| " + " | ".join(formatted_cells) + " |"
        markdown_table.append(row)
        
        # Add header separator after the first row
        if i == 0:
            separator = "| " + " | ".join(["---"] * len(cells)) + " |"
            markdown_table.append(separator)
    
    # Remove rows where all cells are empty or just spaces
    filtered_rows = []
    for i, row in enumerate(markdown_table):
        cells = row.split('|')[1:-1]  # Remove the first and last empty elements
        cells = [cell.strip() for cell in cells]
        
        # Skip separator row (contains only "---", for the first row)
        if i == 0 or all(cell == "---" for cell in cells):
            filtered_rows.append(row)
            continue
            
        # Check if row has at least one non-empty cell or row header is not empty
        if any(cell and not any(c in cell for c in ['\ufeff', '\ufffd', " "]) for cell in cells) or cells[0] != "":
            filtered_rows.append(row)
    
    # Check for empty columns
    if filtered_rows:
        # Get the number of columns from the separator row
        separator_idx = next((i for i, row in enumerate(filtered_rows) if "---" in row), -1)
        if separator_idx != -1:
            num_cols = len(filtered_rows[separator_idx].split('|')) - 2  # -2 for the empty elements at start/end
            
            # Check each column
            empty_col_indices = []
            for col_idx in range(num_cols):
                is_empty = True
                for row_idx, row in enumerate(filtered_rows):
                    if "---" in row:  # Skip separator row
                        continue
                    cells = row.split('|')[1:-1]
                    if col_idx < len(cells) and cells[col_idx].strip() and cells[col_idx].strip() != " ":
                        is_empty = False
                        break
                if is_empty:
                    empty_col_indices.append(col_idx)
            
            # Remove empty columns
            if empty_col_indices:
                new_rows = []
                for row in filtered_rows:
                    cells = row.split('|')[1:-1]
                    new_cells = [cells[i] for i in range(len(cells)) if i not in empty_col_indices]
                    new_row = "| " + " | ".join(new_cells) + " |"
                    new_rows.append(new_row)
                filtered_rows = new_rows
    
    markdown_table = filtered_rows
    
    # Join all rows into a single string
    markdown_output = "\n".join(markdown_table)

    return markdown_output

def chunk_10k_10q_html(text: str) -> Dict[int, str]:
    """
    10-K, 10-Q HTML 파일을 p 태그와 table 태그 단위로 문서 순서에 맞게 청킹합니다.
    
    Returns:
        Dict[int, str]: 인덱스-텍스트 형태의 딕셔너리
    """
    soup = BeautifulSoup(text, 'html.parser')
    chunks = {}
    
    # 모든 p 태그와 table 태그를 찾아서 문서 순서대로 정렬
    elements = soup.find_all(['p', 'table'])
    
    # 순서대로 처리
    for idx, element in enumerate(elements):
        if element.name == 'p' and element.text.strip():
            chunks[idx] = element.text.strip()
        elif element.name == 'table' and element.text.strip():
            chunks[idx] = element.text.strip()
    
    return chunks


def chunk_earnings_html(text: str) -> Dict[int, str]:
    """
    Earnings HTML 파일을 각 발화자(strong 태그)와 발화 내용(p 태그)을 각각 별도 청크로 처리합니다.
    
    Returns:
        Dict[int, str]: 인덱스-텍스트 형태의 딕셔너리
    """
    soup = BeautifulSoup(text, 'html.parser')
    chunks = {}
    
    # 문서의 모든 요소를 순서대로 처리
    elements = soup.find_all(['strong', 'p'])
    
    for idx, element in enumerate(elements):
        if element.name == 'strong':
            # 발화자 정보 추출
            speaker_text = element.text.strip()
            
            # span이 있는지 확인하고 처리
            if ' - ' in speaker_text:
                speaker_name = speaker_text.split(' - ')[0].strip()
                speaker_role = speaker_text.split(' - ')[1].strip()
                formatted_speaker = f"{speaker_name} - {speaker_role}"
            else:
                formatted_speaker = speaker_text
                
            # 발화자를 별도 청크로 저장
            if formatted_speaker:
                chunks[idx] = formatted_speaker
        
        elif element.name == 'p':
            # 발화 내용을 별도 청크로 저장
            content = element.text.strip()
            if content:
                chunks[idx] = content
    
    return chunks


def chunk_8k_json(text: str) -> Dict[int, str]:
    """
    8-K JSON 파일을 content 기준으로 청킹합니다.
    
    Returns:
        Dict[int, str]: 인덱스-텍스트 형태의 딕셔너리
    """
    data = json.loads(text)
    chunks = {}
    idx = 0
    
    for item in data:
        # 각 아이템의 blocks를 기준으로 청킹
        if "content" in item:
            chunks[idx] = item["content"].strip()
            idx += 1
        
    return chunks


def chunk_def14a_json(text: str) -> Dict[int, str]:
    """
    DEF14A JSON 파일을, JSON 리스트 내의 각 항목을 기준으로 청킹합니다.
    
    Returns:
        Dict[int, str]: 인덱스-텍스트 형태의 딕셔너리
    """
    data = json.loads(text)
    chunks = {}
    idx = 0
    
    # DEF14A는 리스트 내 딕셔너리 항목을 기준으로 청킹
    for item in data:
        # 각 아이템을 하나의 청크로 간주
        if isinstance(item, dict):
            # 주요 텍스트 필드가 있는지 확인
            if "content" in item and item["content"].strip():
                chunks[idx] = item["content"].strip()
                idx += 1
    
    return chunks



def get_chunk(text: str, file_type: str) -> Dict[int, str]:
    """
    파일 타입에 따라 적절한 chunking 메서드를 호출합니다.
    
    Args:
        text: 청킹할 텍스트
        file_type: 파일 타입 (10-K, 8-K, 10-Q, Earnings, DEF14A, CSV)
    
    Returns:
        Dict[int, str]: 인덱스-텍스트 형태의 딕셔너리
    """
    if file_type == "10-K" or file_type == "10-Q":
        return chunk_10k_10q_html(text)
    elif file_type == "8-K":
        return chunk_8k_json(text)
    elif file_type == "DEF14A":
        return chunk_def14a_json(text)
    elif file_type == "Earnings":
        return chunk_earnings_html(text)
    else:
        # 기본적으로는 줄바꿈 기준으로 chunking
        chunks = {}
        for idx, line in enumerate(text.split("\n")):
            chunks[idx] = line
        return chunks

