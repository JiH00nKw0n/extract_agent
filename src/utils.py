import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Set

import dotenv
import pandas as pd
import requests
from bs4 import BeautifulSoup
from nltk import sent_tokenize, word_tokenize
from tenacity import retry, stop_after_attempt, wait_fixed

from src._default import DEFAULT_EMPTY_PARSED_COMPLETION

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


def chunk_10k_10q_html(text: str) -> Dict[int, str]:
    """
    Chunks 10-K and 10-Q HTML files by p tags and table tags in document order.
    
    Returns:
        Dict[int, str]: Dictionary with index-text pairs
    """
    soup = BeautifulSoup(text, 'html.parser')
    chunks = {}

    # Find all p tags and table tags and sort them in document order
    elements = soup.find_all(['p', 'table'])

    # Process in order
    for idx, element in enumerate(elements):
        if element.name == 'p' and element.text.strip():
            chunks[idx] = element.text.strip()
        elif element.name == 'table' and element.text.strip():
            chunks[idx] = element.text.strip()

    return chunks


def chunk_earnings_html(text: str) -> Dict[int, str]:
    """
    Processes Earnings HTML files by treating each speaker (strong tag) and their speech content (p tag) as separate chunks.
    
    Returns:
        Dict[int, str]: Dictionary with index-text pairs
    """
    soup = BeautifulSoup(text, 'html.parser')
    chunks = {}

    # Process all elements in document order
    elements = soup.find_all(['strong', 'p'])

    for idx, element in enumerate(elements):
        if element.name == 'strong':
            # Extract speaker information
            speaker_text = element.text.strip()

            # Check and process if span exists
            if ' - ' in speaker_text:
                speaker_name = speaker_text.split(' - ')[0].strip()
                speaker_role = speaker_text.split(' - ')[1].strip()
                formatted_speaker = f"{speaker_name} - {speaker_role}"
            else:
                formatted_speaker = speaker_text

            # Save speaker as a separate chunk
            if formatted_speaker:
                chunks[idx] = formatted_speaker

        elif element.name == 'p':
            # Save speech content as a separate chunk
            content = element.text.strip()
            if content:
                chunks[idx] = content

    return chunks


def chunk_8k_json(text: str) -> Dict[int, str]:
    """
    Chunks 8-K JSON files based on content.
    
    Returns:
        Dict[int, str]: Dictionary with index-text pairs
    """
    data = json.loads(text)
    chunks = {}
    idx = 0

    for item in data:
        # Chunk based on blocks in each item
        if "content" in item:
            chunks[idx] = item["content"].strip()
            idx += 1

    return chunks


def chunk_def14a_json(text: str) -> Dict[int, str]:
    """
    Chunks DEF14A JSON files based on each item in the JSON list.
    
    Returns:
        Dict[int, str]: Dictionary with index-text pairs
    """
    data = json.loads(text)
    chunks = {}
    idx = 0

    # Chunk based on dictionary items in the list
    for item in data:
        # Consider each item as one chunk
        if isinstance(item, dict):
            # Check if main text field exists
            if "content" in item and item["content"].strip():
                chunks[idx] = item["content"].strip()
                idx += 1

    return chunks


def get_chunk(text: str, file_type: str) -> Dict[int, str]:
    """
    Calls the appropriate chunking method based on file type.
    
    Args:
        text: Text to be chunked
        file_type: File type (10-K, 8-K, 10-Q, Earnings, DEF14A, CSV)
    
    Returns:
        Dict[int, str]: Dictionary with index-text pairs
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
        # Default chunking by newline
        chunks = {}
        for idx, line in enumerate(text.split("\n")):
            chunks[idx] = line
        return chunks


def duplicate_token_count(text_a: str, text_b: str) -> int:
    tokens_a = set(word_tokenize(text_a.lower()))
    tokens_b = set(word_tokenize(text_b.lower()))
    return len(tokens_a & tokens_b)


def check_valid_value(html_content: str, value: str) -> str:
    """
    value의 앞부분부터 시작해서 html_content에 존재하는 가장 긴 문자열을 반환

    Args:
        html_content (str): 검색할 HTML 내용
        value (str): 확인할 값

    Returns:
        str: html_content에 존재하는 value의 가장 긴 앞부분 문자열
    """
    if not value or not html_content:
        return ""

    # value의 앞부분부터 점진적으로 늘려가면서 html_content에 존재하는지 확인
    longest_match = ""

    for i in range(1, len(value) + 1):
        substring = value[:i]
        if substring in html_content:
            longest_match = substring
        else:
            # 더 이상 매치되지 않으면 중단
            break

    return longest_match
