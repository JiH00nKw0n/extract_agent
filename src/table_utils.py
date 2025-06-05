import re

from bs4 import BeautifulSoup

from src.utils import is_numeric_value


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

    # (2) Parse with BeautifulSoup
    soup = BeautifulSoup(clean_html, "html.parser")
    table = soup.find("table")
    if not table:
        return []  # Return empty list if no table found
    rows = table.find_all("tr")

    # (3) Extract and filter table data
    result_records = []
    for row in rows:
        cols = row.find_all("td")
        parsed_row_texts = [col.get_text(strip=True) for col in cols]

        if not parsed_row_texts:
            continue  # Skip empty rows

        row_name = parsed_row_texts[0]

        # Skip the entire row if row_name is empty or BOM character
        if not row_name or row_name == '\ufeff':
            continue

        # Process cells after the first column (row_name)
        for value in parsed_row_texts[1:]:
            # Create record only if value is not empty and is numeric
            if value and is_numeric_value(value):
                record = {
                    "title": row_name,
                    "value": value  # Store original value
                }
                result_records.append(record)

    return result_records


def extract_table_with_preceding_text(html_content: str) -> list:
    """
    Extracts tables and their preceding continuous text from HTML.

    Args:
        html_content: HTML string

    Returns:
        list: Each element is in the form {'content': 'text+table', 'table_only': 'table only'}
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    # Get all elements in order
    all_elements = soup.find_all(['p', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'table'])
    
    # 테이블이 있는지 확인
    tables = soup.find_all('table')
    if not tables:
        raise ValueError("Can't find any table in the HTML.")

    result_chunks = []
    current_text_elements = []

    preceding_text = ""
    
    for element in all_elements:
        if element.name == 'table':
            # When a table is found

            # Combine accumulated text elements
            for text_elem in current_text_elements:
                text_content = text_elem.get_text(strip=True)
                if text_content:  # Only if not empty text
                    preceding_text += text_content + "\n"

            # Extract table HTML
            table_html = str(element)
            table_html = parse_html_table_to_markdown(table_html)

            # Create chunk combining text and table
            combined_content = preceding_text.strip()
            if combined_content:
                combined_content += "\n\n" + table_html
            else:
                combined_content = table_html

            return {
                'content': combined_content,
                'table_only': table_html,
                'preceding_text': preceding_text.strip()
                }

        else:
            # For text elements (p, div, h1-h6 etc.)
            text_content = element.get_text(strip=True)
            if text_content:  # Only add if not empty text
                current_text_elements.append(element)
            else:
                current_text_elements = []


def parse_html_table_to_markdown(raw_html: str) -> str:
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
                    r'padding:\s*(-?\d+(?:\.\d+)?)(?:pt|px|em|rem)?\s+(-?\d+(?:\.\d+)?)(?:pt|px|em|rem)?\s+(-?\d+(?:\.\d+)?)(?:pt|px|em|rem)?\s+(-?\d+(?:\.\d+)?)(?:pt|px|em|rem)?',
                    # padding: top right bottom left
                    r'padding:\s*(-?\d+(?:\.\d+)?)(?:pt|px|em|rem)?\s+(-?\d+(?:\.\d+)?)(?:pt|px|em|rem)?',
                    # padding: vertical horizontal
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
                            value = value * 16  # 1em ≈ 16px (default font size)
                        elif unit == 'rem':
                            value = value * 16  # 1rem ≈ 16px (default font size)

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


if __name__ == "__main__":
    # 실제 HTML 파일로 extract_table_with_preceding_text 함수 테스트
    html_file_path = "/Users/junekwon/Desktop/Projects/extract_agent/data/8-k_sample/chipotle/2015Q1.html"
    
    try:
        with open(html_file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        print(f"✅ HTML 파일 읽기 완료: {html_file_path}")
        print(f"파일 크기: {len(html_content)} 글자\n")
        
        # extract_table_with_preceding_text 함수 실행
        results = extract_table_with_preceding_text(html_content)
        print(f"총 {len(results)}개의 테이블이 추출되었습니다.\n")
        
        for i, result in enumerate(results):
            print(f"=== 테이블 {i+1} ===")
            print(f"전체 내용 길이: {len(result['content'])} 글자")
            print(f"앞의 텍스트 길이: {len(result['preceding_text'])} 글자")
            print(f"테이블만 길이: {len(result['table_only'])} 글자")
            
            print("\n📝 앞의 텍스트 (처음 200자):")
            preceding_text = result['preceding_text'][:200]
            print(f"'{preceding_text}{'...' if len(result['preceding_text']) > 200 else ''}'")
            
            print("\n📊 테이블 내용 (처음 300자):")
            table_content = result['table_only'][:300]
            print(f"'{table_content}{'...' if len(result['table_only']) > 300 else ''}'")
            
            print("-" * 80)
            
    except FileNotFoundError:
        print(f"❌ 파일을 찾을 수 없습니다: {html_file_path}")
        
        # 대체 예시로 기존 코드 실행
        print("\n대신 예시 HTML로 테스트합니다:")
        raw_html_input = """<table>\n<tr>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n</tr>\n<tr>\n<td>﻿</td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n</tr>\n<tr>\n<td>﻿</td>\n<td colspan="5">December 31,</td>\n</tr>\n<tr>\n<td>﻿</td>\n<td colspan="2">2023</td>\n<td></td>\n<td colspan="2">2022</td>\n</tr>\n<tr>\n<td>﻿</td>\n<td colspan="2">(unaudited)</td>\n<td></td>\n<td colspan="2"></td>\n</tr>\n<tr>\n<td>Assets</td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n</tr>\n<tr>\n<td>Current assets:</td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n</tr>\n<tr>\n<td>Cash and cash equivalents</td>\n<td>$</td>\n<td>560,609</td>\n<td></td>\n<td>$</td>\n<td>384,000</td>\n</tr>\n<tr>\n<td>Accounts receivable, net</td>\n<td></td>\n<td>115,535</td>\n<td></td>\n<td></td>\n<td>106,880</td>\n</tr>\n<tr>\n<td>Inventory</td>\n<td></td>\n<td>39,309</td>\n<td></td>\n<td></td>\n<td>35,668</td>\n</tr>\n<tr>\n<td>Prepaid expenses and other current assets</td>\n<td></td>\n<td>117,462</td>\n<td></td>\n<td></td>\n<td>86,412</td>\n</tr>\n<tr>\n<td>Income tax receivable</td>\n<td></td>\n<td>52,960</td>\n<td></td>\n<td></td>\n<td>47,741</td>\n</tr>\n<tr>\n<td>Investments</td>\n<td></td>\n<td>734,838</td>\n<td></td>\n<td></td>\n<td>515,136</td>\n</tr>\n<tr>\n<td>Total current assets</td>\n<td></td>\n<td>1,620,713</td>\n<td></td>\n<td></td>\n<td>1,175,837</td>\n</tr>\n<tr>\n<td>Leasehold improvements, property and equipment, net</td>\n<td></td>\n<td>2,170,038</td>\n<td></td>\n<td></td>\n<td>1,951,147</td>\n</tr>\n<tr>\n<td>Long-term investments</td>\n<td></td>\n<td>564,488</td>\n<td></td>\n<td></td>\n<td>388,055</td>\n</tr>\n<tr>\n<td>Restricted cash</td>\n<td></td>\n<td>25,554</td>\n<td></td>\n<td></td>\n<td>24,966</td>\n</tr>\n<tr>\n<td>Operating lease assets</td>\n<td></td>\n<td>3,578,548</td>\n<td></td>\n<td></td>\n<td>3,302,402</td>\n</tr>\n<tr>\n<td>Other assets</td>\n<td></td>\n<td>63,082</td>\n<td></td>\n<td></td>\n<td>63,158</td>\n</tr>\n<tr>\n<td>Goodwill</td>\n<td></td>\n<td>21,939</td>\n<td></td>\n<td></td>\n<td>21,939</td>\n</tr>\n<tr>\n<td>Total assets</td>\n<td>$</td>\n<td>8,044,362</td>\n<td></td>\n<td>$</td>\n<td>6,927,504</td>\n</tr>\n<tr>\n<td>Liabilities and shareholders' equity</td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n</tr>\n<tr>\n<td>Current liabilities:</td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n</tr>\n<tr>\n<td>Accounts payable</td>\n<td>$</td>\n<td>197,646</td>\n<td></td>\n<td>$</td>\n<td>184,566</td>\n</tr>\n<tr>\n<td>Accrued payroll and benefits</td>\n<td></td>\n<td>227,537</td>\n<td></td>\n<td></td>\n<td>170,456</td>\n</tr>\n<tr>\n<td>Accrued liabilities</td>\n<td></td>\n<td>147,688</td>\n<td></td>\n<td></td>\n<td>147,539</td>\n</tr>\n<tr>\n<td>Unearned revenue</td>\n<td></td>\n<td>209,680</td>\n<td></td>\n<td></td>\n<td>183,071</td>\n</tr>\n<tr>\n<td>Current operating lease liabilities</td>\n<td></td>\n<td>248,074</td>\n<td></td>\n<td></td>\n<td>236,248</td>\n</tr>\n<tr>\n<td>Total current liabilities</td>\n<td></td>\n<td>1,030,625</td>\n<td></td>\n<td></td>\n<td>921,880</td>\n</tr>\n<tr>\n<td>Long-term operating lease liabilities</td>\n<td></td>\n<td>3,803,551</td>\n<td></td>\n<td></td>\n<td>3,495,162</td>\n</tr>\n<tr>\n<td>Deferred income tax liabilities</td>\n<td></td>\n<td>89,109</td>\n<td></td>\n<td></td>\n<td>98,623</td>\n</tr>\n<tr>\n<td>Other liabilities</td>\n<td></td>\n<td>58,870</td>\n<td></td>\n<td></td>\n<td>43,816</td>\n</tr>\n<tr>\n<td>Total liabilities</td>\n<td></td>\n<td>4,982,155</td>\n<td></td>\n<td></td>\n<td>4,559,481</td>\n</tr>\n<tr>\n<td>Shareholders' equity:</td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n</tr>\n<tr>\n<td>Preferred stock, $0.01 par value, 600,000 shares authorized, no shares issued as of December 31, 2023 and December 31, 2022, respectively</td>\n<td></td>\n<td>-</td>\n<td></td>\n<td></td>\n<td>-</td>\n</tr>\n<tr>\n<td>Common stock, $0.01 par value, 230,000 shares authorized, 37,483 and 37,320 shares issued as of December 31, 2023 and December 31, 2022, respectively</td>\n<td></td>\n<td>375</td>\n<td></td>\n<td></td>\n<td>373</td>\n</tr>\n<tr>\n<td>Additional paid-in capital</td>\n<td></td>\n<td>1,956,160</td>\n<td></td>\n<td></td>\n<td>1,829,304</td>\n</tr>\n<tr>\n<td>Treasury stock, at cost, 10,057 and 9,693 common shares as of December 31, 2023 and December 31, 2022, respectively</td>\n<td></td>\n<td>(4,944,656)</td>\n<td></td>\n<td></td>\n<td>(4,282,014)</td>\n</tr>\n<tr>\n<td>Accumulated other comprehensive loss</td>\n<td></td>\n<td>(6,657)</td>\n<td></td>\n<td></td>\n<td>(7,888)</td>\n</tr>\n<tr>\n<td>Retained earnings</td>\n<td></td>\n<td>6,056,985</td>\n<td></td>\n<td></td>\n<td>4,828,248</td>\n</tr>\n<tr>\n<td>Total shareholders' equity</td>\n<td></td>\n<td>3,062,207</td>\n<td></td>\n<td></td>\n<td>2,368,023</td>\n</tr>\n<tr>\n<td>Total liabilities and shareholders' equity</td>\n<td>$</td>\n<td>8,044,362</td>\n<td></td>\n<td>$</td>\n<td>6,927,504</td>\n</tr>\n</table>"""

        # Call function and print results (first 10 records only)
        parsed_result = parse_html_table(raw_html_input)
        print(f"✅ Successfully extracted {len(parsed_result)} records. (with filtering applied)")
        # Print first few records for verification
        for record in parsed_result[:10]:
            print(record)
