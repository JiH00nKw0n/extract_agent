import json

from bs4 import BeautifulSoup


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
        is_negative = True # Mark as negative

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
    clean_html = raw_html.replace("\\n", "\n") # Escape sequence correction if needed

    # (2) BeautifulSoup로 파싱
    soup = BeautifulSoup(clean_html, "html.parser")
    table = soup.find("table")
    if not table:
        return [] # 테이블이 없으면 빈 리스트 반환
    rows = table.find_all("tr")

    # (3) 테이블 데이터 추출 및 필터링
    result_records = []
    for row in rows:
        cols = row.find_all("td")
        parsed_row_texts = [col.get_text(strip=True) for col in cols]

        if not parsed_row_texts:
            continue # 빈 행 스킵

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
                    "value": value  # 원본 값 저장
                }
                result_records.append(record)

    return result_records

# 예제 사용법 (기존 raw_html 데이터 사용)
raw_html_input = """<table>\n<tr>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n</tr>\n<tr>\n<td>﻿</td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n</tr>\n<tr>\n<td>﻿</td>\n<td colspan="5">December 31,</td>\n</tr>\n<tr>\n<td>﻿</td>\n<td colspan="2">2023</td>\n<td></td>\n<td colspan="2">2022</td>\n</tr>\n<tr>\n<td>﻿</td>\n<td colspan="2">(unaudited)</td>\n<td></td>\n<td colspan="2"></td>\n</tr>\n<tr>\n<td>Assets</td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n</tr>\n<tr>\n<td>Current assets:</td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n</tr>\n<tr>\n<td>Cash and cash equivalents</td>\n<td>$</td>\n<td>560,609</td>\n<td></td>\n<td>$</td>\n<td>384,000</td>\n</tr>\n<tr>\n<td>Accounts receivable, net</td>\n<td></td>\n<td>115,535</td>\n<td></td>\n<td></td>\n<td>106,880</td>\n</tr>\n<tr>\n<td>Inventory</td>\n<td></td>\n<td>39,309</td>\n<td></td>\n<td></td>\n<td>35,668</td>\n</tr>\n<tr>\n<td>Prepaid expenses and other current assets</td>\n<td></td>\n<td>117,462</td>\n<td></td>\n<td></td>\n<td>86,412</td>\n</tr>\n<tr>\n<td>Income tax receivable</td>\n<td></td>\n<td>52,960</td>\n<td></td>\n<td></td>\n<td>47,741</td>\n</tr>\n<tr>\n<td>Investments</td>\n<td></td>\n<td>734,838</td>\n<td></td>\n<td></td>\n<td>515,136</td>\n</tr>\n<tr>\n<td>Total current assets</td>\n<td></td>\n<td>1,620,713</td>\n<td></td>\n<td></td>\n<td>1,175,837</td>\n</tr>\n<tr>\n<td>Leasehold improvements, property and equipment, net</td>\n<td></td>\n<td>2,170,038</td>\n<td></td>\n<td></td>\n<td>1,951,147</td>\n</tr>\n<tr>\n<td>Long-term investments</td>\n<td></td>\n<td>564,488</td>\n<td></td>\n<td></td>\n<td>388,055</td>\n</tr>\n<tr>\n<td>Restricted cash</td>\n<td></td>\n<td>25,554</td>\n<td></td>\n<td></td>\n<td>24,966</td>\n</tr>\n<tr>\n<td>Operating lease assets</td>\n<td></td>\n<td>3,578,548</td>\n<td></td>\n<td></td>\n<td>3,302,402</td>\n</tr>\n<tr>\n<td>Other assets</td>\n<td></td>\n<td>63,082</td>\n<td></td>\n<td></td>\n<td>63,158</td>\n</tr>\n<tr>\n<td>Goodwill</td>\n<td></td>\n<td>21,939</td>\n<td></td>\n<td></td>\n<td>21,939</td>\n</tr>\n<tr>\n<td>Total assets</td>\n<td>$</td>\n<td>8,044,362</td>\n<td></td>\n<td>$</td>\n<td>6,927,504</td>\n</tr>\n<tr>\n<td>Liabilities and shareholders' equity</td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n</tr>\n<tr>\n<td>Current liabilities:</td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n</tr>\n<tr>\n<td>Accounts payable</td>\n<td>$</td>\n<td>197,646</td>\n<td></td>\n<td>$</td>\n<td>184,566</td>\n</tr>\n<tr>\n<td>Accrued payroll and benefits</td>\n<td></td>\n<td>227,537</td>\n<td></td>\n<td></td>\n<td>170,456</td>\n</tr>\n<tr>\n<td>Accrued liabilities</td>\n<td></td>\n<td>147,688</td>\n<td></td>\n<td></td>\n<td>147,539</td>\n</tr>\n<tr>\n<td>Unearned revenue</td>\n<td></td>\n<td>209,680</td>\n<td></td>\n<td></td>\n<td>183,071</td>\n</tr>\n<tr>\n<td>Current operating lease liabilities</td>\n<td></td>\n<td>248,074</td>\n<td></td>\n<td></td>\n<td>236,248</td>\n</tr>\n<tr>\n<td>Total current liabilities</td>\n<td></td>\n<td>1,030,625</td>\n<td></td>\n<td></td>\n<td>921,880</td>\n</tr>\n<tr>\n<td>Long-term operating lease liabilities</td>\n<td></td>\n<td>3,803,551</td>\n<td></td>\n<td></td>\n<td>3,495,162</td>\n</tr>\n<tr>\n<td>Deferred income tax liabilities</td>\n<td></td>\n<td>89,109</td>\n<td></td>\n<td></td>\n<td>98,623</td>\n</tr>\n<tr>\n<td>Other liabilities</td>\n<td></td>\n<td>58,870</td>\n<td></td>\n<td></td>\n<td>43,816</td>\n</tr>\n<tr>\n<td>Total liabilities</td>\n<td></td>\n<td>4,982,155</td>\n<td></td>\n<td></td>\n<td>4,559,481</td>\n</tr>\n<tr>\n<td>Shareholders' equity:</td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n<td></td>\n</tr>\n<tr>\n<td>Preferred stock, $0.01 par value, 600,000 shares authorized, no shares issued as of December 31, 2023 and December 31, 2022, respectively</td>\n<td></td>\n<td>-</td>\n<td></td>\n<td></td>\n<td>-</td>\n</tr>\n<tr>\n<td>Common stock, $0.01 par value, 230,000 shares authorized, 37,483 and 37,320 shares issued as of December 31, 2023 and December 31, 2022, respectively</td>\n<td></td>\n<td>375</td>\n<td></td>\n<td></td>\n<td>373</td>\n</tr>\n<tr>\n<td>Additional paid-in capital</td>\n<td></td>\n<td>1,956,160</td>\n<td></td>\n<td></td>\n<td>1,829,304</td>\n</tr>\n<tr>\n<td>Treasury stock, at cost, 10,057 and 9,693 common shares as of December 31, 2023 and December 31, 2022, respectively</td>\n<td></td>\n<td>(4,944,656)</td>\n<td></td>\n<td></td>\n<td>(4,282,014)</td>\n</tr>\n<tr>\n<td>Accumulated other comprehensive loss</td>\n<td></td>\n<td>(6,657)</td>\n<td></td>\n<td></td>\n<td>(7,888)</td>\n</tr>\n<tr>\n<td>Retained earnings</td>\n<td></td>\n<td>6,056,985</td>\n<td></td>\n<td></td>\n<td>4,828,248</td>\n</tr>\n<tr>\n<td>Total shareholders' equity</td>\n<td></td>\n<td>3,062,207</td>\n<td></td>\n<td></td>\n<td>2,368,023</td>\n</tr>\n<tr>\n<td>Total liabilities and shareholders' equity</td>\n<td>$</td>\n<td>8,044,362</td>\n<td></td>\n<td>$</td>\n<td>6,927,504</td>\n</tr>\n</table>"""

# 함수 호출 및 결과 출력 (첫 10개 레코드만)
parsed_result = parse_html_table(raw_html_input)
print(f"✅ 총 {len(parsed_result)}개의 레코드를 추출했습니다. (필터링 적용됨)")
# 결과 확인을 위해 일부만 출력
for record in parsed_result[:10]:
    print(record)

# JSONL 파일로 저장하고 싶다면 아래 주석 해제
# output_filename = "output_filtered_numeric.jsonl"
# with open(output_filename, "w", encoding="utf-8") as f:
#     for record in parsed_result:
#         f.write(json.dumps(record, ensure_ascii=False) + "\n")
# print(f"✅ 결과가 {output_filename} 파일에 저장되었습니다.")