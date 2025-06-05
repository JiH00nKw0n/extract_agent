import re

from bs4 import BeautifulSoup, Tag


def split_by_hr_blocks(html: str):
    parts = re.split(r'(?i)<hr\b[^>]*>', html)
    return [p.strip() for p in parts if p.strip()]


def get_text_from_html(html: str):
    soup = BeautifulSoup(html, 'html.parser')
    return soup.get_text(separator=' ', strip=True)

def split_html_by_table(html: str):
    soup = BeautifulSoup(html, 'html.parser')
    body = soup.body or soup

    segments, chunk = [], []

    for node in body.children:
        chunk.append(str(node))          # 현재 노드 문자열을 누적
        if isinstance(node, Tag) and node.name == 'table':
            # 테이블이 끝나는 순간까지 포함해 조각을 확정
            segments.append(''.join(chunk))
            chunk = []                   # 버퍼 초기화

    if chunk:                            # 남은 부분 처리
        segments.append(''.join(chunk))

    return [s for s in segments if s.strip()]  # 공백만 있는 조각 제거

def split_html(html: str):
    result_list = []
    parts = split_by_hr_blocks(html)
    for p in parts:
        result_list.extend(split_html_by_table(p))
        
    return result_list

# 사용 예시
if __name__ == "__main__":
    with open(
            "/Users/junekwon/Desktop/Projects/extract_agent/data/downloaded_files/file_key_files/YUM_2018-10-31_8-K_EXHIBIT_99.1_original.htm", 'r', encoding='utf-8'
    ) as f:
        html_content = f.read()

    lists = split_by_hr_blocks(html_content)
    for idx, block in enumerate(lists, 1):
        print(f"\n{'=' * 20} BLOCK {idx} {'=' * 20}\n")
        for x in split_html_by_table(block):
            print(get_text_from_html(x)[:800])
            print("-"*30)
