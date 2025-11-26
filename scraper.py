import json
import datetime
import asyncio
import aiohttp
import re
import FinanceDataReader as fdr
from bs4 import BeautifulSoup

# ==========================================
# 설정: 2000개 고속 수집 (3분 컷)
# ==========================================
MAX_STOCKS = 2000
CONCURRENT_REQUESTS = 10

print(f"상위 {MAX_STOCKS}개 종목을 고속으로 수집합니다...")

# 종목 리스트 가져오기
df = fdr.StockListing('KRX')
df = df[~df['Name'].str.endswith('우')]
df = df.sort_values(by='Marcap', ascending=False)
target_stocks = df.head(MAX_STOCKS)

async def fetch(session, code, name, sem):
    async with sem:
        url = f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}
        
        try:
            # ssl=False: 보안 검사 패스하고 속도 업
            async with session.get(url, headers=headers, timeout=15, ssl=False) as response:
                if response.status != 200: return None
                
                # [핵심 1] 한글 깨짐 방지
                raw_data = await response.read()
                try:
                    html = raw_data.decode('euc-kr')
                except:
                    html = raw_data.decode('utf-8', errors='ignore')
                
                soup = BeautifulSoup(html, 'html.parser')
                
                # 가격 가져오기
                price_tag = soup.select_one('#svdMainChartTxt11')
                price = price_tag.text.strip() if price_tag else "-"
                
                consensus = 0.0
                opinion = "의견없음"
                target_price = "-"
                
                # ---------------------------------------------------------
                # [탐색 1단계] 리스트(dl/dt/dd) 뒤지기 (기존 방식)
                # ---------------------------------------------------------
                dls = soup.select('dl')
                for dl in dls:
                    dt = dl.select_one('dt')
                    dd = dl.select_one('dd')
                    if dt and dd:
                        title = dt.text.strip()
                        value = dd.text.strip()
                        if '투자의견' in title:
                            opinion = value
                        elif '목표주가' in title:
                            target_price = value

                # ---------------------------------------------------------
                # [탐색 2단계] 표(Table) 뒤지기 (사진 보고 추가한 기능!)
                # ---------------------------------------------------------
                # 만약 1단계에서 못 찾았거나 점수가 0점이면, 표를 뒤집니다.
                if consensus == 0.0:
                    tables = soup.select('table')
                    for table in tables:
                        # 표의 헤더(제목)들을 다 가져옴
                        headers = [th.text.strip() for th in table.select('thead th')]
                        # 표의 내용(데이터)들을 다 가져옴
                        cells = [td.text.strip() for td in table.select('tbody tr td')]
                        
                        # "투자의견"이라는 제목이 몇 번째 칸에 있는지 확인
                        if '투자의견' in headers:
                            idx = headers.index('투자의견')
                            if idx < len(cells):
                                val = cells[idx] # 그 위치의 데이터 가져오기
                                if val: opinion = val
                        
                        # "목표주가" 위치 확인
                        if '목표주가' in headers:
                            idx = headers.index('목표주가')
                            if idx < len(cells):
                                val = cells[idx]
                                if val: target_price = val

                # ---------------------------------------------------------
                # [최종 정리] 찾아낸 글자에서 숫자만 쏙 뽑기 (정규식)
                # ---------------------------------------------------------
                # 예: "4.00 Buy" -> 4.0
                match = re.search(r'([0-9]+\.[0-9]+|[0-9]+)', opinion)
                if match:
                    consensus = float(match.group())
                
                # "4.00 Buy" -> "Buy"만 남기기 (화면 표시용)
                clean_opinion = re.sub(r'[0-9\.]+', '', opinion).replace('점', '').strip()
                if clean_opinion:
                    opinion = clean_opinion

                return {
                    "code": code, "name": name, "price": price,
                    "consensus": consensus, "opinion": opinion, "target_price": target_price,
                    "url": url
                }
        except:
            return None

async def main():
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for index, row in target_stocks.iterrows():
            task = asyncio.create_task(fetch(session, row['Code'], row['Name'], semaphore))
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        valid_results = [r for r in results if r is not None]
        
        print(f"✅ 총 {len(valid_results)}개 수집 완료!")
        
        output = {
            "updated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "stocks": valid_results
        }

        with open("data.json", "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    asyncio.run(main())
