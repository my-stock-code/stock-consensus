import json
import datetime
import asyncio
import aiohttp
import re  # 1. 숫자만 추출하는 도구 추가
import FinanceDataReader as fdr
from bs4 import BeautifulSoup

# ==========================================
# 설정: 2000개 고속 수집
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
            async with session.get(url, headers=headers, timeout=10) as response:
                if response.status != 200: return None
                
                # 2. 한글 깨짐 방지 (안전장치)
                try:
                    html = await response.text()
                except:
                    html = await response.read()
                    html = html.decode('euc-kr', errors='replace')
                
                soup = BeautifulSoup(html, 'html.parser')
                
                # 가격
                price_tag = soup.select_one('#svdMainChartTxt11')
                price = price_tag.text.strip() if price_tag else "-"
                
                consensus = 0.0
                opinion = "의견없음"
                target_price = "-"
                
                # 투자의견 찾기 (모든 dl 태그 뒤지기)
                dls = soup.select('dl')
                for dl in dls:
                    dt = dl.select_one('dt')
                    dd = dl.select_one('dd')
                    
                    if dt and dd:
                        title = dt.text.strip()
                        value = dd.text.strip()
                        
                        if '투자의견' in title:
                            opinion = value
                            # 3. "4.00매수", "4.00", "4점" 등 어떤 형태든 앞의 숫자만 추출
                            # 정규식: 숫자(\d)와 점(.)이 붙어있는 패턴 찾기
                            match = re.search(r'([0-9]+\.[0-9]+|[0-9]+)', value)
                            if match:
                                consensus = float(match.group())
                            
                            # 글자만 남기기 (4.00매수 -> 매수)
                            clean_opinion = re.sub(r'[0-9\.]+', '', value).replace('점', '').strip()
                            if clean_opinion:
                                opinion = clean_opinion
                                
                        elif '목표주가' in title:
                            target_price = value

                return {
                    "code": code, "name": name, "price": price,
                    "consensus": consensus, "opinion": opinion, "target_price": target_price,
                    "url": url
                }
        except:
            return None

async def main():
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
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
