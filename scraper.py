import json
import datetime
import asyncio
import aiohttp
import FinanceDataReader as fdr
from bs4 import BeautifulSoup

# ==========================================
# 설정: 2000개 (3분 컷 가능)
# ==========================================
MAX_STOCKS = 2000
CONCURRENT_REQUESTS = 10  # 로봇 10마리 동시 출동

print(f"상위 {MAX_STOCKS}개 종목을 고속으로 수집합니다...")

# 1. 종목 리스트 가져오기
df = fdr.StockListing('KRX')
df = df[~df['Name'].str.endswith('우')]
df = df.sort_values(by='Marcap', ascending=False)
target_stocks = df.head(MAX_STOCKS)

async def fetch(session, code, name, sem):
    # 동시에 너무 많이 접속하면 차단당하니까 순서 지키기
    async with sem:
        url = f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}
        
        try:
            async with session.get(url, headers=headers, timeout=10) as response:
                if response.status != 200: return None
                html = await response.text()
                
                soup = BeautifulSoup(html, 'html.parser')
                
                # 가격
                price_tag = soup.select_one('#svdMainChartTxt11')
                price = price_tag.text.strip() if price_tag else "-"
                
                consensus = 0.0
                opinion = "의견없음"
                target_price = "-"
                
                # 투자의견 찾기
                summary_table = soup.select('#corp_group2 dl')
                for item in summary_table:
                    text = item.text
                    if '투자의견' in text:
                        dd = item.select_one('dd')
                        if dd:
                            raw = dd.text.strip()
                            opinion = raw
                            try:
                                if '점' in raw: consensus = float(raw.split('점')[0])
                                elif raw.replace('.','').isdigit(): consensus = float(raw)
                                if ']' in opinion: opinion = opinion.split(']')[1].strip()
                                if '점' in opinion: opinion = opinion.split('점')[1].strip()
                            except: pass
                    elif '목표주가' in text:
                        dd = item.select_one('dd')
                        if dd: target_price = dd.text.strip()

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
        
        # 전체 로봇 출동 및 결과 모으기
        results = await asyncio.gather(*tasks)
        
        # 실패한 것(None) 빼고 정리
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
