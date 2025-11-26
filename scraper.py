import json
import datetime
import time
import requests
import FinanceDataReader as fdr
from bs4 import BeautifulSoup

# ==========================================
# 설정: 상위 2000개 수집 (전체 종목 커버)
# ==========================================
MAX_STOCKS = 2000

print(f"전체 종목 중 상위 {MAX_STOCKS}개를 수집합니다...")
# 한국거래소(KRX) 전체 리스트 가져오기
df = fdr.StockListing('KRX')
# 우선주(뒤에 '우' 붙은거)는 뺌
df = df[~df['Name'].str.endswith('우')]
# 시가총액 순서대로 정렬해서 2000개 자르기
df = df.sort_values(by='Marcap', ascending=False)
target_stocks = df.head(MAX_STOCKS)

def get_consensus(code, name):
    try:
        # 로봇 아닌 척 위장하기
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}
        url = f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701"
        
        response = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(response.text, 'html.parser')

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
                        # "4.00매수" 에서 숫자만 추출
                        if '점' in raw: consensus = float(raw.split('점')[0])
                        elif raw.replace('.','').isdigit(): consensus = float(raw)
                        # 글자 정리
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
    except: return None

result_list = []
count = 0

# 하나씩 긁어오기 시작
for index, row in target_stocks.iterrows():
    count += 1
    # 50개마다 로그 출력 (잘 되고 있나 확인용)
    if count % 50 == 0:
        print(f"[{count}/{MAX_STOCKS}] {row['Name']} 수집 중...")
    
    data = get_consensus(row['Code'], row['Name'])
    if data: result_list.append(data)
    time.sleep(0.1) # 0.1초 휴식 (차단 방지)

output = {
    "updated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "stocks": result_list
}

with open("data.json", "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=4)

print("✅ 수집 완료!")


