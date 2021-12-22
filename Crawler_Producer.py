# 패키지 호출
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from kafka import KafkaProducer
import time
import timeit
import json

chrome_options = webdriver.ChromeOptions()  # 드라이버 셋팅
chrome_options.add_argument('headless')  # 내부 창을 띄울 수 없으므로 설정
chrome_options.add_argument('no-sandbox')
chrome_options.add_argument('disable-dev-shm-usage')
driver = webdriver.Chrome('chromedriver', options=chrome_options)
time.sleep(1)  # interval
# 카프카 프로듀서 생성
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         # 속성값을 JSON 형태로 직렬화
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))  # 속성값을 JSON 형태로 직렬화

# 마지막 페이지 찾기


def get_last():
    pages = driver.find_elements_by_class_name("page-link")
    last = int(pages[-2].text)
    return last


# 채용 공고 페이지 url 수집 및 저장
def get_urls(last):
    links = []  # url들을 담을 리스트 생성
    for page in range(1, last+1):
        # 프로그래머스 사이트 접속
        driver.get(
            f"https://programmers.co.kr/job?min_salary=&min_career=&min_employees=&order=recent&page={page}")
        time.sleep(1)
        print(f"{page} 스크랩 중")
    #         html = driver.page_source
    #         soup = bs4.BeautifulSoup(html,"html.parser")
    #         hrefs = soup.select(.position-title).get['href']
        js = 'return [...document.querySelectorAll(".position-title a")].map(a=>a.href)'
        hrefs = driver.execute_script(js)  # 자바스크립트 querySelector를 이용하여 가져오기

        for href in hrefs:
            link = href
            links.append(link)  # url 넣기
    return links


# 리스트 links를 사용하여 채용 공고 수집 및 저장
def get_info(links):
    start_time = timeit.default_timer()  # 시작 시간 체크
    recruit = []  # 채용정보 리스트 생성
    for number, link in enumerate(links):
        informations = {}
        stackinfo = []
        url = link
        driver.get(url)  # 채용공고 페이지 열기
        time.sleep(1)
        # 회사 이름, 공고명 수집
        company = driver.find_elements_by_xpath("//h4[@class='sub-title'][1]")
        notice = driver.find_elements_by_xpath("//h2[@class='title']")

        # 컨텐츠 수집(기술스택, 회사정보)
        stacks = driver.find_elements_by_xpath(
            "//tr[@class='heavy-use']//td//code")
        index = driver.find_elements_by_xpath(
            "//tbody//tr//td[@class='t-label']")
        content = driver.find_elements_by_xpath("//td[@class='t-content']")
        for c, n in zip(company, notice):
            company = c.text
            notice = n.text
        for s in stacks:
            stack = s.text
            stackinfo.append(stack)
        for i, c in zip(index, content):
            info = i.text
            content = c.text
            informations[info] = content
            # 회사정보는 규격화 된 elements가 아닌 순서대로 나열이 되어 있어서,
            # 리스트로 넣을 경우 매칭이 어려울 수 있기에 딕셔너리 형태로 순서대로 저장
        doc = {
            'notice': notice,
            'url': url,
            'stack': stackinfo,
            'company': company,
            'information': informations
        }
        index = number+1
        recruit.append(doc)  # 담은 정보들을 리스트에 추가
        print(f"{index}번째 저장중")
        producer.send('test', value=doc)  # 카프카 토픽('test') 수집된 데이터 전송
        producer.flush()

    terminate_time = timeit.default_timer()  # 종료 시간 체크
    print("%f초 걸렸습니다." % (terminate_time - start_time))
