from __future__ import annotations

import json
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, List

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential_jitter


SEARCH_URL = "https://hh.ru/search/vacancy"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

SEARCH_QUERIES = [
    "python",
    "data engineer",
    "spark",
    "airflow",
    "ml engineer",
]

AREA = 1
PAGES = 2
PER_PAGE = 50

OUT_PATH = Path("data/raw/vacancies_raw.json")

VACANCY_ID_RE = re.compile(r"/vacancy/(\d+)")
SALARY_RE = re.compile(r"(\d[\d\s\u00A0]*)", re.UNICODE)

#Это короткая карточка из поиска
@dataclass
class VacancyBrief: 
    vacancy_id: int
    name: str
    url: str
    employer_name: Optional[str]
    area_name: Optional[str]
    published_at_text: Optional[str]

#Это полная запись после захода внутрь вакансии
@dataclass
class VacancyDetail:
    vacancy_id: int
    title: str
    url: str
    employer: Optional[str]
    region: Optional[str]
    published_at: str
    salary_from: Optional[int]
    salary_to: Optional[int]
    salary_currency: Optional[str]
    schedule: Optional[str]
    employment: Optional[str]
    experience: Optional[str]
    skills: List[str]
    search_query: str
    raw_loaded_at: str


def text_or_none(node):
    return node.get_text(strip=True) if node else None


def parse_salary(text: Optional[str]):
    if not text:
        return None, None, None

    currency = "RUR" if "руб" in text.lower() else None
    nums = [int(re.sub(r"\D+", "", m)) for m in SALARY_RE.findall(text)]

    if "от" in text.lower() and nums:
        return nums[0], None, currency

    if "до" in text.lower() and nums:
        return None, nums[0], currency

    if len(nums) >= 2:
        return nums[0], nums[1], currency

    if len(nums) == 1:
        return nums[0], nums[0], currency

    return None, None, currency


def parse_published_at(text: Optional[str]) -> datetime:
    now = datetime.now(timezone.utc)

    if not text:
        return now

    lower = text.lower()

    if "сегодня" in lower:
        return now

    if "вчера" in lower:
        return now - timedelta(days=1)

    months = {
        "января": 1,
        "февраля": 2,
        "марта": 3,
        "апреля": 4,
        "мая": 5,
        "июня": 6,
        "июля": 7,
        "августа": 8,
        "сентября": 9,
        "октября": 10,
        "ноября": 11,
        "декабря": 12,
    }

    match = re.search(r"(\d{1,2})\s+([а-яё]+)\s+(\d{4})?", lower)

    if match:
        day = int(match.group(1))
        month = months.get(match.group(2))
        year = int(match.group(3)) if match.group(3) else now.year

        if month:
            return datetime(year, month, day, tzinfo=timezone.utc)

    return now

#Это безопасный запрос: делает http get, если ошибка - повторяет до 5 раз, делает паузу 0.5 сек.
@retry(wait=wait_exponential_jitter(initial=0.5, max=4), stop=stop_after_attempt(5))
def http_get(session: requests.Session, url: str, params: Optional[dict] = None) -> requests.Response:
    response = session.get(url, params=params, timeout=30)
    response.raise_for_status()
    time.sleep(0.5)
    return response

#Парсит страницу поиска
def parse_list_page(html: str) -> List[VacancyBrief]:
    soup = BeautifulSoup(html, "html.parser")

    #Находит карточки
    cards = soup.select(
        'div.serp-item, '
        'div.vacancy-serp-item, '
        'div[data-qa="vacancy-serp__vacancy"]'
    )

    result = []

    #Из каждой карточки достаем: title, url, vacansy_id, employer, area, published_at
    for card in cards:
        title = card.select_one('a[data-qa="serp-item__title"]')

        if not title:
            continue

        url = title["href"].split("?")[0]

        match = VACANCY_ID_RE.search(url)
        if not match:
            continue

        vacancy_id = int(match.group(1))

        employer = text_or_none(card.select_one('[data-qa="vacancy-serp__vacancy-employer"]'))
        area = text_or_none(card.select_one('[data-qa="vacancy-serp__vacancy-address"]'))
        published_at = text_or_none(card.select_one('[data-qa="vacancy-serp__vacancy-date"]'))

        result.append(
            VacancyBrief(
                vacancy_id=vacancy_id,
                name=title.text.strip(),
                url=url,
                employer_name=employer,
                area_name=area,
                published_at_text=published_at,
            )
        )

    return result


#Заходит в конкретную вакансию и достаёт: название, работодатель, регион, зарплата, опыт, занятость, график, skills
#И возвращает VacancyDetail
def parse_vacancy_detail(html: str, brief: VacancyBrief, search_query: str) -> VacancyDetail:
    soup = BeautifulSoup(html, "html.parser")

    title = text_or_none(soup.select_one('h1[data-qa="vacancy-title"]')) or brief.name
    employer = text_or_none(soup.select_one('[data-qa="vacancy-company-name"]')) or brief.employer_name
    region = text_or_none(soup.select_one('[data-qa="vacancy-view-location"]')) or brief.area_name

    salary_text = text_or_none(
        soup.select_one('[data-qa="vacancy-salary"], [data-qa="vacancy-view-salary"]')
    )

    salary_from, salary_to, salary_currency = parse_salary(salary_text)

    experience = text_or_none(soup.select_one('[data-qa="vacancy-experience"]'))
    employment = text_or_none(soup.select_one('[data-qa="vacancy-view-employment-mode"]'))
    schedule = text_or_none(soup.select_one('[data-qa="vacancy-schedule"]'))

    skills = [node.text.strip() for node in soup.select('[data-qa="skills-element"]')]

    published_text = (
        text_or_none(soup.select_one('[data-qa="vacancy-view-creation-time"]'))
        or brief.published_at_text
    )

    published_at = parse_published_at(published_text)

    return VacancyDetail(
        vacancy_id=brief.vacancy_id,
        title=title,
        url=brief.url,
        employer=employer,
        region=region,
        published_at=published_at.isoformat(),
        salary_from=salary_from,
        salary_to=salary_to,
        salary_currency=salary_currency,
        schedule=schedule,
        employment=employment,
        experience=experience,
        skills=skills,
        search_query=search_query,
        raw_loaded_at=datetime.now(timezone.utc).isoformat(),
    )


def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session

#Это главный цикл по одному поисковому запросу (страница 0 → карточки → детали вакансий, страница 1 → карточки → детали вакансий)
def crawl_query(session: requests.Session, query: str) -> List[VacancyDetail]:
    vacancies = []

    for page in range(PAGES):
        print(f"Fetching list: query={query}, page={page}")

        response = http_get(
            session=session,
            url=SEARCH_URL,
            params={
                "text": query,
                "area": AREA,
                "page": page,
                "items_on_page": PER_PAGE,
            },
        )

        briefs = parse_list_page(response.text)

        if not briefs:
            print(f"No vacancies found for query={query}, page={page}")
            break

        for brief in briefs:
            try:
                print(f"Fetching detail: {brief.vacancy_id} | {brief.name}")
                detail_response = http_get(session, brief.url)
                detail = parse_vacancy_detail(detail_response.text, brief, query)
                vacancies.append(detail)
            except Exception as exc:
                print(f"Failed vacancy_id={brief.vacancy_id}: {exc}")

    return vacancies


def main() -> None:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True) #создаёт папку data/raw

    session = create_session() #создаёт HTTP-сессию

    all_vacancies = []

    for query in SEARCH_QUERIES: #проходит по запросам
        vacancies = crawl_query(session, query)
        all_vacancies.extend(vacancies)

    unique = {}
    for vacancy in all_vacancies: #собирает вакансии
        unique[vacancy.vacancy_id] = vacancy 

    result = [asdict(v) for v in unique.values()] #убирает дубли

    with OUT_PATH.open("w", encoding="utf-8") as f: #пишет JSON
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(result)} vacancies to {OUT_PATH}")


if __name__ == "__main__":
    main()
