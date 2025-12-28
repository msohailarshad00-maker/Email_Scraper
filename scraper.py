import re
import aiohttp
import asyncio
import csv
from urllib.parse import urljoin, unquote
from tqdm.asyncio import tqdm_asyncio
from bs4 import BeautifulSoup
from email_validator import validate_email
from playwright.async_api import async_playwright

# Configuration
DOMAINS_FILE = 'domains.txt'
OUTPUT_FILE = 'try 1.csv'
TIMEOUT = 60
CONCURRENCY_LIMIT = 30
MAX_RETRIES = 2
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9'
}

EMAIL_PATTERN = re.compile(r'''
    (?:[\w.%+-]+
    (?:@|\[at\]|\(at\)| at | à | à|＠)
    [\w-]+
    (?:\.|\[dot\]|\(dot\)| dot |。)
    [\w-]+)
    |
    [\w.%+-]+@[\w.-]+\.[a-zA-Z]{2,}
''', re.VERBOSE | re.IGNORECASE)


def cleanup_email(email: str) -> str:
    email = unquote(email)
    return (email
            .lower()
            .replace('[at]', '@')
            .replace('(at)', '@')
            .replace(' at ', '@')
            .replace(' à ', '@')
            .replace('＠', '@')
            .replace('&#64;', '@')
            .replace('&#46;', '.')
            .replace(' dot ', '.')
            .replace('。', '.')
            .replace('[dot]', '.')
            .replace('(dot)', '.')
            .replace(' ', '')
            )


async def fetch_emails_playwright(url: str, deep=False):
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(ignore_https_errors=True)
            page = await context.new_page()
            timeout_ms = (TIMEOUT * 1000) * (2 if deep else 1)
            await page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
            html = await page.content()
            await browser.close()
    except Exception as e:
        print(f"Playwright error fetching {url}: {e}")
        return []

    soup = BeautifulSoup(html, 'html.parser')
    emails_text = [match[0] or match[1] for match in EMAIL_PATTERN.findall(soup.get_text(separator=' '))]
    emails_mailto = []
    for a in soup.select('a[href^="mailto:" i]'):
        href = a['href']
        if 'mailto:' in href.lower():
            email_part = href.split(':', 1)[-1].split('?')[0].split('#')[0]
            decoded_email = unquote(email_part.strip())
            emails_mailto.append(decoded_email)
    return [cleanup_email(e) for e in set(emails_text + emails_mailto)]


async def fetch_emails(session, url: str, deep=False):
    if deep:
        return await fetch_emails_playwright(url, deep=True)

    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as response:
            if response.status != 200:
                raise aiohttp.ClientError(f"Status {response.status}")
            text = await response.text()
    except Exception:
        return await fetch_emails_playwright(url)

    soup = BeautifulSoup(text, 'html.parser')
    emails_text = [match[0] or match[1] for match in EMAIL_PATTERN.findall(soup.get_text(separator=' '))]
    emails_mailto = []
    for a in soup.select('a[href^="mailto:" i]'):
        href = a['href']
        if 'mailto:' in href.lower():
            email_part = href.split(':', 1)[-1].split('?')[0].split('#')[0]
            decoded_email = unquote(email_part.strip())
            emails_mailto.append(decoded_email)

    return [cleanup_email(e) for e in set(emails_text + emails_mailto)]


async def discover_relevant_pages(session, base_url: str):
    try:
        async with session.get(base_url, timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as response:
            if response.status != 200:
                return []
            text = await response.text()
            soup = BeautifulSoup(text, 'html.parser')

            relevant_links = set()
            for tag in ['footer', 'aside', 'nav']:
                section = soup.find(tag)
                if section:
                    for a in section.select('a[href]'):
                        href = a.get('href', '').strip()
                        if href:
                            full_url = urljoin(base_url, href)
                            relevant_links.add(full_url)

            keywords = ['contact', 'about', 'team', 'support', 'impressum', 'privacy']
            for a in soup.select('a[href]'):
                href = a.get('href', '').strip().lower()
                if any(k in href for k in keywords):
                    full_url = urljoin(base_url, href)
                    relevant_links.add(full_url)

            return list(relevant_links)[:10]  # limit to avoid too many
    except Exception as e:
        print(f"Error discovering pages for {base_url}: {e}")
        return []


async def check_domain(session, domain: str, line_number: int, deep=False):
    if not domain.strip():
        return {'line_number': line_number, 'domain': '', 'emails': 'Not found', 'target_website': ''}

    emails = []
    target_website = None
    protocols = ['https', 'http']

    for protocol in protocols:
        if emails:
            break
        base_url = f"{protocol}://{domain}"
        relevant_pages = await discover_relevant_pages(session, base_url)
        if not relevant_pages:
            relevant_pages = [base_url]

        for _ in range(MAX_RETRIES + 1):
            if emails:
                break
            for page in relevant_pages:
                new_emails = await fetch_emails(session, page, deep=deep)
                if new_emails:
                    emails = list(set(emails + new_emails))
                    target_website = page
                    break

    valid_emails = []
    for email in set(emails):
        try:
            validate_email(email, check_deliverability=False)
            valid_emails.append(email)
        except:
            pass

    return {
        'line_number': line_number,
        'domain': domain,
        'emails': ', '.join(valid_emails) if valid_emails else 'Not found',
        'target_website': target_website or base_url if emails else ''
    }


async def main():
    # Read domains and preserve order + line numbers
    with open(DOMAINS_FILE, 'r', encoding='utf-8') as f:
        domains = [(idx + 1, line.strip().replace('http://', '').replace('https://', '').split()[0])
                   for idx, line in enumerate(f) if line.strip()]

    if not domains:
        print("No domains found in input file.")
        return

    # Prepare output CSV with header
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['domain', 'emails', 'target_website'])

    conn = aiohttp.TCPConnector(limit=CONCURRENCY_LIMIT, ssl=False)
    async with aiohttp.ClientSession(connector=conn, headers=HEADERS,
                                     timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as session:

        # PASS 1: Normal scraping
        tasks = [check_domain(session, domain, line_no) for line_no, domain in domains]
        results_pass1 = []
        for future in tqdm_asyncio.as_completed(tasks, desc='Pass 1: Processing domains'):
            result = await future
            results_pass1.append(result)

            # Continuously append to CSV
            with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([result['domain'], result['emails'], result['target_website']])

        # PASS 2: Deep retry only for "Not found"
        to_retry = [r for r in results_pass1 if r['emails'] == 'Not found']
        if to_retry:
            print(f"\nRetrying {len(to_retry)} domains with deep mode (Playwright)...\n")
            retry_tasks = [check_domain(session, r['domain'], r['line_number'], deep=True) for r in to_retry]
            for future in tqdm_asyncio.as_completed(retry_tasks, desc='Pass 2: Deep retry'):
                new_result = await future

                # Update the CSV row (we overwrite the entire file with updated data to keep order)
                # Instead, we re-write all rows in correct order after Pass 2
                pass

        # Final step: Re-write entire CSV in exact original input order with updated results
        final_results = {r['line_number']: r for r in results_pass1}
        for r in to_retry:
            # Update if deep mode found emails
            deep_result = await check_domain(session, r['domain'], r['line_number'], deep=True)  # already done above, but we merge
            if deep_result['emails'] != 'Not found':
                final_results[deep_result['line_number']] = deep_result

        # Sort by original line number and write final clean CSV
        sorted_final = sorted(final_results.values(), key=lambda x: x['line_number'])
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['domain', 'emails', 'target_website'])
            for r in sorted_final:
                writer.writerow([r['domain'], r['emails'], r['target_website']])

        print(f"\nDone! Results saved to {OUTPUT_FILE} in exact input order.")


if __name__ == '__main__':
    asyncio.run(main())
