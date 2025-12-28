import re
import aiohttp
import asyncio
import csv
from urllib.parse import urljoin, unquote
from tqdm.asyncio import tqdm_asyncio
from bs4 import BeautifulSoup
from email_validator import validate_email, EmailNotValidError

# ==================== CONFIGURATION ====================
DOMAINS_FILE = 'domains.txt'
OUTPUT_FILE = 'try 1.csv'
TIMEOUT = 15                  # Faster timeout
CONCURRENCY_LIMIT = 10        # Safe for most systems (was 30 → too much)
DELAY_BETWEEN_REQUESTS = 0.5  # Be gentle, avoid hammering
MAX_RETRIES = 2
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml',
}

# Better email regex
EMAIL_PATTERN = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

def cleanup_email(email: str) -> str:
    email = unquote(email.lower())
    replacements = {
        '[at]': '@', '(at)': '@', ' at ': '@', ' à ': '@', '＠': '@',
        '[dot]': '.', '(dot)': '.', ' dot ': '.', '。': '.'
    }
    for old, new in replacements.items():
        email = email.replace(old, new)
    return email.strip()

async def fetch_page(session, url: str):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as resp:
            if resp.status == 200:
                return await resp.text()
            return None
    except (aiohttp.ClientError, asyncio.TimeoutError, UnicodeDecodeError):
        return None

async def extract_emails_from_html(html: str):
    if not html:
        return []
    
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text(separator=' ')
    
    # Find from text
    found = set(EMAIL_PATTERN.findall(text))
    
    # Find from mailto links
    for a in soup.find_all('a', href=True):
        href = a['href'].lower()
        if href.startswith('mailto:'):
            email = href[7:].split('?')[0].strip()
            found.add(unquote(email))
    
    cleaned = [cleanup_email(e) for e in found]
    
    # Validate
    valid = []
    for e in cleaned:
        try:
            validate_email(e, check_deliverability=False)
            valid.append(e)
        except EmailNotValidError:
            pass
    
    return list(set(valid))

async def get_relevant_pages(session, base_url: str):
    html = await fetch_page(session, base_url)
    if not html:
        return [base_url]
    
    soup = BeautifulSoup(html, 'html.parser')
    links = set()
    
    # Priority pages
    keywords = ['contact', 'about', 'impressum', 'team', 'support', 'privacy', 'kontakt']
    for a in soup.find_all('a', href=True):
        href = a['href'].lower()
        if any(k in href for k in keywords):
            full = urljoin(base_url, a['href'])
            links.add(full)
    
    # Add home page
    links.add(base_url)
    
    return list(links)[:5]  # Max 5 pages per domain

async def check_domain(session, domain: str, line_number: int):
    domain = domain.strip()
    if not domain:
        return {'domain': '', 'emails': 'Not found', 'target_website': '', 'line_number': line_number}
    
    protocols = ['https://', 'http://']
    all_emails = set()
    target_website = ''
    
    for protocol in protocols:
        if all_emails:
            break
        base_url = protocol + domain
        
        pages = await get_relevant_pages(session, base_url)
        
        for page_url in pages:
            if all_emails:
                break
                
            for attempt in range(MAX_RETRIES + 1):
                if all_emails:
                    break
                    
                await asyncio.sleep(DELAY_BETWEEN_REQUESTS)  # Be gentle
                
                html = await fetch_page(session, page_url)
                emails = await extract_emails_from_html(html)
                
                if emails:
                    all_emails.update(emails)
                    target_website = page_url
                    break
    
    emails_str = ', '.join(sorted(all_emails)) if all_emails else 'Not found'
    
    return {
        'domain': domain,
        'emails': emails_str,
        'target_website': target_website or (f"https://{domain}" if all_emails else ''),
        'line_number': line_number
    }

async def main():
    # Read domains with line numbers
    with open(DOMAINS_FILE, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip()]
    
    domains_with_idx = []
    for idx, line in enumerate(lines):
        clean_domain = line.replace('http://', '').replace('https://', '').split()[0]
        domains_with_idx.append((idx + 1, clean_domain))
    
    if not domains_with_idx:
        print("No domains found.")
        return
    
    # Write header
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['domain', 'emails', 'target_website'])
    
    # Connector with safe limits
    connector = aiohttp.TCPConnector(
        limit=CONCURRENCY_LIMIT,
        limit_per_host=3,        # Don't hammer same domain
        ssl=False,
        keepalive_timeout=30
    )
    
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    async with aiohttp.ClientSession(connector=connector, headers=HEADERS, timeout=timeout) as session:
        tasks = [
            check_domain(session, domain, line_no)
            for line_no, domain in domains_with_idx
        ]
        
        results = []
        for future in tqdm_asyncio.as_completed(tasks, desc="Scraping domains"):
            result = await future
            results.append(result)
            
            # Continuously save
            with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([result['domain'], result['emails'], result['target_website']])
    
    # Final sorted clean write
    results.sort(key=lambda x: x['line_number'])
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['domain', 'emails', 'target_website'])
        for r in results:
            writer.writerow([r['domain'], r['emails'], r['target_website']])
    
    print(f"\nCompleted! Results saved to '{OUTPUT_FILE}'")
    print(f"   → {len([r for r in results if r['emails'] != 'Not found'])} domains with emails found")

if __name__ == '__main__':
    asyncio.run(main())
