import pandas as pd
import os
from collections import Counter, defaultdict
from urllib.parse import urlparse

# Path to the latest allGigs file (update if needed)
LATEST_CSV = max(
    [f for f in os.listdir('../Freelance Directory') if f.endswith('allGigs.csv')],
    key=lambda x: os.path.getmtime(os.path.join('../Freelance Directory', x))
)
CSV_PATH = os.path.join('../Freelance Directory', LATEST_CSV)
OUTPUT_PATH = os.path.join('../Freelance Directory', 'companies_to_call.csv')
URL_LIST_PATH = '../Important_allGigs/URL_list.csv'

# Simple keyword-to-industry mapping (expand as needed)
INDUSTRY_KEYWORDS = {
    'IT': ['developer', 'engineer', 'cloud', 'java', 'python', 'frontend', 'backend', 'security', 'data', 'applicatie', 'architect'],
    'Finance': ['controller', 'finance', 'accountant', 'boekhouder', 'financial'],
    'Marketing': ['marketing', 'seo', 'content', 'editor', 'copywriter', 'media'],
    'Consulting': ['consultant', 'advies', 'advisor'],
    'HR': ['recruiter', 'hr', 'payroll', 'talent', 'business partner'],
    'Design': ['designer', 'design', 'ux', 'ui', 'vormgever', 'creative'],
    'Legal': ['juridisch', 'legal', 'advocaat', 'jurist'],
    'Education': ['onderwijs', 'teacher', 'docent', 'trainer'],
    'Healthcare': ['zorg', 'verpleegkundige', 'arts', 'medewerker', 'gezondheid'],
    'Project Management': ['projectleider', 'projectmanager', 'manager', 'coördinator'],
    'Government': ['gemeente', 'overheid', 'beleid', 'beleidsmedewerker'],
}

def infer_industry(titles):
    found = set()
    for title in titles:
        t = title.lower()
        for industry, keywords in INDUSTRY_KEYWORDS.items():
            if any(kw in t for kw in keywords):
                found.add(industry)
    return ', '.join(sorted(found)) if found else 'Other'

def extract_homepages(urls):
    homepages = set()
    for url in urls:
        try:
            parsed = urlparse(url)
            if parsed.scheme and parsed.netloc:
                homepages.add(f"{parsed.scheme}://{parsed.netloc}")
        except Exception:
            continue
    return '; '.join(sorted(homepages))

def main():
    df = pd.read_csv(CSV_PATH)
    # Read companies to exclude from URL_list.csv
    try:
        url_list_df = pd.read_csv(URL_LIST_PATH, sep=';')
        exclude_companies = set(url_list_df['Company_name'].astype(str).str.strip())
    except Exception as e:
        print(f"Warning: Could not read URL_list.csv, no companies will be excluded. Error: {e}")
        exclude_companies = set()

    company_counts = df['Company'].value_counts()
    company_urls = defaultdict(set)
    company_titles = defaultdict(list)

    for _, row in df.iterrows():
        company = str(row['Company']).strip()
        url = str(row['URL']).strip()
        title = str(row['Title']).strip()
        if company and company not in exclude_companies:
            company_urls[company].add(url)
            company_titles[company].append(title)

    output = []
    for company, count in company_counts.items():
        if company in exclude_companies:
            continue
        urls = sorted(company_urls[company])
        industries = infer_industry(company_titles[company])
        homepages = extract_homepages(urls)
        output.append({
            'Company': company,
            'Count': count,
            'URLs': '; '.join(urls),
            'Homepages': homepages,
            'Industries': industries
        })

    out_df = pd.DataFrame(output)
    out_df = out_df.sort_values(by='Count', ascending=False)
    out_df.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved company summary to {OUTPUT_PATH}")

if __name__ == "__main__":
    main() 