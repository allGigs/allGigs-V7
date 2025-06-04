import pandas as pd

# Process each source
FD_lists = {}
for index, URL_link in URL_list['URL'].items():
    try:
        if URL_list['Company_name'].iloc[index] == 'Yacht':
            try:
                files_read = pd.read_csv(URL_link, sep=',')
            except:
                try:
                    files_read = pd.read_csv(URL_link, sep=';')
                except:
                    files_read = pd.read_csv(URL_link, sep='\t')
        else:
            files_read = pd.read_csv(URL_link, sep=',')
        company_name = URL_list['Company_name'].iloc[index]
        result = freelance_directory(files_read, company_name)
        if result is not None:
            result['Source'] = company_name  # Add source column for duplicate check
            FD_lists[company_name] = result
            print(f"[INFO] Processed {company_name}: {len(result)} rows")
    except pd.errors.EmptyDataError:
        print(f"[WARNING] No data found at {URL_link} for {company_name}.")
    except pd.errors.ParserError as e:
        print(f"[ERROR] Parsing CSV from {URL_link} for {company_name}: {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected error for {company_name}: {e}")

# Combine all lists
FD_concat = pd.concat(FD_lists.values(), ignore_index=True)

# Check for duplicates based on Title and Location across sources
duplicates = FD_concat[FD_concat.duplicated(subset=['Title', 'Location'], keep=False)]
if not duplicates.empty:
    print("[DUPLICATE WARNING] Duplicates found based on Title and Location across sources:")
    print(duplicates[['Title', 'Location', 'Company', 'Source']].sort_values(by=['Title', 'Location']))
else:
    print("[INFO] No duplicates found based on Title and Location across sources.")

# Clean and standardize the data
FD_concat = FD_concat[['Title', 'Location', 'rate', 'Company', 'Summary', 'URL', 'start']] 