import os
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Supabase configuration
SUPABASE_URL = "https://lfwgzoltxrfutexrjahr.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

if not SUPABASE_SERVICE_ROLE_KEY:
    raise ValueError("SUPABASE_SERVICE_ROLE_KEY environment variable is not set")

# Create Supabase client with service role key
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def setup_rls_policies():
    # Enable RLS on tables
    tables = ['Allgigs_All_vacancies_NEW', 'Allgigs_All_vacancies']
    
    for table in tables:
        # Enable RLS
        enable_rls_sql = f"""
        ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;
        """
        
        # Create policies
        create_read_policy_sql = f"""
        CREATE POLICY "Allow read access to authenticated users" ON {table}
        FOR SELECT
        TO authenticated
        USING (true);
        """
        
        create_service_role_policy_sql = f"""
        CREATE POLICY "Allow all operations to service role" ON {table}
        FOR ALL
        TO service_role
        USING (true);
        """
        
        try:
            # Execute SQL commands
            supabase.query(enable_rls_sql).execute()
            supabase.query(create_read_policy_sql).execute()
            supabase.query(create_service_role_policy_sql).execute()
            print(f"RLS policies set up for {table}")
        except Exception as e:
            print(f"Error setting up RLS for {table}: {str(e)}")

if __name__ == "__main__":
    setup_rls_policies() 