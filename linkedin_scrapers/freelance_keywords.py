"""
freelance_keywords.py (moved into linkedin_scrapers)
----------------------------------------------------

Centralised list of keywords and phrases that are typically associated with
freelancing, contract work, and gig-based employment.
"""

FREELANCE_KEYWORDS: list[str] = [
    # Core terminology
    "freelance", "freelancer", "freelancers", "freelancing",
    "contract", "contractor", "contractors", "contracting",
    "gig", "gigs", "gig-work", "gig work",
    "independent", "independent contractor",
    # Removed consultant variants per user request
    "project-based", "project based", "project work",

    # Availability / call-outs
    "open to freelance",
    "taking on freelance", "accepting freelance", "hire a freelancer",
    "looking for freelancer", "looking for a freelancer", "seeking freelancer",

    # Popular freelance platforms / references (removed user-requested terms)
    "freelancer.com", "peopleperhour",

    # Job titles / qualifiers that often indicate freelance or contract nature
    "interim", "fractional", "part-time", "part time",

    # Miscellaneous related terms
    "side hustle", "side-hustle", "outsourced", "outsourcing",
]

# ---------------------------------------------------------------------------
# Keyword → Industry mapping (minimum 5 keywords per industry)
# ---------------------------------------------------------------------------
INDUSTRY_KEYWORDS: dict[str, str] = {
    # Software / IT
    "developer": "Software Development",
    "software": "Software Development",
    "engineer": "Software Development",
    "programmer": "Software Development",
    "full stack": "Software Development",

    # Data / AI
    "data scientist": "Data Science",
    "data analyst": "Data Science",
    "machine learning": "Data Science",
    "ai": "Data Science",
    "business intelligence": "Data Science",

    # Cybersecurity
    "security analyst": "Cybersecurity",
    "cybersecurity": "Cybersecurity",
    "penetration tester": "Cybersecurity",
    "soc": "Cybersecurity",
    "ethical hacker": "Cybersecurity",

    # Design / Creative
    "designer": "Design",
    "graphic": "Design",
    "ux": "Design",
    "ui": "Design",
    "illustrator": "Design",

    # Marketing
    "marketing": "Marketing",
    "seo": "Marketing",
    "sem": "Marketing",
    "content marketing": "Marketing",
    "growth": "Marketing",

    # Content / Copy
    "copywriter": "Copywriting",
    "copywriting": "Copywriting",
    "content writer": "Copywriting",
    "technical writer": "Copywriting",
    "blogger": "Copywriting",

    # Video / Media
    "videographer": "Video Production",
    "video editor": "Video Production",
    "motion graphics": "Video Production",
    "animation": "Video Production",
    "video production": "Video Production",

    # Photography
    "photographer": "Photography",
    "photo editor": "Photography",
    "product photography": "Photography",
    "wedding photographer": "Photography",
    "event photographer": "Photography",

    # HR / People
    "hr": "Human Resources",
    "recruiter": "Human Resources",
    "talent acquisition": "Human Resources",
    "people operations": "Human Resources",
    "compensation": "Human Resources",

    # Finance / Accounting
    "accountant": "Finance",
    "finance": "Finance",
    "bookkeeper": "Finance",
    "controller": "Finance",
    "financial analyst": "Finance",

    # Sales / Bizdev
    "sales": "Sales",
    "business development": "Sales",
    "account executive": "Sales",
    "sales manager": "Sales",
    "lead generation": "Sales",

    # Customer Service
    "customer service": "Customer Service",
    "support": "Customer Service",
    "helpdesk": "Customer Service",
    "call center": "Customer Service",
    "customer success": "Customer Service",

    # Project Management
    "project manager": "Project Management",
    "pmp": "Project Management",
    "scrum master": "Project Management",
    "agile coach": "Project Management",
    "program manager": "Project Management",

    # Product Management
    "product manager": "Product Management",
    "product owner": "Product Management",
    "product strategy": "Product Management",
    "product lead": "Product Management",
    "go to market": "Product Management",

    # Engineering (non-software)
    "mechanical engineer": "Engineering",
    "electrical engineer": "Engineering",
    "civil engineer": "Engineering",
    "structural engineer": "Engineering",
    "chemical engineer": "Engineering",

    # Construction / Trades
    "carpenter": "Trades / Construction",
    "plumber": "Trades / Construction",
    "electrician": "Trades / Construction",
    "welder": "Trades / Construction",
    "construction": "Trades / Construction",

    # Architecture
    "architect": "Architecture",
    "architectural designer": "Architecture",
    "interior designer": "Architecture",
    "urban planner": "Architecture",
    "landscape architect": "Architecture",

    # Real Estate
    "real estate": "Real Estate",
    "property manager": "Real Estate",
    "broker": "Real Estate",
    "estate agent": "Real Estate",
    "mortgage": "Real Estate",

    # Logistics / Supply Chain
    "logistics": "Logistics",
    "supply chain": "Logistics",
    "warehouse": "Logistics",
    "transport": "Logistics",
    "procurement": "Logistics",

    # Manufacturing / Operations
    "manufacturing": "Manufacturing",
    "production": "Manufacturing",
    "lean": "Manufacturing",
    "six sigma": "Manufacturing",
    "operations": "Manufacturing",

    # Healthcare
    "nurse": "Healthcare",
    "doctor": "Healthcare",
    "physician": "Healthcare",
    "therapist": "Healthcare",
    "pharmacist": "Healthcare",

    # Education
    "teacher": "Education",
    "tutor": "Education",
    "instructor": "Education",
    "professor": "Education",
    "trainer": "Education",

    # Legal
    "lawyer": "Legal",
    "attorney": "Legal",
    "paralegal": "Legal",
    "legal counsel": "Legal",
    "legal assistant": "Legal",

    # Translation / Linguistics
    "translator": "Translation",
    "translation": "Translation",
    "interpreter": "Translation",
    "localization": "Translation",
    "subtitling": "Translation",

    # Administrative / Virtual Assistance
    "virtual assistant": "Administrative",
    "admin": "Administrative",
    "executive assistant": "Administrative",
    "data entry": "Administrative",
    "office manager": "Administrative",

    # Hospitality / Events
    "chef": "Hospitality",
    "cook": "Hospitality",
    "event planner": "Hospitality",
    "bartender": "Hospitality",
    "hotel": "Hospitality",

    # Analytics / BI
    "analyst": "Analytics",
    "business intelligence": "Analytics",
    "insights": "Analytics",
    "reporting": "Analytics",
    "data visualization": "Analytics",

    # QA / Testing
    "qa": "Quality Assurance",
    "tester": "Quality Assurance",
    "quality assurance": "Quality Assurance",
    "automation testing": "Quality Assurance",
    "test engineer": "Quality Assurance",

    # Security (physical / ops)
    "security guard": "Security",
    "loss prevention": "Security",
    "safety officer": "Security",
    "security officer": "Security",
    "cctv": "Security",

    # Research / Science
    "research": "Research",
    "scientist": "Research",
    "lab": "Research",
    "r&d": "Research",
    "clinical": "Research",
} 