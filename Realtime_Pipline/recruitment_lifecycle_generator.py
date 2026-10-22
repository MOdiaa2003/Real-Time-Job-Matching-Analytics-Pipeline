
# #!/usr/bin/env python3
# """
# Dynamic Data Generator with Lifecycle Management
# ================================================
# Chooses randomly (with weights) between:
#   - Entities: candidate, job_post, application
#   - Operations: insert, update, delete

# Respects realistic lifecycles:
#   Candidate:  active → changes availability → deactivates profile
#   Job:        created → salary change → deadline change → skills change 
#               → work_mode change → closed
#   Application: applied → viewed → interview → offer → {rejected | accepted}

# IMPROVEMENTS APPLIED:
#   1. INTELLIGENT APPLICATIONS (70% category-matched, 30% random)
#      - Category-matched picks (_pick_job_for_category /
#        _pick_candidate_for_category) draw ONLY from candidates_by_category /
#        jobs_by_category, which are populated at the exact moment an entity
#        finishes its CDC warm-up (see improvement #7) - never from the
#        broader candidate_categories/job_categories maps, which also
#        contain entities still warming. This guarantees every
#        category-matched application references a candidate_id/post_id
#        that has already had time to propagate through
#        Debezium -> Kafka -> the match engine's dimension tables.
#      - HARD category gate in compute_match_score(): job_category equality
#        is a prerequisite for a high-priority match, not a weighted
#        contributor. Cross-category applications are capped at a low score
#        (0-20) regardless of any incidental skill-name overlap, so a
#        HIGH_PRIORITY-tier match can only ever come from a same-category
#        pair. Within a category match, score is 100% skills overlap.
#      - Keeps match engine completely independent - no schema/column
#        changes, no new fields it needs to know about.

#   2. CANDIDATE INTERNAL CONSISTENCY
#      - Career history is built from temporal progression templates
#      - Category → Seniority → Title → Experience → Skills → Salary → Companies
#      - All derived from a single coherent career timeline

#   3. TEMPORAL CAREER HISTORY
#      - previous_companies and previous_industries derived from dated roles
#      - Example: 2020-2022 Junior Analyst → 2022-2024 Analyst → 2024-2026 Senior
#      - Years of experience, current title, current company all derived from history

#   4. JOB INTERNAL CONSISTENCY
#      - Company → Industry → Category → Title → Seniority → Experience → Skills → Salary → Location
#      - Industry derived from company (or category fallback)
#      - Salary adjusted for region cost-of-living
#      - Location weighted by industry geography

#   5. REALISTIC APPLICATION DATES
#      - 0-2 days: 35%, 3-7 days: 35%, 8-14 days: 20%, 15+ days: 10%
#      - Deadline decay: probability drops as deadline approaches

#   6. REALISTIC VIEWING BEHAVIOR
#      - 72% of applications eventually viewed, 28% never viewed
#      - Viewing delays: 1-3 days (50%), 3-7 days (30%), 7-14 days (15%), 14+ days (5%)

#   7. CDC WARM-UP DELAY (fixes unresolved_rows in the match engine)
#      - A brand-new candidate/job sits in warming_candidates/warming_jobs
#        for 60s (comfortably longer than the match engine's 30s micro-batch
#        trigger) before it's added to active_candidates/active_jobs AND to
#        candidates_by_category/jobs_by_category.
#      - insert_application() (both the category-matched path via
#        _pick_job_for_category/_pick_candidate_for_category, and the random
#        30% path via active_candidates/active_jobs) can therefore never
#        reference an id that hasn't already been confirmed replicated.
#        Without this, applications routinely referenced a candidate/job
#        created milliseconds earlier - long before Debezium had a chance to
#        publish it - which the match engine could only ever resolve as
#        unresolved -> pending -> eventually dead-lettered.

# NOTE ON ARCHITECTURE
# ---------------------
# This script is a PURE Postgres writer. It does not talk to Kafka at all.
# Change capture is handled downstream by a Debezium PostgreSQL connector
# that reads the WAL via logical decoding and publishes events to Kafka.

#     PostgreSQL -> WAL -> Debezium -> Kafka -> {candidate,job,application}_events

# The `created_at` / `updated_at` / `version` columns and `REPLICA IDENTITY
# FULL` setting exist specifically so Debezium can emit full before/after
# row images without any app-level envelope building.
# """

# import random
# import numpy as np
# import datetime
# import psycopg2
# import os
# from dotenv import load_dotenv
# import time
# import json
# import uuid
# import re
# from collections import defaultdict

# # ── Import your data module ──────────────────────────────────────────────
# from data_used import (
#     top_300_unique, job_titles, job_certifications, jobs_skills,
#     num_skills_needed, num_certifications_needed,
#     job_posting_platforms, employment_types,
#     country_languages, location, work_modes, industries,
#     category_to_experience, matching_keywords, employment_rules,
#     salary_ranges_per_year, level_to_years, experience_years,
#     apply_methods,
#     male_names, female_names, nationalities, Gpa,
#     education_levels,
#     # NEW: weighted category + consistency mappings
#     choose_weighted_category, CATEGORY_WEIGHTS,
#     company_industry_map, industry_location_weights, category_industries,
#     region_salary_multipliers, seniority_experience_years,
#     career_progression_templates,
#     APPLICATION_DATE_DISTRIBUTION, VIEW_PROBABILITY, VIEW_DELAY_DISTRIBUTION,
#     pick_from_distribution
# )

# # ── Configuration ────────────────────────────────────────────────────────
# load_dotenv()
# CONFIG = {
#     "total_operations": 1000000,
#     "weights": {
#         ("candidate", "insert"):     7,
#         ("candidate", "update"):    5,
#         ("candidate", "delete"):     1,

#         ("job_post", "insert"):     10,
#         ("job_post", "update"):      4,
#         ("job_post", "delete"):      1,

#         ("application", "insert"): 150,
#         ("application", "update"):  15,
#         ("application", "delete"):   1,
#     }
# }


# # ── PostgreSQL Connection ────────────────────────────────────────────────
# def connect_postgres():
#     return psycopg2.connect(
#         host=os.getenv("POSTGRES_HOST", "localhost"),
#         port=os.getenv("POSTGRES_PORT", "5432"),
#         dbname=os.getenv("POSTGRES_DB", "postgres"),
#         user=os.getenv("POSTGRES_USER", "postgres"),
#         password=os.getenv("POSTGRES_PASSWORD", "postgres")
#     )

# # ── Schema Setup (adds lifecycle columns + applications table) ───────────
# def ensure_schema(conn):
#     cur = conn.cursor()

#     # Candidate: add CDC/SCD columns if missing
#     cur.execute("""
#         DO $$ 
#         BEGIN
#             IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
#                            WHERE table_name='candidate_info' AND column_name='created_at') 
#             THEN
#                 ALTER TABLE candidate_info 
#                 ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#                 ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#                 ADD COLUMN version INT DEFAULT 1;
#             END IF;
#         END $$;
#     """)

#     # Job posts: add CDC/SCD columns if missing
#     cur.execute("""
#         DO $$ 
#         BEGIN
#             IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
#                            WHERE table_name='job_posts' AND column_name='created_at') 
#             THEN
#                 ALTER TABLE job_posts 
#                 ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#                 ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#                 ADD COLUMN version INT DEFAULT 1;
#             END IF;
#         END $$;
#     """)

#     # Applications: add CDC/SCD columns if missing
#     cur.execute("""
#         DO $$ 
#         BEGIN
#             IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
#                            WHERE table_name='applications' AND column_name='created_at') 
#             THEN
#                 ALTER TABLE applications 
#                 ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#                 ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#                 ADD COLUMN version INT DEFAULT 1;
#             END IF;
#         END $$;
#     """)

#     # CRITICAL for Debezium: ensure full row images on UPDATE/DELETE
#     cur.execute("ALTER TABLE candidate_info REPLICA IDENTITY FULL")
#     cur.execute("ALTER TABLE job_posts REPLICA IDENTITY FULL")
#     cur.execute("ALTER TABLE applications REPLICA IDENTITY FULL")

#     conn.commit()
#     cur.close()

# def create_base_tables(conn):
#     """Create original tables if they don't exist"""
#     cur = conn.cursor()
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS job_posts (
#             post_id VARCHAR(50) PRIMARY KEY,
#             company TEXT,
#             category TEXT,
#             job_title TEXT,
#             application_method TEXT,
#             post_date TIMESTAMP,
#             deadline TIMESTAMP,
#             certifications TEXT[],
#             skills_needed TEXT[],
#             platform TEXT,
#             employment_type TEXT,
#             city TEXT,
#             country TEXT,
#             language TEXT,
#             work_mode TEXT,
#             industry TEXT,
#             detailed_experience_level TEXT,
#             salary INT,
#             years_experience TEXT,
#             status VARCHAR(20) DEFAULT 'open',
#             lifecycle_stage INT DEFAULT 0
#         );
#     """)
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS candidate_info (
#             candidate_id VARCHAR(50) PRIMARY KEY,
#             name TEXT,
#             gender TEXT,
#             date_of_birth INT,
#             nationality TEXT,
#             location TEXT,
#             graduation_year INT,
#             GPA FLOAT,
#             education_level TEXT,
#             job_category TEXT,
#             current_title TEXT,
#             seniority TEXT,
#             experience_range TEXT,
#             current_company TEXT,
#             previous_companies TEXT[],
#             previous_industries TEXT[],
#             skills TEXT[],
#             certifications TEXT[],
#             current_salary INT,
#             is_active BOOLEAN DEFAULT TRUE
#         );
#     """)
#     cur.execute("""
# CREATE TABLE IF NOT EXISTS applications (
#     application_id VARCHAR(50) PRIMARY KEY,
#     candidate_id VARCHAR(50),
#     post_id VARCHAR(50),

#     status VARCHAR(30),
#     lifecycle_stage INT DEFAULT 0,

#     applied_date TIMESTAMP,
#     last_updated TIMESTAMP,

#     final_outcome VARCHAR(30),

#     FOREIGN KEY (candidate_id)
#         REFERENCES candidate_info(candidate_id),

#     FOREIGN KEY (post_id)
#         REFERENCES job_posts(post_id)
# );
# """)
#     conn.commit()
#     cur.close()

# # ═══════════════════════════════════════════════════════════════════════
# #  DATA GENERATION HELPERS
# # ═══════════════════════════════════════════════════════════════════════

# def get_education_by_age_group(age=None):
#     if age is None:
#         age = random.randint(20, 65)
#     if age < 25:
#         weights = [40, 20, 25, 5, 2, 1, 2, 5]
#     elif age < 35:
#         weights = [25, 15, 30, 15, 5, 3, 4, 3]
#     elif age < 50:
#         weights = [30, 15, 25, 12, 6, 4, 5, 3]
#     else:
#         weights = [45, 15, 20, 8, 4, 2, 3, 3]
#     return random.choices(education_levels, weights=weights, k=1)[0]

# def detect_seniority(title):
#     for keyword, level in matching_keywords.items():
#         if keyword.lower() in title.lower():
#             return level
#     return "Mid Level"

# def get_salary_range(det_level_exp: str, salary_dict: dict):
#     for key in salary_dict:
#         if key.lower() in det_level_exp.lower():
#             return (salary_dict[key], key)
#     return ({"min": 0, "max": 0}, None)

# # ── NEW: Build temporal career history ───────────────────────────────────

# def build_career_history(category, current_year=2026):
#     """
#     Build a realistic temporal career history from progression templates.
#     Returns: (current_title, total_years, skills, previous_companies, 
#               previous_industries, current_company, salary, seniority)
#     """
#     template = career_progression_templates.get(category, [])
#     if not template:
#         # Fallback for categories without detailed templates
#         return _build_fallback_career(category, current_year)

#     # Decide how far up the ladder this candidate has climbed
#     # Weighted toward mid-level (most realistic population distribution)
#     max_rung = len(template) - 1
#     progress_weights = []
#     for i in range(max_rung + 1):
#         # More people at junior/mid levels, fewer at senior/exec
#         if i <= 1:
#             progress_weights.append(30)
#         elif i <= 3:
#             progress_weights.append(25)
#         elif i <= 4:
#             progress_weights.append(15)
#         else:
#             progress_weights.append(5)

#     top_rung = random.choices(range(max_rung + 1), weights=progress_weights, k=1)[0]

#     # Build the career timeline
#     career_path = template[:top_rung + 1]
#     total_years = sum(years for _, years, _ in career_path)

#     # Work backwards from current_year to assign dates
#     timeline = []
#     end_year = current_year
#     for title, years, skills in reversed(career_path):
#         start_year = end_year - years
#         # Pick a random company for this role
#         company = random.choice(top_300_unique)
#         # Derive industry from company
#         industry = company_industry_map.get(company, random.choice(category_industries.get(category, industries)))
#         timeline.insert(0, {
#             "title": title,
#             "company": company,
#             "industry": industry,
#             "start_year": start_year,
#             "end_year": end_year,
#             "skills": skills
#         })
#         end_year = start_year

#     current_role = timeline[-1]
#     current_title = current_role["title"]
#     current_company = current_role["company"]
#     seniority = detect_seniority(current_title)

#     # Collect all previous companies (excluding current)
#     previous_companies = [t["company"] for t in timeline[:-1]]
#     previous_industries = [t["industry"] for t in timeline[:-1]]

#     # Aggregate skills (current + some from previous roles, with recency bias)
#     all_skills = set()
#     for i, t in enumerate(timeline):
#         # More recent roles contribute more skills
#         recency_weight = 1.0 if i == len(timeline) - 1 else 0.6 if i == len(timeline) - 2 else 0.3
#         num_to_take = max(1, int(len(t["skills"]) * recency_weight))
#         all_skills.update(random.sample(t["skills"], min(num_to_take, len(t["skills"]))))

#     # Add some category-typical skills if we have too few
#     category_skills = jobs_skills.get(category, [])
#     if len(all_skills) < 8 and category_skills:
#         needed = min(8 - len(all_skills), len(category_skills))
#         all_skills.update(random.sample(category_skills, needed))

#     skills = list(all_skills)

#     # Compute salary from seniority + some randomness
#     sal_range, _ = get_salary_range(seniority, salary_ranges_per_year)
#     base_salary = random.randint(sal_range["min"], sal_range["max"])

#     # Experience range string
#     if total_years == 0:
#         exp_range = "0 years"
#     elif total_years <= 2:
#         exp_range = f"{total_years} years"
#     else:
#         exp_range = f"{max(0, total_years - 2)}-{total_years} years"

#     return {
#         "current_title": current_title,
#         "total_years": total_years,
#         "experience_range": exp_range,
#         "skills": skills,
#         "previous_companies": previous_companies,
#         "previous_industries": previous_industries,
#         "current_company": current_company,
#         "salary": base_salary,
#         "seniority": seniority,
#         "career_timeline": timeline  # For debugging/metrics, not stored in DB
#     }


# def _build_fallback_career(category, current_year=2026):
#     """Fallback for categories without detailed progression templates."""
#     titles = job_titles.get(category, ["Professional"])
#     title = random.choice(titles)
#     seniority = detect_seniority(title)

#     # Get tight experience range
#     exp_info = seniority_experience_years.get(seniority, {"min": 3, "max": 7, "mode": 5})
#     total_years = random.randint(exp_info["min"], exp_info["max"])

#     # Build fake timeline
#     num_prev = max(0, total_years // 3)
#     prev_comp = [random.choice(top_300_unique) for _ in range(num_prev)]
#     prev_ind = [company_industry_map.get(c, random.choice(industries)) for c in prev_comp]

#     curr_comp = random.choice(top_300_unique) if seniority != "Internship" else ""

#     if total_years == 0:
#         exp_range = "0 years"
#     elif total_years <= 2:
#         exp_range = f"{total_years} years"
#     else:
#         exp_range = f"{max(0, total_years - 2)}-{total_years} years"

#     sal_range, _ = get_salary_range(seniority, salary_ranges_per_year)
#     salary = random.randint(sal_range["min"], sal_range["max"])

#     skills = random.sample(jobs_skills.get(category, []), min(num_skills_needed.get(category, 10), len(jobs_skills.get(category, []))))

#     return {
#         "current_title": title,
#         "total_years": total_years,
#         "experience_range": exp_range,
#         "skills": skills,
#         "previous_companies": prev_comp,
#         "previous_industries": prev_ind,
#         "current_company": curr_comp,
#         "salary": salary,
#         "seniority": seniority,
#         "career_timeline": []
#     }


# # ── NEW: Job post internal consistency helpers ───────────────────────────

# def get_industry_for_job(category, company):
#     """Derive industry from company (if known), else from category."""
#     if company in company_industry_map:
#         return company_industry_map[company]
#     # Fallback: pick an industry that commonly hires this category
#     valid_industries = category_industries.get(category, industries)
#     return random.choice(valid_industries)


# def get_location_by_industry(industry):
#     """Pick location weighted by industry geography."""
#     weights_dict = industry_location_weights.get(industry, industry_location_weights["default"])
#     continent = random.choices(list(weights_dict.keys()), list(weights_dict.values()), k=1)[0]
#     loc = random.choice(location[continent])
#     city, country = [p.strip() for p in loc.split(",")]
#     return continent, city, country


# def apply_region_salary_multiplier(base_salary, continent):
#     """Adjust salary for cost of living in the region."""
#     multiplier = region_salary_multipliers.get(continent, 1.0)
#     # Add some noise so it's not perfectly deterministic
#     noise = random.uniform(0.9, 1.1)
#     adjusted = int(base_salary * multiplier * noise)
#     return max(15000, adjusted)  # Floor at 15k


# # ── NEW: Application match score & probability ───────────────────────────

# def compute_match_score(candidate_category, job_category, candidate_skills, job_skills):
#     """
#     Compute a match score between candidate and job.
#     Used ONLY for deciding whether to create an application.
#     Does NOT affect the database schema or Debezium in any way.

#     HARD RULE: job_category equality is a PREREQUISITE for a high-priority
#     match, not just a weighted contributor. jobs_skills (data_used.py) is
#     a disjoint skill vocabulary per category, so any skill overlap across
#     categories is coincidental, not a genuine match - it should never be
#     enough on its own to produce a "high priority" application.

#       - Different category → score capped low (0-20), regardless of any
#         incidental skill-name overlap. application_probability() puts
#         this permanently in the "low" tier (<50 → 1-5% chance).
#       - Same category      → score is 100% skills overlap (Jaccard),
#         0-100. Category equality already qualifies the pair; how strong
#         a match it is within the field is entirely about skills now.

#     Returns score 0-100.
#     """
#     cand_set = set(s.lower() for s in (candidate_skills or []))
#     job_set = set(s.lower() for s in (job_skills or []))

#     if candidate_category != job_category:
#         # Cross-category: cap well under every tier above "low" so this
#         # can never become a high-priority match no matter how the skill
#         # names happen to line up.
#         stray_overlap = len(cand_set & job_set)
#         return min(20.0, stray_overlap * 5.0)

#     if not cand_set or not job_set:
#         return 0.0

#     intersection = len(cand_set & job_set)
#     union = len(cand_set | job_set)
#     skill_score = (intersection / union * 100) if union > 0 else 0.0
#     return round(skill_score, 1)


# def application_probability(match_score):
#     """
#     Convert match score to application probability.
#     Non-deterministic: high match = high probability, but not guaranteed.

#     RECALIBRATED. The original bands (1-5% below 50, nothing above 8%
#     until score>=50) assumed scores would commonly reach 50-100. In
#     practice, compute_match_score() is a Jaccard skills overlap: two
#     independent samples drawn from the same category's skill pool rarely
#     overlap more than ~20-30%, so real scores cluster in 0-30 even for
#     good same-category pairs (see the Score Distribution line in the
#     generator's own progress log). Under the old bands that meant ~97% of
#     ALL application attempts - matched or random - fell into the 1-5%
#     floor, which collapsed total application volume to well under the
#     candidate count despite application:insert having a much higher
#     operation weight than candidate/job inserts. These bands are rescaled
#     to the score range this scoring function actually produces, so the
#     "higher score = higher probability, category match required for the
#     top tier" signal still holds, but overall volume matches the intended
#     "many applications per candidate" behavior.
#     """
#     if match_score >= 60:
#         # Strong same-category skill overlap (rare) → 75-95% chance
#         return random.uniform(0.75, 0.95)
#     elif match_score >= 30:
#         # Solid same-category skill overlap → 45-70% chance
#         return random.uniform(0.45, 0.70)
#     elif match_score >= 10:
#         # Some overlap, or a lucky cross-category stray match → 22-42% chance
#         return random.uniform(0.22, 0.42)
#     else:
#         # Little/no overlap (the common case, incl. all cross-category
#         # applications with no stray overlap) → still happens sometimes,
#         # modeling realistic "speculative" applications
#         return random.uniform(0.10, 0.20)


# # ── NEW: Realistic application date with deadline decay ─────────────────

# def generate_application_date(post_date, deadline):
#     """
#     Generate realistic application date with:
#     - Weighted distribution (0-2d: 35%, 3-7d: 35%, 8-14d: 20%, 15+d: 10%)
#     - Deadline decay: probability drops as deadline approaches
#     """
#     job_lifespan_days = (deadline - post_date).days
#     if job_lifespan_days <= 0:
#         return post_date

#     # Pick a bucket from distribution
#     buckets = APPLICATION_DATE_DISTRIBUTION
#     chosen = random.choices(buckets, weights=[w for _, _, w in buckets], k=1)[0]
#     min_day, max_day, _ = chosen

#     # Apply within bucket
#     upper_bound = min(max_day, job_lifespan_days)
#     if min_day >= upper_bound:
#         day_offset = max(0, upper_bound - 1)  # or just 0
#     else:
#         day_offset = random.randint(min_day, upper_bound)

#     # Deadline decay: if close to deadline, chance to apply earlier
#     days_remaining = job_lifespan_days - day_offset
#     if days_remaining < 3:
#         # Very close to deadline — 60% chance to shift earlier
#         if random.random() < 0.60:
#             day_offset = max(0, day_offset - random.randint(1, 5))

#     applied_date = post_date + datetime.timedelta(days=day_offset)
#     return min(applied_date, deadline)


# # ═══════════════════════════════════════════════════════════════════════
# #  GENERATORS  (FULLY REWRITTEN FOR INTERNAL CONSISTENCY)
# # ═══════════════════════════════════════════════════════════════════════

# def generate_job_post():
#     """
#     Generate a job post with full internal consistency:
#     Company → Industry → Category → Title → Seniority → Experience → Skills → Salary → Location
#     """
#     post_id = str(uuid.uuid4())
#     app_method = random.choice(apply_methods)

#     # Post date and deadline
#     start, end = datetime.datetime(2025, 1, 1), datetime.datetime(2028, 12, 31, 23, 59, 59)
#     total_seconds = int((end - start).total_seconds())
#     post_dt = start + datetime.timedelta(seconds=np.random.randint(total_seconds))
#     deadline_dt = post_dt + datetime.timedelta(days=random.randint(15, 60))

#     # 1. Pick weighted category
#     category = choose_weighted_category()

#     # 2. Pick company
#     company = random.choice(top_300_unique)

#     # 3. Derive industry from company (or category fallback)
#     industry = get_industry_for_job(category, company)

#     # 4. Pick title from category
#     title = random.choice(job_titles[category])
#     seniority = detect_seniority(title)

#     # 5. Derive experience requirement from seniority (tight range)
#     exp_info = seniority_experience_years.get(seniority, {"min": 3, "max": 7, "mode": 5})
#     req_years = random.randint(exp_info["min"], exp_info["max"])
#     if req_years == 0:
#         years_exp = "0 years"
#     elif req_years <= 2:
#         years_exp = f"{req_years} years"
#     else:
#         years_exp = f"{max(0, req_years - 2)}-{req_years} years"

#     # 6. Pick skills from category
#     num_skills = num_skills_needed.get(category, 12)
#     all_cat_skills = jobs_skills.get(category, [])
#     if len(all_cat_skills) >= num_skills:
#         skills = random.sample(all_cat_skills, num_skills)
#     else:
#         skills = all_cat_skills

#     # 7. Compute salary from seniority, then adjust for region
#     sal_range, matched_key = get_salary_range(seniority, salary_ranges_per_year)
#     base_salary = random.randint(sal_range["min"], sal_range["max"])

#     # 8. Pick location weighted by industry geography
#     continent, city, country = get_location_by_industry(industry)

#     # 9. Apply region salary multiplier
#     salary = apply_region_salary_multiplier(base_salary, continent)

#     # 10. Derive language from country
#     language = random.choice(country_languages.get(country, ["English"]))

#     # Other fields
#     min_c, max_c = num_certifications_needed.get(category, (0, 2))
#     all_certs = job_certifications.get(category, [])
#     if len(all_certs) >= min_c:
#         certs = random.sample(all_certs, random.randint(min_c, min(max_c, len(all_certs))))
#     else:
#         certs = all_certs

#     platform = random.choice(job_posting_platforms)
#     allowed_types = employment_rules.get(seniority, ["Full-time", "Contract"])
#     emp_type = random.choice(allowed_types)
#     mode = random.choice(work_modes)
#     det_level_exp = "Internship" if emp_type == "Internship" else seniority

#     return {
#         "post_id": post_id, "company": company, "category": category,
#         "job_title": title, "application_method": app_method,
#         "post_date": post_dt, "deadline": deadline_dt,
#         "certifications": certs, "skills_needed": skills,
#         "platform": platform, "employment_type": emp_type,
#         "city": city, "country": country, "language": language,
#         "work_mode": mode, "industry": industry,
#         "detailed_experience_level": det_level_exp,
#         "salary": salary, "years_experience": years_exp,
#         "status": "open", "lifecycle_stage": 0
#     }


# def generate_candidate_info():
#     """
#     Generate a candidate with full internal consistency and temporal career history.
#     Career profile → Category → Seniority → Title → Experience → Skills → Salary → Companies
#     """
#     chosen_gender = random.choice(["male", "female"])
#     candidate_id = str(uuid.uuid4())
#     name = random.choice(male_names) if chosen_gender == "male" else random.choice(female_names)
#     nat = random.choice(nationalities)
#     gpa = random.choice(Gpa)

#     # 1. Pick weighted category
#     category = choose_weighted_category()

#     # 2. Build temporal career history from templates
#     career = build_career_history(category, current_year=2026)

#     # 3. Derive all fields from career history
#     current_title = career["current_title"]
#     seniority = career["seniority"]
#     years_exp = career["experience_range"]
#     total_years = career["total_years"]
#     skills = career["skills"]
#     previous_companies = career["previous_companies"]
#     previous_industries = career["previous_industries"]
#     current_company = career["current_company"]
#     salary = career["salary"]

#     # 4. Compute graduation year and DOB from career start
#     # Assume started career at age 22 (graduation)
#     career_start_year = 2026 - total_years
#     grad_year = career_start_year
#     dob = grad_year - 22

#     # 5. Education based on age
#     age = 2026 - dob
#     edu = get_education_by_age_group(age)

#     # 6. Certifications based on seniority
#     min_c, max_c = num_certifications_needed.get(category, (0, 2))
#     all_certs = job_certifications.get(category, [])
#     if len(all_certs) >= min_c:
#         certs = random.sample(all_certs, random.randint(min_c, min(max_c, len(all_certs))))
#     else:
#         certs = all_certs

#     # 7. Location (independent, but weighted by where the industry clusters)
#     # Pick a location that makes sense for the candidate's current industry
#     if previous_industries:
#         primary_industry = previous_industries[-1] if previous_industries else "Software & Internet"
#     else:
#         primary_industry = company_industry_map.get(current_company, "Software & Internet")

#     continent, city, country = get_location_by_industry(primary_industry)
#     loc = f"{city}, {country}"

#     return {
#         "candidate_id": candidate_id, "name": name, "gender": chosen_gender,
#         "date_of_birth": dob, "nationality": nat, "location": loc,
#         "graduation_year": grad_year, "GPA": gpa, "education_level": edu,
#         "job_category": category, "current_title": current_title,
#         "seniority": seniority, "experience_range": years_exp,
#         "current_company": current_company, "previous_companies": previous_companies,
#         "previous_industries": previous_industries, "skills": skills,
#         "certifications": certs, "current_salary": salary,
#         "is_active": True
#     }

# # ═══════════════════════════════════════════════════════════════════════
# #  DATABASE OPERATIONS (UNCHANGED — Debezium-safe)
# # ═══════════════════════════════════════════════════════════════════════

# def insert_job_post(conn, job):
#     cur = conn.cursor()
#     now = datetime.datetime.now()

#     query = """
#         INSERT INTO job_posts (
#             post_id, company, category, job_title, application_method, post_date, deadline,
#             certifications, skills_needed, platform, employment_type,
#             city, country, language, work_mode, industry,
#             detailed_experience_level, salary, years_experience, status, lifecycle_stage,
#             created_at, updated_at, version
#         ) VALUES (
#             %(post_id)s, %(company)s, %(category)s, %(job_title)s, %(application_method)s,
#             %(post_date)s, %(deadline)s, %(certifications)s, %(skills_needed)s,
#             %(platform)s, %(employment_type)s, %(city)s, %(country)s, %(language)s,
#             %(work_mode)s, %(industry)s, %(detailed_experience_level)s,
#             %(salary)s, %(years_experience)s, %(status)s, %(lifecycle_stage)s,
#             %(created_at)s, %(updated_at)s, %(version)s
#         )
#     """

#     job["created_at"] = now
#     job["updated_at"] = now
#     job["version"] = 1

#     cur.execute(query, job)
#     conn.commit()
#     cur.close()
#     return job

# def insert_candidate(conn, cand):
#     cur = conn.cursor()
#     now = datetime.datetime.now()

#     query = """
#         INSERT INTO candidate_info (
#             candidate_id, name, gender, date_of_birth, nationality, location,
#             graduation_year, GPA, education_level, job_category,
#             current_title, seniority, experience_range, current_company,
#             previous_companies, previous_industries, skills, certifications,
#             current_salary, is_active,
#             created_at, updated_at, version
#         ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::text[],%s::text[],%s::text[],%s::text[],%s,%s,%s,%s,%s)
#     """

#     values = (
#         cand["candidate_id"], cand["name"], cand["gender"], cand["date_of_birth"],
#         cand["nationality"], cand["location"], cand["graduation_year"], cand["GPA"],
#         cand["education_level"], cand["job_category"], cand["current_title"],
#         cand["seniority"], cand["experience_range"], cand["current_company"],
#         cand["previous_companies"], cand["previous_industries"], cand["skills"],
#         cand["certifications"], cand["current_salary"], cand["is_active"],
#         now, now, 1
#     )

#     cur.execute(query, values)
#     conn.commit()
#     cur.close()

#     cand["created_at"] = now
#     cand["updated_at"] = now
#     cand["version"] = 1
#     return cand

# def _insert_application_db(conn, app):
#     cur = conn.cursor()
#     cur.execute("""
#         INSERT INTO applications (
#             application_id, candidate_id, post_id, status, lifecycle_stage,
#             applied_date, last_updated, created_at, updated_at, version
#         ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#     """, (
#         app["application_id"], app["candidate_id"], app["post_id"],
#         app["status"], app["lifecycle_stage"], app["applied_date"],
#         app["last_updated"], app["created_at"], app["updated_at"], app["version"]
#     ))
#     conn.commit()
#     cur.close()

# # ═══════════════════════════════════════════════════════════════════════
# #  LIFECYCLE UPDATE LOGIC (MODIFIED: category tracking + viewing)
# # ═══════════════════════════════════════════════════════════════════════

# class LifecycleEngine:
    
    
    
#     JOB_TRANSITIONS = {
#         0: ("salary_change", 1),
#         1: ("deadline_change", 2),
#         2: ("skills_change", 3),
#         3: ("work_mode_change", 4),
#         4: ("close_job", 5),
#     }

#     # MODIFIED: Realistic viewing behavior
#     # 72% viewed, 28% never viewed
#     # Delays: 1-3d (50%), 3-7d (30%), 7-14d (15%), 14+d (5%)
#     APP_TRANSITIONS = {
#         0: ("viewed", 1),
#     }

#     def __init__(self, conn):
#         self.conn = conn
#         self.active_candidates = {}
#         self.inactive_candidates = set()
#         self.active_jobs = {}
#         self.closed_jobs = set()
#         self.active_applications = {}
#         self.final_applications = set()

#         # NEW: Category tracking for intelligent matching
#         self.candidate_categories = {}   # candidate_id -> job_category
#         self.candidate_skills = {}       # candidate_id -> skills list
#         self.job_categories = {}         # post_id -> category
#         self.job_skills = {}             # post_id -> skills list

#         # NEW: O(1) category -> {id, ...} reverse indices, populated ONLY
#         # for entities that have finished warm-up (i.e. are in
#         # active_candidates / active_jobs). This is what
#         # _pick_job_for_category()/_pick_candidate_for_category() draw
#         # from, so a category-matched application can never reference an
#         # entity that hasn't had time to propagate through
#         # Debezium -> Kafka -> the match engine's dimension tables yet -
#         # closing the same CDC race the warm-up delay targets, but for the
#         # majority (category-matched) application path specifically.
#         self.candidates_by_category = defaultdict(set)
#         self.jobs_by_category = defaultdict(set)

#         # NEW: Warm-up tracking — entities must age 60s before use in applications
#         self.warming_candidates = {}   # candidate_id -> warm_until_timestamp
#         self.warming_jobs = {}         # post_id -> warm_until_timestamp
#         WARM_UP_SECONDS = 60

#         self._load_state()
#     # ADD THIS METHOD HERE — inside the class, same indent as __init__
#     def _promote_warmed_entities(self):
#         """Move candidates/jobs from warming pool to active pool after 60s delay.

#         This is the ONLY place candidates_by_category/jobs_by_category get
#         populated - i.e. an id becomes selectable by
#         _pick_candidate_for_category()/_pick_job_for_category() at exactly
#         the same moment it becomes selectable via active_candidates/
#         active_jobs. There is no path for a still-warming entity to be
#         picked by the category-matched application logic.
#         """
#         now = datetime.datetime.now()

#         ready_candidates = [
#             cid for cid, warm_until in self.warming_candidates.items()
#             if now >= warm_until
#         ]
#         for cid in ready_candidates:
#             self.active_candidates[cid] = {
#                 "profile_updates": 0,
#                 "company_changes": 0,
#                 "location_changes": 0,
#                 "availability": "available",
#                 "is_active": True
#             }
#             self.candidates_by_category[self.candidate_categories.get(cid)].add(cid)
#             del self.warming_candidates[cid]
        
#         ready_jobs = [
#             pid for pid, warm_until in self.warming_jobs.items()
#             if now >= warm_until
#         ]
#         for pid in ready_jobs:
#             self.active_jobs[pid] = 0
#             self.jobs_by_category[self.job_categories.get(pid)].add(pid)
#             del self.warming_jobs[pid]

#     def _load_state(self):
#         """Hydrate state from existing DB records"""
#         cur = self.conn.cursor()

#         cur.execute("SELECT candidate_id, is_active, job_category, skills FROM candidate_info")
#         for cid, active, job_category, skills in cur.fetchall():
#             if active:
#                 self.active_candidates[cid] = {
#                     "profile_updates": 0,
#                     "company_changes": 0,
#                     "location_changes": 0,
#                     "availability": "available",
#                     "is_active": True
#                 }
#                 self.candidate_categories[cid] = job_category
#                 self.candidate_skills[cid] = skills or []
#                 self.candidates_by_category[job_category].add(cid)
#             else:
#                 self.inactive_candidates.add(cid)

#         cur.execute("SELECT post_id, status, lifecycle_stage, category, skills_needed FROM job_posts")
#         for pid, status, stage, category, skills in cur.fetchall():
#             if status == 'open':
#                 self.active_jobs[pid] = stage
#                 self.job_categories[pid] = category
#                 self.job_skills[pid] = skills or []
#                 self.jobs_by_category[category].add(pid)
#             else:
#                 self.closed_jobs.add(pid)

#         cur.execute("SELECT application_id, status, lifecycle_stage FROM applications WHERE final_outcome IS NULL")
#         for aid, status, stage in cur.fetchall():
#             self.active_applications[aid] = stage

#         cur.execute("SELECT application_id FROM applications WHERE final_outcome IS NOT NULL")
#         for (aid,) in cur.fetchall():
#             self.final_applications.add(aid)

#         cur.close()
#         print(f"🔋 State loaded: {len(self.active_candidates)} candidates, "
#               f"{len(self.active_jobs)} jobs, {len(self.active_applications)} active apps")

#     # ── Candidate Lifecycle ───────────────────────────────────────────
    
#     def update_candidate(self):
#         if not self.active_candidates:
#             return None

#         cid = random.choice(list(self.active_candidates.keys()))
#         state = self.active_candidates[cid]
#         cur = self.conn.cursor()
#         now = datetime.datetime.now()
#         event_ts = now.isoformat()

#         cur.execute("""
#             SELECT candidate_id, name, gender, date_of_birth, nationality, location,
#                 graduation_year, GPA, education_level, job_category,
#                 current_title, seniority, experience_range, current_company,
#                 previous_companies, previous_industries, skills, certifications,
#                 current_salary, is_active, created_at, updated_at, version
#             FROM candidate_info WHERE candidate_id=%s
#         """, (cid,))
#         cols = [d[0] for d in cur.description]
#         before = dict(zip(cols, cur.fetchone()))

#         roll = random.random()
#         desc = ""
#         new_version = before["version"]

#         if roll < 0.80:
#             action = random.choice(["new_company", "left_job", "relocated"])
#             state["profile_updates"] += 1

#             if action == "new_company":
#                 new_comp = random.choice(top_300_unique)
#                 cur.execute("""
#                     UPDATE candidate_info 
#                     SET current_company=%s, updated_at=%s, version=version+1
#                     WHERE candidate_id=%s
#                     RETURNING version
#                 """, (new_comp, now, cid))
#                 state["company_changes"] += 1
#                 state["availability"] = "employed"
#                 desc = f"new company → {new_comp}"

#             elif action == "left_job":
#                 cur.execute("""
#                     UPDATE candidate_info 
#                     SET current_company=%s, updated_at=%s, version=version+1
#                     WHERE candidate_id=%s
#                     RETURNING version
#                 """, ("", now, cid))
#                 state["company_changes"] += 1
#                 state["availability"] = "available"
#                 desc = "left job"

#             else:
#                 continent = random.choice(list(location.keys()))
#                 new_loc = random.choice(location[continent])
#                 cur.execute("""
#                     UPDATE candidate_info 
#                     SET location=%s, updated_at=%s, version=version+1
#                     WHERE candidate_id=%s
#                     RETURNING version
#                 """, (new_loc, now, cid))
#                 state["location_changes"] += 1
#                 desc = f"relocated → {new_loc}"

#             new_version = cur.fetchone()[0]
#             desc += f" (v{new_version})"

#         elif roll < 0.90:
#             cur.execute("""
#                 UPDATE candidate_info 
#                 SET is_active=FALSE, updated_at=%s, version=version+1
#                 WHERE candidate_id=%s
#                 RETURNING version
#             """, (now, cid))
#             new_version = cur.fetchone()[0]
#             state["is_active"] = False
#             state["availability"] = "inactive"
#             del self.active_candidates[cid]
#             self.inactive_candidates.add(cid)
#             self.candidates_by_category[self.candidate_categories.get(cid)].discard(cid)
#             self.candidate_categories.pop(cid, None)
#             self.candidate_skills.pop(cid, None)
#             desc = f"deactivated (v{new_version})"

#         else:
#             desc = "no-op (deletion slot)"

#         cur.execute("""
#             SELECT candidate_id, name, gender, date_of_birth, nationality, location,
#                 graduation_year, GPA, education_level, job_category,
#                 current_title, seniority, experience_range, current_company,
#                 previous_companies, previous_industries, skills, certifications,
#                 current_salary, is_active, created_at, updated_at, version
#             FROM candidate_info WHERE candidate_id=%s
#         """, (cid,))
#         after = dict(zip(cols, cur.fetchone()))

#         self.conn.commit()
#         cur.close()

#         return {
#             "entity": "candidate",
#             "entity_id": cid,
#             "operation": "UPDATE",
#             "event_timestamp": event_ts,
#             "version": new_version,
#             "before": before,
#             "after": after,
#             "action": desc,
#         }

#     def delete_candidate(self):
#         pool = list(self.inactive_candidates)
#         if not pool:
#             return None

#         cid = random.choice(pool)
#         cur = self.conn.cursor()
#         now = datetime.datetime.now()
#         event_ts = now.isoformat()

#         cur.execute("""
#             SELECT candidate_id, name, gender, date_of_birth, nationality, location,
#                 graduation_year, GPA, education_level, job_category,
#                 current_title, seniority, experience_range, current_company,
#                 previous_companies, previous_industries, skills, certifications,
#                 current_salary, is_active, created_at, updated_at, version
#             FROM candidate_info WHERE candidate_id=%s
#         """, (cid,))
#         cols = [d[0] for d in cur.description]
#         before = dict(zip(cols, cur.fetchone()))

#         cur.execute("""
#             UPDATE candidate_info 
#             SET is_active=FALSE, 
#                 name='[DELETED]', 
#                 current_company='[DELETED]', 
#                 location='[DELETED]',
#                 skills='{}'::text[],
#                 certifications='{}'::text[],
#                 previous_companies='{}'::text[],
#                 previous_industries='{}'::text[],
#                 updated_at=%s, 
#                 version=version+1
#             WHERE candidate_id=%s
#             RETURNING version
#         """, (now, cid))

#         row = cur.fetchone()
#         new_version = row[0] if row else before["version"] + 1

#         cur.execute("""
#             SELECT candidate_id, name, gender, date_of_birth, nationality, location,
#                 graduation_year, GPA, education_level, job_category,
#                 current_title, seniority, experience_range, current_company,
#                 previous_companies, previous_industries, skills, certifications,
#                 current_salary, is_active, created_at, updated_at, version
#             FROM candidate_info WHERE candidate_id=%s
#         """, (cid,))
#         after = dict(zip(cols, cur.fetchone()))

#         self.conn.commit()
#         cur.close()

#         self.inactive_candidates.discard(cid)
#         self.candidates_by_category[self.candidate_categories.get(cid)].discard(cid)
#         self.candidate_categories.pop(cid, None)
#         self.candidate_skills.pop(cid, None)

#         return {
#             "entity": "candidate",
#             "entity_id": cid,
#             "operation": "DELETE",
#             "event_timestamp": event_ts,
#             "version": new_version,
#             "before": before,
#             "after": after,
#             "action": f"soft-deleted (v{new_version})"
#         }

#     # ── Job Lifecycle ───────────────────────────────────────────────────
#     def update_job(self):
#         if not self.active_jobs:
#             return None

#         pid = random.choice(list(self.active_jobs.keys()))
#         stage = self.active_jobs[pid]
#         cur = self.conn.cursor()
#         now = datetime.datetime.now()
#         event_ts = now.isoformat()

#         cur.execute("""
#             SELECT post_id, company, category, job_title, application_method, post_date, deadline,
#                 certifications, skills_needed, platform, employment_type,
#                 city, country, language, work_mode, industry,
#                 detailed_experience_level, salary, years_experience, status, lifecycle_stage,
#                 created_at, updated_at, version
#             FROM job_posts WHERE post_id=%s
#         """, (pid,))
#         cols = [d[0] for d in cur.description]
#         before = dict(zip(cols, cur.fetchone()))

#         roll = random.random()
#         desc = ""
#         new_version = before["version"]

#         if roll < 0.85 and stage < 5:
#             transition_name, next_stage = self.JOB_TRANSITIONS[stage]

#             if transition_name == "salary_change":
#                 old_sal = before["salary"]
#                 delta = random.choice([-1, 1]) * random.randint(int(old_sal * 0.05), int(old_sal * 0.20))
#                 new_sal = max(10000, old_sal + delta)
#                 cur.execute("""
#                     UPDATE job_posts 
#                     SET salary=%s, lifecycle_stage=%s, updated_at=%s, version=version+1
#                     WHERE post_id=%s
#                     RETURNING version
#                 """, (new_sal, next_stage, now, pid))

#             elif transition_name == "deadline_change":
#                 old_dl = before["deadline"]
#                 delta_days = random.randint(-7, 21)
#                 new_dl = old_dl + datetime.timedelta(days=delta_days)
#                 cur.execute("""
#                     UPDATE job_posts 
#                     SET deadline=%s, lifecycle_stage=%s, updated_at=%s, version=version+1
#                     WHERE post_id=%s
#                     RETURNING version
#                 """, (new_dl, next_stage, now, pid))

#             elif transition_name == "skills_change":
#                 old_skills = before["skills_needed"]
#                 cat = before["category"]
#                 all_skills = jobs_skills.get(cat, [])
#                 available = [s for s in all_skills if s not in old_skills]
#                 if available:
#                     new_skills = old_skills + random.sample(available, min(2, len(available)))
#                     cur.execute("""
#                         UPDATE job_posts 
#                         SET skills_needed=%s, lifecycle_stage=%s, updated_at=%s, version=version+1
#                         WHERE post_id=%s
#                         RETURNING version
#                     """, (new_skills, next_stage, now, pid))
#                 else:
#                     cur.execute("""
#                         UPDATE job_posts 
#                         SET lifecycle_stage=%s, updated_at=%s, version=version+1
#                         WHERE post_id=%s
#                         RETURNING version
#                     """, (next_stage, now, pid))

#             elif transition_name == "work_mode_change":
#                 old_mode = before["work_mode"]
#                 new_mode = "Hybrid" if old_mode == "Remote" else random.choice([m for m in work_modes if m != old_mode])
#                 cur.execute("""
#                     UPDATE job_posts 
#                     SET work_mode=%s, lifecycle_stage=%s, updated_at=%s, version=version+1
#                     WHERE post_id=%s
#                     RETURNING version
#                 """, (new_mode, next_stage, now, pid))

#             else:
#                 cur.close()
#                 return {"entity": "job_post", "entity_id": pid, "operation": "UPDATE",
#                         "event_timestamp": event_ts, "version": before["version"],
#                         "before": before, "after": before, "action": f"unknown transition: {transition_name}"}

#             version_row = cur.fetchone()
#             if version_row is None:
#                 cur.close()
#                 self.active_jobs.pop(pid, None)
#                 self.jobs_by_category[self.job_categories.get(pid)].discard(pid)
#                 self.job_categories.pop(pid, None)
#                 self.job_skills.pop(pid, None)
#                 return {"entity": "job_post", "entity_id": pid, "operation": "UPDATE",
#                         "event_timestamp": event_ts, "version": before["version"],
#                         "before": before, "after": before, "action": "job vanished"}

#             new_version = version_row[0]
#             self.active_jobs[pid] = next_stage
#             desc = f"{transition_name} (v{new_version})"

#         else:
#             cur.execute("""
#                 UPDATE job_posts 
#                 SET status='closed', lifecycle_stage=5, updated_at=%s, version=version+1
#                 WHERE post_id=%s
#                 RETURNING version
#             """, (now, pid))

#             version_row = cur.fetchone()
#             if version_row is None:
#                 cur.close()
#                 self.active_jobs.pop(pid, None)
#                 self.jobs_by_category[self.job_categories.get(pid)].discard(pid)
#                 self.job_categories.pop(pid, None)
#                 self.job_skills.pop(pid, None)
#                 return {"entity": "job_post", "entity_id": pid, "operation": "UPDATE",
#                         "event_timestamp": event_ts, "version": before["version"],
#                         "before": before, "after": before, "action": "job vanished on close"}

#             new_version = version_row[0]
#             del self.active_jobs[pid]
#             self.closed_jobs.add(pid)
#             self.jobs_by_category[self.job_categories.get(pid)].discard(pid)
#             self.job_categories.pop(pid, None)
#             self.job_skills.pop(pid, None)
#             desc = f"closed (v{new_version})"

#         cur.execute("""
#             SELECT post_id, company, category, job_title, application_method, post_date, deadline,
#                 certifications, skills_needed, platform, employment_type,
#                 city, country, language, work_mode, industry,
#                 detailed_experience_level, salary, years_experience, status, lifecycle_stage,
#                 created_at, updated_at, version
#             FROM job_posts WHERE post_id=%s
#         """, (pid,))
#         after = dict(zip(cols, cur.fetchone()))

#         self.conn.commit()
#         cur.close()
#         return {
#             "entity": "job_post",
#             "entity_id": pid,
#             "operation": "UPDATE",
#             "event_timestamp": event_ts,
#             "version": new_version,
#             "before": before,
#             "after": after,
#             "action": desc
#         }

#     def delete_job(self):
#         pool = list(self.closed_jobs) or list(self.active_jobs.keys())
#         if not pool:
#             return None
#         pid = random.choice(pool)
#         cur = self.conn.cursor()
#         now = datetime.datetime.now()
#         event_ts = now.isoformat()

#         cur.execute("""
#             SELECT post_id, company, category, job_title, application_method, post_date, deadline,
#                 certifications, skills_needed, platform, employment_type,
#                 city, country, language, work_mode, industry,
#                 detailed_experience_level, salary, years_experience, status, lifecycle_stage,
#                 created_at, updated_at, version
#             FROM job_posts WHERE post_id=%s
#         """, (pid,))
#         cols = [d[0] for d in cur.description]
#         row = cur.fetchone()
#         if row is None:
#             cur.close()
#             self.active_jobs.pop(pid, None)
#             self.closed_jobs.discard(pid)
#             self.jobs_by_category[self.job_categories.get(pid)].discard(pid)
#             self.job_categories.pop(pid, None)
#             self.job_skills.pop(pid, None)
#             return None
#         before = dict(zip(cols, row))

#         cur.execute("DELETE FROM job_posts WHERE post_id=%s RETURNING version", (pid,))
#         version_row = cur.fetchone()
#         new_version = version_row[0] if version_row else before["version"] + 1

#         self.conn.commit()
#         cur.close()

#         self.active_jobs.pop(pid, None)
#         self.closed_jobs.discard(pid)
#         self.jobs_by_category[self.job_categories.get(pid)].discard(pid)
#         self.job_categories.pop(pid, None)
#         self.job_skills.pop(pid, None)

#         return {
#             "entity": "job_post",
#             "entity_id": pid,
#             "operation": "DELETE",
#             "event_timestamp": event_ts,
#             "version": new_version,
#             "before": before,
#             "after": None,
#             "action": f"deleted (v{new_version})"
#         }

#     # ── Application Lifecycle (MODIFIED: intelligent matching + realistic dates) ─

#     def _pick_candidate_for_category(self, target_category):
#         """Pick a random WARM candidate whose job_category matches
#         target_category. Draws from candidates_by_category (populated only
#         at promotion out of warm-up), not from the broader
#         candidate_categories map, so this can never return an id that
#         hasn't propagated through Debezium -> Kafka yet."""
#         matching = self.candidates_by_category.get(target_category)
#         if matching:
#             return random.choice(list(matching))
#         return None

#     def _pick_job_for_category(self, target_category):
#         """Pick a random OPEN, WARM job whose category matches
#         target_category. Draws from jobs_by_category (populated only at
#         promotion out of warm-up), not from the broader job_categories
#         map, so this can never return an id that hasn't propagated through
#         Debezium -> Kafka yet. This is what guarantees that every
#         category-matched application (70% of applications) references a
#         candidate_id/post_id already confirmed sent to Kafka - closing the
#         CDC race that otherwise shows up downstream as unresolved_rows."""
#         matching = self.jobs_by_category.get(target_category)
#         if matching:
#             return random.choice(list(matching))
#         return None

#     def insert_application(self):
#         """
#         Create an application with intelligent matching:
#         - Compute match score (category + skills overlap)
#         - Use score to determine application probability
#         - 70% of attempts are category-matched, 30% are random cross-category
#         - Realistic application dates with deadline decay
#         """
#         if not self.active_candidates or not self.active_jobs:
#             return None

#         app_id = str(uuid.uuid4())

#         # Decide: category-matched (70%) or random (30%)
#         use_matched = random.random() < 0.70

#         if use_matched:
#             # Category-matched: pick candidate, then find job in same category
#             cid = random.choice(list(self.active_candidates.keys()))
#             cand_category = self.candidate_categories.get(cid)
#             cand_skills = self.candidate_skills.get(cid, [])

#             if cand_category:
#                 pid = self._pick_job_for_category(cand_category)
#                 if pid is None:
#                     # Fallback: no matching job available
#                     pid = random.choice(list(self.active_jobs.keys()))
#                     match_type = "matched-cand-fallback"
#                 else:
#                     match_type = "category-matched"
#             else:
#                 pid = random.choice(list(self.active_jobs.keys()))
#                 match_type = "no-category"
#         else:
#             # Random cross-category application (30%)
#             cid = random.choice(list(self.active_candidates.keys()))
#             pid = random.choice(list(self.active_jobs.keys()))
#             match_type = "random"
#             cand_category = self.candidate_categories.get(cid)
#             cand_skills = self.candidate_skills.get(cid, [])

#         # Get job info for match score
#         job_category = self.job_categories.get(pid, "")
#         job_skills = self.job_skills.get(pid, [])

#         # Compute match score
#         match_score = compute_match_score(cand_category, job_category, cand_skills, job_skills)

#         # Determine if application actually happens (probability-based)
#         prob = application_probability(match_score)
#         if random.random() > prob:
#             # Application rejected by probability — skip this insert
#             # But we still return a result for logging
#             return {
#                 "entity": "application",
#                 "entity_id": f"skipped-{app_id[:8]}",
#                 "operation": "INSERT",
#                 "event_timestamp": datetime.datetime.now().isoformat(),
#                 "version": 0,
#                 "before": None,
#                 "after": None,
#                 "action": f"SKIPPED (score={match_score}, prob={prob:.1%}) [{match_type}]"
#             }

#         # Get post date and deadline for realistic application date
#         cur = self.conn.cursor()
#         cur.execute("SELECT post_date, deadline FROM job_posts WHERE post_id = %s", (pid,))
#         row = cur.fetchone()
#         cur.close()
#         if not row:
#             return None
#         post_date, deadline = row

#         # Generate realistic application date
#         applied_date = generate_application_date(post_date, deadline)

#         now = datetime.datetime.now()

#         app = {
#             "application_id": app_id,
#             "candidate_id": cid,
#             "post_id": pid,
#             "status": "applied",
#             "lifecycle_stage": 0,
#             "applied_date": applied_date,
#             "last_updated": applied_date,
#             "created_at": now,
#             "updated_at": now,
#             "version": 1
#         }

#         _insert_application_db(self.conn, app)
#         self.active_applications[app_id] = 0

#         return {
#             "entity": "application",
#             "entity_id": app_id,
#             "operation": "INSERT",
#             "event_timestamp": now.isoformat(),
#             "version": 1,
#             "before": None,
#             "after": app,
#             "action": f"applied {applied_date.strftime('%Y-%m-%d')} score={match_score} [{match_type}]"
#         }

#     def update_application(self):
#         """
#         MODIFIED: Realistic viewing behavior.
#         - 72% of applications eventually get viewed
#         - Viewing delays follow realistic distribution
#         """
#         if not self.active_applications:
#             return None

#         aid = random.choice(list(self.active_applications.keys()))
#         stage = self.active_applications[aid]
#         cur = self.conn.cursor()

#         if stage != 0:
#             return None

#         # Check if this application gets viewed (72% probability)
#         if random.random() > VIEW_PROBABILITY:
#             # Never viewed — mark as "ignored" but keep in active_applications
#             # so it doesn't get processed again
#             self.active_applications[aid] = -1  # Special: ignored
#             cur.close()
#             return {
#                 "entity": "application",
#                 "entity_id": aid,
#                 "operation": "UPDATE",
#                 "event_timestamp": datetime.datetime.now().isoformat(),
#                 "version": 1,
#                 "before": None,
#                 "after": None,
#                 "action": "→ never-viewed (ignored)"
#             }

#         transition_name, next_stage = self.APP_TRANSITIONS[stage]

#         cur.execute("""
#             SELECT application_id, candidate_id, post_id, status, lifecycle_stage,
#                 applied_date, last_updated, final_outcome, created_at, updated_at, version
#             FROM applications WHERE application_id=%s
#         """, (aid,))
#         cols = [d[0] for d in cur.description]
#         before = dict(zip(cols, cur.fetchone()))

#         base_date = before["applied_date"]

#         # Realistic viewing delay distribution
#         delay_days = pick_from_distribution(VIEW_DELAY_DISTRIBUTION)
#         now = base_date + datetime.timedelta(days=delay_days)
#         event_ts = datetime.datetime.now().isoformat()

#         cur.execute("""
#             UPDATE applications 
#             SET status=%s, lifecycle_stage=%s, last_updated=%s, updated_at=%s, version=version+1
#             WHERE application_id=%s
#             RETURNING version
#         """, (transition_name, next_stage, now, now, aid))

#         version_row = cur.fetchone()
#         if version_row is None:
#             cur.close()
#             self.active_applications.pop(aid, None)
#             return {"entity": "application", "entity_id": aid, "operation": "UPDATE",
#                     "event_timestamp": event_ts, "version": before["version"],
#                     "before": before, "after": before, "action": "application vanished"}

#         new_version = version_row[0]
#         self.active_applications[aid] = next_stage

#         cur.execute("""
#             SELECT application_id, candidate_id, post_id, status, lifecycle_stage,
#                 applied_date, last_updated, final_outcome, created_at, updated_at, version
#             FROM applications WHERE application_id=%s
#         """, (aid,))
#         after = dict(zip(cols, cur.fetchone()))

#         self.conn.commit()
#         cur.close()
#         return {
#             "entity": "application",
#             "entity_id": aid,
#             "operation": "UPDATE",
#             "event_timestamp": event_ts,
#             "version": new_version,
#             "before": before,
#             "after": after,
#             "action": f"→ {transition_name} after {delay_days}d (v{new_version})"
#         }

#     def delete_application(self):
#         pool = list(self.final_applications) or list(self.active_applications.keys())
#         if not pool:
#             return None
#         aid = random.choice(pool)
#         cur = self.conn.cursor()
#         now = datetime.datetime.now()
#         event_ts = now.isoformat()

#         cur.execute("""
#             SELECT application_id, candidate_id, post_id, status, lifecycle_stage,
#                 applied_date, last_updated, final_outcome, created_at, updated_at, version
#             FROM applications WHERE application_id=%s
#         """, (aid,))
#         cols = [d[0] for d in cur.description]
#         row = cur.fetchone()
#         if row is None:
#             cur.close()
#             self.active_applications.pop(aid, None)
#             self.final_applications.discard(aid)
#             return None
#         before = dict(zip(cols, row))

#         cur.execute("DELETE FROM applications WHERE application_id=%s RETURNING version", (aid,))
#         version_row = cur.fetchone()
#         new_version = version_row[0] if version_row else before["version"] + 1

#         self.conn.commit()
#         cur.close()

#         self.active_applications.pop(aid, None)
#         self.final_applications.discard(aid)

#         return {
#             "entity": "application",
#             "entity_id": aid,
#             "operation": "DELETE",
#             "event_timestamp": event_ts,
#             "version": new_version,
#             "before": before,
#             "after": None,
#             "action": f"deleted (v{new_version})"
#         }

# # ═══════════════════════════════════════════════════════════════════════
# #  OPERATION CHOOSER
# # ═══════════════════════════════════════════════════════════════════════

# def choose_operation(engine):
#     weights = CONFIG["weights"]
#     options = []
#     w = []

#     options.append(("candidate", "insert"))
#     w.append(weights[("candidate", "insert")])
#     if engine.active_candidates:
#         options.append(("candidate", "update"))
#         w.append(weights[("candidate", "update")])
#     if engine.inactive_candidates:
#         options.append(("candidate", "delete"))
#         w.append(weights[("candidate", "delete")])

#     options.append(("job_post", "insert"))
#     w.append(weights[("job_post", "insert")])
#     if engine.active_jobs:
#         options.append(("job_post", "update"))
#         w.append(weights[("job_post", "update")])

#     # NEW: Only create applications if BOTH pools have active (warmed) entities
#     has_ready_candidates = len(engine.active_candidates) > 0
#     has_ready_jobs = len(engine.active_jobs) > 0
#     if has_ready_candidates and has_ready_jobs:
#         options.append(("application", "insert"))
#         w.append(weights[("application", "insert")])
#     if engine.active_applications:
#         options.append(("application", "update"))
#         w.append(weights[("application", "update")])

#     return random.choices(options, weights=w, k=1)[0]

# # ═══════════════════════════════════════════════════════════════════════
# #  MAIN PIPELINE
# # ═══════════════════════════════════════════════════════════════════════

# def main():
#     conn = connect_postgres()
#     create_base_tables(conn)
#     ensure_schema(conn)

#     engine = LifecycleEngine(conn)

#     stats = defaultdict(int)
#     match_stats = {"category-matched": 0, "matched-cand-fallback": 0,
#                    "no-category": 0, "random": 0, "skipped": 0}
#     score_distribution = defaultdict(int)
#     start = time.time()

#     for i in range(1, CONFIG["total_operations"] + 1):
#         # NEW: Promote any entities that have finished their 60s warm-up
#         engine._promote_warmed_entities()
#         entity, operation = choose_operation(engine)
#         result = None

#         # ── CANDIDATE ─────────────────────────────────────────────────
#         if entity == "candidate":
#             if operation == "insert":
#                 cand = generate_candidate_info()
#                 cand = insert_candidate(conn, cand)

#                                 # NEW: Put into warming pool, not active yet
#                 warm_until = cand["created_at"] + datetime.timedelta(seconds=60)
#                 engine.warming_candidates[cand["candidate_id"]] = warm_until
#                 # Still track category/skills for when it becomes active
#                 engine.candidate_categories[cand["candidate_id"]] = cand["job_category"]
#                 engine.candidate_skills[cand["candidate_id"]] = cand["skills"]
#                 result = {
#                     "entity": "candidate",
#                     "entity_id": cand["candidate_id"],
#                     "operation": "INSERT",
#                     "event_timestamp": cand["created_at"].isoformat(),
#                     "version": 1,
#                     "before": None,
#                     "after": cand,
#                     "action": "inserted"
#                 }

#             elif operation == "update":
#                 result = engine.update_candidate()

#             elif operation == "delete":
#                 result = engine.delete_candidate()

#         # ── JOB POST ──────────────────────────────────────────────────
#         elif entity == "job_post":
#             if operation == "insert":
#                 job = generate_job_post()
#                 job = insert_job_post(conn, job)

#                 # NEW: Put into warming pool, not active yet
#                 warm_until = job["created_at"] + datetime.timedelta(seconds=60)
#                 engine.warming_jobs[job["post_id"]] = warm_until
#                 engine.job_categories[job["post_id"]] = job["category"]
#                 engine.job_skills[job["post_id"]] = job["skills_needed"]

#                 result = {
#                     "entity": "job_post",
#                     "entity_id": job["post_id"],
#                     "operation": "INSERT",
#                     "event_timestamp": job["created_at"].isoformat(),
#                     "version": 1,
#                     "before": None,
#                     "after": job,
#                     "action": "inserted"
#                 }

#             elif operation == "update":
#                 result = engine.update_job()

#             elif operation == "delete":
#                 result = engine.delete_job()

#         # ── APPLICATION ───────────────────────────────────────────────
#         elif entity == "application":
#             if operation == "insert":
#                 result = engine.insert_application()
#                 if result and "action" in result:
#                     if "SKIPPED" in result["action"]:
#                         match_stats["skipped"] += 1
#                     else:
#                         for key in ["category-matched", "matched-cand-fallback", "no-category", "random"]:
#                             if f"[{key}]" in result["action"]:
#                                 match_stats[key] += 1
#                                 break
#                         # Track score distribution
#                         if "score=" in result["action"]:
#                             try:
#                                 score_str = result["action"].split("score=")[1].split()[0]
#                                 score = float(score_str)
#                                 score_bucket = f"{int(score // 10 * 10)}-{int(score // 10 * 10 + 9)}"
#                                 score_distribution[score_bucket] += 1
#                             except:
#                                 pass

#             elif operation == "update":
#                 result = engine.update_application()

#             elif operation == "delete":
#                 result = engine.delete_application()

#         if result:
#             stats[f"{entity}_{operation}"] += 1
#             key = f"{entity}:{operation}"
#             print(f"[{i:05d}] {key:25s} | {result['entity_id'][:8]}... | {result['action']}")

#         if i % 500 == 0:
#             print(f"\n📊 Progress: {i} ops | Stats: {dict(stats)}")
#             print(f"📊 Match Stats: {dict(match_stats)}")
#             if score_distribution:
#                 print(f"📊 Score Distribution: {dict(score_distribution)}")
#             print()

#     conn.close()

#     elapsed = time.time() - start
#     print(f"\n{'='*60}")
#     print(f"🏁 Completed {CONFIG['total_operations']} operations in {elapsed:.2f}s")
#     print(f"📈 Final Stats: {dict(stats)}")
#     print(f"📈 Match Stats: {dict(match_stats)}")
#     if score_distribution:
#         print(f"📈 Score Distribution: {dict(score_distribution)}")
#     total_matched = match_stats["category-matched"] + match_stats["matched-cand-fallback"]
#     total_apps = sum(v for k, v in match_stats.items() if k != "skipped")
#     if total_apps > 0:
#         print(f"📈 Category Match Rate: {total_matched/total_apps*100:.1f}% ({total_matched}/{total_apps})")
#     print(f"{'='*60}")

# if __name__ == "__main__":
#     main()
#!/usr/bin/env python3
"""
Dynamic Data Generator with Lifecycle Management
================================================
Chooses randomly (with weights) between:
  - Entities: candidate, job_post, application
  - Operations: insert, update, delete

Respects realistic lifecycles:
  Candidate:  active → changes availability → deactivates profile
  Job:        created → salary change → deadline change → skills change 
              → work_mode change → closed
  Application: applied → viewed → interview → offer → {rejected | accepted}

IMPROVEMENTS APPLIED:
  1. INTELLIGENT APPLICATIONS (70% category-matched, 30% random)
     - Category-matched picks (_pick_job_for_category /
       _pick_candidate_for_category) draw ONLY from candidates_by_category /
       jobs_by_category, which are populated at the exact moment an entity
       finishes its CDC warm-up (see improvement #7) - never from the
       broader candidate_categories/job_categories maps, which also
       contain entities still warming. This guarantees every
       category-matched application references a candidate_id/post_id
       that has already had time to propagate through
       Debezium -> Kafka -> the match engine's dimension tables.
     - HARD category gate in compute_match_score(): job_category equality
       is a prerequisite for a high-priority match, not a weighted
       contributor. Cross-category applications are capped at a low score
       (0-20) regardless of any incidental skill-name overlap, so a
       HIGH_PRIORITY-tier match can only ever come from a same-category
       pair. Within a category match, score is 100% skills overlap.
     - Keeps match engine completely independent - no schema/column
       changes, no new fields it needs to know about.

  2. CANDIDATE INTERNAL CONSISTENCY
     - Career history is built from temporal progression templates
     - Category → Seniority → Title → Experience → Skills → Salary → Companies
     - All derived from a single coherent career timeline

  3. TEMPORAL CAREER HISTORY
     - previous_companies and previous_industries derived from dated roles
     - Example: 2020-2022 Junior Analyst → 2022-2024 Analyst → 2024-2026 Senior
     - Years of experience, current title, current company all derived from history

  4. JOB INTERNAL CONSISTENCY
     - Company → Industry → Category → Title → Seniority → Experience → Skills → Salary → Location
     - Industry derived from company (or category fallback)
     - Salary adjusted for region cost-of-living
     - Location weighted by industry geography

  5. REALISTIC APPLICATION DATES
     - 0-2 days: 35%, 3-7 days: 35%, 8-14 days: 20%, 15+ days: 10%
     - Deadline decay: probability drops as deadline approaches

  6. REALISTIC VIEWING BEHAVIOR
     - 72% of applications eventually viewed, 28% never viewed
     - Viewing delays: 1-3 days (50%), 3-7 days (30%), 7-14 days (15%), 14+ days (5%)

  7. CDC WARM-UP DELAY (fixes unresolved_rows in the match engine)
     - A brand-new candidate/job sits in warming_candidates/warming_jobs
       for 60s (comfortably longer than the match engine's 30s micro-batch
       trigger) before it's added to active_candidates/active_jobs AND to
       candidates_by_category/jobs_by_category.
     - insert_application() (both the category-matched path via
       _pick_job_for_category/_pick_candidate_for_category, and the random
       30% path via active_candidates/active_jobs) can therefore never
       reference an id that hasn't already been confirmed replicated.
       Without this, applications routinely referenced a candidate/job
       created milliseconds earlier - long before Debezium had a chance to
       publish it - which the match engine could only ever resolve as
       unresolved -> pending -> eventually dead-lettered.

NOTE ON ARCHITECTURE
---------------------
This script is a PURE Postgres writer. It does not talk to Kafka at all.
Change capture is handled downstream by a Debezium PostgreSQL connector
that reads the WAL via logical decoding and publishes events to Kafka.

    PostgreSQL -> WAL -> Debezium -> Kafka -> {candidate,job,application}_events

The `created_at` / `updated_at` / `version` columns and `REPLICA IDENTITY
FULL` setting exist specifically so Debezium can emit full before/after
row images without any app-level envelope building.
"""

import random
import numpy as np
import datetime
import psycopg2
import os
import argparse
from dotenv import load_dotenv
import time
import json
import uuid
import re
from collections import defaultdict

# ── Import your data module ──────────────────────────────────────────────
from data_used import (
    top_300_unique, job_titles, job_certifications, jobs_skills,
    num_skills_needed, num_certifications_needed,
    job_posting_platforms, employment_types,
    country_languages, location, work_modes, industries,
    category_to_experience, matching_keywords, employment_rules,
    salary_ranges_per_year, level_to_years, experience_years,
    apply_methods,
    male_names, female_names, nationalities, Gpa,
    education_levels,
    # NEW: weighted category + consistency mappings
    choose_weighted_category, CATEGORY_WEIGHTS,
    company_industry_map, industry_location_weights, category_industries,
    region_salary_multipliers, seniority_experience_years,
    career_progression_templates,
    APPLICATION_DATE_DISTRIBUTION, VIEW_PROBABILITY, VIEW_DELAY_DISTRIBUTION,
    pick_from_distribution
)

# ── Configuration ────────────────────────────────────────────────────────
load_dotenv()
CONFIG = {
    "total_operations": 1000000,
    # 0 = run exactly total_operations ops; >0 = stop after N seconds
    "duration_seconds": 0,
    # ── configurable lifecycle rates (0 = default behavior, unchanged) ──
    # job_close_rate: extra probability of the periodic close branch in
    #   update_job() (default close probability is 0.15).
    "job_close_rate": 0.0,
    # job_reopen_rate: weight (x100) of the new ("job_post", "reopen")
    #   operation; 0 disables reopening entirely (no weight added).
    "job_reopen_rate": 0.0,
    # profile_update_rate: extra weight (x100) added to ("candidate","update").
    "profile_update_rate": 0.0,
    # app_status_transition_rate: >0 enables the FULL application lifecycle
    #   chain (viewed -> interview -> offer -> accepted/rejected with
    #   final_outcome). 0 keeps the original view-only behavior.
    "app_status_transition_rate": 0.0,
    "weights": {
        ("candidate", "insert"):     7,
        ("candidate", "update"):    5,
        ("candidate", "delete"):     1,

        ("job_post", "insert"):     10,
        ("job_post", "update"):      4,
        ("job_post", "delete"):      1,

        ("application", "insert"): 150,
        ("application", "update"):  15,
        ("application", "delete"):   1,
    }
}

# ── Env-var overrides (CLI flags applied in main()) ───────────────────
_ENV_RATE_KEYS = {
    "JOB_CLOSE_RATE":            ("job_close_rate",            float),
    "JOB_REOPEN_RATE":           ("job_reopen_rate",           float),
    "PROFILE_UPDATE_RATE":       ("profile_update_rate",       float),
    "APP_STATUS_TRANSITION_RATE":("app_status_transition_rate",float),
}
for _k, (_cfg_key, _cast) in _ENV_RATE_KEYS.items():
    _v = os.getenv(_k)
    if _v not in (None, ""):
        CONFIG[_cfg_key] = _cast(_v)

def parse_cli_overrides():
    """Apply CLI rate knobs + duration/ops on top of CONFIG/env defaults.

    Deliberately separated so the pure-Postgres writer keeps working from
    a plain `python recruitment_lifecycle_generator.py` run with zero args.
    """
    parser = argparse.ArgumentParser(
        description="Synthetic recruitment workload -> PostgreSQL only (CDC via Debezium)")
    parser.add_argument("--ops", "--total-operations", dest="ops", type=int, default=None,
                        help="override total number of operations (default: CONFIG)")
    parser.add_argument("--duration", type=int, default=None,
                        help="stop after N seconds instead of a fixed op count")
    parser.add_argument("--job-close-rate", type=float, dest="job_close_rate", default=None,
                        help="extra close probability added to the 0.15 default")
    parser.add_argument("--job-reopen-rate", type=float, dest="job_reopen_rate", default=None,
                        help="weight (x100) of reopened jobs; 0 = disabled")
    parser.add_argument("--profile-update-rate", type=float, dest="profile_update_rate", default=None,
                        help="extra weight (x100) for candidate profile updates")
    parser.add_argument("--app-status-transition-rate", type=float,
                        dest="app_status_transition_rate", default=None,
                        help=">0 enables full application lifecycle transitions")
    args = parser.parse_args()
    if args.ops is not None:
        CONFIG["total_operations"] = args.ops
    if args.duration is not None:
        CONFIG["duration_seconds"] = args.duration
    if args.job_close_rate is not None:
        CONFIG["job_close_rate"] = args.job_close_rate
    if args.job_reopen_rate is not None:
        CONFIG["job_reopen_rate"] = args.job_reopen_rate
    if args.profile_update_rate is not None:
        CONFIG["profile_update_rate"] = args.profile_update_rate
    if args.app_status_transition_rate is not None:
        CONFIG["app_status_transition_rate"] = args.app_status_transition_rate


# ── PostgreSQL Connection ────────────────────────────────────────────────
def connect_postgres():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres")
    )

# ── Schema Setup (adds lifecycle columns + applications table) ───────────
def ensure_schema(conn):
    cur = conn.cursor()

    # Candidate: add CDC/SCD columns if missing
    cur.execute("""
        DO $$ 
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                           WHERE table_name='candidate_info' AND column_name='created_at') 
            THEN
                ALTER TABLE candidate_info 
                ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ADD COLUMN version INT DEFAULT 1;
            END IF;
        END $$;
    """)

    # Job posts: add CDC/SCD columns if missing
    cur.execute("""
        DO $$ 
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                           WHERE table_name='job_posts' AND column_name='created_at') 
            THEN
                ALTER TABLE job_posts 
                ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ADD COLUMN version INT DEFAULT 1;
            END IF;
        END $$;
    """)

    # Applications: add CDC/SCD columns if missing
    cur.execute("""
        DO $$ 
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                           WHERE table_name='applications' AND column_name='created_at') 
            THEN
                ALTER TABLE applications 
                ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ADD COLUMN version INT DEFAULT 1;
            END IF;
        END $$;
    """)

    # CRITICAL for Debezium: ensure full row images on UPDATE/DELETE
    cur.execute("ALTER TABLE candidate_info REPLICA IDENTITY FULL")
    cur.execute("ALTER TABLE job_posts REPLICA IDENTITY FULL")
    cur.execute("ALTER TABLE applications REPLICA IDENTITY FULL")

    conn.commit()
    cur.close()

def create_base_tables(conn):
    """Create original tables if they don't exist"""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS job_posts (
            post_id VARCHAR(50) PRIMARY KEY,
            company TEXT,
            category TEXT,
            job_title TEXT,
            application_method TEXT,
            post_date TIMESTAMP,
            deadline TIMESTAMP,
            certifications TEXT[],
            skills_needed TEXT[],
            platform TEXT,
            employment_type TEXT,
            city TEXT,
            country TEXT,
            language TEXT,
            work_mode TEXT,
            industry TEXT,
            detailed_experience_level TEXT,
            salary INT,
            years_experience TEXT,
            status VARCHAR(20) DEFAULT 'open',
            lifecycle_stage INT DEFAULT 0
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidate_info (
            candidate_id VARCHAR(50) PRIMARY KEY,
            name TEXT,
            gender TEXT,
            date_of_birth INT,
            nationality TEXT,
            location TEXT,
            graduation_year INT,
            GPA FLOAT,
            education_level TEXT,
            job_category TEXT,
            current_title TEXT,
            seniority TEXT,
            experience_range TEXT,
            current_company TEXT,
            previous_companies TEXT[],
            previous_industries TEXT[],
            skills TEXT[],
            certifications TEXT[],
            current_salary INT,
            is_active BOOLEAN DEFAULT TRUE
        );
    """)
    cur.execute("""
CREATE TABLE IF NOT EXISTS applications (
    application_id VARCHAR(50) PRIMARY KEY,
    candidate_id VARCHAR(50),
    post_id VARCHAR(50),

    status VARCHAR(30),
    lifecycle_stage INT DEFAULT 0,

    applied_date TIMESTAMP,
    last_updated TIMESTAMP,

    final_outcome VARCHAR(30),

    FOREIGN KEY (candidate_id)
        REFERENCES candidate_info(candidate_id),

    FOREIGN KEY (post_id)
        REFERENCES job_posts(post_id)
);
""")
    conn.commit()
    cur.close()

# ═══════════════════════════════════════════════════════════════════════
#  DATA GENERATION HELPERS
# ═══════════════════════════════════════════════════════════════════════

def get_education_by_age_group(age=None):
    if age is None:
        age = random.randint(20, 65)
    if age < 25:
        weights = [40, 20, 25, 5, 2, 1, 2, 5]
    elif age < 35:
        weights = [25, 15, 30, 15, 5, 3, 4, 3]
    elif age < 50:
        weights = [30, 15, 25, 12, 6, 4, 5, 3]
    else:
        weights = [45, 15, 20, 8, 4, 2, 3, 3]
    return random.choices(education_levels, weights=weights, k=1)[0]

def detect_seniority(title):
    for keyword, level in matching_keywords.items():
        if keyword.lower() in title.lower():
            return level
    return "Mid Level"

def get_salary_range(det_level_exp: str, salary_dict: dict):
    for key in salary_dict:
        if key.lower() in det_level_exp.lower():
            return (salary_dict[key], key)
    return ({"min": 0, "max": 0}, None)

# ── NEW: core-skill-biased sampling ──────────────────────────────────────
#
# WHY THIS EXISTS: match_engine's skill_score is 100 * overlap / needed,
# where overlap = array_intersect(candidate.skills, job.skills_needed).
# Both sides sample independently (uniform random.sample) from the same
# jobs_skills[category] pool. For two independent uniform samples, expected
# overlap fraction is roughly candidate_sample_size / pool_size - typically
# ~25-35%, which caps skill_score's contribution (weight 0.30) well below
# what's needed to clear overall_score >= 80 even when every other sub-score
# maxes out. That's structural, not a bug in either side individually - it's
# what "sample two random subsets of the same list" always produces.
#
# FIX: designate a stable "core" subset of each category's skill pool (the
# first CORE_SKILL_RATIO fraction of jobs_skills[category] - fixed, not
# re-shuffled per call, so it's the SAME subset every time this category is
# sampled). Bias both candidate and job sampling toward that shared core.
# This raises the overlap floor for same-category pairs without forcing
# every pair to match: the CORE_SKILL_BIAS fraction drawn from core pushes
# typical overlap up, and the remaining periphery draw keeps enough
# variance that only the "lucky" upper tail crosses the HIGH_PRIORITY
# threshold - which is what gives a stable ~1% hit rate instead of ~0.03%.
#
# TUNING: these two constants are your dials. Start here, then watch
# `high_priority_rows` / `scored_rows` in the batch metrics log and adjust:
#   - hit rate too low  -> raise CORE_SKILL_BIAS (more overlap pressure)
#   - hit rate too high -> lower CORE_SKILL_BIAS, or lower CORE_SKILL_RATIO
#     (smaller shared core = less overlap even when biased toward it)
# Pool sizes vary by category, so the same constants can land at slightly
# different rates per category - if one category is consistently over/under
# target, it's fine to override these per-category rather than globally.
CORE_SKILL_RATIO = 0.5   # fraction of each category's pool treated as "core"
CORE_SKILL_BIAS = 0.7    # fraction of a sample drawn from the core subset

def _sample_category_skills(category, count):
    """Sample `count` skills for `category`, biased toward a stable
    per-category 'core' subset so independently-sampled candidate and job
    skill sets overlap more than pure uniform sampling would produce."""
    pool = jobs_skills.get(category, [])
    if not pool or count <= 0:
        return []
    count = min(count, len(pool))

    core_size = max(1, int(len(pool) * CORE_SKILL_RATIO))
    core = pool[:core_size]
    periphery = pool[core_size:]

    core_take = min(len(core), int(round(count * CORE_SKILL_BIAS)))
    periphery_take = min(len(periphery), count - core_take)
    if core_take + periphery_take < count:
        # Periphery ran short (small pool) - backfill from remaining core.
        core_take = min(len(core), count - periphery_take)

    sampled = random.sample(core, core_take) + random.sample(periphery, periphery_take)
    random.shuffle(sampled)
    return sampled

# ── NEW: Build temporal career history ───────────────────────────────────

def build_career_history(category, current_year=2026):
    """
    Build a realistic temporal career history from progression templates.
    Returns: (current_title, total_years, skills, previous_companies, 
              previous_industries, current_company, salary, seniority)
    """
    template = career_progression_templates.get(category, [])
    if not template:
        # Fallback for categories without detailed templates
        return _build_fallback_career(category, current_year)

    # Decide how far up the ladder this candidate has climbed
    # Weighted toward mid-level (most realistic population distribution)
    max_rung = len(template) - 1
    progress_weights = []
    for i in range(max_rung + 1):
        # More people at junior/mid levels, fewer at senior/exec
        if i <= 1:
            progress_weights.append(30)
        elif i <= 3:
            progress_weights.append(25)
        elif i <= 4:
            progress_weights.append(15)
        else:
            progress_weights.append(5)

    top_rung = random.choices(range(max_rung + 1), weights=progress_weights, k=1)[0]

    # Build the career timeline
    career_path = template[:top_rung + 1]
    total_years = sum(years for _, years, _ in career_path)

    # Work backwards from current_year to assign dates
    timeline = []
    end_year = current_year
    for title, years, skills in reversed(career_path):
        start_year = end_year - years
        # Pick a random company for this role
        company = random.choice(top_300_unique)
        # Derive industry from company
        industry = company_industry_map.get(company, random.choice(category_industries.get(category, industries)))
        timeline.insert(0, {
            "title": title,
            "company": company,
            "industry": industry,
            "start_year": start_year,
            "end_year": end_year,
            "skills": skills
        })
        end_year = start_year

    current_role = timeline[-1]
    current_title = current_role["title"]
    current_company = current_role["company"]
    seniority = detect_seniority(current_title)

    # Collect all previous companies (excluding current)
    previous_companies = [t["company"] for t in timeline[:-1]]
    previous_industries = [t["industry"] for t in timeline[:-1]]

    # Aggregate skills FROM THE SAME VOCABULARY jobs draw from
    # (jobs_skills[category]) instead of the template's own per-stage tags.
    #
    # ROOT CAUSE FOUND: career_progression_templates' stage tags use
    # different, more specific wording than jobs_skills[category] - e.g.
    # "Spark" vs "Big Data Tools (Hadoop/Spark)", "Kafka" vs nothing at
    # all, "AWS" vs "Cloud Data Platforms (AWS/Azure/GCP)". match_engine's
    # skill_score_expr does an EXACT-STRING array_intersect (lowered/
    # trimmed, no fuzzy matching) between candidate.skills and
    # job.skills_needed - and job.skills_needed is always sampled from
    # jobs_skills[category] (see generate_job_post below). Verified only
    # ~30-50% of template tags exist verbatim in the pool, and several
    # (like "Kafka") never do at all - so skill_score was structurally
    # capped near zero for almost every application regardless of category
    # match, which alone was enough to keep overall_score below the
    # HIGH_PRIORITY threshold (85) no matter how well location/salary/
    # experience lined up. Sampling straight from jobs_skills[category]
    # here guarantees the two sides share a vocabulary that CAN overlap.
    category_skill_pool = jobs_skills.get(category, [])
    # A more senior candidate has picked up more of the category's skill
    # surface over their career - scale the sample size with how far up
    # the ladder they've climbed, capped at the pool size.
    base_count = max(6, num_skills_needed.get(category, 12))
    target_skill_count = min(len(category_skill_pool), base_count + min(len(timeline) - 1, 4))
    skills = _sample_category_skills(category, target_skill_count)

    # Compute salary from seniority + some randomness
    sal_range, _ = get_salary_range(seniority, salary_ranges_per_year)
    base_salary = random.randint(sal_range["min"], sal_range["max"])

    # Experience range string
    if total_years == 0:
        exp_range = "0 years"
    elif total_years <= 2:
        exp_range = f"{total_years} years"
    else:
        exp_range = f"{max(0, total_years - 2)}-{total_years} years"

    return {
        "current_title": current_title,
        "total_years": total_years,
        "experience_range": exp_range,
        "skills": skills,
        "previous_companies": previous_companies,
        "previous_industries": previous_industries,
        "current_company": current_company,
        "salary": base_salary,
        "seniority": seniority,
        "career_timeline": timeline  # For debugging/metrics, not stored in DB
    }


def _build_fallback_career(category, current_year=2026):
    """Fallback for categories without detailed progression templates."""
    titles = job_titles.get(category, ["Professional"])
    title = random.choice(titles)
    seniority = detect_seniority(title)

    # Get tight experience range
    exp_info = seniority_experience_years.get(seniority, {"min": 3, "max": 7, "mode": 5})
    total_years = random.randint(exp_info["min"], exp_info["max"])

    # Build fake timeline
    num_prev = max(0, total_years // 3)
    prev_comp = [random.choice(top_300_unique) for _ in range(num_prev)]
    prev_ind = [company_industry_map.get(c, random.choice(industries)) for c in prev_comp]

    curr_comp = random.choice(top_300_unique) if seniority != "Internship" else ""

    if total_years == 0:
        exp_range = "0 years"
    elif total_years <= 2:
        exp_range = f"{total_years} years"
    else:
        exp_range = f"{max(0, total_years - 2)}-{total_years} years"

    sal_range, _ = get_salary_range(seniority, salary_ranges_per_year)
    salary = random.randint(sal_range["min"], sal_range["max"])

    skills = _sample_category_skills(category, num_skills_needed.get(category, 10))

    return {
        "current_title": title,
        "total_years": total_years,
        "experience_range": exp_range,
        "skills": skills,
        "previous_companies": prev_comp,
        "previous_industries": prev_ind,
        "current_company": curr_comp,
        "salary": salary,
        "seniority": seniority,
        "career_timeline": []
    }


# ── NEW: Job post internal consistency helpers ───────────────────────────

def get_industry_for_job(category, company):
    """Derive industry from company (if known), else from category."""
    if company in company_industry_map:
        return company_industry_map[company]
    # Fallback: pick an industry that commonly hires this category
    valid_industries = category_industries.get(category, industries)
    return random.choice(valid_industries)


def get_location_by_industry(industry):
    """Pick location weighted by industry geography."""
    weights_dict = industry_location_weights.get(industry, industry_location_weights["default"])
    continent = random.choices(list(weights_dict.keys()), list(weights_dict.values()), k=1)[0]
    loc = random.choice(location[continent])
    city, country = [p.strip() for p in loc.split(",")]
    return continent, city, country


def apply_region_salary_multiplier(base_salary, continent):
    """Adjust salary for cost of living in the region."""
    multiplier = region_salary_multipliers.get(continent, 1.0)
    # Add some noise so it's not perfectly deterministic
    noise = random.uniform(0.9, 1.1)
    adjusted = int(base_salary * multiplier * noise)
    return max(15000, adjusted)  # Floor at 15k


# ── NEW: Application match score & probability ───────────────────────────

def compute_match_score(candidate_category, job_category, candidate_skills, job_skills):
    """
    Compute a match score between candidate and job.
    Used ONLY for deciding whether to create an application.
    Does NOT affect the database schema or Debezium in any way.

    HARD RULE: job_category equality is a PREREQUISITE for a high-priority
    match, not just a weighted contributor. jobs_skills (data_used.py) is
    a disjoint skill vocabulary per category, so any skill overlap across
    categories is coincidental, not a genuine match - it should never be
    enough on its own to produce a "high priority" application.

      - Different category → score capped low (0-20), regardless of any
        incidental skill-name overlap. application_probability() puts
        this permanently in the "low" tier (<50 → 1-5% chance).
      - Same category      → score is 100% skills overlap (Jaccard),
        0-100. Category equality already qualifies the pair; how strong
        a match it is within the field is entirely about skills now.

    Returns score 0-100.
    """
    cand_set = set(s.lower() for s in (candidate_skills or []))
    job_set = set(s.lower() for s in (job_skills or []))

    if candidate_category != job_category:
        # Cross-category: cap well under every tier above "low" so this
        # can never become a high-priority match no matter how the skill
        # names happen to line up.
        stray_overlap = len(cand_set & job_set)
        return min(20.0, stray_overlap * 5.0)

    if not cand_set or not job_set:
        return 0.0

    intersection = len(cand_set & job_set)
    union = len(cand_set | job_set)
    skill_score = (intersection / union * 100) if union > 0 else 0.0
    return round(skill_score, 1)


def application_probability(match_score):
    """
    Convert match score to application probability.
    Non-deterministic: high match = high probability, but not guaranteed.

    RECALIBRATED. The original bands (1-5% below 50, nothing above 8%
    until score>=50) assumed scores would commonly reach 50-100. In
    practice, compute_match_score() is a Jaccard skills overlap: two
    independent samples drawn from the same category's skill pool rarely
    overlap more than ~20-30%, so real scores cluster in 0-30 even for
    good same-category pairs (see the Score Distribution line in the
    generator's own progress log). Under the old bands that meant ~97% of
    ALL application attempts - matched or random - fell into the 1-5%
    floor, which collapsed total application volume to well under the
    candidate count despite application:insert having a much higher
    operation weight than candidate/job inserts. These bands are rescaled
    to the score range this scoring function actually produces, so the
    "higher score = higher probability, category match required for the
    top tier" signal still holds, but overall volume matches the intended
    "many applications per candidate" behavior.
    """
    if match_score >= 60:
        # Strong same-category skill overlap (rare) → 75-95% chance
        return random.uniform(0.75, 0.95)
    elif match_score >= 30:
        # Solid same-category skill overlap → 45-70% chance
        return random.uniform(0.45, 0.70)
    elif match_score >= 10:
        # Some overlap, or a lucky cross-category stray match → 22-42% chance
        return random.uniform(0.22, 0.42)
    else:
        # Little/no overlap (the common case, incl. all cross-category
        # applications with no stray overlap) → still happens sometimes,
        # modeling realistic "speculative" applications
        return random.uniform(0.10, 0.20)


# ── NEW: Realistic application date with deadline decay ─────────────────

def generate_application_date(post_date, deadline):
    """
    Generate realistic application date with:
    - Weighted distribution (0-2d: 35%, 3-7d: 35%, 8-14d: 20%, 15+d: 10%)
    - Deadline decay: probability drops as deadline approaches
    """
    job_lifespan_days = (deadline - post_date).days
    if job_lifespan_days <= 0:
        return post_date

    # Pick a bucket from distribution
    buckets = APPLICATION_DATE_DISTRIBUTION
    chosen = random.choices(buckets, weights=[w for _, _, w in buckets], k=1)[0]
    min_day, max_day, _ = chosen

    # Apply within bucket
    upper_bound = min(max_day, job_lifespan_days)
    if min_day >= upper_bound:
        day_offset = max(0, upper_bound - 1)  # or just 0
    else:
        day_offset = random.randint(min_day, upper_bound)

    # Deadline decay: if close to deadline, chance to apply earlier
    days_remaining = job_lifespan_days - day_offset
    if days_remaining < 3:
        # Very close to deadline — 60% chance to shift earlier
        if random.random() < 0.60:
            day_offset = max(0, day_offset - random.randint(1, 5))

    applied_date = post_date + datetime.timedelta(days=day_offset)
    return min(applied_date, deadline)


# ═══════════════════════════════════════════════════════════════════════
#  GENERATORS  (FULLY REWRITTEN FOR INTERNAL CONSISTENCY)
# ═══════════════════════════════════════════════════════════════════════

def generate_job_post():
    """
    Generate a job post with full internal consistency:
    Company → Industry → Category → Title → Seniority → Experience → Skills → Salary → Location
    """
    post_id = str(uuid.uuid4())
    app_method = random.choice(apply_methods)

    # Post date and deadline
    start, end = datetime.datetime(2025, 1, 1), datetime.datetime(2028, 12, 31, 23, 59, 59)
    total_seconds = int((end - start).total_seconds())
    post_dt = start + datetime.timedelta(seconds=np.random.randint(total_seconds))
    deadline_dt = post_dt + datetime.timedelta(days=random.randint(15, 60))

    # 1. Pick weighted category
    category = choose_weighted_category()

    # 2. Pick company
    company = random.choice(top_300_unique)

    # 3. Derive industry from company (or category fallback)
    industry = get_industry_for_job(category, company)

    # 4. Pick title from category
    title = random.choice(job_titles[category])
    seniority = detect_seniority(title)

    # 5. Derive experience requirement from seniority (tight range)
    exp_info = seniority_experience_years.get(seniority, {"min": 3, "max": 7, "mode": 5})
    req_years = random.randint(exp_info["min"], exp_info["max"])
    if req_years == 0:
        years_exp = "0 years"
    elif req_years <= 2:
        years_exp = f"{req_years} years"
    else:
        years_exp = f"{max(0, req_years - 2)}-{req_years} years"

    # 6. Pick skills from category (core-biased - see _sample_category_skills
    # for why: this is what gives same-category candidate/job pairs a
    # structurally higher skill overlap instead of near-zero expected overlap)
    num_skills = num_skills_needed.get(category, 12)
    skills = _sample_category_skills(category, num_skills)

    # 7. Compute salary from seniority, then adjust for region
    sal_range, matched_key = get_salary_range(seniority, salary_ranges_per_year)
    base_salary = random.randint(sal_range["min"], sal_range["max"])

    # 8. Pick location weighted by industry geography
    continent, city, country = get_location_by_industry(industry)

    # 9. Apply region salary multiplier
    salary = apply_region_salary_multiplier(base_salary, continent)

    # 10. Derive language from country
    language = random.choice(country_languages.get(country, ["English"]))

    # Other fields
    min_c, max_c = num_certifications_needed.get(category, (0, 2))
    all_certs = job_certifications.get(category, [])
    if len(all_certs) >= min_c:
        certs = random.sample(all_certs, random.randint(min_c, min(max_c, len(all_certs))))
    else:
        certs = all_certs

    platform = random.choice(job_posting_platforms)
    allowed_types = employment_rules.get(seniority, ["Full-time", "Contract"])
    emp_type = random.choice(allowed_types)
    mode = random.choice(work_modes)
    det_level_exp = "Internship" if emp_type == "Internship" else seniority

    return {
        "post_id": post_id, "company": company, "category": category,
        "job_title": title, "application_method": app_method,
        "post_date": post_dt, "deadline": deadline_dt,
        "certifications": certs, "skills_needed": skills,
        "platform": platform, "employment_type": emp_type,
        "city": city, "country": country, "language": language,
        "work_mode": mode, "industry": industry,
        "detailed_experience_level": det_level_exp,
        "salary": salary, "years_experience": years_exp,
        "status": "open", "lifecycle_stage": 0
    }


def generate_candidate_info():
    """
    Generate a candidate with full internal consistency and temporal career history.
    Career profile → Category → Seniority → Title → Experience → Skills → Salary → Companies
    """
    chosen_gender = random.choice(["male", "female"])
    candidate_id = str(uuid.uuid4())
    name = random.choice(male_names) if chosen_gender == "male" else random.choice(female_names)
    nat = random.choice(nationalities)
    gpa = random.choice(Gpa)

    # 1. Pick weighted category
    category = choose_weighted_category()

    # 2. Build temporal career history from templates
    career = build_career_history(category, current_year=2026)

    # 3. Derive all fields from career history
    current_title = career["current_title"]
    seniority = career["seniority"]
    years_exp = career["experience_range"]
    total_years = career["total_years"]
    skills = career["skills"]
    previous_companies = career["previous_companies"]
    previous_industries = career["previous_industries"]
    current_company = career["current_company"]
    salary = career["salary"]

    # 4. Compute graduation year and DOB from career start
    # Assume started career at age 22 (graduation)
    career_start_year = 2026 - total_years
    grad_year = career_start_year
    dob = grad_year - 22

    # 5. Education based on age
    age = 2026 - dob
    edu = get_education_by_age_group(age)

    # 6. Certifications based on seniority
    min_c, max_c = num_certifications_needed.get(category, (0, 2))
    all_certs = job_certifications.get(category, [])
    if len(all_certs) >= min_c:
        certs = random.sample(all_certs, random.randint(min_c, min(max_c, len(all_certs))))
    else:
        certs = all_certs

    # 7. Location (independent, but weighted by where the industry clusters)
    # Pick a location that makes sense for the candidate's current industry
    if previous_industries:
        primary_industry = previous_industries[-1] if previous_industries else "Software & Internet"
    else:
        primary_industry = company_industry_map.get(current_company, "Software & Internet")

    continent, city, country = get_location_by_industry(primary_industry)
    loc = f"{city}, {country}"

    return {
        "candidate_id": candidate_id, "name": name, "gender": chosen_gender,
        "date_of_birth": dob, "nationality": nat, "location": loc,
        "graduation_year": grad_year, "GPA": gpa, "education_level": edu,
        "job_category": category, "current_title": current_title,
        "seniority": seniority, "experience_range": years_exp,
        "current_company": current_company, "previous_companies": previous_companies,
        "previous_industries": previous_industries, "skills": skills,
        "certifications": certs, "current_salary": salary,
        "is_active": True
    }

# ═══════════════════════════════════════════════════════════════════════
#  DATABASE OPERATIONS (UNCHANGED — Debezium-safe)
# ═══════════════════════════════════════════════════════════════════════

def insert_job_post(conn, job):
    cur = conn.cursor()
    now = datetime.datetime.now()

    query = """
        INSERT INTO job_posts (
            post_id, company, category, job_title, application_method, post_date, deadline,
            certifications, skills_needed, platform, employment_type,
            city, country, language, work_mode, industry,
            detailed_experience_level, salary, years_experience, status, lifecycle_stage,
            created_at, updated_at, version
        ) VALUES (
            %(post_id)s, %(company)s, %(category)s, %(job_title)s, %(application_method)s,
            %(post_date)s, %(deadline)s, %(certifications)s, %(skills_needed)s,
            %(platform)s, %(employment_type)s, %(city)s, %(country)s, %(language)s,
            %(work_mode)s, %(industry)s, %(detailed_experience_level)s,
            %(salary)s, %(years_experience)s, %(status)s, %(lifecycle_stage)s,
            %(created_at)s, %(updated_at)s, %(version)s
        )
    """

    job["created_at"] = now
    job["updated_at"] = now
    job["version"] = 1

    cur.execute(query, job)
    conn.commit()
    cur.close()
    return job

def insert_candidate(conn, cand):
    cur = conn.cursor()
    now = datetime.datetime.now()

    query = """
        INSERT INTO candidate_info (
            candidate_id, name, gender, date_of_birth, nationality, location,
            graduation_year, GPA, education_level, job_category,
            current_title, seniority, experience_range, current_company,
            previous_companies, previous_industries, skills, certifications,
            current_salary, is_active,
            created_at, updated_at, version
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::text[],%s::text[],%s::text[],%s::text[],%s,%s,%s,%s,%s)
    """

    values = (
        cand["candidate_id"], cand["name"], cand["gender"], cand["date_of_birth"],
        cand["nationality"], cand["location"], cand["graduation_year"], cand["GPA"],
        cand["education_level"], cand["job_category"], cand["current_title"],
        cand["seniority"], cand["experience_range"], cand["current_company"],
        cand["previous_companies"], cand["previous_industries"], cand["skills"],
        cand["certifications"], cand["current_salary"], cand["is_active"],
        now, now, 1
    )

    cur.execute(query, values)
    conn.commit()
    cur.close()

    cand["created_at"] = now
    cand["updated_at"] = now
    cand["version"] = 1
    return cand

def _insert_application_db(conn, app):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO applications (
            application_id, candidate_id, post_id, status, lifecycle_stage,
            applied_date, last_updated, created_at, updated_at, version
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        app["application_id"], app["candidate_id"], app["post_id"],
        app["status"], app["lifecycle_stage"], app["applied_date"],
        app["last_updated"], app["created_at"], app["updated_at"], app["version"]
    ))
    conn.commit()
    cur.close()

# ═══════════════════════════════════════════════════════════════════════
#  LIFECYCLE UPDATE LOGIC (MODIFIED: category tracking + viewing)
# ═══════════════════════════════════════════════════════════════════════

class LifecycleEngine:
    
    
    
    JOB_TRANSITIONS = {
        0: ("salary_change", 1),
        1: ("deadline_change", 2),
        2: ("skills_change", 3),
        3: ("work_mode_change", 4),
        4: ("close_job", 5),
    }

    # MODIFIED: Realistic viewing behavior
    # 72% viewed, 28% never viewed
    # Delays: 1-3d (50%), 3-7d (30%), 7-14d (15%), 14+d (5%)
    # Stage 1..3 entries are only used when app_status_transition_rate > 0
    # (full lifecycle chain); default behavior stops at "viewed".
    APP_TRANSITIONS = {
        0: ("viewed", 1),
        1: ("interview", 2),
        2: ("offer", 3),
    }

    def __init__(self, conn):
        self.conn = conn
        self.active_candidates = {}
        self.inactive_candidates = set()
        self.active_jobs = {}
        self.closed_jobs = set()
        self.active_applications = {}
        self.final_applications = set()

        # NEW: Category tracking for intelligent matching
        self.candidate_categories = {}   # candidate_id -> job_category
        self.candidate_skills = {}       # candidate_id -> skills list
        self.job_categories = {}         # post_id -> category
        self.job_skills = {}             # post_id -> skills list

        # NEW: O(1) category -> {id, ...} reverse indices, populated ONLY
        # for entities that have finished warm-up (i.e. are in
        # active_candidates / active_jobs). This is what
        # _pick_job_for_category()/_pick_candidate_for_category() draw
        # from, so a category-matched application can never reference an
        # entity that hasn't had time to propagate through
        # Debezium -> Kafka -> the match engine's dimension tables yet -
        # closing the same CDC race the warm-up delay targets, but for the
        # majority (category-matched) application path specifically.
        self.candidates_by_category = defaultdict(set)
        self.jobs_by_category = defaultdict(set)

        # NEW: Warm-up tracking — entities must age 60s before use in applications
        self.warming_candidates = {}   # candidate_id -> warm_until_timestamp
        self.warming_jobs = {}         # post_id -> warm_until_timestamp
        WARM_UP_SECONDS = 60

        self._load_state()
    # ADD THIS METHOD HERE — inside the class, same indent as __init__
    def _promote_warmed_entities(self):
        """Move candidates/jobs from warming pool to active pool after 60s delay.

        This is the ONLY place candidates_by_category/jobs_by_category get
        populated - i.e. an id becomes selectable by
        _pick_candidate_for_category()/_pick_job_for_category() at exactly
        the same moment it becomes selectable via active_candidates/
        active_jobs. There is no path for a still-warming entity to be
        picked by the category-matched application logic.
        """
        now = datetime.datetime.now()

        ready_candidates = [
            cid for cid, warm_until in self.warming_candidates.items()
            if now >= warm_until
        ]
        for cid in ready_candidates:
            self.active_candidates[cid] = {
                "profile_updates": 0,
                "company_changes": 0,
                "location_changes": 0,
                "availability": "available",
                "is_active": True
            }
            self.candidates_by_category[self.candidate_categories.get(cid)].add(cid)
            del self.warming_candidates[cid]
        
        ready_jobs = [
            pid for pid, warm_until in self.warming_jobs.items()
            if now >= warm_until
        ]
        for pid in ready_jobs:
            self.active_jobs[pid] = 0
            self.jobs_by_category[self.job_categories.get(pid)].add(pid)
            del self.warming_jobs[pid]

    def _load_state(self):
        """Hydrate state from existing DB records"""
        cur = self.conn.cursor()

        cur.execute("SELECT candidate_id, is_active, job_category, skills FROM candidate_info")
        for cid, active, job_category, skills in cur.fetchall():
            if active:
                self.active_candidates[cid] = {
                    "profile_updates": 0,
                    "company_changes": 0,
                    "location_changes": 0,
                    "availability": "available",
                    "is_active": True
                }
                self.candidate_categories[cid] = job_category
                self.candidate_skills[cid] = skills or []
                self.candidates_by_category[job_category].add(cid)
            else:
                self.inactive_candidates.add(cid)

        cur.execute("SELECT post_id, status, lifecycle_stage, category, skills_needed FROM job_posts")
        for pid, status, stage, category, skills in cur.fetchall():
            if status == 'open':
                self.active_jobs[pid] = stage
                self.job_categories[pid] = category
                self.job_skills[pid] = skills or []
                self.jobs_by_category[category].add(pid)
            else:
                self.closed_jobs.add(pid)

        cur.execute("SELECT application_id, status, lifecycle_stage FROM applications WHERE final_outcome IS NULL")
        for aid, status, stage in cur.fetchall():
            self.active_applications[aid] = stage

        cur.execute("SELECT application_id FROM applications WHERE final_outcome IS NOT NULL")
        for (aid,) in cur.fetchall():
            self.final_applications.add(aid)

        cur.close()
        print(f"🔋 State loaded: {len(self.active_candidates)} candidates, "
              f"{len(self.active_jobs)} jobs, {len(self.active_applications)} active apps")

    # ── Candidate Lifecycle ───────────────────────────────────────────
    
    def update_candidate(self):
        if not self.active_candidates:
            return None

        cid = random.choice(list(self.active_candidates.keys()))
        state = self.active_candidates[cid]
        cur = self.conn.cursor()
        now = datetime.datetime.now()
        event_ts = now.isoformat()

        cur.execute("""
            SELECT candidate_id, name, gender, date_of_birth, nationality, location,
                graduation_year, GPA, education_level, job_category,
                current_title, seniority, experience_range, current_company,
                previous_companies, previous_industries, skills, certifications,
                current_salary, is_active, created_at, updated_at, version
            FROM candidate_info WHERE candidate_id=%s
        """, (cid,))
        cols = [d[0] for d in cur.description]
        before = dict(zip(cols, cur.fetchone()))

        roll = random.random()
        desc = ""
        new_version = before["version"]

        if roll < 0.80:
            action = random.choice(["new_company", "left_job", "relocated"])
            state["profile_updates"] += 1

            if action == "new_company":
                new_comp = random.choice(top_300_unique)
                cur.execute("""
                    UPDATE candidate_info 
                    SET current_company=%s, updated_at=%s, version=version+1
                    WHERE candidate_id=%s
                    RETURNING version
                """, (new_comp, now, cid))
                state["company_changes"] += 1
                state["availability"] = "employed"
                desc = f"new company → {new_comp}"

            elif action == "left_job":
                cur.execute("""
                    UPDATE candidate_info 
                    SET current_company=%s, updated_at=%s, version=version+1
                    WHERE candidate_id=%s
                    RETURNING version
                """, ("", now, cid))
                state["company_changes"] += 1
                state["availability"] = "available"
                desc = "left job"

            else:
                continent = random.choice(list(location.keys()))
                new_loc = random.choice(location[continent])
                cur.execute("""
                    UPDATE candidate_info 
                    SET location=%s, updated_at=%s, version=version+1
                    WHERE candidate_id=%s
                    RETURNING version
                """, (new_loc, now, cid))
                state["location_changes"] += 1
                desc = f"relocated → {new_loc}"

            new_version = cur.fetchone()[0]
            desc += f" (v{new_version})"

        elif roll < 0.90:
            cur.execute("""
                UPDATE candidate_info 
                SET is_active=FALSE, updated_at=%s, version=version+1
                WHERE candidate_id=%s
                RETURNING version
            """, (now, cid))
            new_version = cur.fetchone()[0]
            state["is_active"] = False
            state["availability"] = "inactive"
            del self.active_candidates[cid]
            self.inactive_candidates.add(cid)
            self.candidates_by_category[self.candidate_categories.get(cid)].discard(cid)
            self.candidate_categories.pop(cid, None)
            self.candidate_skills.pop(cid, None)
            desc = f"deactivated (v{new_version})"

        else:
            desc = "no-op (deletion slot)"

        cur.execute("""
            SELECT candidate_id, name, gender, date_of_birth, nationality, location,
                graduation_year, GPA, education_level, job_category,
                current_title, seniority, experience_range, current_company,
                previous_companies, previous_industries, skills, certifications,
                current_salary, is_active, created_at, updated_at, version
            FROM candidate_info WHERE candidate_id=%s
        """, (cid,))
        after = dict(zip(cols, cur.fetchone()))

        self.conn.commit()
        cur.close()

        return {
            "entity": "candidate",
            "entity_id": cid,
            "operation": "UPDATE",
            "event_timestamp": event_ts,
            "version": new_version,
            "before": before,
            "after": after,
            "action": desc,
        }

    def delete_candidate(self):
        pool = list(self.inactive_candidates)
        if not pool:
            return None

        cid = random.choice(pool)
        cur = self.conn.cursor()
        now = datetime.datetime.now()
        event_ts = now.isoformat()

        cur.execute("""
            SELECT candidate_id, name, gender, date_of_birth, nationality, location,
                graduation_year, GPA, education_level, job_category,
                current_title, seniority, experience_range, current_company,
                previous_companies, previous_industries, skills, certifications,
                current_salary, is_active, created_at, updated_at, version
            FROM candidate_info WHERE candidate_id=%s
        """, (cid,))
        cols = [d[0] for d in cur.description]
        before = dict(zip(cols, cur.fetchone()))

        cur.execute("""
            UPDATE candidate_info 
            SET is_active=FALSE, 
                name='[DELETED]', 
                current_company='[DELETED]', 
                location='[DELETED]',
                skills='{}'::text[],
                certifications='{}'::text[],
                previous_companies='{}'::text[],
                previous_industries='{}'::text[],
                updated_at=%s, 
                version=version+1
            WHERE candidate_id=%s
            RETURNING version
        """, (now, cid))

        row = cur.fetchone()
        new_version = row[0] if row else before["version"] + 1

        cur.execute("""
            SELECT candidate_id, name, gender, date_of_birth, nationality, location,
                graduation_year, GPA, education_level, job_category,
                current_title, seniority, experience_range, current_company,
                previous_companies, previous_industries, skills, certifications,
                current_salary, is_active, created_at, updated_at, version
            FROM candidate_info WHERE candidate_id=%s
        """, (cid,))
        after = dict(zip(cols, cur.fetchone()))

        self.conn.commit()
        cur.close()

        self.inactive_candidates.discard(cid)
        self.candidates_by_category[self.candidate_categories.get(cid)].discard(cid)
        self.candidate_categories.pop(cid, None)
        self.candidate_skills.pop(cid, None)

        return {
            "entity": "candidate",
            "entity_id": cid,
            "operation": "DELETE",
            "event_timestamp": event_ts,
            "version": new_version,
            "before": before,
            "after": after,
            "action": f"soft-deleted (v{new_version})"
        }

    # ── Job Lifecycle ───────────────────────────────────────────────────
    def update_job(self):
        if not self.active_jobs:
            return None

        pid = random.choice(list(self.active_jobs.keys()))
        stage = self.active_jobs[pid]
        cur = self.conn.cursor()
        now = datetime.datetime.now()
        event_ts = now.isoformat()

        cur.execute("""
            SELECT post_id, company, category, job_title, application_method, post_date, deadline,
                certifications, skills_needed, platform, employment_type,
                city, country, language, work_mode, industry,
                detailed_experience_level, salary, years_experience, status, lifecycle_stage,
                created_at, updated_at, version
            FROM job_posts WHERE post_id=%s
        """, (pid,))
        cols = [d[0] for d in cur.description]
        before = dict(zip(cols, cur.fetchone()))

        roll = random.random()
        desc = ""
        new_version = before["version"]

        close_prob = 0.15 + float(CONFIG.get("job_close_rate", 0.0))
        close_prob = max(0.0, min(0.9, close_prob))

        if roll < (1.0 - close_prob) and stage < 5:
            transition_name, next_stage = self.JOB_TRANSITIONS[stage]

            if transition_name == "salary_change":
                old_sal = before["salary"]
                delta = random.choice([-1, 1]) * random.randint(int(old_sal * 0.05), int(old_sal * 0.20))
                new_sal = max(10000, old_sal + delta)
                cur.execute("""
                    UPDATE job_posts 
                    SET salary=%s, lifecycle_stage=%s, updated_at=%s, version=version+1
                    WHERE post_id=%s
                    RETURNING version
                """, (new_sal, next_stage, now, pid))

            elif transition_name == "deadline_change":
                old_dl = before["deadline"]
                delta_days = random.randint(-7, 21)
                new_dl = old_dl + datetime.timedelta(days=delta_days)
                cur.execute("""
                    UPDATE job_posts 
                    SET deadline=%s, lifecycle_stage=%s, updated_at=%s, version=version+1
                    WHERE post_id=%s
                    RETURNING version
                """, (new_dl, next_stage, now, pid))

            elif transition_name == "skills_change":
                old_skills = before["skills_needed"]
                cat = before["category"]
                all_skills = jobs_skills.get(cat, [])
                available = [s for s in all_skills if s not in old_skills]
                if available:
                    new_skills = old_skills + random.sample(available, min(2, len(available)))
                    cur.execute("""
                        UPDATE job_posts 
                        SET skills_needed=%s, lifecycle_stage=%s, updated_at=%s, version=version+1
                        WHERE post_id=%s
                        RETURNING version
                    """, (new_skills, next_stage, now, pid))
                else:
                    cur.execute("""
                        UPDATE job_posts 
                        SET lifecycle_stage=%s, updated_at=%s, version=version+1
                        WHERE post_id=%s
                        RETURNING version
                    """, (next_stage, now, pid))

            elif transition_name == "work_mode_change":
                old_mode = before["work_mode"]
                new_mode = "Hybrid" if old_mode == "Remote" else random.choice([m for m in work_modes if m != old_mode])
                cur.execute("""
                    UPDATE job_posts 
                    SET work_mode=%s, lifecycle_stage=%s, updated_at=%s, version=version+1
                    WHERE post_id=%s
                    RETURNING version
                """, (new_mode, next_stage, now, pid))

            else:
                cur.close()
                return {"entity": "job_post", "entity_id": pid, "operation": "UPDATE",
                        "event_timestamp": event_ts, "version": before["version"],
                        "before": before, "after": before, "action": f"unknown transition: {transition_name}"}

            version_row = cur.fetchone()
            if version_row is None:
                cur.close()
                self.active_jobs.pop(pid, None)
                self.jobs_by_category[self.job_categories.get(pid)].discard(pid)
                self.job_categories.pop(pid, None)
                self.job_skills.pop(pid, None)
                return {"entity": "job_post", "entity_id": pid, "operation": "UPDATE",
                        "event_timestamp": event_ts, "version": before["version"],
                        "before": before, "after": before, "action": "job vanished"}

            new_version = version_row[0]
            self.active_jobs[pid] = next_stage
            desc = f"{transition_name} (v{new_version})"

        else:
            cur.execute("""
                UPDATE job_posts 
                SET status='closed', lifecycle_stage=5, updated_at=%s, version=version+1
                WHERE post_id=%s
                RETURNING version
            """, (now, pid))

            version_row = cur.fetchone()
            if version_row is None:
                cur.close()
                self.active_jobs.pop(pid, None)
                self.jobs_by_category[self.job_categories.get(pid)].discard(pid)
                self.job_categories.pop(pid, None)
                self.job_skills.pop(pid, None)
                return {"entity": "job_post", "entity_id": pid, "operation": "UPDATE",
                        "event_timestamp": event_ts, "version": before["version"],
                        "before": before, "after": before, "action": "job vanished on close"}

            new_version = version_row[0]
            del self.active_jobs[pid]
            self.closed_jobs.add(pid)
            self.jobs_by_category[self.job_categories.get(pid)].discard(pid)
            self.job_categories.pop(pid, None)
            self.job_skills.pop(pid, None)
            desc = f"closed (v{new_version})"

        cur.execute("""
            SELECT post_id, company, category, job_title, application_method, post_date, deadline,
                certifications, skills_needed, platform, employment_type,
                city, country, language, work_mode, industry,
                detailed_experience_level, salary, years_experience, status, lifecycle_stage,
                created_at, updated_at, version
            FROM job_posts WHERE post_id=%s
        """, (pid,))
        after = dict(zip(cols, cur.fetchone()))

        self.conn.commit()
        cur.close()
        return {
            "entity": "job_post",
            "entity_id": pid,
            "operation": "UPDATE",
            "event_timestamp": event_ts,
            "version": new_version,
            "before": before,
            "after": after,
            "action": desc
        }

    def reopen_job(self):
        """Reopen a previously closed job (status -> 'open', stage reset to 0, version+1).

        The reopened job re-enters the 60s warming pool (NOT active_jobs), so
        it cannot be selected for new applications until Debezium has had time
        to publish the CDC event - same discipline as _promote_warmed_entities.
        """
        if not self.closed_jobs:
            return None
        pid = random.choice(list(self.closed_jobs))
        cur = self.conn.cursor()
        now = datetime.datetime.now()
        event_ts = now.isoformat()

        cur.execute("""
            SELECT post_id, company, category, job_title, application_method, post_date, deadline,
                certifications, skills_needed, platform, employment_type,
                city, country, language, work_mode, industry,
                detailed_experience_level, salary, years_experience, status, lifecycle_stage,
                created_at, updated_at, version
            FROM job_posts WHERE post_id=%s
        """, (pid,))
        cols = [d[0] for d in cur.description]
        row = cur.fetchone()
        if row is None:
            cur.close()
            self.closed_jobs.discard(pid)
            return None
        before = dict(zip(cols, row))

        cur.execute("""
            UPDATE job_posts
            SET status='open', lifecycle_stage=0, updated_at=%s, version=version+1
            WHERE post_id=%s
            RETURNING version
        """, (now, pid))
        version_row = cur.fetchone()
        cur.close()
        if version_row is None:
            self.closed_jobs.discard(pid)
            return None

        new_version = version_row[0]
        self.closed_jobs.discard(pid)
        self.job_categories[pid] = before["category"]
        self.job_skills[pid] = before["skills_needed"] or []
        self.warming_jobs[pid] = now + datetime.timedelta(seconds=60)

        after = dict(before)
        after["status"] = "open"
        after["lifecycle_stage"] = 0
        after["version"] = new_version
        after["updated_at"] = now

        self.conn.commit()
        return {
            "entity": "job_post",
            "entity_id": pid,
            "operation": "UPDATE",
            "event_timestamp": event_ts,
            "version": new_version,
            "before": before,
            "after": after,
            "action": f"reopened (v{new_version})"
        }

    def delete_job(self):
        pool = list(self.closed_jobs) or list(self.active_jobs.keys())
        if not pool:
            return None
        pid = random.choice(pool)
        cur = self.conn.cursor()
        now = datetime.datetime.now()
        event_ts = now.isoformat()

        cur.execute("""
            SELECT post_id, company, category, job_title, application_method, post_date, deadline,
                certifications, skills_needed, platform, employment_type,
                city, country, language, work_mode, industry,
                detailed_experience_level, salary, years_experience, status, lifecycle_stage,
                created_at, updated_at, version
            FROM job_posts WHERE post_id=%s
        """, (pid,))
        cols = [d[0] for d in cur.description]
        row = cur.fetchone()
        if row is None:
            cur.close()
            self.active_jobs.pop(pid, None)
            self.closed_jobs.discard(pid)
            self.jobs_by_category[self.job_categories.get(pid)].discard(pid)
            self.job_categories.pop(pid, None)
            self.job_skills.pop(pid, None)
            return None
        before = dict(zip(cols, row))

        cur.execute("DELETE FROM job_posts WHERE post_id=%s RETURNING version", (pid,))
        version_row = cur.fetchone()
        new_version = version_row[0] if version_row else before["version"] + 1

        self.conn.commit()
        cur.close()

        self.active_jobs.pop(pid, None)
        self.closed_jobs.discard(pid)
        self.jobs_by_category[self.job_categories.get(pid)].discard(pid)
        self.job_categories.pop(pid, None)
        self.job_skills.pop(pid, None)

        return {
            "entity": "job_post",
            "entity_id": pid,
            "operation": "DELETE",
            "event_timestamp": event_ts,
            "version": new_version,
            "before": before,
            "after": None,
            "action": f"deleted (v{new_version})"
        }

    # ── Application Lifecycle (MODIFIED: intelligent matching + realistic dates) ─

    def _pick_candidate_for_category(self, target_category):
        """Pick a random WARM candidate whose job_category matches
        target_category. Draws from candidates_by_category (populated only
        at promotion out of warm-up), not from the broader
        candidate_categories map, so this can never return an id that
        hasn't propagated through Debezium -> Kafka yet."""
        matching = self.candidates_by_category.get(target_category)
        if matching:
            return random.choice(list(matching))
        return None

    def _pick_job_for_category(self, target_category):
        """Pick a random OPEN, WARM job whose category matches
        target_category. Draws from jobs_by_category (populated only at
        promotion out of warm-up), not from the broader job_categories
        map, so this can never return an id that hasn't propagated through
        Debezium -> Kafka yet. This is what guarantees that every
        category-matched application (70% of applications) references a
        candidate_id/post_id already confirmed sent to Kafka - closing the
        CDC race that otherwise shows up downstream as unresolved_rows."""
        matching = self.jobs_by_category.get(target_category)
        if matching:
            return random.choice(list(matching))
        return None

    def insert_application(self):
        """
        Create an application with intelligent matching:
        - Compute match score (category + skills overlap)
        - Use score to determine application probability
        - 70% of attempts are category-matched, 30% are random cross-category
        - Realistic application dates with deadline decay
        """
        if not self.active_candidates or not self.active_jobs:
            return None

        app_id = str(uuid.uuid4())

        # Decide: category-matched (70%) or random (30%)
        use_matched = random.random() < 0.70

        if use_matched:
            # Category-matched: pick candidate, then find job in same category
            cid = random.choice(list(self.active_candidates.keys()))
            cand_category = self.candidate_categories.get(cid)
            cand_skills = self.candidate_skills.get(cid, [])

            if cand_category:
                pid = self._pick_job_for_category(cand_category)
                if pid is None:
                    # Fallback: no matching job available
                    pid = random.choice(list(self.active_jobs.keys()))
                    match_type = "matched-cand-fallback"
                else:
                    match_type = "category-matched"
            else:
                pid = random.choice(list(self.active_jobs.keys()))
                match_type = "no-category"
        else:
            # Random cross-category application (30%)
            cid = random.choice(list(self.active_candidates.keys()))
            pid = random.choice(list(self.active_jobs.keys()))
            match_type = "random"
            cand_category = self.candidate_categories.get(cid)
            cand_skills = self.candidate_skills.get(cid, [])

        # Get job info for match score
        job_category = self.job_categories.get(pid, "")
        job_skills = self.job_skills.get(pid, [])

        # Compute match score
        match_score = compute_match_score(cand_category, job_category, cand_skills, job_skills)

        # Determine if application actually happens (probability-based)
        prob = application_probability(match_score)
        if random.random() > prob:
            # Application rejected by probability — skip this insert
            # But we still return a result for logging
            return {
                "entity": "application",
                "entity_id": f"skipped-{app_id[:8]}",
                "operation": "INSERT",
                "event_timestamp": datetime.datetime.now().isoformat(),
                "version": 0,
                "before": None,
                "after": None,
                "action": f"SKIPPED (score={match_score}, prob={prob:.1%}) [{match_type}]"
            }

        # Get post date and deadline for realistic application date
        cur = self.conn.cursor()
        cur.execute("SELECT post_date, deadline FROM job_posts WHERE post_id = %s", (pid,))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        post_date, deadline = row

        # Generate realistic application date
        applied_date = generate_application_date(post_date, deadline)

        now = datetime.datetime.now()

        app = {
            "application_id": app_id,
            "candidate_id": cid,
            "post_id": pid,
            "status": "applied",
            "lifecycle_stage": 0,
            "applied_date": applied_date,
            "last_updated": applied_date,
            "created_at": now,
            "updated_at": now,
            "version": 1
        }

        _insert_application_db(self.conn, app)
        self.active_applications[app_id] = 0

        return {
            "entity": "application",
            "entity_id": app_id,
            "operation": "INSERT",
            "event_timestamp": now.isoformat(),
            "version": 1,
            "before": None,
            "after": app,
            "action": f"applied {applied_date.strftime('%Y-%m-%d')} score={match_score} [{match_type}]"
        }

    def update_application(self):
        """
        MODIFIED: Realistic viewing behavior.
        - 72% of applications eventually get viewed
        - Viewing delays follow realistic distribution
        """
        if not self.active_applications:
            return None

        aid = random.choice(list(self.active_applications.keys()))
        stage = self.active_applications[aid]
        cur = self.conn.cursor()

        rate = float(CONFIG.get("app_status_transition_rate", 0.0))
        # Default behavior (rate == 0): only stage-0 applications transition
        # (to "viewed"). With rate > 0 the FULL chain is enabled:
        # viewed(1) -> interview(2) -> offer(3) -> accepted|rejected(4).
        if stage != 0 and rate <= 0:
            return None
        if stage not in (0, 1, 2, 3):
            return None

        # Later stages advance with probability = rate per generator cycle.
        if stage > 0 and random.random() > rate:
            return None

        # Check if this application gets viewed (72% probability)
        if stage == 0 and random.random() > VIEW_PROBABILITY:
            # Never viewed — mark as "ignored" but keep in active_applications
            # so it doesn't get processed again
            self.active_applications[aid] = -1  # Special: ignored
            cur.close()
            return {
                "entity": "application",
                "entity_id": aid,
                "operation": "UPDATE",
                "event_timestamp": datetime.datetime.now().isoformat(),
                "version": 1,
                "before": None,
                "after": None,
                "action": "→ never-viewed (ignored)"
            }

        transition_name, next_stage = self.APP_TRANSITIONS[stage]

        # On the final stage, stop chain-walking and decide the outcome that
        # stamps final_outcome (accepted 30% / rejected 70%).
        if stage == 3 and rate > 0:
            transition_name = "accepted" if random.random() < 0.3 else "rejected"
            next_stage = 4

        cur.execute("""
            SELECT application_id, candidate_id, post_id, status, lifecycle_stage,
                applied_date, last_updated, final_outcome, created_at, updated_at, version
            FROM applications WHERE application_id=%s
        """, (aid,))
        cols = [d[0] for d in cur.description]
        before = dict(zip(cols, cur.fetchone()))

        base_date = before["applied_date"]

        # Realistic viewing delay distribution
        delay_days = pick_from_distribution(VIEW_DELAY_DISTRIBUTION)
        now = base_date + datetime.timedelta(days=delay_days)
        event_ts = datetime.datetime.now().isoformat()

        cur.execute("""
            UPDATE applications 
            SET status=%s, lifecycle_stage=%s, last_updated=%s, updated_at=%s, version=version+1
            WHERE application_id=%s
            RETURNING version
        """, (transition_name, next_stage, now, now, aid))

        version_row = cur.fetchone()
        if version_row is None:
            cur.close()
            self.active_applications.pop(aid, None)
            return {"entity": "application", "entity_id": aid, "operation": "UPDATE",
                    "event_timestamp": event_ts, "version": before["version"],
                    "before": before, "after": before, "action": "application vanished"}

        new_version = version_row[0]
        self.active_applications[aid] = next_stage

        if rate > 0 and next_stage == 4:
            cur.execute("""
                UPDATE applications
                SET final_outcome=%s
                WHERE application_id=%s
            """, (transition_name, aid))
            self.active_applications.pop(aid, None)
            self.final_applications.add(aid)

        cur.execute("""
            SELECT application_id, candidate_id, post_id, status, lifecycle_stage,
                applied_date, last_updated, final_outcome, created_at, updated_at, version
            FROM applications WHERE application_id=%s
        """, (aid,))
        after = dict(zip(cols, cur.fetchone()))

        self.conn.commit()
        cur.close()
        return {
            "entity": "application",
            "entity_id": aid,
            "operation": "UPDATE",
            "event_timestamp": event_ts,
            "version": new_version,
            "before": before,
            "after": after,
            "action": f"→ {transition_name} after {delay_days}d (v{new_version})"
        }

    def delete_application(self):
        pool = list(self.final_applications) or list(self.active_applications.keys())
        if not pool:
            return None
        aid = random.choice(pool)
        cur = self.conn.cursor()
        now = datetime.datetime.now()
        event_ts = now.isoformat()

        cur.execute("""
            SELECT application_id, candidate_id, post_id, status, lifecycle_stage,
                applied_date, last_updated, final_outcome, created_at, updated_at, version
            FROM applications WHERE application_id=%s
        """, (aid,))
        cols = [d[0] for d in cur.description]
        row = cur.fetchone()
        if row is None:
            cur.close()
            self.active_applications.pop(aid, None)
            self.final_applications.discard(aid)
            return None
        before = dict(zip(cols, row))

        cur.execute("DELETE FROM applications WHERE application_id=%s RETURNING version", (aid,))
        version_row = cur.fetchone()
        new_version = version_row[0] if version_row else before["version"] + 1

        self.conn.commit()
        cur.close()

        self.active_applications.pop(aid, None)
        self.final_applications.discard(aid)

        return {
            "entity": "application",
            "entity_id": aid,
            "operation": "DELETE",
            "event_timestamp": event_ts,
            "version": new_version,
            "before": before,
            "after": None,
            "action": f"deleted (v{new_version})"
        }

# ═══════════════════════════════════════════════════════════════════════
#  OPERATION CHOOSER
# ═══════════════════════════════════════════════════════════════════════

def choose_operation(engine):
    weights = CONFIG["weights"]
    options = []
    w = []

    options.append(("candidate", "insert"))
    w.append(weights[("candidate", "insert")])

    # NEW: configurable profile-update weight (extra relative weight on top
    # of the table default; profile_update_rate=0 keeps the original weight)
    w_cand_update = weights[("candidate", "update")] + max(
        0.0, float(CONFIG.get("profile_update_rate", 0.0)) * 100)
    if engine.active_candidates:
        options.append(("candidate", "update"))
        w.append(w_cand_update)

    if engine.inactive_candidates:
        options.append(("candidate", "delete"))
        w.append(weights[("candidate", "delete")])

    options.append(("job_post", "insert"))
    w.append(weights[("job_post", "insert")])
    if engine.active_jobs:
        options.append(("job_post", "update"))
        w.append(weights[("job_post", "update")])

    # NEW: configurable reopen weight (0 = disabled, not appended)
    if engine.closed_jobs and float(CONFIG.get("job_reopen_rate", 0.0)) > 0:
        options.append(("job_post", "reopen"))
        w.append(max(1, int(CONFIG["job_reopen_rate"] * 100)))

    # NEW: Only create applications if BOTH pools have active (warmed) entities
    has_ready_candidates = len(engine.active_candidates) > 0
    has_ready_jobs = len(engine.active_jobs) > 0
    if has_ready_candidates and has_ready_jobs:
        options.append(("application", "insert"))
        w.append(weights[("application", "insert")])
    if engine.active_applications:
        options.append(("application", "update"))
        w.append(weights[("application", "update")])

    return random.choices(options, weights=w, k=1)[0]

# ═══════════════════════════════════════════════════════════════════════
#  MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════════

def main():
    parse_cli_overrides()
    conn = connect_postgres()
    create_base_tables(conn)
    ensure_schema(conn)

    engine = LifecycleEngine(conn)

    stats = defaultdict(int)
    match_stats = {"category-matched": 0, "matched-cand-fallback": 0,
                   "no-category": 0, "random": 0, "skipped": 0}
    score_distribution = defaultdict(int)
    start = time.time()

    duration = float(CONFIG.get("duration_seconds") or 0)
    max_ops = CONFIG["total_operations"]
    i = 0
    while True:
        i += 1
        if duration <= 0 and i > max_ops:
            break
        if duration > 0 and (time.time() - start) > duration:
            break
        # NEW: Promote any entities that have finished their 60s warm-up
        engine._promote_warmed_entities()
        entity, operation = choose_operation(engine)
        result = None

        # ── CANDIDATE ─────────────────────────────────────────────────
        if entity == "candidate":
            if operation == "insert":
                cand = generate_candidate_info()
                cand = insert_candidate(conn, cand)

                                # NEW: Put into warming pool, not active yet
                warm_until = cand["created_at"] + datetime.timedelta(seconds=60)
                engine.warming_candidates[cand["candidate_id"]] = warm_until
                # Still track category/skills for when it becomes active
                engine.candidate_categories[cand["candidate_id"]] = cand["job_category"]
                engine.candidate_skills[cand["candidate_id"]] = cand["skills"]
                result = {
                    "entity": "candidate",
                    "entity_id": cand["candidate_id"],
                    "operation": "INSERT",
                    "event_timestamp": cand["created_at"].isoformat(),
                    "version": 1,
                    "before": None,
                    "after": cand,
                    "action": "inserted"
                }

            elif operation == "update":
                result = engine.update_candidate()

            elif operation == "delete":
                result = engine.delete_candidate()

        # ── JOB POST ──────────────────────────────────────────────────
        elif entity == "job_post":
            if operation == "insert":
                job = generate_job_post()
                job = insert_job_post(conn, job)

                # NEW: Put into warming pool, not active yet
                warm_until = job["created_at"] + datetime.timedelta(seconds=60)
                engine.warming_jobs[job["post_id"]] = warm_until
                engine.job_categories[job["post_id"]] = job["category"]
                engine.job_skills[job["post_id"]] = job["skills_needed"]

                result = {
                    "entity": "job_post",
                    "entity_id": job["post_id"],
                    "operation": "INSERT",
                    "event_timestamp": job["created_at"].isoformat(),
                    "version": 1,
                    "before": None,
                    "after": job,
                    "action": "inserted"
                }

            elif operation == "update":
                result = engine.update_job()

            elif operation == "reopen":
                result = engine.reopen_job()

            elif operation == "delete":
                result = engine.delete_job()

        # ── APPLICATION ───────────────────────────────────────────────
        elif entity == "application":
            if operation == "insert":
                result = engine.insert_application()
                if result and "action" in result:
                    if "SKIPPED" in result["action"]:
                        match_stats["skipped"] += 1
                    else:
                        for key in ["category-matched", "matched-cand-fallback", "no-category", "random"]:
                            if f"[{key}]" in result["action"]:
                                match_stats[key] += 1
                                break
                        # Track score distribution
                        if "score=" in result["action"]:
                            try:
                                score_str = result["action"].split("score=")[1].split()[0]
                                score = float(score_str)
                                score_bucket = f"{int(score // 10 * 10)}-{int(score // 10 * 10 + 9)}"
                                score_distribution[score_bucket] += 1
                            except:
                                pass

            elif operation == "update":
                result = engine.update_application()

            elif operation == "delete":
                result = engine.delete_application()

        if result:
            stats[f"{entity}_{operation}"] += 1
            key = f"{entity}:{operation}"
            print(f"[{i:05d}] {key:25s} | {result['entity_id'][:8]}... | {result['action']}")

        if i % 500 == 0:
            print(f"\n📊 Progress: {i} ops | Stats: {dict(stats)}")
            print(f"📊 Match Stats: {dict(match_stats)}")
            if score_distribution:
                print(f"📊 Score Distribution: {dict(score_distribution)}")
            print()

    conn.close()

    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"🏁 Completed {CONFIG['total_operations']} operations in {elapsed:.2f}s")
    print(f"📈 Final Stats: {dict(stats)}")
    print(f"📈 Match Stats: {dict(match_stats)}")
    if score_distribution:
        print(f"📈 Score Distribution: {dict(score_distribution)}")
    total_matched = match_stats["category-matched"] + match_stats["matched-cand-fallback"]
    total_apps = sum(v for k, v in match_stats.items() if k != "skipped")
    if total_apps > 0:
        print(f"📈 Category Match Rate: {total_matched/total_apps*100:.1f}% ({total_matched}/{total_apps})")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
    