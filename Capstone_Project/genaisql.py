import os, json, sys, getpass
from datetime import datetime
import pandas as pd
import mysql.connector
from groq import Groq
from colorama import Fore, Style, init
from decimal import Decimal

init(autoreset=True)

class GenAIMigrationPipeline:
    def __init__(self):
        self.groq_client = None
        self.config = {
            'model': 'llama-3.3-70b-versatile',  # Default supported model
            'temperature': 0.1,
            'max_tokens': 2000
        }
        self.mysql_conn = None
        self.results = {}
        self.data_dir = "data"

    # --- STEP 0: Check CSV files ---
    def check_csv_files(self):
        required_files = ["CUSTOMERS.csv","INVENTORY.csv","SALES.csv"]
        missing = [f for f in required_files if not os.path.exists(os.path.join(self.data_dir,f))]
        if missing:
            print(f"{Fore.RED}❌ Missing CSV files in '{self.data_dir}': {missing}{Style.RESET_ALL}")
            sys.exit(1)

    # --- STEP 1: Connect to MySQL ---
    def connect_mysql(self):
        print(f"\n{Fore.YELLOW}MySQL Connection Setup{Style.RESET_ALL}")
        host = input("Host (default: localhost): ") or "localhost"
        user = input("User (default: root): ") or "root"
        pwd = getpass.getpass("Password: ")
        db = input("Database to create/use (default: retail_dw): ") or "retail_dw"

        try:
            self.mysql_conn = mysql.connector.connect(host=host, user=user, password=pwd)
            cur = self.mysql_conn.cursor()
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
            cur.execute(f"USE {db}")
            print(f"{Fore.GREEN}✓ Connected to MySQL and using database `{db}`{Style.RESET_ALL}")
        except mysql.connector.Error as e:
            print(f"{Fore.RED}❌ MySQL connection failed: {e}{Style.RESET_ALL}")
            sys.exit(1)

    # --- STEP 2: Initialize Groq ---
    def init_groq(self):
        print(f"\n{Fore.YELLOW}Groq API Setup{Style.RESET_ALL}")
        key = getpass.getpass("Groq API key: ")
        self.groq_client = Groq(api_key=key)
        model_name = input(f"Enter Groq model to use (default: {self.config['model']}): ") or self.config['model']
        self.config['model'] = model_name

    # --- STEP 3: Prompt LLM ---
    def prompt_llm(self, system_prompt, user_prompt):
        try:
            resp = self.groq_client.chat.completions.create(
                model=self.config['model'],
                messages=[{"role":"system","content":system_prompt},{"role":"user","content":user_prompt}],
                temperature=self.config['temperature'],
                max_tokens=self.config['max_tokens']
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            print(f"{Fore.RED}❌ Groq API error: {e}{Style.RESET_ALL}")
            sys.exit(1)

    # --- STEP 4: Drop tables if exist ---
    def drop_tables_if_exist(self):
        cur = self.mysql_conn.cursor()
        for table in ["SALES","INVENTORY","CUSTOMERS"]:
            try:
                cur.execute(f"DROP TABLE IF EXISTS {table}")
                print(f"{Fore.YELLOW}✓ Dropped table if existed: {table}{Style.RESET_ALL}")
            except:
                pass
        self.mysql_conn.commit()

    # --- STEP 5: Design Schema ---
    def design_schema(self):
        print(f"\n{Fore.YELLOW}Step 1: Designing Schema with GenAI{Style.RESET_ALL}")
        schema_text = ""
        for csvfile in ["CUSTOMERS.csv","INVENTORY.csv","SALES.csv"]:
            path = os.path.join(self.data_dir, csvfile)
            df = pd.read_csv(path, nrows=5)
            schema_text += f"\nCSV: {csvfile}\n"
            schema_text += "\n".join([f"- {c}: {str(df[c].dtype)}" for c in df.columns])+"\n"

        sys_prompt = "You are an expert database architect. Generate MySQL CREATE TABLE scripts based on given CSV columns and datatypes. Use appropriate PK/FK constraints. For phone numbers, use BIGINT or VARCHAR to avoid out-of-range errors."
        user_prompt = f"Here are the CSV structures:\n{schema_text}\nReturn only SQL without markdown formatting."
        sql = self.prompt_llm(sys_prompt, user_prompt)
        self.results['schema_sql'] = sql

        cur = self.mysql_conn.cursor()
        tables = sql.split(";")
        # Create parent tables first
        for stmt in tables:
            stmt_clean = stmt.replace("```sql","").replace("```","").strip()
            if not stmt_clean or "SALES" in stmt_clean: continue
            try:
                cur.execute(stmt_clean)
                print(f"{Fore.GREEN}✓ Executed: {stmt_clean.splitlines()[0]}{Style.RESET_ALL}")
            except Exception as e:
                print(f"{Fore.RED}Error executing SQL: {stmt_clean}\n{e}{Style.RESET_ALL}")
        # Child tables
        for stmt in tables:
            stmt_clean = stmt.replace("```sql","").replace("```","").strip()
            if not stmt_clean or "SALES" not in stmt_clean: continue
            try:
                cur.execute(stmt_clean)
                print(f"{Fore.GREEN}✓ Executed: {stmt_clean.splitlines()[0]}{Style.RESET_ALL}")
            except Exception as e:
                print(f"{Fore.RED}Error executing SQL: {stmt_clean}\n{e}{Style.RESET_ALL}")
        self.mysql_conn.commit()
        print(f"{Fore.GREEN}✓ Schema creation complete{Style.RESET_ALL}")

    # --- STEP 6: Import CSV data safely ---
    def import_data(self):
        print(f"\n{Fore.YELLOW}Step 2: Importing CSV Data{Style.RESET_ALL}")
        cur = self.mysql_conn.cursor()
        for fname in ["CUSTOMERS.csv","INVENTORY.csv","SALES.csv"]:
            path = os.path.join(self.data_dir, fname)
            table = fname.split(".")[0]
            df = pd.read_csv(path)
            if table=="CUSTOMERS" and "phone_number" in df.columns:
                df["phone_number"] = df["phone_number"].apply(str)
            cols = ",".join(df.columns)
            vals_placeholder = ",".join(["%s"]*len(df.columns))
            data = [tuple(r) for r in df.to_numpy()]
            try:
                cur.executemany(f"INSERT IGNORE INTO {table} ({cols}) VALUES ({vals_placeholder})", data)
                self.mysql_conn.commit()
                print(f"{Fore.GREEN}✓ Data loaded into {table}{Style.RESET_ALL}")
            except Exception as e:
                print(f"{Fore.RED}❌ Failed to load data into {table}: {e}{Style.RESET_ALL}")

    # --- STEP 7: Generate Validation SQL ---
    def validate_data(self):
        print(f"\n{Fore.YELLOW}Step 3: Generating Validation Queries{Style.RESET_ALL}")
        sys_prompt = """You are a data QA expert. Write SQL queries to:
1. Count rows in CUSTOMERS, INVENTORY, SALES
2. Verify every SALES.customer_id exists in CUSTOMERS
3. Verify every SALES.product_id exists in INVENTORY
4. Total of SALES.total_amount"""
        sql = self.prompt_llm(sys_prompt,"Return only SQL without markdown formatting")
        self.results['validation_sql'] = sql
        print(sql)

    # --- STEP 8: Run Validation ---
    def run_validation(self):
        print(f"\n{Fore.YELLOW}Step 3B: Running Validation Queries{Style.RESET_ALL}")
        cur = self.mysql_conn.cursor()
        validation_out=[]
        for stmt in self.results['validation_sql'].split(";"):
            stmt_clean=stmt.replace("```sql","").replace("```","").strip()
            if not stmt_clean: continue
            try:
                cur.execute(stmt_clean)
                res=cur.fetchall()
                validation_out.append({"query":stmt_clean,"result":res})
            except Exception as e:
                validation_out.append({"query":stmt_clean,"error":str(e)})
        self.results['validation_results']=validation_out
        print(f"{Fore.GREEN}✓ Validation results captured{Style.RESET_ALL}")

    # --- STEP 9: Translate PL/SQL ---
    def translate_plsql(self):
        print(f"\n{Fore.YELLOW}Step 4: Translating Oracle PL/SQL{Style.RESET_ALL}")
        path=os.path.join(self.data_dir,"oracle_plsql_procedures.sql")
        if not os.path.exists(path):
            print(f"{Fore.RED}❌ PL/SQL file not found: {path}{Style.RESET_ALL}")
            return
        with open(path) as f:
            plsql=f.read()
        sys_prompt="You are an SQL expert. Convert Oracle PL/SQL procedures/functions into equivalent MySQL stored procedures/functions."
        mysql_code=self.prompt_llm(sys_prompt,plsql)
        self.results['translated_sql']=mysql_code
        print(mysql_code)

    # --- STEP 10: Generate BI Queries ---
    def generate_bi(self):
        print(f"\n{Fore.YELLOW}Step 5: Generating BI KPI Queries{Style.RESET_ALL}")
        sys_prompt="Write MySQL SQL queries for KPIs: monthly sales trend, top 5 customers by revenue, low stock products (<100 qty)."
        sql=self.prompt_llm(sys_prompt,"Return only SQL without markdown formatting")
        self.results['bi_sql']=sql
        print(sql)

    # --- STEP 11: Export Markdown Report ---
    def export_report(self):
        print(f"\n{Fore.YELLOW}Step 6: Exporting Migration Report{Style.RESET_ALL}")
        md = f"# GenAI-Assisted Data Migration Report\n\n"
        md += f"**Run Timestamp:** {datetime.now()}\n\n"
        for k, v in self.results.items():
            # Agar string hai → SQL ya text
            if isinstance(v, str):
                md += f"## {k}\n\n```sql\n{v}\n```\n\n"
            else:
                # Agar JSON me Decimal hai → convert to float
                def default_converter(o):
                    if isinstance(o, Decimal):
                        return float(o)
                    return str(o)  # fallback for any other non-serializable type

                md += f"## {k}\n\n{json.dumps(v, indent=2, default=default_converter)}\n\n"

        outpath = f"output/migration_report_{datetime.now().strftime('%Y%m%d_%H%M')}.md"
        os.makedirs("output", exist_ok=True)
        with open(outpath, "w") as f:
            f.write(md)
        print(f"{Fore.GREEN}✓ Report saved to {outpath}{Style.RESET_ALL}")

    # --- STEP 12: Run Pipeline ---
    def run(self):
        self.check_csv_files()
        self.connect_mysql()
        self.init_groq()
        self.drop_tables_if_exist()
        self.design_schema()
        self.import_data()
        self.validate_data()
        self.run_validation()
        self.translate_plsql()
        self.generate_bi()
        self.export_report()
        print(f"\n{Fore.GREEN}✅ Migration Pipeline Complete{Style.RESET_ALL}")

if __name__=="__main__":
    GenAIMigrationPipeline().run()

