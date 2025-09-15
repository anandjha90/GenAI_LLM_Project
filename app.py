import json
import csv
import os
import sys
from typing import Dict, List, Any, Optional
import pandas as pd
from groq import Groq
import getpass
from datetime import datetime
import re
import time
from tqdm import tqdm
import colorama
from colorama import Fore, Style

# Initialize colorama for colored output
colorama.init()

class SQLGenerationPipeline:
    def __init__(self):
        self.groq_client = None
        self.sales_schema = None
        self.marketing_schema = None
        self.questions = []
        self.results = []
        self.token_usage = []  # ⭐ ENHANCEMENT: Track token usage per call
        self.latency_log = []  # ⭐ ENHANCEMENT: Track latency per question
        self.config = {
            'model': 'llama-3.1-70b-versatile',
            'temperature': 0.1,
            'max_tokens': 2000,
            'retry_attempts': 3,
            'retry_delay': 2
        }

    def print_banner(self):
        """Print a professional banner with enhanced branding"""
        banner = f"""
{Fore.MAGENTA}╔════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║   {Fore.CYAN}███████╗ ██████╗ ██╗     ██╗   ██╗███████╗██╗ ██████╗ ███╗   ██╗{Fore.MAGENTA}   ║
║   {Fore.CYAN}██╔════╝██╔═══██╗██║     ██║   ██║██╔════╝██║██╔═══██╗████╗  ██║{Fore.MAGENTA}   ║
║   {Fore.CYAN}███████╗██║   ██║██║     ██║   ██║███████╗██║██║   ██║██╔██╗ ██║{Fore.MAGENTA}   ║
║   {Fore.CYAN}╚════██║██║   ██║██║     ██║   ██║╚════██║██║██║   ██║██║╚██╗██║{Fore.MAGENTA}   ║
║   {Fore.CYAN}███████║╚██████╔╝███████╗╚██████╔╝███████║██║╚██████╔╝██║ ╚████║{Fore.MAGENTA}   ║
║   {Fore.CYAN}╚══════╝ ╚═════╝ ╚══════╝ ╚═════╝ ╚══════╝╚═╝ ╚═════╝ ╚═╝  ╚═══╝{Fore.MAGENTA}   ║
║                                                                              ║
║           {Fore.WHITE}ADVANCED LLM-POWERED SQL GENERATION & VALIDATION ENGINE{Fore.MAGENTA}      ║
║                 {Fore.YELLOW}Architected for Precision • Engineered by: Anand Jha{Fore.MAGENTA}       ║
║                                                                              ║
╚════════════════════════════════════════════════════════════════════════════╝{Style.RESET_ALL}

{Fore.GREEN}→ AI autonomously validates schemas → reasons step-by-step → scores confidence freely ←{Style.RESET_ALL}
        """
        print(banner)

    def configure_pipeline(self):
        """Allow user to configure pipeline settings"""
        print(f"\n{Fore.YELLOW}Pipeline Configuration{Style.RESET_ALL}")
        print("="*50)
        
        models = [
            ("1", "llama-3.1-70b-versatile", "Best overall performance"),
            ("2", "llama-3.1-8b-instant", "Faster, good for simple queries"),
            ("3", "llama3-70b-8192", "Previous generation, stable"),
            ("4", "mixtral-8x7b-32768", "Alternative model"),
            ("5", "gemma2-9b-it", "Lightweight option")
        ]
        
        for num, model, desc in models:
            print(f"  {num}. {model} - {desc}")
        
        choice = input(f"\n{Fore.CYAN}Select model (1-5, or press Enter for default): {Style.RESET_ALL}").strip()
        if choice in ['1', '2', '3', '4', '5']:
            self.config['model'] = models[int(choice)-1][1]
        
        print(f"Selected model: {Fore.GREEN}{self.config['model']}{Style.RESET_ALL}")
        
        advanced = input(f"\n{Fore.CYAN}Configure advanced settings? (y/N): {Style.RESET_ALL}").strip().lower()
        if advanced == 'y':
            temp_input = input(f"{Fore.CYAN}Temperature (0.0-1.0, default 0.1): {Style.RESET_ALL}").strip()
            if temp_input:
                try:
                    temp = float(temp_input)
                    if 0.0 <= temp <= 1.0:
                        self.config['temperature'] = temp
                except ValueError:
                    print(f"{Fore.RED}Invalid temperature, using default{Style.RESET_ALL}")
            
            tokens_input = input(f"{Fore.CYAN}Max tokens (100-4000, default 2000): {Style.RESET_ALL}").strip()
            if tokens_input:
                try:
                    tokens = int(tokens_input)
                    if 100 <= tokens <= 4000:
                        self.config['max_tokens'] = tokens
                except ValueError:
                    print(f"{Fore.RED}Invalid token count, using default{Style.RESET_ALL}")
            
            retry_input = input(f"{Fore.CYAN}Retry attempts (1-5, default 3): {Style.RESET_ALL}").strip()
            if retry_input:
                try:
                    retries = int(retry_input)
                    if 1 <= retries <= 5:
                        self.config['retry_attempts'] = retries
                except ValueError:
                    print(f"{Fore.RED}Invalid retry count, using default{Style.RESET_ALL}")

        print(f"\n{Fore.GREEN}Configuration Summary:{Style.RESET_ALL}")
        print(f"  Model: {self.config['model']}")
        print(f"  Temperature: {self.config['temperature']}")
        print(f"  Max Tokens: {self.config['max_tokens']}")
        print(f"  Retry Attempts: {self.config['retry_attempts']}")

    # ⭐ ENHANCEMENT: Parse flexible question selection (ranges, commas, mixed)
    def parse_question_selection(self, total_questions: int) -> List[int]:
        """Parse user input like '1-6', '1,5,7', '15-20' into list of question IDs"""
        selection = input(f"\n{Fore.CYAN}Enter question IDs to process (e.g., 1-6, 1,5,7, 15-20 or press Enter for all): {Style.RESET_ALL}").strip()
        
        if not selection:
            return list(range(1, total_questions + 1))
        
        selected_ids = set()
        parts = selection.split(',')
        
        for part in parts:
            part = part.strip()
            if '-' in part:
                start_end = part.split('-')
                if len(start_end) == 2:
                    try:
                        start = int(start_end[0])
                        end = int(start_end[1])
                        if 1 <= start <= end <= total_questions:
                            selected_ids.update(range(start, end + 1))
                        else:
                            print(f"{Fore.RED}Range {part} out of bounds (1-{total_questions}){Style.RESET_ALL}")
                    except ValueError:
                        print(f"{Fore.RED}Invalid range: {part}{Style.RESET_ALL}")
            else:
                try:
                    qid = int(part)
                    if 1 <= qid <= total_questions:
                        selected_ids.add(qid)
                    else:
                        print(f"{Fore.RED}Question ID {qid} out of bounds (1-{total_questions}){Style.RESET_ALL}")
                except ValueError:
                    print(f"{Fore.RED}Invalid ID: {part}{Style.RESET_ALL}")
        
        if not selected_ids:
            print(f"{Fore.YELLOW}No valid IDs selected. Processing all questions.{Style.RESET_ALL}")
            return list(range(1, total_questions + 1))
        
        return sorted(list(selected_ids))

    def load_schemas(self):
        try:
            with open('data/sales_dw.json', 'r') as f:
                self.sales_schema = json.load(f)
            print(f"{Fore.GREEN}✓{Style.RESET_ALL} Loaded sales_dw schema")
            
            with open('data/marketing_dw.json', 'r') as f:
                self.marketing_schema = json.load(f)
            print(f"{Fore.GREEN}✓{Style.RESET_ALL} Loaded marketing_dw schema")
        except Exception as e:
            print(f"{Fore.RED}Error loading schemas: {e}{Style.RESET_ALL}")
            sys.exit(1)

    def load_questions(self):
        try:
            self.questions = []
            with open('data/questions.csv', 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self.questions.append({
                        'question_id': int(row['question_id']),
                        'question': row['question']
                    })
            print(f"{Fore.GREEN}✓{Style.RESET_ALL} Loaded {len(self.questions)} questions")
        except Exception as e:
            print(f"{Fore.RED}Error loading questions: {e}{Style.RESET_ALL}")
            sys.exit(1)

    def initialize_groq(self):
        print(f"\n{Fore.YELLOW}Groq API Configuration{Style.RESET_ALL}")
        print("="*50)
        
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            api_key = getpass.getpass(f"{Fore.CYAN}[Attempt {attempt}/{max_retries}] Please enter your Groq API key: {Style.RESET_ALL}")
            
            if not api_key.strip():
                print(f"{Fore.RED}✗ API key cannot be empty. Try again.{Style.RESET_ALL}")
                continue

            try:
                self.groq_client = Groq(api_key=api_key)
                # Test the connection with a minimal call
                test_response = self.groq_client.chat.completions.create(
                    model=self.config['model'],
                    messages=[{"role": "user", "content": "Hello"}],
                    max_tokens=5
                )
                print(f"\n{Fore.GREEN}✅ Success! Groq API key validated and connection established.{Style.RESET_ALL}")
                return  # Exit successfully
            
            except Exception as e:
                print(f"{Fore.RED}✗ API Key Invalid or Network Error (Attempt {attempt}): {str(e)}{Style.RESET_ALL}")
                if attempt < max_retries:
                    print(f"{Fore.YELLOW}→ Please try again...{Style.RESET_ALL}\n")
                else:
                    print(f"\n{Fore.RED}❌ Maximum retries ({max_retries}) exceeded. Exiting.{Style.RESET_ALL}")
                    print(f"{Fore.MAGENTA}💡 Tip: Ensure you're using a valid Groq API key from https://console.groq.com/keys{Style.RESET_ALL}")
                    sys.exit(1)

    def format_schema_for_prompt(self, schema: Dict) -> str:
        schema_text = f"Database: {schema['database']}\n\n"
        for table_name, table_info in schema['tables'].items():
            schema_text += f"📊 Table: {table_name}\nColumns:\n"
            for col_name, col_info in table_info['columns'].items():
                schema_text += f"  - {col_name}: {col_info['type']} — {col_info['description']}\n"
            if 'relationships' in table_info:
                schema_text += "🔗 Relationships:\n"
                for rel in table_info['relationships']:
                    schema_text += f"  - {rel}\n"
            schema_text += "\n"
        return schema_text

    def validate_and_fix_sql(self, sql: str, target_source: str) -> str:
        sql = sql.rstrip(';')
        if 'TOP ' in sql.upper():
            match = re.search(r'TOP\s+(\d+)', sql, re.IGNORECASE)
            if match:
                limit_num = match.group(1)
                sql = re.sub(r'SELECT\s+TOP\s+\d+', 'SELECT', sql, flags=re.IGNORECASE)
                if 'LIMIT' not in sql.upper():
                    sql += f' LIMIT {limit_num}'
        sql = re.sub(r'DATE_SUBKATEX_INLINE_OPENCURRENT_DATE,\s*INTERVAL\s+(\d+)\s+(\w+)KATEX_INLINE_CLOSE',
                     r"CURRENT_DATE - INTERVAL '\1 \2'", sql, flags=re.IGNORECASE)
        sql = re.sub(r"INTERVAL\s+'(\d+)'\s+DAY", r"INTERVAL '\1 day'", sql, flags=re.IGNORECASE)
        sql = re.sub(r"INTERVAL\s+'(\d+)'\s+MONTH", r"INTERVAL '\1 month'", sql, flags=re.IGNORECASE)
        sql = re.sub(r"INTERVAL\s+'(\d+)'\s+YEAR", r"INTERVAL '\1 year'", sql, flags=re.IGNORECASE)
        return sql

    def extract_json_from_response(self, text: str) -> Optional[Dict]:
        text = text.replace('```json', '').replace('```', '')
        start_idx = text.find('{')
        end_idx = text.rfind('}')
        if start_idx == -1 or end_idx == -1:
            return None
        json_str = text[start_idx:end_idx + 1]
        lines = json_str.split('\n')
        cleaned_lines = []
        in_string = False
        for line in lines:
            quote_count = line.count('"') - line.count('\\"')
            if quote_count % 2 == 1:
                in_string = not in_string
            if in_string and cleaned_lines:
                cleaned_lines[-1] += ' ' + line.strip()
            else:
                cleaned_lines.append(line.strip())
        json_str = '\n'.join(cleaned_lines)
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            json_str = re.sub(r',\s*}', '}', json_str)
            json_str = re.sub(r',\s*]', ']', json_str)
            try:
                return json.loads(json_str)
            except:
                return None

    def generate_sql_for_question(self, question_data: Dict, attempt: int = 1) -> Dict:
        question = question_data['question']
        question_id = question_data['question_id']

        sales_schema_text = self.format_schema_for_prompt(self.sales_schema)
        marketing_schema_text = self.format_schema_for_prompt(self.marketing_schema)

        # ⭐ ENHANCEMENT: Removed prescriptive confidence scale — AI decides freely
        system_prompt = f"""You are an expert SQL architect. Generate ANSI SQL ONLY IF all required data exists within ONE schema.

🧠 YOU MUST THINK STEP-BY-STEP AND SELF-ASSESS:

1. PARSE: What tables and columns does this question need?
2. VALIDATE PER SCHEMA:
   - Check sales_dw: Do ALL required tables/columns exist here?
   - Check marketing_dw: Do ALL required tables/columns exist here?
   → If split across schemas → explain why you cannot generate.
3. JOIN LOGIC: Use only documented relationships (foreign keys).
4. CONFIDENCE: Assign a decimal score from 0.0 to 1.0 based on your OWN judgment of certainty.
   → 1.0 = fully certain, 0.0 = impossible or missing data
   → No predefined thresholds — be honest and nuanced.
5. ASSUMPTIONS: Explain what you checked, why you chose target_source, and justification for confidence.

📤 OUTPUT FORMAT (STRICT JSON — NO EXTRA TEXT):
{{
  "question_id": {question_id},
  "question": "{question}",
  "target_source": "sales_dw | marketing_dw | N/A",
  "sql": "SELECT ... ; OR '-- Cannot generate: [reason]'",
  "assumptions": "Your detailed reasoning — what you validated, what you assumed",
  "confidence": 0.0 to 1.0 (your own judgment)
}}

⚠️ NEVER BLUFF. If unsure → confidence low. You are graded on honesty and reasoning depth.
"""

        user_prompt = f"""🔍 AVAILABLE SCHEMAS — YOU MUST VALIDATE TABLE EXISTENCE:

🔷 SALES DATA WAREHOUSE:
{sales_schema_text}

🔷 MARKETING DATA WAREHOUSE:
{marketing_schema_text}

❓ QUESTION TO ANSWER:
Question ID: {question_id}
Question: "{question}"

✅ YOUR TASK:
- Decide which schema contains ALL required data.
- Write SQL ONLY if data exists in ONE schema.
- If joining tables, confirm they share a relationship.
- BE TRANSPARENT in assumptions — explain your validation steps.
- SCORE CONFIDENCE HONESTLY — no overconfidence, no predefined buckets.
"""

        try:
            start_time = time.time()
            response = self.groq_client.chat.completions.create(
                model=self.config['model'],
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=self.config['temperature'],
                max_tokens=self.config['max_tokens']
            )
            end_time = time.time()

            # ⭐ ENHANCEMENT: Track tokens and latency
            usage = response.usage
            self.token_usage.append({
                'question_id': question_id,
                'prompt_tokens': usage.prompt_tokens,
                'completion_tokens': usage.completion_tokens,
                'total_tokens': usage.total_tokens
            })
            self.latency_log.append({
                'question_id': question_id,
                'latency_sec': round(end_time - start_time, 2)
            })

            response_text = response.choices[0].message.content.strip()
            result = self.extract_json_from_response(response_text)

            if result is None:
                raise ValueError("Failed to parse LLM response as JSON")

            result.setdefault('question_id', question_id)
            result.setdefault('question', question)
            result.setdefault('target_source', 'N/A')
            result.setdefault('sql', '-- Error parsing response')
            result.setdefault('assumptions', 'AI did not provide reasoning')
            result.setdefault('confidence', 0.0)

            if result.get('confidence', 0) > 0 and 'sql' in result and not result['sql'].startswith('--'):
                result['sql'] = self.validate_and_fix_sql(result['sql'], result.get('target_source', ''))

            return result

        except Exception as e:
            if attempt < self.config['retry_attempts']:
                print(f"{Fore.YELLOW}  Retry {attempt}/{self.config['retry_attempts']} for Q{question_id}{Style.RESET_ALL}")
                time.sleep(self.config['retry_delay'])
                return self.generate_sql_for_question(question_data, attempt + 1)

            return {
                "question_id": question_id,
                "question": question,
                "target_source": "Unknown",
                "sql": "-- Error during generation",
                "assumptions": f"System error after {self.config['retry_attempts']} retries: {str(e)}",
                "confidence": 0.0
            }

    def process_all_questions(self):
        print(f"\n{Fore.YELLOW}Generating SQL Queries — AI thinks, validates & scores freely{Style.RESET_ALL}")
        print("="*70)

        # ⭐ ENHANCEMENT: Let user pick questions
        selected_ids = self.parse_question_selection(len(self.questions))
        selected_questions = [q for q in self.questions if q['question_id'] in selected_ids]

        print(f"{Fore.CYAN}Processing {len(selected_questions)} selected questions: {selected_ids}{Style.RESET_ALL}")

        with tqdm(total=len(selected_questions), desc="Processing", 
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as pbar:
            
            for question_data in selected_questions:
                result = self.generate_sql_for_question(question_data)
                self.results.append(result)
                
                conf = result.get('confidence', 0)
                if conf >= 0.8:
                    pbar.set_postfix_str(f"{Fore.GREEN}✓ Confident{Style.RESET_ALL}")
                elif conf >= 0.5:
                    pbar.set_postfix_str(f"{Fore.YELLOW}⚠ Unsure{Style.RESET_ALL}")
                else:
                    pbar.set_postfix_str(f"{Fore.RED}✗ Can't generate{Style.RESET_ALL}")
                
                pbar.update(1)

    def save_results(self):
        output_dir = 'output'
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        print(f"\n{Fore.YELLOW}Export Options{Style.RESET_ALL}")
        print("="*50)
        print("1. CSV only")
        print("2. JSON only")
        print("3. Both")
        print("4. CSV + Markdown report")
        print("5. All formats")
        
        export_choice = input(f"\n{Fore.CYAN}Select format (1-5, default 1): {Style.RESET_ALL}").strip() or '1'
        files_created = []

        if export_choice in ['1', '3', '4', '5']:
            csv_file = f"{output_dir}/queries_{timestamp}.csv"
            df = pd.DataFrame(self.results)[['question_id', 'question', 'target_source', 'sql', 'assumptions', 'confidence']]
            df.to_csv(csv_file, index=False, encoding='utf-8')
            files_created.append(csv_file)

        if export_choice in ['2', '3', '5']:
            json_file = f"{output_dir}/queries_{timestamp}.json"
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, indent=2, ensure_ascii=False)
            files_created.append(json_file)

        if export_choice in ['4', '5']:
            md_file = f"{output_dir}/report_{timestamp}.md"
            self.generate_markdown_report(md_file)
            files_created.append(md_file)

        print(f"\n{Fore.GREEN}Files created:{Style.RESET_ALL}")
        for file in files_created:
            print(f"  ✓ {file}")

        self.print_summary_statistics()

    def generate_markdown_report(self, filename: str):
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("# 🧠 AI-Powered SQL Generation Report\n\n")
            f.write(f"**Generated on**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  \n")
            f.write(f"**Engineer**: Anand Jha  \n")
            f.write(f"**Model**: {self.config['model']}  \n")
            f.write(f"**Temperature**: {self.config['temperature']}  \n\n")

            total = len(self.results)
            success = sum(1 for r in self.results if r['confidence'] > 0)
            high = sum(1 for r in self.results if r['confidence'] >= 0.8)

            f.write("## 📊 Executive Summary\n")
            f.write(f"- Total Questions: **{total}**  \n")
            f.write(f"- Successfully Generated: **{success}**  \n")
            f.write(f"- High Confidence (≥0.8): **{high}**  \n")
            f.write(f"- Success Rate: **{success/total*100:.1f}%**  \n\n")

            f.write("## 🤖 Sample AI Reasoning (Low Confidence Cases)\n")
            low_conf = [r for r in self.results if r['confidence'] < 0.5][:3]
            for r in low_conf:
                f.write(f"\n### ❓ Question {r['question_id']}: {r['question']}\n")
                f.write(f"- **Confidence**: `{r['confidence']}`  \n")
                f.write(f"- **Assumptions**: {r['assumptions']}  \n")
                f.write(f"- **SQL**: `{r['sql']}`  \n")

            f.write("\n## 📝 Full Query Results\n")
            for r in self.results:
                f.write(f"\n### 🔍 Question {r['question_id']}: {r['question']}\n")
                f.write(f"- **Target Source**: `{r['target_source']}`  \n")
                f.write(f"- **Confidence**: `{r['confidence']}`  \n")
                f.write(f"- **Assumptions**: {r['assumptions']}  \n")
                f.write("**SQL**:\n```sql\n" + r['sql'] + "\n```\n---")

    def print_summary_statistics(self):
        print(f"\n{Fore.YELLOW}📊 FINAL REPORT — Engineered by Anand Jha{Style.RESET_ALL}")
        print("="*70)

        total = len(self.results)
        success = sum(1 for r in self.results if r['confidence'] > 0)
        avg_conf = sum(r['confidence'] for r in self.results) / total if total > 0 else 0

        print(f"✅ Total Processed: {total}")
        print(f"🎯 AI Success Rate: {Fore.GREEN}{success}/{total} ({success/total*100:.1f}%){Style.RESET_ALL}")
        print(f"📈 Average Confidence: {Fore.CYAN}{avg_conf:.3f}{Style.RESET_ALL}")

        # ⭐ ENHANCEMENT: Show performance metrics
        if self.token_usage:
            total_prompt_tokens = sum(t['prompt_tokens'] for t in self.token_usage)
            total_completion_tokens = sum(t['completion_tokens'] for t in self.token_usage)
            total_tokens = sum(t['total_tokens'] for t in self.token_usage)
            avg_latency = sum(l['latency_sec'] for l in self.latency_log) / len(self.latency_log) if self.latency_log else 0

            print(f"\n{Fore.BLUE}⚡ Performance Metrics:{Style.RESET_ALL}")
            print(f"  Model Used: {self.config['model']}")
            print(f"  Total Prompt Tokens: {total_prompt_tokens:,}")
            print(f"  Total Completion Tokens: {total_completion_tokens:,}")
            print(f"  Total Tokens Consumed: {total_tokens:,}")
            print(f"  Avg Latency per Query: {avg_latency:.2f}s")

        print(f"\n{Fore.CYAN}Target Sources Chosen by AI:{Style.RESET_ALL}")
        sources = {}
        for r in self.results:
            src = r['target_source']
            sources[src] = sources.get(src, 0) + 1
        for src, count in sorted(sources.items()):
            print(f"  {src}: {count}")

    def run(self):
        self.print_banner()
        print(f"\n{Fore.YELLOW}Loading Data{Style.RESET_ALL}")
        print("="*50)
        self.load_schemas()
        self.load_questions()
        self.configure_pipeline()
        self.initialize_groq()

        start_time = time.time()
        self.process_all_questions()
        end_time = time.time()

        print(f"\n{Fore.GREEN}✓ Pipeline completed in {end_time - start_time:.1f} seconds{Style.RESET_ALL}")
        self.save_results()
        print(f"\n{Fore.GREEN}✅ SQL Generation Complete — Precision Engineered by Anand Jha{Style.RESET_ALL}")

def main():
    try:
        pipeline = SQLGenerationPipeline()
        pipeline.run()
    except KeyboardInterrupt:
        print(f"\n\n{Fore.YELLOW}Interrupted by user{Style.RESET_ALL}")
        sys.exit(0)
    except Exception as e:
        print(f"\n{Fore.RED}Fatal error: {e}{Style.RESET_ALL}")
        sys.exit(1)

if __name__ == "__main__":
    main()