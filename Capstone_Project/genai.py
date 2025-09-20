import os, json
from datetime import datetime
import pandas as pd
import mysql.connector
import streamlit as st
import plotly.express as px
from groq import Groq

DATA_DIR = r"C:\Users\PC\OneDrive\Desktop\sqlprojectwithGENAI\data"

class GenAIMigrationPipeline:
    def __init__(self, mysql_config, groq_key, groq_model):
        self.groq_client = Groq(api_key=groq_key) if groq_key else None
        self.config = {
            'model': groq_model or 'llama-3.3-70b-versatile',
            'temperature': 0.1,
            'max_tokens': 2000
        }
        self.mysql_config = mysql_config
        self.mysql_conn = None
        self.results = {}
        self.data_dir = DATA_DIR

    def check_csv_files(self):
        required = ["CUSTOMERS.csv", "INVENTORY.csv", "SALES.csv"]
        missing = [f for f in required if not os.path.exists(os.path.join(self.data_dir, f))]
        if missing:
            st.error(f"❌ Missing CSV files: {missing}")
            st.stop()

    def connect_mysql(self):
        try:
            self.mysql_conn = mysql.connector.connect(
                host=self.mysql_config["host"],
                user=self.mysql_config["user"],
                password=self.mysql_config["password"]
            )
            cur = self.mysql_conn.cursor()
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {self.mysql_config['database']}")
            cur.execute(f"USE {self.mysql_config['database']}")
            st.success(f"✓ Connected to MySQL `{self.mysql_config['database']}`")
        except mysql.connector.Error as e:
            st.error(f"MySQL connection failed: {e}")
            st.stop()

    def prompt_llm(self, system_prompt, user_prompt):
        if not self.groq_client:
            return ""
        try:
            resp = self.groq_client.chat.completions.create(
                model=self.config['model'],
                messages=[{"role": "system", "content": system_prompt},
                          {"role": "user", "content": user_prompt}],
                temperature=self.config['temperature'],
                max_tokens=self.config['max_tokens']
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            st.error(f"Groq API error: {e}")
            return ""

    def drop_tables_if_exist(self):
        cur = self.mysql_conn.cursor()
        for table in ["SALES", "INVENTORY", "CUSTOMERS"]:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        self.mysql_conn.commit()
        st.info("Dropped existing tables (if any)")

    def design_schema(self):
        schema_text = ""
        for csvfile in ["CUSTOMERS.csv","INVENTORY.csv","SALES.csv"]:
            path = os.path.join(self.data_dir, csvfile)
            df = pd.read_csv(path, nrows=5)
            schema_text += f"\nCSV: {csvfile}\n"
            schema_text += "\n".join([f"- {c}: {str(df[c].dtype)}" for c in df.columns])+"\n"

        sys_prompt = "Generate MySQL CREATE TABLE scripts with PK/FK. Return only SQL."
        sql = self.prompt_llm(sys_prompt, schema_text)
        self.results['schema_sql'] = sql

        cur = self.mysql_conn.cursor()
        for stmt in sql.split(";"):
            stmt_clean = stmt.replace("```sql","").replace("```","").strip()
            if stmt_clean:
                try:
                    cur.execute(stmt_clean)
                except Exception as e:
                    st.error(f"Error executing SQL: {stmt_clean}\n{e}")
        self.mysql_conn.commit()
        st.success("Schema created")

    def import_data(self):
        cur = self.mysql_conn.cursor()
        for fname in ["CUSTOMERS.csv","INVENTORY.csv","SALES.csv"]:
            path = os.path.join(self.data_dir, fname)
            table = fname.split(".")[0]
            df = pd.read_csv(path)
            if table=="CUSTOMERS" and "phone_number" in df.columns:
                df["phone_number"] = df["phone_number"].astype(str)
            cols = ",".join(df.columns)
            vals = ",".join(["%s"]*len(df.columns))
            data = [tuple(r) for r in df.to_numpy()]
            try:
                cur.executemany(f"INSERT IGNORE INTO {table} ({cols}) VALUES ({vals})", data)
                self.mysql_conn.commit()
                st.success(f"Loaded {len(data)} rows into {table}")
            except Exception as e:
                st.error(f"Failed to load {table}: {e}")

    def validate_data(self):
        sys_prompt = """Write SQL to:
1. Count rows in CUSTOMERS, INVENTORY, SALES
2. Verify every SALES.customer_id exists in CUSTOMERS
3. Verify every SALES.product_id exists in INVENTORY
4. Total of SALES.total_amount"""
        sql = self.prompt_llm(sys_prompt, "Return only SQL")
        self.results['validation_sql'] = sql or ""

        cur = self.mysql_conn.cursor()
        results=[]
        for stmt in self.results['validation_sql'].split(";"):
            stmt_clean=stmt.replace("```sql","").replace("```","").strip()
            if stmt_clean:
                try:
                    cur.execute(stmt_clean)
                    results.append({"query":stmt_clean,"result":cur.fetchall()})
                except Exception as e:
                    results.append({"query":stmt_clean,"error":str(e)})
        self.results['validation_results']=results
        st.subheader("Validation Results")
        st.json(results)

    def translate_plsql(self):
        path=os.path.join(self.data_dir,"oracle_plsql_procedures.sql")
        if not os.path.exists(path):
            st.info("No PL/SQL file found")
            return
        with open(path) as f: plsql=f.read()
        sys_prompt="Convert Oracle PL/SQL to MySQL stored procedures."
        mysql_code=self.prompt_llm(sys_prompt,plsql)
        self.results['translated_sql']=mysql_code
        st.subheader("Translated PL/SQL")
        st.code(mysql_code, language="sql")

    def generate_bi(self):
        sys_prompt="Write MySQL queries: monthly sales trend, top 5 customers, low stock (<100)."
        sql=self.prompt_llm(sys_prompt,"Return only SQL")
        self.results['bi_sql']=sql
        st.subheader("Generated BI Queries")
        st.code(sql, language="sql")

    def export_report(self):
        md=f"# Migration Report\n\nRun: {datetime.now()}\n\n"
        for k,v in self.results.items():
            if isinstance(v,str):
                md+=f"## {k}\n\n```sql\n{v}\n```\n\n"
            else:
                md+=f"## {k}\n\n{json.dumps(v,indent=2,default=str)}\n\n"
        os.makedirs("output",exist_ok=True)
        outpath=f"output/migration_report_{datetime.now().strftime('%Y%m%d_%H%M')}.md"
        with open(outpath,"w") as f: f.write(md)
        st.success(f"Report saved: {outpath}")

# ---------------- STREAMLIT APP ----------------
st.set_page_config(page_title="GenAI Migration Dashboard", layout="wide")
st.title("🧠 GenAI-Assisted Migration Dashboard")

tab1, tab2 = st.tabs(["⚙️ Migration Pipeline", "📊 BI Dashboard"])

# ----- TAB 1: Pipeline -----
with tab1:
    st.sidebar.header("Database Settings")
    host=st.sidebar.text_input("MySQL Host","localhost")
    user=st.sidebar.text_input("MySQL User","root")
    password=st.sidebar.text_input("Password", type="password")
    database=st.sidebar.text_input("Database","retail_dw")

    st.sidebar.header("Groq Settings")
    groq_key=st.sidebar.text_input("Groq API Key", type="password")
    groq_model=st.sidebar.text_input("Groq Model","llama-3.3-70b-versatile")

    if st.button("🚀 Run Full Migration"):
        pipe=GenAIMigrationPipeline(
            {"host":host,"user":user,"password":password,"database":database},
            groq_key, groq_model
        )
        pipe.check_csv_files()
        pipe.connect_mysql()
        pipe.drop_tables_if_exist()
        pipe.design_schema()
        pipe.import_data()
        pipe.validate_data()
        pipe.translate_plsql()
        pipe.generate_bi()
        pipe.export_report()

# ----- TAB 2: Dashboard -----
with tab2:
    # Paths
    customers_path = os.path.join(DATA_DIR,"CUSTOMERS.csv")
    inventory_path = os.path.join(DATA_DIR,"INVENTORY.csv")
    sales_path = os.path.join(DATA_DIR,"SALES.csv")

    if all(os.path.exists(p) for p in [customers_path, inventory_path, sales_path]):
        customers = pd.read_csv(customers_path)
        inventory = pd.read_csv(inventory_path)
        sales = pd.read_csv(sales_path)

        # Clean & convert types
        if "sale_date" in sales.columns:
            sales["sale_date"] = pd.to_datetime(sales["sale_date"], errors="coerce")
        if "join_date" in customers.columns:
            customers["join_date"] = pd.to_datetime(customers["join_date"], errors="coerce")
        for col in ["total_amount","quantity"]:
            if col in sales.columns:
                sales[col] = pd.to_numeric(sales[col], errors="coerce").fillna(0)
        for col in ["price_per_unit","quantity_in_stock"]:
            if col in inventory.columns:
                inventory[col] = pd.to_numeric(inventory[col], errors="coerce").fillna(0)

        # --- KPIs ---
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("💰 Total Sales", f"{sales['total_amount'].sum():,.2f}")
        c2.metric("👥 Customers", str(customers.shape[0]))
        c3.metric("📦 Products", str(inventory.shape[0]))
        c4.metric("🛒 Transactions", str(sales.shape[0]))

        st.markdown("---")

        # --- Monthly Sales Trend ---
        st.subheader("📈 Monthly Sales Trend")
        df_month = sales.dropna(subset=["sale_date"]).copy()
        if not df_month.empty:
            df_month["month"] = df_month["sale_date"].dt.to_period("M").dt.to_timestamp()
            monthly_sales = df_month.groupby("month")["total_amount"].sum().reset_index()
            fig_sales = px.line(
                monthly_sales, x="month", y="total_amount",
                title="Monthly Sales Trend",
                markers=True,
                labels={"month":"Month","total_amount":"Sales Amount"}
            )
            fig_sales.update_layout(yaxis_tickprefix="$")
            st.plotly_chart(fig_sales, use_container_width=True)

        # --- Top Customers ---
        st.subheader("👑 Top 10 Customers")
        top_customers = sales.groupby("customer_id")["total_amount"].sum().reset_index()
        top_customers = top_customers.merge(customers, on="customer_id", how="left")
        top_customers = top_customers.sort_values("total_amount", ascending=False).head(10)
        fig_customers = px.bar(
            top_customers, x="customer_name", y="total_amount",
            title="Top 10 Customers by Sales",
            labels={"customer_name":"Customer","total_amount":"Sales Amount"},
            text="total_amount"
        )
        fig_customers.update_traces(texttemplate='$%{text:.2f}', textposition='outside')
        st.plotly_chart(fig_customers, use_container_width=True)

        # --- Top Products ---
        st.subheader("🏆 Top 10 Products")
        top_products = sales.groupby("product_id")["total_amount"].sum().reset_index()
        top_products = top_products.merge(inventory, on="product_id", how="left")
        top_products = top_products.sort_values("total_amount", ascending=False).head(10)
        fig_products = px.bar(
            top_products, x="product_name", y="total_amount",
            title="Top 10 Products by Sales",
            labels={"product_name":"Product","total_amount":"Sales Amount"},
            text="total_amount"
        )
        fig_products.update_traces(texttemplate='$%{text:.2f}', textposition='outside')
        st.plotly_chart(fig_products, use_container_width=True)

        # --- Low Stock Table ---
        st.subheader("⚠️ Low Stock Products (<100 units)")
        low_stock = inventory[inventory["quantity_in_stock"] < 100]
        st.dataframe(low_stock, use_container_width=True)
        st.download_button(
            "Download Low Stock CSV",
            low_stock.to_csv(index=False).encode("utf-8"),
            file_name="low_stock.csv"
        )

    else:
        st.warning("⚠️ CSV files not found in data folder.")
