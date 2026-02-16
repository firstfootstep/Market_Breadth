import pandas as pd
import numpy as np
from tradingview_screener import Query, col

# ==========================================
# 1. ส่วนดึงข้อมูล (Data Fetching)
# ==========================================

def get_rs_data():
    print("⏳ กำลังดึงข้อมูลหุ้นไทยเพื่อคำนวณ RS...")
    # เลือก Column ตามที่คุณต้องการ
    columns = [
        'name', 'sector', 'close', 'change', 'volume',
        'EMA10', 'EMA50', 'market_cap_basic',
        'Perf.W', 'Perf.1M', 'Perf.3M', 'Perf.6M', 'Perf.Y',
        'total_revenue_qoq_growth_fq', 'total_revenue_yoy_growth_fq',
        'net_income_qoq_growth_fq', 'net_income_yoy_growth_fq', 'type'
    ]
    
    try:
        q = (Query()
             .select(*columns)
             .set_markets('thailand')
             .limit(3000)
             .get_scanner_data())
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return pd.DataFrame()
    
    if not q:
        return pd.DataFrame()
    
    # --- FIX: แก้ไขตรงนี้ (Handle tuple return) ---
    # ถ้า q เป็น tuple (count, data) ให้เอาตัวที่ 2 (index 1)
    if isinstance(q, tuple):
        data_rows = q[1]
    else:
        data_rows = q
        
    if not data_rows:
        return pd.DataFrame()

    # สร้าง DataFrame จาก list of dictionaries
    df = pd.DataFrame([row['d'] for row in data_rows])
    
    # กรองหุ้น: ตัด -R, -F, ราคา >= 1 บาท, เอาเฉพาะ Stock และ DR
    mask = (
        (~df['name'].str.endswith(('-R', '-F'))) & 
        (df['close'] >= 1) &
        (df['type'].isin(['stock', 'dr']))
    )
    df = df[mask].copy()
    
    # คำนวณ RS Score
    weights = {'Perf.W': 0.10, 'Perf.1M': 0.20, 'Perf.3M': 0.30, 'Perf.6M': 0.25, 'Perf.Y': 0.15}
    df['rs_weight'] = 0
    for col_name, w in weights.items():
        if col_name in df.columns:
            df['rs_weight'] += df[col_name].fillna(0) * w
            
    df['rs_rank'] = df['rs_weight'].rank(pct=True) * 100
    
    # แบ่งกลุ่ม (Bins)
    bins = [0, 80, 85, 90, 95, 101]
    labels = ['RS Below 80', 'RS80-85', 'RS85-90', 'RS90-95', 'RS95-100']
    df['rs_category'] = pd.cut(df['rs_rank'], bins=bins, labels=labels)
    
    print(f"✅ ดึงข้อมูลสำเร็จ: {len(df)} หุ้น")
    return df

def get_dr_data(df_all):
    print("⏳ กำลังทำข้อมูล DR...")
    # ดึง Mapping จาก Google Sheet
    url = "https://docs.google.com/spreadsheets/d/1bf6c9tJ4LwZixr7q79C1AEiDmkmoFgt7JFS3eWQTVMc/export?format=csv&gid=1600792422"
    try:
        df_map = pd.read_csv(url)
    except:
        print("⚠️ ไม่สามารถดึง Google Sheet Mapping ได้ (ใช้ข้อมูลดิบแทน)")
        df_map = pd.DataFrame(columns=['Symbol', 'Underlying', 'Country'])

    # กรองเฉพาะ DR
    if df_all.empty:
        return pd.DataFrame()

    df_dr = df_all[df_all['type'] == 'dr'].copy()
    
    # Merge
    if not df_map.empty:
        df_merged = pd.merge(
            df_dr, df_map[['Symbol', 'Underlying', 'Country']],
            left_on='name', right_on='Symbol', how='left'
        )
    else:
        df_merged = df_dr
        df_merged['Country'] = 'Unknown'
        df_merged['Underlying'] = '-'
    
    # เติมค่าว่าง
    if 'Country' in df_merged.columns:
        df_merged['Country'] = df_merged['Country'].fillna('Unknown')
    if 'Underlying' in df_merged.columns:
        df_merged['Underlying'] = df_merged['Underlying'].fillna('-')
    
    return df_merged

# ==========================================
# 2. ส่วนจัดรูปแบบ (Formatting)
# ==========================================

def format_df(df, cols_to_keep, rename_dict):
    # เลือก Column (เฉพาะที่มีอยู่จริง กัน Error)
    valid_cols = [c for c in cols_to_keep if c in df.columns]
    df_out = df[valid_cols].copy()
    
    # ปัดทศนิยม 2 ตำแหน่ง
    for c in df_out.columns:
        if df_out[c].dtype in ['float64', 'float32']:
            df_out[c] = df_out[c].apply(lambda x: f"{x:,.2f}" if pd.notnull(x) else "-")
            
    # เปลี่ยนชื่อ Column
    df_out = df_out.rename(columns=rename_dict)
    return df_out

# ==========================================
# 3. สร้างไฟล์ HTML (Main)
# ==========================================

def main():
    # 1. เตรียมข้อมูล
    df_raw = get_rs_data()
    
    if df_raw.empty:
        print("❌ ไม่พบข้อมูลหุ้น จบการทำงาน")
        return

    # --- ตาราง RS Ranking ---
    rs_cols = [
        'name', 'sector', 'close', 'change', 'volume', 
        'EMA10', 'EMA50', 'rs_rank', 'rs_category',
        'total_revenue_qoq_growth_fq', 'total_revenue_yoy_growth_fq',
        'net_income_qoq_growth_fq', 'net_income_yoy_growth_fq'
    ]
    rs_rename = {
        'name': 'Symbol', 'sector': 'Sector', 'close': 'Price', 'change': '%Change',
        'volume': 'Volume', 'rs_rank': 'RS Score', 'rs_category': 'Group',
        'total_revenue_qoq_growth_fq': 'Rev QoQ%', 'total_revenue_yoy_growth_fq': 'Rev YoY%',
        'net_income_qoq_growth_fq': 'Net QoQ%', 'net_income_yoy_growth_fq': 'Net YoY%'
    }
    
    df_rs = df_raw.sort_values('rs_rank', ascending=False)
    df_rs_final = format_df(df_rs, rs_cols, rs_rename)
    
    # --- ตาราง DR Scan ---
    df_dr_raw = get_dr_data(df_raw)
    dr_cols = ['name', 'Underlying', 'sector', 'Country', 'close', 'change', 'Perf.1M', 'Perf.3M']
    dr_rename = {
        'name': 'Symbol', 'sector': 'Sector', 'close': 'Price', 'change': '%Change',
        'Perf.1M': '1M %', 'Perf.3M': '3M %'
    }
    
    if not df_dr_raw.empty:
        df_dr_final = format_df(df_dr_raw, dr_cols, dr_rename)
        table_dr_html = df_dr_final.to_html(index=False, table_id="drTable", classes="display compact", border=0)
    else:
        table_dr_html = "<p>ไม่พบข้อมูล DR</p>"

    # 2. แปลงเป็น HTML Table (Clean)
    table_rs_html = df_rs_final.to_html(index=False, table_id="rsTable", classes="display compact", border=0)

    # 3. เขียนไฟล์ HTML Template (ฝัง JS DataTables เพื่อให้เสิช/กรองได้)
    html_content = f"""
    <!DOCTYPE html>
    <html lang="th">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>My Stock Dashboard</title>
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.4/css/jquery.dataTables.min.css">
        <link href="https://fonts.googleapis.com/css2?family=Sarabun:wght@400;600&display=swap" rel="stylesheet">
        
        <style>
            body {{ font-family: 'Sarabun', sans-serif; background-color: #f4f6f9; margin: 0; padding: 20px; }}
            .container {{ max-width: 1400px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
            h1, h2 {{ color: #2c3e50; }}
            
            /* Tabs Style */
            .tab {{ overflow: hidden; border-bottom: 1px solid #ccc; margin-bottom: 20px; }}
            .tab button {{ background-color: inherit; float: left; border: none; outline: none; cursor: pointer; padding: 14px 16px; transition: 0.3s; font-size: 16px; font-weight: bold; color: #555; }}
            .tab button:hover {{ background-color: #ddd; }}
            .tab button.active {{ background-color: #3498db; color: white; }}
            .tabcontent {{ display: none; animation: fadeEffect 1s; }}
            @keyframes fadeEffect {{ from {{opacity: 0;}} to {{opacity: 1;}} }}

            /* Table Style */
            table.dataTable thead th {{ background-color: #34495e; color: white; text-align: center !important; }}
            table.dataTable tbody td {{ text-align: center !important; }}
            
            /* Filter Dropdown */
            .filter-box {{ margin-bottom: 15px; padding: 10px; background: #ecf0f1; border-radius: 5px; }}
        </style>
    </head>
    <body>

    <div class="container">
        <h1 style="text-align: center;">📊 My Stock Dashboard</h1>
        <p style="text-align: center; color: gray;">อัปเดตข้อมูลอัตโนมัติ: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}</p>

        <div class="tab">
            <button class="tablinks" onclick="openTab(event, 'MarketBreadth')" id="defaultOpen">🏠 Market Breadth</button>
            <button class="tablinks" onclick="openTab(event, 'RSRanking')">🏆 RS Ranking</button>
            <button class="tablinks" onclick="openTab(event, 'DRScan')">🌍 DR Global Scan</button>
        </div>

        <div id="MarketBreadth" class="tabcontent">
            <div style="text-align: center;">
                <h2>ภาวะตลาด (Market Breadth)</h2>
                <img src="dashboard_set.png" alt="Market Breadth Chart" style="max-width: 100%; height: auto; border: 1px solid #ddd; border-radius: 5px;">
                <p><i>*รูปนี้อัปเดตตามไฟล์ dashboard_set.png ใน GitHub</i></p>
            </div>
        </div>

        <div id="RSRanking" class="tabcontent">
            <h2>🏆 RS Ranking (Stock & DR)</h2>
            <div class="filter-box">
                <b>🔍 Filter RS Group: </b> 
                <select id="rsFilter" style="padding: 5px;">
                    <option value="">Show All</option>
                    <option value="RS95-100">RS95-100</option>
                    <option value="RS90-95">RS90-95</option>
                    <option value="RS85-90">RS85-90</option>
                    <option value="RS80-85">RS80-85</option>
                    <option value="RS Below 80">RS Below 80</option>
                </select>
            </div>
            {table_rs_html}
        </div>

        <div id="DRScan" class="tabcontent">
            <h2>🌍 DR Global Scan</h2>
            <div class="filter-box">
                <b>🔍 Filter Country: </b>
                <select id="countryFilter" style="padding: 5px;">
                    <option value="">Show All</option>
                    </select>
            </div>
            {table_dr_html}
        </div>
    </div>

    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.4/js/jquery.dataTables.min.js"></script>
    
    <script>
        // Tab Logic
        function openTab(evt, tabName) {{
            var i, tabcontent, tablinks;
            tabcontent = document.getElementsByClassName("tabcontent");
            for (i = 0; i < tabcontent.length; i++) {{ tabcontent[i].style.display = "none"; }}
            tablinks = document.getElementsByClassName("tablinks");
            for (i = 0; i < tablinks.length; i++) {{ tablinks[i].className = tablinks[i].className.replace(" active", ""); }}
            document.getElementById(tabName).style.display = "block";
            evt.currentTarget.className += " active";
        }}
        document.getElementById("defaultOpen").click();

        $(document).ready(function() {{
            // 1. Setup RS Table
            var tableRS = $('#rsTable').DataTable({{
                "pageLength": 25,
                "order": [[ 5, "desc" ]] // Sort by RS Score (col index 5) default
            }});
            
            // Custom Filter for RS
            $('#rsFilter').on('change', function() {{
                tableRS.column(6).search(this.value).draw(); // RS Group is at col index 6
            }});

            // 2. Setup DR Table
            var tableDR = $('#drTable').DataTable({{ "pageLength": 25 }});
            
            // Populate Country Filter automatically
            var countries = tableDR.column(3).data().unique().sort(); // Country is at col index 3
            $.each(countries, function(index, value) {{
                if(value) $('#countryFilter').append('<option value="'+value+'">'+value+'</option>');
            }});
            
            // Custom Filter for Country
            $('#countryFilter').on('change', function() {{
                tableDR.column(3).search(this.value).draw();
            }});
        }});
    </script>

    </body>
    </html>
    """
    
    # Save File
    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html_content)
    print("✅ สร้างไฟล์ index.html เรียบร้อย!")

if __name__ == "__main__":
    main()
