# -*- coding: utf-8 -*-
"""Build an ATS-friendly .docx CV for Sahatsawat Thongma.
Single column, native text (clean Thai + English), standard headings.
Run: python build_cv_docx.py  ->  cv-sahatsawat-thongma.docx
"""
from docx import Document
from docx.shared import Pt, RGBColor, Mm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.oxml import OxmlElement

INK = RGBColor(0x1A, 0x1A, 0x1A)
BLUE = RGBColor(0x00, 0x43, 0xCE)
GREY = RGBColor(0x55, 0x55, 0x55)
FONT = "Tahoma"  # Thai-capable, ubiquitous, ATS-safe

doc = Document()

# ---- base style + page margins ----
normal = doc.styles["Normal"]
normal.font.name = FONT
normal.font.size = Pt(10.5)
normal.font.color.rgb = INK
# ensure Thai (complex-script) also uses the same font
rpr = normal.element.get_or_add_rPr()
rfonts = rpr.get_or_add_rFonts()
rfonts.set(qn("w:cs"), FONT)
rfonts.set(qn("w:ascii"), FONT)
rfonts.set(qn("w:hAnsi"), FONT)
pf = normal.paragraph_format
pf.space_after = Pt(4)
pf.line_spacing = 1.12

for s in doc.sections:
    s.top_margin = Mm(15)
    s.bottom_margin = Mm(15)
    s.left_margin = Mm(16)
    s.right_margin = Mm(16)


def _set_run_fonts(run):
    rpr = run._element.get_or_add_rPr()
    rf = rpr.get_or_add_rFonts()
    rf.set(qn("w:ascii"), FONT)
    rf.set(qn("w:hAnsi"), FONT)
    rf.set(qn("w:cs"), FONT)


def add_run(p, text, bold=False, size=None, color=None):
    r = p.add_run(text)
    r.bold = bold
    if size:
        r.font.size = Pt(size)
    r.font.color.rgb = color if color else INK
    _set_run_fonts(r)
    return r


def para(space_after=4, space_before=0, align=None):
    p = doc.add_paragraph()
    p.paragraph_format.space_after = Pt(space_after)
    p.paragraph_format.space_before = Pt(space_before)
    if align:
        p.alignment = align
    return p


def bottom_border(p):
    """Add a bottom border (rule) under a paragraph -> section underline."""
    pPr = p._p.get_or_add_pPr()
    pbdr = OxmlElement("w:pBdr")
    bottom = OxmlElement("w:bottom")
    bottom.set(qn("w:val"), "single")
    bottom.set(qn("w:sz"), "10")
    bottom.set(qn("w:space"), "2")
    bottom.set(qn("w:color"), "1A1A1A")
    pbdr.append(bottom)
    pPr.append(pbdr)


def section_heading(text):
    p = para(space_before=10, space_after=5)
    add_run(p, text.upper(), bold=True, size=11, color=INK)
    bottom_border(p)


def bullet(runs):
    """runs: list of (text, bold) tuples."""
    p = doc.add_paragraph(style="List Bullet")
    p.paragraph_format.space_after = Pt(3)
    p.paragraph_format.line_spacing = 1.12
    for text, bold in runs:
        add_run(p, text, bold=bold)
    return p


# ===================== HEADER =====================
p = para(space_after=1)
add_run(p, "สหัสวรรษ ทองมา (Sahatsawat Thongma)", bold=True, size=20, color=INK)

p = para(space_after=4)
add_run(p, "Data Engineer", bold=True, size=12.5, color=BLUE)

p = para(space_after=1)
add_run(p, "sahatsawat.thongma@gmail.com  |  +66 83-691-4296  |  พันท้ายนรสิงห์, สมุทรสาคร (เปิดรับ remote / hybrid)", size=9.5, color=GREY)
p = para(space_after=2)
add_run(p, "github.com/sthongma  |  sthongma.github.io/portfolio/resume.html", size=9.5, color=GREY)

# ===================== SUMMARY =====================
section_heading("Professional Summary")
p = para(align=WD_ALIGN_PARAGRAPH.JUSTIFY)
add_run(p,
    "Data Engineer ประสบการณ์ 4 ปีในสาย e-commerce — ออกแบบ สร้าง และดูแล data platform "
    "เต็มระบบในตัวคนเดียว (end-to-end) ให้บริษัท e-commerce ระดับร้อยล้านบาทต่อเดือน ปัจจุบันดูแล "
    "PostgreSQL data warehouse ขนาด 59 GB ที่ ingest 100K+ records/วัน ผ่าน ELT pipelines ด้วย "
    "178 dbt models ใต้ 420 data tests บน medallion architecture 5 ชั้น + dimensional (Kimball) modeling "
    "orchestrate ด้วย Apache Airflow ใน Docker ปี 2026 ปิด migration ใหญ่ 2 ตัวจบในไตรมาสเดียว "
    "(Azure SQL Server → PostgreSQL on-prem และ Azure Data Factory → Airflow) ปัจจุบันโฟกัสที่ "
    "data reliability — จับเคสที่ pipeline เขียวแต่ข้อมูลหายเงียบ ๆ "
    "งานที่สร้างช่วยให้ทีม 25 คนประหยัดเวลารวม 25–75 ชม./วัน")

# ===================== SKILLS =====================
section_heading("Technical Skills")
skills = [
    ("Data Warehouse: ", "PostgreSQL, SQL Server, T-SQL, dbt, Medallion architecture, Kimball / star schema, SCD type 2"),
    ("Data Engineering: ", "Python, Pandas, ETL / ELT, Apache Airflow, Docker, SQLAlchemy"),
    ("Reliability & Observability: ", "dbt tests & data contracts, source freshness SLAs, Elementary Data, pipeline monitoring & alerting, backup & disaster recovery, PostgreSQL administration"),
    ("App Development: ", "FastAPI, React / TypeScript, REST / JSON API"),
    ("Ingestion & BI: ", "Playwright (RPA), Power BI, Power Query"),
    ("Cloud — Azure: ", "Blob Storage, App Service, Data Factory (ADF), Azure SQL, Document Intelligence"),
    ("Developer Tooling: ", "Git, GitHub Actions, AI pair-programming (Claude Code, Cursor)"),
]
for label, rest in skills:
    p = para(space_after=3)
    add_run(p, label, bold=True)
    add_run(p, rest)

# ===================== EXPERIENCE =====================
section_heading("Professional Experience")

# --- EP Asia Group ---
p = para(space_after=0)
add_run(p, "EP Asia Group", bold=True, size=11)
add_run(p, "          May 2025 – Present", size=9.5, color=GREY)
p = para(space_after=3)
add_run(p, "Data Engineer — E-Commerce Data Platform", bold=True, size=10, color=BLUE)

bullet([("สร้าง ", False), ("data warehouse ตัวแรกของบริษัท", True),
        (" — แทน Google Sheets/Excel ที่กระจัดกระจาย ให้ทีม 25+ คน ประหยัดเวลารวมทั้งทีม ", False),
        ("25–75 ชั่วโมง/วัน", True), (" และจบปัญหา reconcile ตัวเลขข้าม spreadsheet", False)])

bullet([("ออกแบบและสร้าง ", False), ("platform ทั้งระบบ", True),
        (" — ", False), ("178 dbt models", True),
        (" บน medallion architecture 5 ชั้น (bronze → staging → silver → intermediate → gold) พร้อม ", False),
        ("420 data tests", True), (", source-freshness checks และ ", False),
        ("SCD type-2 dimensional modeling", True), ("; ป้อนด้วย ", False),
        ("Python + Playwright ingestion framework", True),
        (" ที่เขียนเองมาแทน Power Automate Desktop 40 flows เดิม orchestrate ทุกคืนด้วย ", False),
        ("Apache Airflow ใน Docker", True), ("; ingest ", False), ("100K+ records/วัน", True),
        (" จาก 8 source systems แล้วเสิร์ฟ ", False), ("FastAPI + React dashboards", True),
        (" ให้ทีม exec, procurement, warehouse และ fulfillment พร้อมทาง self-service แบบ read-only ให้คนที่ไม่ใช่สาย tech ดึงเลขเองได้", False)])

bullet([("ปิด platform migration ใหญ่ 2 ตัวจบในไตรมาสเดียว", True),
        (" — ย้าย warehouse จาก ", False), ("Azure SQL Server → self-hosted PostgreSQL", True),
        (" (dbt-sqlserver → dbt-postgres) และ", False),
        ("ปลดระวาง Azure Data Factory ทั้ง 46 pipelines ที่เคยสร้างเอง", True),
        (" แทนด้วย Airflow DAGs ที่เขียนเอง ลด cloud cost โดย dashboard ไม่ดาวน์เลย", False)])

bullet([("Data reliability engineering", True),
        (" — ไล่ปิดคลาสความพังแบบ \"pipeline เขียวแต่ข้อมูลหาย\": freshness canary ที่วัดเทียบ source สดแทนการเชื่อ exit code และ value-presence gate ที่บังคับเป็น hard failure; ", False),
        ("กู้เลขพัสดุคืน 23,677 เลข", True), (" ที่กฎ dedup ลบทิ้งไปเงียบ ๆ และแก้ที่รากจน ", False),
        ("28.5% ของยอดขายเดือนหนึ่งที่ไม่ติดชื่อร้านเหลือศูนย์", True),
        (" พร้อม test กันไม่ให้กลับมาโดยไม่มีใครรู้", False)])

bullet([("ทำ ", False), ("backup ตัวแรกของ warehouse ขนาด 59 GB ที่เดิมไม่เคยมี backup เลย", True),
        (" — nightly pg_dump stream ขึ้น Azure Blob พร้อม verification task แยกและ retention 35 วัน; และย้ายเส้นทางไฟล์ของ bronze จาก OneDrive ไปเป็น ", False),
        ("Azure Blob data lake", True), (" ตัดรากของ ingestion failure ทั้งคลาส", False)])

bullet([("แทนที่ vendor WMS ด้วย custom apps", True),
        (" (WMS, barcode scanner, invoice OCR) ที่ป้อนข้อมูลเข้า warehouse — พนักงานคลัง (รวมที่ไม่ใช้ภาษาไทย) เรียนรู้ได้ใน 5 นาที รองรับ ", False),
        ("1,000+ orders/วัน", True), (" พร้อม audit trail เต็มรูปแบบ; ", False),
        ("ดูแลระบบ production ทั้ง 7 ตัวคนเดียว", True),
        (" ตั้งแต่ architecture, build, deploy ไปจนถึง on-call", False)])

# --- Traveler ---
p = para(space_before=8, space_after=0)
add_run(p, "Traveler Co., Ltd.", bold=True, size=11)
add_run(p, "          2022 – 2025", size=9.5, color=GREY)
p = para(space_after=3)
add_run(p, "Accountant — ERP & BI Analyst", bold=True, size=10, color=BLUE)

bullet([("วาง ", False), ("Ecount ERP master data ตั้งแต่ศูนย์", True),
        (" — กำหนด chart of accounts, customer/vendor master และมาตรฐาน SKU mapping ข้าม marketplaces + ERP ให้เป็น single source of truth ของ catalog ปิดปัญหา SKU mapping กำกวมที่ทำให้สต็อกไม่ตรงกันข้ามช่องทาง", False)])
bullet([("สร้าง ", False), ("Power BI reports", True), (" และใช้ ", False), ("Power Query", True),
        (" reconcile ข้อมูล ERP ระหว่างทีม sales และ finance — สร้าง monthly reporting ที่สม่ำเสมอ ปิด recurring discrepancy ที่เกิดซ้ำ", False)])

# ===================== PROJECT =====================
section_heading("Selected Project")
p = para(space_after=2)
add_run(p, "E-Commerce Data Pipeline & Analytics Platform — built & operated end-to-end (solo)", bold=True)
p = para(space_after=3)
add_run(p, "59 GB PostgreSQL warehouse · 10.6M rows rebuilt nightly · 100K+ rows/day · 1,000+ orders/day · 178 dbt models · 420 data tests · 11 Airflow DAGs · 32 file types", size=9.5, color=GREY)
p = para()
add_run(p, "Data platform เต็มระบบ — ดึง raw data จาก 8 source systems (marketplaces, OMS, internal apps, logistics) "
           "ผ่าน Python + Playwright และ direct database pull ลงใน Azure Blob data lake แล้วเข้า warehouse แบบ medallion 5 ชั้น "
           "บน PostgreSQL orchestrate ด้วย Apache Airflow ใน Docker "
           "แล้วเสิร์ฟผ่าน FastAPI + React dashboard (real-time KPIs, price alerts, procurement, drilldowns) "
           "แทนการที่เคยมีคนต้องนั่งโหลดและ refresh ไฟล์ด้วยมือทุกเช้า")

# ===================== EDUCATION =====================
section_heading("Education")
p = para(space_after=0, space_before=2)
add_run(p, "วิศวกรรมศาสตรบัณฑิต (วศ.บ.) สาขาวิศวกรรมคอมพิวเตอร์ — กำลังศึกษา", bold=True)
p = para(space_after=4)
add_run(p, "มหาวิทยาลัยเอเชียอาคเนย์ (Southeast Asia University) · ระบบการศึกษาทางไกลทางอินเทอร์เน็ต · ส.ค. 2026 – คาดว่าจบปี 2029 · เรียนคู่กับงานประจำ (เข้าเรียนสดช่วงเย็นวันธรรมดาและวันหยุด)", size=9.5, color=GREY)
p = para(space_after=0, space_before=2)
add_run(p, "มัธยมศึกษาตอนปลาย (ม.6) — สายวิทย์-คณิต (Science–Math)", bold=True)
p = para(space_after=2)
add_run(p, "โรงเรียนเตรียมอุดมศึกษาพัฒนาการ อุดรธานี (Triam Udom Suksa Pattanakan Udon Thani) · จบปี 2562 / 2019", size=9.5, color=GREY)

# ===================== CERTIFICATIONS =====================
section_heading("Certifications")
edu = [
    ("Power BI & Excel for Data Analysis",
     "9Expert Online Training — Power BI modeling, DAX และ Power Query (ใช้จริงกับ finance & sales reconciliation ที่ Traveler Co.)"),
    ("Python Programming Foundations",
     "Skooldio · Jun 2025 — Core Python สำหรับงานข้อมูล ใช้เป็นฐานของ ETL scripts, FastAPI services และ Airflow operators ใน production"),
]
for title, meta in edu:
    p = para(space_after=0, space_before=2)
    add_run(p, title, bold=True)
    p = para(space_after=2)
    add_run(p, meta, size=9.5, color=GREY)

doc.save("cv-sahatsawat-thongma.docx")
print("saved cv-sahatsawat-thongma.docx")
