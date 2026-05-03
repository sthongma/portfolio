window.resumeI18n = {
    en: {
      documentTitle: "Sahatsawat Thongma — Data Engineer & Solo Builder",
      htmlLang: "en",
      name: "Sahatsawat Thongma",
      roleTitle: "Data Engineer &amp; Solo Builder &nbsp;·&nbsp; E-Commerce Platforms",
      location: "Samut Sakhon, Thailand",
      tabs: ["Resume", "Projects"],
      sectionSummary: "Summary",
      summary: [
        "<strong>Sole data engineer at a 100M+ THB/month e-commerce business</strong> — designed and shipped the entire data platform plus production operational apps (WMS, barcode scanner, OCR) used daily by 25+ staff. AI-augmented development workflow lets me deliver end-to-end systems alone."
      ],
      sectionExperience: "Experience",
      epPeriod: "2025 – Present",
      epRole: "Data Engineer &nbsp;·&nbsp; E-Commerce Data Platform",
      epBullets: [
        "<strong>Built the company's first data warehouse single-handedly</strong> — replaced fragmented Google Sheets/Excel files (where staff spent 1–3 hours/day searching and one person manually downloaded files every morning) with a single source of truth that 25+ staff now query directly.",
        "Designed a <strong>dbt medallion architecture</strong> with 193 models across 9 business domains, ingesting <strong>100K+ rows/day (~400 MB/day)</strong> from 4 marketplaces (Shopee, Lazada, TikTok, Facebook) plus internal systems — serving executive, procurement, warehouse, and fulfillment reporting.",
        "Built ingestion stack with Power Automate, Python, and 46 Azure Data Factory pipelines, plus a FastAPI + React config app with AI-assisted column analysis — letting non-developers add new file mappings safely without my involvement.",
        "<strong>Replaced the previous third-party WMS</strong> (which staff struggled to use, with system stock counts that didn't match the physical inventory) with custom WMS, barcode scanner, and Azure-powered invoice OCR — foreign warehouse staff now learn the new system in under 5 minutes; <strong>1,200+ orders/day</strong> flow through with full audit trail."
      ],
      travelerRoles: [
        "Accountant / ERP &amp; BI Analyst &nbsp;·&nbsp; Graphic Designer, E-Commerce"
      ],
      travelerBullets: [
        "Managed Ecount ERP workflows for sales and finance teams, resolved data issues, and trained new users.",
        "Built Power BI reporting and used Power Query, SQL, and Python to reconcile ERP exports for sales and finance.",
        "Created e-commerce product imagery and maintained product/SKU data across marketplace and ERP systems."
      ],
      projectTaglines: [
        "Full data platform serving 9 business domains, 1,200+ orders/day, and 100K+ rows/day across 4 marketplaces.",
        "Custom WMS that replaced the previous third-party system — interactive visual map, foreign staff trained in under 5 min, full stock audit trail.",
        "Real-time multi-user barcode scanning system with workflow enforcement.",
        "AI-assisted invoice processing that reads Thai e-commerce PDF invoices and fills structured records automatically."
      ],
      projectBullets: [
        [
          "Ingests data from 6+ sources (Shopee, Lazada, TikTok, Facebook, OMS, DHL, internal WMS/BSN) → 4-layer medallion architecture with dbt — replaces a daily manual download routine that one person used to do every morning.",
          "<strong>Bronze config web app</strong> (FastAPI + React) — self-service UI for file types, validation rules, and AI-assisted column analysis/mapping suggestions, so non-developers can onboard new sources safely.",
          "Serves <strong>FastAPI + React dashboards</strong> with real-time KPIs, price alerts, procurement tracking, and operational drilldowns — driving daily restock decisions for procurement.",
          "Runs automatically outside business hours with data quality checks and email alerts on failure or stale data."
        ],
        [
          "<strong>Replaced the previous WMS</strong> — staff (including foreign workers) now learn the system in under 5 minutes versus weeks of struggling with the prior tool, and stock movements are traceable for the first time (full audit trail per item).",
          "<strong>Interactive warehouse grid map</strong> (dynamic CSS Grid, server-side) with 4-level color-coded stock states (empty / low / ok / critical) that instantly surface low, incomplete, or misplaced slots — plus annotations for reserved zones and merged-cell support for pallet-sized locations.",
          "Real-time inventory tracking + 4-type task workflow (pick, count, move, receive) with task chat, mobile barcode/QR scanning, and bilingual UI (Thai/English, 736 i18n keys).",
          "19 granular permissions, 21 SQLAlchemy models, 62 Jinja2 templates — deployed on Azure App Service with GitHub Actions CI/CD."
        ],
        [
          "Processes concurrent-user scans with validation + duplicate detection, and enforces workflow sequences through job dependency rules (e.g., \"Release\" before \"Outbound\") — replaces manual paper logs.",
          "Custom notifications + audio alerts, full scan history with filtering and Excel export."
        ],
        [
          "Uses Azure Document Intelligence to read invoice files and automatically populate fields such as invoice number, dates, amounts, vendor/customer, tax IDs, and VAT.",
          "Supports Thai-specific formats (Buddhist Era dates, Thai Baht, UTF-8), caches results by file hash, and exports completed records to branded Excel."
        ]
      ],
      compactProjects: [
        "Generic Excel/CSV-to-SQL Server ETL tool with Tkinter GUI and CLI for non-technical users. Supports Replace and Upsert modes, auto-schema detection, batch processing, column mapping, and metadata tracking. Ships as a Windows installer via PyInstaller.",
        "File sync utility for staging raw data files of any type (Excel, CSV, PDF, images, archives, ...) into the pipeline's landing folders. Features SQLite-backed history tracking for deduplication, per-mapping copy/move modes, depth and date filtering, collision handling, dry-run preview, and progress reporting. Configured through a single CSV so non-developers can update mappings without code changes.",
        "Lightweight Flask service that receives webhook events from DHL (shipment and tracking updates) and persists them to SQL Server via PyODBC — a minimal integration bridge that streams logistics events straight into the warehouse data layer.",
        "Python scraper that collects flash-sale and return data from e-commerce platforms, feeding raw files into the Bronze layer of the main data pipeline."
      ],
      sectionSkills: "Technical Skills",
      sectionEducation: "Education",
      university: "Udon Thani Rajabhat University",
      universityPeriod: "Coursework completed (Year 1)",
      universityDegree: "Mechatronics &amp; Robotics Engineering — left after first year to run an online store full-time, which became the foundation for everything that followed: <strong>graphic design → accounting → data analytics → data engineering</strong>."
    },
    th: {
      documentTitle: "สหัสวรรษ ทองมา — Data Engineer & Solo Builder",
      htmlLang: "th",
      name: "สหัสวรรษ ทองมา",
      roleTitle: "Data Engineer &amp; Solo Builder &nbsp;·&nbsp; สาย E-Commerce",
      location: "สมุทรสาคร, ประเทศไทย",
      tabs: ["Resume", "Projects"],
      sectionSummary: "เกี่ยวกับผม",
      summary: [
        "<strong>Data Engineer คนเดียวที่ดูแลระบบทั้งบริษัท e-commerce ยอดขาย 100 ล้านบาทขึ้นไปต่อเดือน</strong> — ออกแบบและส่งมอบ data platform ครบ พร้อมแอปทำงานจริง (WMS, barcode scanner, OCR) ที่ทีม 25+ คนใช้ทุกวัน ใช้ AI ช่วยพัฒนา ทำให้ส่งของได้คนเดียวครบทั้งระบบ"
      ],
      sectionExperience: "ประสบการณ์ทำงาน",
      epPeriod: "2025 – ปัจจุบัน",
      epRole: "Data Engineer &nbsp;·&nbsp; E-Commerce Data Platform",
      epBullets: [
        "<strong>สร้าง data warehouse ตัวแรกของบริษัทคนเดียว</strong> — แทน Google Sheets/Excel ที่กระจัดกระจาย (เคยต้องค้นหาเอกสาร 1–3 ชั่วโมง/คน/วัน และมีคนนั่งโหลดไฟล์ทุกเช้า) เปลี่ยนเป็นที่เก็บข้อมูลที่เดียว ที่ทีม 25+ คนเปิดดูได้ทันที",
        "ออกแบบ <strong>dbt medallion architecture</strong> 193 models ครอบคลุม 9 business domains รับข้อมูล <strong>100K+ rows/วัน (~400 MB/วัน)</strong> จาก 4 marketplaces (Shopee, Lazada, TikTok, Facebook) และระบบภายใน — เสิร์ฟรายงานให้ผู้บริหาร จัดซื้อ คลังสินค้า และทีม fulfillment",
        "สร้าง ingestion ด้วย Power Automate, Python และ Azure Data Factory 46 pipelines พร้อม FastAPI + React config app ที่มี AI ช่วยวิเคราะห์คอลัมน์ — ทำให้คนที่ไม่ใช่ developer เพิ่ม mapping ใหม่ได้เองอย่างปลอดภัย",
        "<strong>เปลี่ยนระบบ WMS เดิม</strong> (ที่คนใช้ยาก และ stock ในระบบไม่ตรงกับของจริงในคลัง) เป็น WMS, barcode scanner และ invoice OCR ที่เขียนเอง — คนงานคลังต่างชาติเรียนไม่ถึง 5 นาทีก็ใช้ได้ ระบบรองรับ <strong>1,200+ orders/วัน</strong> พร้อมประวัติของทุกชิ้นครบ"
      ],
      travelerRoles: [
        "Accountant / ERP &amp; BI Analyst &nbsp;·&nbsp; Graphic Designer, E-Commerce"
      ],
      travelerBullets: [
        "ดูแล Ecount ERP workflows ให้ทีมขายและการเงิน แก้ปัญหา data และสอน user ใหม่",
        "สร้าง Power BI reporting และใช้ Power Query, SQL, Python reconcile ERP exports สำหรับ sales/finance",
        "ออกแบบรูปสินค้า e-commerce และดูแล product/SKU data ระหว่าง marketplace กับ ERP"
      ],
      projectTaglines: [
        "Data platform เต็มระบบ เสิร์ฟ 9 business domains รับ 1,200+ orders/วัน และ 100K+ rows/วัน จาก 4 marketplaces",
        "WMS ที่เขียนเองมาแทนระบบเดิม — แผนที่คลังกดดูของได้ คนงานต่างชาติเรียนไม่ถึง 5 นาทีก็ใช้ได้ track ของได้ทุกการเคลื่อนไหว",
        "ระบบ barcode scanner หลายคนใช้พร้อมกันแบบ real-time พร้อมบังคับลำดับงาน",
        "ระบบ AI อ่านไฟล์ invoice PDF ของ e-commerce ไทย แล้วลงข้อมูลเป็น structured records ให้อัตโนมัติ"
      ],
      projectBullets: [
        [
          "ดึง raw data จาก 6+ sources (Shopee, Lazada, TikTok, Facebook, OMS, DHL, internal WMS/BSN) → medallion architecture 4 ชั้นด้วย dbt — แทนการที่เคยมีคนนั่งโหลดไฟล์ทุกเช้า",
          "<strong>Bronze config web app</strong> (FastAPI + React) — self-service UI สำหรับ file types, validation rules และ AI-assisted column analysis/mapping suggestions ทำให้คนที่ไม่ใช่ developer เพิ่ม source ใหม่ได้เองอย่างปลอดภัย",
          "Serve <strong>FastAPI + React dashboards</strong> — real-time KPIs, price alerts, procurement tracking และ operational drilldowns ที่ฝ่ายจัดซื้อใช้ตัดสินใจซื้อสินค้าทุกวัน",
          "รันอัตโนมัตินอกเวลาทำงานพร้อม data quality checks และ email alerts เมื่อ pipeline fail หรือข้อมูลค้าง"
        ],
        [
          "<strong>มาแทนระบบ WMS เดิม</strong> — จากที่คนงาน (รวมคนงานต่างชาติ) ใช้ยากและสอนนาน เปลี่ยนเป็นเรียนไม่ถึง 5 นาทีก็ใช้ได้ และ track การเคลื่อนไหวของของทุกชิ้นได้ครบ ซึ่งเดิมทำไม่ได้เลย",
          "<strong>Interactive warehouse grid map</strong> (dynamic CSS Grid, server-side) แสดง stock 4 ระดับ (empty / low / ok / critical) เห็นจุดที่ของน้อย ไม่ครบ หรือวางไม่ตรงทันที — พร้อม annotation สำหรับ reserved zones และ merged-cell support",
          "Real-time inventory tracking + task workflow 4 ประเภท (pick, count, move, receive) พร้อม task chat, mobile barcode/QR scanning และ bilingual UI (ไทย/อังกฤษ, 736 i18n keys)",
          "19 granular permissions, 21 SQLAlchemy models, 62 Jinja2 templates — deploy บน Azure App Service ด้วย GitHub Actions CI/CD"
        ],
        [
          "ประมวลผล scan พร้อม validation + duplicate detection สำหรับ concurrent users และ enforce workflow ด้วย job dependency rules (เช่น \"Release\" ก่อน \"Outbound\") — แทนการจดมือในกระดาษ",
          "Custom notifications + audio alerts, เก็บ scan history ครบพร้อม filter และ Excel export"
        ],
        [
          "ใช้ Azure Document Intelligence อ่านไฟล์ invoice แล้วกรอกข้อมูลให้อัตโนมัติ เช่น เลข invoice, วันที่, ยอดเงิน, vendor/customer, เลขผู้เสียภาษี และ VAT",
          "รองรับ format เฉพาะไทย (พ.ศ., เงินบาท, UTF-8), cache ด้วย file hash เพื่อลดการเรียก API ซ้ำ และ export records ที่ลงข้อมูลแล้วเป็น Excel พร้อม branding"
        ]
      ],
      compactProjects: [
        "ETL tool สำหรับโหลด Excel/CSV เข้า SQL Server พร้อม Tkinter GUI และ CLI ให้ user ที่ไม่ใช่ developer ใช้งานได้ รองรับ Replace และ Upsert mode, auto-schema detection, batch processing, column mapping และ metadata tracking แพ็คเป็น Windows installer ด้วย PyInstaller",
        "File sync utility สำหรับ stage raw data files ทุกประเภท (Excel, CSV, PDF, images, archives, ...) เข้า landing folder ของ pipeline — มี SQLite history tracking กัน duplicate, copy/move modes ต่อ mapping, depth + date filtering, collision handling, dry-run preview และ progress bar config ผ่าน CSV ไฟล์เดียวให้ user ที่ไม่ใช่ developer แก้ source / destination mapping ได้",
        "Flask service เบา ๆ ที่รับ webhook event จาก DHL (shipment / tracking updates) และ persist ลง SQL Server ด้วย PyODBC — เป็น integration bridge ระหว่าง DHL notification กับ warehouse data",
        "Python scraper ที่เก็บ flash-sale และ return data จาก e-commerce platforms ส่ง raw files ต่อเข้า Bronze layer ของ main data pipeline"
      ],
      sectionSkills: "ทักษะทางเทคนิค",
      sectionEducation: "การศึกษา",
      university: "มหาวิทยาลัยราชภัฏอุดรธานี",
      universityPeriod: "เรียนครบรายวิชาชั้นปีที่ 1",
      universityDegree: "วิศวกรรมเมคคาทรอนิกส์และหุ่นยนต์ — ออกหลังจบปี 1 ไปทำร้านค้า online เต็มตัว ซึ่งกลายเป็นจุดเริ่มของทุกอย่างที่ตามมา: <strong>graphic design → accounting → data analytics → data engineering</strong>"
    }
  };
