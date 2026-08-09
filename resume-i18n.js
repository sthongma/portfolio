window.resumeI18n = {
    en: {
      documentTitle: "Sahatsawat Thongma — Data Engineer",
      htmlLang: "en",
      name: "Sahatsawat Thongma",
      roleTitle: "Data Engineer",
      location: "Samut Sakhon, Thailand",
      tabs: ["Resume", "Projects"],
      sectionSummary: "Summary",
      summary: [
        "<strong>Data engineer building production data platforms.</strong> 4 years in e-commerce, running a <strong>59 GB PostgreSQL warehouse</strong> — 178 dbt models under 420 data tests, medallion + dimensional modeling, 100K+ records/day — for a nine-figure monthly-revenue business. In 2026 I completed two platform migrations: the warehouse off Azure SQL Server onto self-hosted PostgreSQL, and orchestration off Azure Data Factory onto <strong>Apache Airflow in Docker</strong>. Current focus is <strong>data reliability</strong> — catching the failure mode where a pipeline runs green while the data silently goes missing. Stack: <strong>dbt · Airflow · PostgreSQL · Python · Azure · FastAPI/React</strong>. Builds for non-technical business users — warehouse, procurement, exec."
      ],
      sectionExperience: "Experience",
      epPeriod: "May 2025 – Present",
      epRole: "Data Engineer &nbsp;·&nbsp; E-Commerce Data Platform",
      epBullets: [
        "<strong>Built the company's first data warehouse</strong> — replaced scattered Google Sheets/Excel for 25+ staff, saving <strong>25–75 person-hours/day across the team</strong> and ending cross-spreadsheet reconciliation errors.",
        "<strong>Designed and built the whole platform</strong> — <strong>178 dbt models</strong> on a 5-layer medallion architecture with <strong>420 data tests</strong> and SCD-2 dimensional modeling, fed by a <strong>Python + Playwright ingestion framework</strong> I wrote to replace 40 Power Automate Desktop flows, orchestrated nightly by <strong>Apache Airflow in Docker</strong>. Ingests <strong>100K+ records/day</strong> from 8 source systems and serves <strong>FastAPI + React dashboards</strong> to exec, procurement, warehouse and fulfillment — plus a read-only self-service path so non-technical staff pull their own numbers without going through me.",
        "<strong>Completed two platform migrations in a single quarter</strong> — moved the warehouse from <strong>Azure SQL Server to self-hosted PostgreSQL</strong>, and <strong>decommissioned all 46 Azure Data Factory pipelines I had previously built</strong>, replacing them with Airflow DAGs. Cut cloud spend with no dashboard downtime.",
        "<strong>Data reliability engineering</strong> — went after the failure class where <em>the pipeline is green but the data is missing</em>: freshness canaries measured against live source, and value-presence gates enforced as hard failures. <strong>Recovered 23,677 tracking numbers</strong> a dedup rule had silently blanked, and fixed at the root a defect that left <strong>28.5% of a month's sales revenue unattributed to a store</strong> — now zero, with a test that stops it returning unnoticed.",
        "<strong>Shipped the first backup the 59 GB warehouse ever had</strong> — nightly <code>pg_dump</code> streamed to Azure Blob with an independent verification task and 35-day retention. Also moved bronze file transport off OneDrive onto a <strong>Blob data lake</strong>, removing an entire class of ingestion failures at the root.",
        "<strong>Replaced the vendor WMS</strong> with custom apps (WMS, barcode scanner, invoice OCR) — warehouse staff, including non-Thai speakers, learn it in under 5 minutes; <strong>1,000+ orders/day</strong> with a full audit trail. <strong>Operate all 7 production systems single-handed</strong>: architecture, build, deploy and on-call."
      ],
      travelerRoles: [
        "Accountant / ERP &amp; BI Analyst"
      ],
      travelerBullets: [
        "Set up <strong>Ecount ERP master data from scratch</strong> — chart of accounts, customer/vendor master, and SKU mapping standards across marketplaces and ERP, giving finance and sales a single source of truth and ending the SKU ambiguity that had caused stock misalignment across channels.",
        "Built <strong>Power BI reports</strong> and used <strong>Power Query</strong> to reconcile ERP data across sales and finance — established consistent monthly reporting that closed recurring discrepancies between the two teams."
      ],
      projectTaglines: [
        "Full data platform — a 59 GB PostgreSQL warehouse, 178 dbt models under 420 tests, 100K+ rows/day and 1,000+ orders/day across 4 marketplaces.",
        "Custom WMS that replaced the previous third-party system — interactive visual map, foreign staff trained in under 5 min, full stock audit trail.",
        "Real-time multi-user barcode scanning system with workflow enforcement.",
        "AI-assisted invoice processing that reads Thai e-commerce PDF invoices and fills structured records automatically."
      ],
      projectBullets: [
        [
          "Ingests from <strong>8 source systems</strong> (Shopee, Lazada, TikTok, Facebook, JST OMS, DHL, Flash Express, internal WMS/BSN) via a <strong>Python + Playwright RPA framework</strong> and direct database pulls, landing in an <strong>Azure Blob data lake</strong> → a 5-layer <strong>dbt medallion</strong> warehouse — replacing a daily manual download routine that one person used to do every morning.",
          "Orchestrated by <strong>Apache Airflow in Docker</strong> — 11 DAGs with custom Bronze/dbt operators, <strong>420 dbt data tests</strong>, source-freshness checks, and <strong>Telegram alerts</strong> that fire on failure <em>and</em> on runs that finish green but moved no data.",
          "<strong>Migrated off two platforms in one quarter</strong> — the warehouse from Azure SQL Server to self-hosted PostgreSQL, and the orchestration layer from 46 Azure Data Factory pipelines to Airflow DAGs, with no dashboard downtime.",
          "<strong>Reliability &amp; DR</strong> — freshness canaries measured against live source, value-presence gates enforced as hard failures, and the first disaster-recovery backup the warehouse ever had: nightly <code>pg_dump</code> to Blob, independently verified, 35-day retention."
        ],
        [
          "<strong>Replaced the previous WMS</strong> — staff including foreign workers learn it in under 5 minutes instead of weeks, and stock movements are traceable for the first time (full audit trail per item).",
          "<strong>Interactive warehouse grid map</strong> with 4-level color-coded stock states that instantly surface low, incomplete or misplaced slots — plus a 4-type task workflow (pick, count, move, receive), mobile barcode/QR scanning, and a bilingual Thai/English UI."
        ],
        [
          "Processes concurrent-user scans with validation and duplicate detection, and enforces workflow sequences through job dependency rules (e.g. \"Release\" before \"Outbound\") — replacing manual logs kept on paper, Google Sheets and Excel."
        ],
        [
          "Uses Azure Document Intelligence to read invoice files and populate structured records automatically — invoice number, dates, amounts, vendor/customer, tax IDs and VAT — with Thai-specific formats (Buddhist Era dates, Thai Baht) and hash-based caching."
        ]
      ],
      sectionSkills: "Technical Skills",
      sectionEducation: "Education",
      educationItems: [
        {
          title: "B.Eng. Computer Engineering &nbsp;<em>(in progress)</em>",
          period: "Southeast Asia University &nbsp;·&nbsp; Aug 2026 – 2029 (expected)",
          degree: "Online degree program, studied alongside full-time work — live evening and weekend classes."
        },
        {
          title: "High School — Science &amp; Mathematics",
          period: "Triam Udom Suksa Pattanakan Udon Thani &nbsp;·&nbsp; 2019"
        }
      ],
      sectionLearning: "Certifications",
      learningItems: [
        {
          title: "Power BI &amp; Excel for Data Analysis",
          period: "9Expert Online Training",
          degree: "Power BI modeling, DAX and Power Query — applied to monthly finance &amp; sales reconciliation at Traveler Co."
        },
        {
          title: "Python Programming Foundations",
          period: "Skooldio &nbsp;·&nbsp; Jun 2025",
          degree: "Core Python for data work — the base for the ETL scripts, FastAPI services and Airflow operators now running in production."
        }
      ],
      demoLabels: {
        sales: "Sales Insights demo",
        fulfillment: "Fulfillment demo",
        wms: "Warehouse map demo",
        bsn: "Barcode scanner demo"
      }
    },
    th: {
      documentTitle: "สหัสวรรษ ทองมา — Data Engineer",
      htmlLang: "th",
      name: "สหัสวรรษ ทองมา",
      roleTitle: "Data Engineer",
      location: "สมุทรสาคร, ประเทศไทย",
      tabs: ["Resume", "Projects"],
      sectionSummary: "เกี่ยวกับผม",
      summary: [
        "<strong>Data engineer สร้าง production data platforms</strong> — ประสบการณ์ 4 ปี ในสาย e-commerce ดูแล <strong>PostgreSQL warehouse ขนาด 59 GB</strong> — 178 dbt models ใต้ 420 data tests, medallion + dimensional modeling, ingest 100K+ records/วัน — ให้บริษัทระดับร้อยล้านบาทต่อเดือน · ปี 2026 ปิด migration ใหญ่ 2 ตัว: ย้าย warehouse ออกจาก Azure SQL Server มาอยู่บน PostgreSQL on-prem และย้าย orchestration ออกจาก Azure Data Factory มาเป็น <strong>Apache Airflow ใน Docker</strong> · ตอนนี้โฟกัสที่ <strong>data reliability</strong> — จับเคสที่ pipeline เขียวแต่ข้อมูลหายเงียบ ๆ · Stack: <strong>dbt · Airflow · PostgreSQL · Python · Azure · FastAPI/React</strong> สร้างให้ user สายธุรกิจ (คลัง จัดซื้อ ผู้บริหาร)"
      ],
      sectionExperience: "ประสบการณ์ทำงาน",
      epPeriod: "พ.ค. 2025 – ปัจจุบัน",
      epRole: "Data Engineer &nbsp;·&nbsp; E-Commerce Data Platform",
      epBullets: [
        "<strong>สร้าง data warehouse ตัวแรกของบริษัท</strong> — แทน Google Sheets/Excel ที่กระจัดกระจาย ให้ทีม 25+ คน ประหยัดเวลารวมทั้งทีม <strong>25–75 ชั่วโมง/วัน</strong> และจบปัญหา reconcile ตัวเลขข้าม spreadsheet",
        "<strong>ออกแบบและสร้าง platform ทั้งระบบ</strong> — <strong>178 dbt models</strong> บน medallion architecture 5 ชั้น พร้อม <strong>420 data tests</strong> และ SCD-2 dimensional modeling · ป้อนด้วย <strong>Python + Playwright ingestion framework</strong> ที่เขียนเองมาแทน Power Automate Desktop 40 flows เดิม · orchestrate ทุกคืนด้วย <strong>Apache Airflow ใน Docker</strong> · ingest <strong>100K+ records/วัน</strong> จาก 8 source systems แล้วเสิร์ฟ <strong>FastAPI + React dashboards</strong> ให้ผู้บริหาร จัดซื้อ คลัง และ fulfillment พร้อมทาง self-service แบบ read-only ให้คนที่ไม่ใช่สาย tech ดึงเลขเองได้โดยไม่ต้องผ่านเรา",
        "<strong>ปิด platform migration ใหญ่ 2 ตัวจบในไตรมาสเดียว</strong> — ย้าย warehouse จาก <strong>Azure SQL Server → PostgreSQL on-prem</strong> และ<strong>ปลดระวาง Azure Data Factory ทั้ง 46 pipelines ที่เคยสร้างเอง</strong> แทนด้วย Airflow DAGs · ลดค่า cloud โดย dashboard ไม่ดาวน์เลย",
        "<strong>Data reliability engineering</strong> — ไล่ปิดคลาสความพังที่ <em>pipeline เขียวแต่ข้อมูลหาย</em>: freshness canary ที่วัดเทียบ source สด และ value-presence gate ที่บังคับเป็น hard failure · <strong>กู้เลขพัสดุคืน 23,677 เลข</strong> ที่กฎ dedup ลบทิ้งไปเงียบ ๆ และแก้ที่รากจน <strong>28.5% ของยอดขายเดือนหนึ่งที่ไม่ติดชื่อร้าน เหลือศูนย์</strong> พร้อม test กันไม่ให้กลับมาโดยไม่มีใครรู้",
        "<strong>ทำ backup ตัวแรกของ warehouse 59 GB ที่ไม่เคยมี backup เลย</strong> — nightly <code>pg_dump</code> stream ขึ้น Azure Blob พร้อม task verify แยกต่างหาก และ retention 35 วัน · และย้ายเส้นทางไฟล์ของ bronze ออกจาก OneDrive มาเป็น <strong>Blob data lake</strong> ตัดรากของ ingestion failure ทั้งคลาส",
        "<strong>เปลี่ยน vendor WMS</strong> เป็นแอปที่เขียนเอง (WMS, barcode scanner, invoice OCR) — พนักงานคลังรวมพนักงานต่างชาติเรียนไม่ถึง 5 นาทีก็ใช้ได้ รองรับ <strong>1,000+ orders/วัน</strong> พร้อมประวัติของทุกชิ้น · <strong>ดูแลระบบ production ทั้ง 7 ตัวคนเดียว</strong> ตั้งแต่ออกแบบ build deploy ไปจนถึง on-call"
      ],
      travelerRoles: [
        "Accountant / ERP &amp; BI Analyst"
      ],
      travelerBullets: [
        "<strong>วาง master data ใน Ecount ERP ตั้งแต่ 0</strong> — chart of accounts, customer/vendor master และ SKU mapping standards ข้าม marketplaces + ERP ทำให้ finance กับ sales มี single source of truth เดียวกัน และตัดปัญหา SKU mapping กำกวมที่เคยทำให้สต็อกไม่ตรงระหว่างช่องทาง",
        "สร้าง <strong>Power BI reports</strong> และใช้ <strong>Power Query</strong> reconcile ข้อมูล ERP ของ sales/finance — ทำให้ report รายเดือนตรงและ stable ปิดปัญหาตัวเลขไม่ตรงกันระหว่างสองทีมที่เคยเกิดประจำ"
      ],
      projectTaglines: [
        "Data platform เต็มระบบ — PostgreSQL warehouse 59 GB, 178 dbt models ใต้ 420 tests, รับ 100K+ rows/วัน และ 1,000+ orders/วัน จาก 4 marketplaces",
        "WMS ที่เขียนเองมาแทนระบบเดิม — แผนที่คลังกดดูของได้ คนงานต่างชาติเรียนไม่ถึง 5 นาทีก็ใช้ได้ track ของได้ทุกการเคลื่อนไหว",
        "ระบบ barcode scanner หลายคนใช้พร้อมกันแบบ real-time พร้อมบังคับลำดับงาน",
        "ระบบ AI อ่านไฟล์ invoice PDF ของ e-commerce ไทย แล้วลงข้อมูลเป็น structured records ให้อัตโนมัติ"
      ],
      projectBullets: [
        [
          "ดึงข้อมูลจาก <strong>8 source systems</strong> (Shopee, Lazada, TikTok, Facebook, JST OMS, DHL, Flash Express, internal WMS/BSN) ผ่าน <strong>Python + Playwright RPA framework</strong> และ direct database pull ลงใน <strong>Azure Blob data lake</strong> → <strong>dbt medallion</strong> warehouse 5 ชั้น — แทนการที่เคยมีคนนั่งโหลดไฟล์ทุกเช้า",
          "orchestrate ด้วย <strong>Apache Airflow ใน Docker</strong> — 11 DAGs พร้อม custom Bronze/dbt operators, <strong>420 dbt data tests</strong>, source-freshness checks และ <strong>Telegram alert</strong> ที่ยิงทั้งตอน pipeline fail <em>และ</em> ตอนที่รันจบเขียวแต่ข้อมูลไม่ขยับ",
          "<strong>ย้ายออกจาก 2 platform ในไตรมาสเดียว</strong> — warehouse จาก Azure SQL Server → PostgreSQL on-prem และ orchestration จาก Azure Data Factory 46 pipelines → Airflow DAGs โดย dashboard ไม่ดาวน์เลย",
          "<strong>Reliability &amp; DR</strong> — freshness canary ที่วัดเทียบ source สด, value-presence gate ที่บังคับเป็น hard failure และ backup ตัวแรกของ warehouse: nightly <code>pg_dump</code> ขึ้น Blob พร้อม verify แยกและ retention 35 วัน"
        ],
        [
          "<strong>มาแทนระบบ WMS เดิม</strong> — จากที่คนงาน (รวมคนงานต่างชาติ) ใช้ยากและสอนเป็นสัปดาห์ เปลี่ยนเป็นเรียนไม่ถึง 5 นาทีก็ใช้ได้ และ track การเคลื่อนไหวของของทุกชิ้นได้ครบ ซึ่งเดิมทำไม่ได้เลย",
          "<strong>Interactive warehouse grid map</strong> แสดง stock 4 ระดับ เห็นจุดที่ของน้อย ไม่ครบ หรือวางไม่ตรงทันที — พร้อม task workflow 4 ประเภท (pick, count, move, receive), mobile barcode/QR scanning และ bilingual UI ไทย/อังกฤษ"
        ],
        [
          "ประมวลผล scan พร้อม validation + duplicate detection สำหรับ concurrent users และ enforce workflow ด้วย job dependency rules (เช่น \"Release\" ก่อน \"Outbound\") — แทนการจดมือในกระดาษ Google Sheet และ Excel"
        ],
        [
          "ใช้ Azure Document Intelligence อ่านไฟล์ invoice แล้วลงข้อมูลเป็น structured records ให้อัตโนมัติ — เลข invoice, วันที่, ยอดเงิน, vendor/customer, เลขผู้เสียภาษี และ VAT — รองรับ format เฉพาะไทย (พ.ศ., เงินบาท) และ cache ด้วย file hash"
        ]
      ],
      sectionSkills: "ทักษะทางเทคนิค",
      sectionEducation: "การศึกษา",
      educationItems: [
        {
          title: "วศ.บ. วิศวกรรมคอมพิวเตอร์ &nbsp;<em>(กำลังศึกษา)</em>",
          period: "มหาวิทยาลัยเอเชียอาคเนย์ &nbsp;·&nbsp; ส.ค. 2026 – 2029 (คาดว่าจบ)",
          degree: "หลักสูตรระบบการศึกษาทางไกลทางอินเทอร์เน็ต เรียนคู่กับงานประจำ — เข้าเรียนสดเย็นวันธรรมดาและวันหยุด"
        },
        {
          title: "มัธยมศึกษาตอนปลาย — สายวิทย์-คณิต",
          period: "โรงเรียนเตรียมอุดมศึกษาพัฒนาการ อุดรธานี &nbsp;·&nbsp; 2562 / 2019"
        }
      ],
      sectionLearning: "ประกาศนียบัตร",
      learningItems: [
        {
          title: "Power BI &amp; Excel สำหรับงาน Data Analysis",
          period: "9Expert Online Training",
          degree: "Power BI modeling, DAX และ Power Query — นำมาใช้จริงในการ reconcile ข้อมูล finance &amp; sales รายเดือนที่ Traveler Co."
        },
        {
          title: "Python Programming Foundations",
          period: "Skooldio &nbsp;·&nbsp; มิ.ย. 2025",
          degree: "Python พื้นฐานสำหรับงาน data — เป็นฐานของ ETL scripts, FastAPI services และ Airflow operators ที่รันอยู่บน production ตอนนี้"
        }
      ],
      demoLabels: {
        sales: "ดู Sales Insights",
        fulfillment: "ดู Fulfillment",
        wms: "ดูแผนที่คลัง",
        bsn: "ดู Barcode scanner"
      }
    }
  };
