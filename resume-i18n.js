window.resumeI18n = {
    en: {
      documentTitle: "Sahatsawat Thongma — Data Engineer",
      htmlLang: "en",
      name: "Sahatsawat Thongma",
      roleTitle: "Data Engineer",
      roleTagline: "I build the data systems an e-commerce business runs on — solo, end to end.",
      location: "Samut Sakhon, Thailand",
      tabs: ["Resume", "Projects"],
      sectionSummary: "Summary",
      summary: [
        "<strong>Data engineer who builds systems the whole company actually uses.</strong> 4 years in e-commerce — today I run the entire data platform of a nine-figure monthly-revenue business single-handed. Executives check sales and stock themselves instead of waiting for a spreadsheet, ~40 daily manual jobs now run automatically from 5 a.m., and a team of 30+ gets back <strong>30–90 hours every day</strong>. Current focus: <strong>data reliability</strong> — catching the failure where every job reports success but the numbers are silently wrong, before anyone makes a decision on them. Tools: <strong>dbt · Airflow · PostgreSQL · Python · Azure · FastAPI/React</strong>."
      ],
      sectionExperience: "Experience",
      epPeriod: "May 2025 – Present",
      epRole: "Data Engineer &nbsp;·&nbsp; E-Commerce Data Platform",
      epBullets: [
        "<strong>Built the company's first data warehouse — and the platform around it.</strong> Replaced scattered Google Sheets and Excel for a 30+ person team, giving back <strong>30–90 hours every day</strong>; executives now pull their own sales, stock and delivery numbers instead of waiting for reports, and ~40 daily manual download jobs run themselves from 5 a.m. (dbt, Apache Airflow, PostgreSQL, Python — data from 8 sources incl. Shopee, Lazada, TikTok).",
        "<strong>Caught data failures no one knew were happening — and fixed them at the root.</strong> Recovered <strong>23,677 shipment tracking numbers</strong> the system had silently deleted, and traced <strong>28.5% of a month's sales that were attributed to no store</strong> down to zero — with automatic checks that now raise the alarm before a bad number reaches a business decision (data-reliability tests, freshness monitoring).",
        "<strong>Cut recurring cloud costs and gave the company's data its first safety net.</strong> Moved the warehouse and its scheduling off two paid monthly cloud services onto self-run infrastructure — done in a single quarter with zero dashboard downtime — and shipped the first nightly, independently verified backup the 59 GB warehouse ever had (Azure SQL Server → PostgreSQL, Azure Data Factory → Airflow, pg_dump to Azure Blob).",
        "<strong>Replaced the vendor warehouse system with apps I built</strong> — from first line of code to live on the warehouse floor in <strong>15 days</strong>; staff, including non-Thai speakers, learn it in under 5 minutes, and it handles <strong>1,000+ orders/day</strong> with a full audit trail. I design, build, deploy and stay on call for <strong>all 7 production systems single-handed</strong> (WMS, barcode scanner, invoice OCR — FastAPI/React)."
      ],
      travelerRoles: [
        "Accountant / ERP &amp; BI Analyst"
      ],
      travelerBullets: [
        "<strong>Ended the stock mismatches between sales channels</strong> — set up the company's ERP master data from scratch: chart of accounts, customer/vendor master, and one SKU standard across every marketplace, so finance and sales finally worked from the same numbers (Ecount ERP).",
        "<strong>Closed the recurring gap between sales' numbers and finance's numbers</strong> — built the monthly reports both teams reconcile against, turning a monthly argument into a routine check (Power BI, Power Query)."
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
      roleTagline: "สร้างระบบข้อมูลให้ธุรกิจ e-commerce — คนเดียว ตั้งแต่ต้นจนจบ",
      location: "สมุทรสาคร, ประเทศไทย",
      tabs: ["Resume", "Projects"],
      sectionSummary: "เกี่ยวกับผม",
      summary: [
        "<strong>Data engineer ที่สร้างระบบให้คนทั้งบริษัทได้ใช้จริง</strong> — ประสบการณ์ 4 ปีในสาย e-commerce ปัจจุบันดูแล data platform ทั้งระบบของบริษัทระดับร้อยล้านบาทต่อเดือนคนเดียว · ผู้บริหารดูยอดขาย-สต็อกได้เอง ไม่ต้องรอใครทำ Excel ให้ · งานมือ ~40 งาน/วันหายไป ระบบทำเองตั้งแต่ตีห้า · ทีม 30+ คนได้เวลาคืนวันละ <strong>30–90 ชั่วโมง</strong> · ตอนนี้โฟกัส <strong>data reliability</strong> — จับปัญหาที่ระบบขึ้นว่าสำเร็จแต่ตัวเลขผิดเงียบ ๆ ก่อนที่ใครจะเอาไปตัดสินใจ · Tools: <strong>dbt · Airflow · PostgreSQL · Python · Azure · FastAPI/React</strong>"
      ],
      sectionExperience: "ประสบการณ์ทำงาน",
      epPeriod: "พ.ค. 2025 – ปัจจุบัน",
      epRole: "Data Engineer &nbsp;·&nbsp; E-Commerce Data Platform",
      epBullets: [
        "<strong>สร้าง data warehouse ตัวแรกของบริษัท พร้อม platform รอบตัวมัน</strong> — แทน Google Sheets/Excel ที่กระจัดกระจายให้ทีม 30+ คน คืนเวลาให้ทีมวันละ <strong>30–90 ชั่วโมง</strong> · ผู้บริหารดูยอดขาย สต็อก และงานจัดส่งได้เอง ไม่ต้องรอใครทำรายงานให้ · งานมือ ~40 งาน/วันหายไป ระบบทำเองตั้งแต่ตีห้า (dbt, Apache Airflow, PostgreSQL, Python — ข้อมูลจาก 8 แหล่ง เช่น Shopee, Lazada, TikTok)",
        "<strong>จับปัญหาข้อมูลที่ไม่มีใครรู้ว่ากำลังเกิด แล้วแก้ให้จบที่ราก</strong> — กู้เลขพัสดุคืน <strong>23,677 เลข</strong> ที่ระบบลบทิ้งไปเงียบ ๆ และไล่จนเจอสาเหตุที่ <strong>28.5% ของยอดขายเดือนหนึ่งไม่ติดชื่อร้าน — ตอนนี้เหลือศูนย์</strong> พร้อมวางระบบตรวจอัตโนมัติที่เตือนก่อนตัวเลขผิดจะไปถึงการตัดสินใจ (data-reliability tests, freshness monitoring)",
        "<strong>ลดค่าใช้จ่ายคลาวด์รายเดือน และทำให้ข้อมูลบริษัทมีที่สำรองเป็นครั้งแรก</strong> — ย้าย warehouse และระบบตั้งเวลางานออกจากบริการคลาวด์รายเดือน 2 ตัว มาอยู่บนเครื่องที่ดูแลเอง จบในไตรมาสเดียวโดย dashboard ไม่ดาวน์เลย และทำ backup รายคืนตัวแรกของ warehouse 59 GB พร้อมตรวจสอบผลทุกครั้ง (Azure SQL Server → PostgreSQL, Azure Data Factory → Airflow, pg_dump ขึ้น Azure Blob)",
        "<strong>เปลี่ยนระบบคลังของ vendor เป็นแอปที่เขียนเอง</strong> — จากโค้ดบรรทัดแรกถึงพนักงานใช้หน้างานจริงใน <strong>15 วัน</strong> · พนักงาน (รวมคนไม่ใช้ภาษาไทย) เรียนไม่ถึง 5 นาทีก็ใช้ได้ รองรับ <strong>1,000+ ออเดอร์/วัน</strong> พร้อมประวัติของทุกชิ้น · <strong>ดูแลระบบ production ทั้ง 7 ตัวคนเดียว</strong> ตั้งแต่ออกแบบ สร้าง deploy จนถึง on-call (WMS, barcode scanner, invoice OCR — FastAPI/React)"
      ],
      travelerRoles: [
        "Accountant / ERP &amp; BI Analyst"
      ],
      travelerBullets: [
        "<strong>จบปัญหาสต็อกไม่ตรงกันข้ามช่องทางขาย</strong> — วาง master data ของ ERP ตั้งแต่ศูนย์: ผังบัญชี, ข้อมูลลูกค้า/ผู้ขาย และมาตรฐาน SKU เดียวกันทุก marketplace ให้ finance กับ sales ใช้ตัวเลขชุดเดียวกัน (Ecount ERP)",
        "<strong>ปิดปัญหาตัวเลข sales กับ finance ไม่ตรงกันที่เกิดซ้ำทุกเดือน</strong> — สร้างรายงานรายเดือนกลางที่สองทีมใช้กระทบยอดร่วมกัน (Power BI, Power Query)"
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
