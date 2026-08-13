window.resumeI18n = {
    en: {
      documentTitle: "Sahatsawat Thongma — Data Engineer",
      htmlLang: "en",
      name: "Sahatsawat Thongma",
      roleTitle: "Data Engineer",
      roleTagline: "Data platform · web apps · AI systems — designed, built and operated end to end.",
      location: "Samut Sakhon, Thailand",
      tabs: ["Resume", "Projects · Live Demos"],
      sectionSummary: "Summary",
      summary: [
        "<strong>Data engineer who builds systems the whole company actually uses.</strong> 4 years in e-commerce — today I run the data platform, the warehouse apps and the AI tooling of a business shipping ~2,000 orders a day across 4 marketplaces. Executives check sales and stock themselves instead of waiting for a spreadsheet, and ~40 daily manual jobs now run automatically from 5 a.m. Current focus: <strong>data reliability</strong> — catching the failure where every job reports success but the numbers are silently wrong, before anyone makes a decision on them. Tools: <strong>dbt · Airflow · PostgreSQL · Python · Azure · FastAPI/React</strong>."
      ],
      sectionExperience: "Experience",
      epPeriod: "May 2025 – Present",
      epRole: "Data Engineer &nbsp;·&nbsp; E-Commerce Data Platform",
      epBullets: [
        "<strong>Made the KPI executives read every day trustworthy for the first time.</strong> On-time fulfilment was reported as missing target for nearly all of 2025 — while the ERP insisted 97% had shipped — because the model judged months before its evidence feed existed, and mixed order types with completely different delivery promises into one denominator. I rebuilt how it is measured: today it reads <strong>94.6% across 323,134 orders</strong>, and the misses — <strong>88% of which previously carried no reason at all</strong> — are now fully attributed, so operations can work them down cause by cause (data-coverage floor, scope gate, 6-rung failure taxonomy over a 17-witness fact table).",
        "<strong>Caught data failures no one knew were happening — and fixed them at the root.</strong> Recovered <strong>23,677 shipment tracking numbers</strong> the system had silently deleted, and traced <strong>28.5% of a month's sales that were attributed to no store</strong> down to zero — with automatic checks that now raise the alarm before a bad number reaches a business decision (data-reliability tests, freshness monitoring).",
        "<strong>Turned problems the business used to find out about too late into things it catches up front.</strong> Built price/discount guardrails per SKU × store: replayed across the full sales history they flag <strong>150 SKU-store pairs discounting an average 9.9 points past the company's own ceiling</strong> (worst case 180 points over) — <strong>9.3% of those items' value leaking out as unintended discount</strong>. Built a reorder engine on the dashboard (reorder point, safety stock, days of cover, suggested quantity) replacing a manual cross-check of several spreadsheets — it now covers <strong>2,971 SKUs and flags 373 to order right now</strong>, and catches stale POs the old view still counted as inbound stock.",
        "<strong>Replaced the vendor warehouse system with apps I built, and closed the gaps the ERP could not.</strong> WMS: first line of code to production deploy in <strong>15 days</strong>; staff including non-Thai speakers learn it in under 5 minutes, every pick/count/move lands in an inventory ledger with a full audit trail — <strong>208,189 entries, 36 real users</strong>, ~2,000 orders/day. The barcode scanner closed two ERP gaps: it separates <strong>claims from ordinary returns across 21,152 parcels</strong> and answers whether goods actually made it back — return-leg visibility went from 26.8% to 98.0% via a 28,891-pair outbound↔return tracking crosswalk, surfacing <strong>2,661 parcels in transit back</strong> that were previously invisible.",
        "<strong>Built AI systems that run in production — not just AI-assisted coding.</strong> A data assistant staff query in Thai to get an Excel file or a dashboard straight from the warehouse (SELECT-only enforced at the database, with PII guards), a function-calling chat widget on the dashboard, and a Telegram bot that reports system health.",
        "<strong>Cut recurring cloud costs — measurably — and gave the company's data its first safety net.</strong> Moved the warehouse and its scheduling off two paid monthly cloud services onto self-run infrastructure in a single quarter with zero dashboard downtime. Against the actual Azure bill: <strong>monthly database spend fell 44%</strong> even while a new scanning workload joined the same bill, and <strong>Data Factory went to zero on the day it was retired</strong>. The decision came from measurement, not instinct — the nightly batch was pinned at 98% IO on cloud taking 2 hours versus 20 minutes locally, and scaling vCores was tested and shown to cost more without paying for itself. Also shipped the first nightly, independently verified backup the warehouse ever had. I design, build, deploy and stay on call for <strong>all 7 production systems</strong>."
      ],
      travelerRoles: [
        "Accounting · ERP &amp; BI · Operations (multiple hats)"
      ],
      travelerBullets: [
        "<strong>Ended the stock mismatches between sales channels</strong> — set up the company's ERP master data from scratch: chart of accounts, customer/vendor master, and one SKU standard across every marketplace, so finance and sales finally worked from the same numbers (Ecount ERP).",
        "<strong>Closed the recurring gap between sales' numbers and finance's numbers</strong> — built the monthly reports both teams reconcile against, turning a monthly argument into a routine check (Power BI, Power Query)."
      ],
      projectTaglines: [
        "Full data platform — a 52 GB PostgreSQL warehouse holding 70M rows, 199 dbt models under 494 tests, fed from 8 source systems across 4 sales channels, serving 5 dashboards plus an AI assistant.",
        "Custom WMS that replaced the previous third-party system — interactive visual map, foreign staff trained in under 5 min, full stock audit trail.",
        "Real-time multi-user barcode scanning system with workflow enforcement.",
        "AI-assisted invoice processing that reads Thai e-commerce PDF invoices and fills structured records automatically."
      ],
      projectBullets: [
        [
          "Ingests from <strong>8 source systems</strong> (Shopee, Lazada, TikTok, Facebook, JST OMS, DHL, Flash Express, internal WMS/BSN) via a <strong>Python + Playwright RPA framework</strong> and direct database pulls, landing in an <strong>Azure Blob data lake</strong> → a 5-layer <strong>dbt medallion</strong> warehouse — replacing a daily manual download routine that one person used to do every morning.",
          "Orchestrated by <strong>Apache Airflow in Docker</strong> — 11 DAGs with custom Bronze/dbt operators, <strong>494 dbt data tests</strong>, source-freshness checks, and <strong>Telegram alerts</strong> that fire on failure <em>and</em> on runs that finish green but moved no data.",
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
        procurement: "Purchase Plan demo",
        wms: "Warehouse map demo",
        bsn: "Barcode scanner demo"
      }
    },
    th: {
      documentTitle: "สหัสวรรษ ทองมา — Data Engineer",
      htmlLang: "th",
      name: "สหัสวรรษ ทองมา",
      roleTitle: "Data Engineer",
      roleTagline: "Data platform · Web apps · AI systems — ออกแบบ สร้าง และดูแลเองตั้งแต่ต้นจนจบ",
      location: "สมุทรสาคร, ประเทศไทย",
      tabs: ["Resume", "Projects · Live Demos"],
      sectionSummary: "เกี่ยวกับผม",
      summary: [
        "<strong>Data engineer ที่สร้างระบบให้คนทั้งบริษัทได้ใช้จริง</strong> — ประสบการณ์ 4 ปีในสาย e-commerce ปัจจุบันดูแล data platform, แอปในคลัง และระบบ AI ของบริษัทที่ขายผ่าน <strong>4 marketplace ~2,000 ออเดอร์/วัน</strong> · ผู้บริหารดูยอดขาย-สต็อกได้เอง ไม่ต้องรอใครทำ Excel ให้ · งานมือ ~40 งาน/วันหายไป ระบบทำเองตั้งแต่ตีห้า · ตอนนี้โฟกัส <strong>data reliability</strong> — จับปัญหาที่ระบบขึ้นว่าสำเร็จแต่ตัวเลขผิดเงียบ ๆ ก่อนที่ใครจะเอาไปตัดสินใจ · Tools: <strong>dbt · Airflow · PostgreSQL · Python · Azure · FastAPI/React</strong>"
      ],
      sectionExperience: "ประสบการณ์ทำงาน",
      epPeriod: "พ.ค. 2025 – ปัจจุบัน",
      epRole: "Data Engineer &nbsp;·&nbsp; E-Commerce Data Platform",
      epBullets: [
        "<strong>ทำให้ KPI ที่ผู้บริหารดูทุกวันเชื่อถือได้เป็นครั้งแรก</strong> — อัตราส่งของทันเวลาเคยรายงานว่าปี 2025 หลุดเป้าเกือบทั้งหมด ทั้งที่ ERP ยืนยันว่าส่งของแล้ว 97% เพราะโมเดลตัดสินช่วงเวลาที่ feed ต้นทางยังไม่เกิด และเอาออเดอร์ที่สัญญาคนละแบบ (พรีออเดอร์/B2B/ขายส่ง) มารวมในตัวหารเดียวกัน · รื้อวิธีวัดใหม่ทั้งชุด → ปัจจุบัน <strong>94.6% บน 323,134 ออเดอร์</strong> และเคสที่หลุดซึ่ง <strong>88% เคยไม่มีสาเหตุกำกับ</strong> ตอนนี้ระบุได้ครบ ทีมปฏิบัติการไล่แก้เป็นรายสาเหตุได้ครั้งแรก",
        "<strong>จับปัญหาข้อมูลที่ไม่มีใครรู้ว่ากำลังเกิด แล้วแก้ให้จบที่ราก</strong> — กู้เลขพัสดุคืน <strong>23,677 เลข</strong> ที่ระบบลบทิ้งไปเงียบ ๆ และไล่จนเจอสาเหตุที่ <strong>28.5% ของยอดขายเดือนหนึ่งไม่ติดชื่อร้าน — ตอนนี้เหลือศูนย์</strong> พร้อมวางระบบตรวจอัตโนมัติที่เตือนก่อนตัวเลขผิดจะไปถึงการตัดสินใจ (data-reliability tests, freshness monitoring)",
        "<strong>เปลี่ยนปัญหาที่เคย “รู้ตอนสายไปแล้ว” ให้ดักได้ล่วงหน้า</strong> — ระบบเทียบราคาขายจริงกับเพดานราคา/ส่วนลดราย SKU × ร้าน: ย้อนตรวจทั้งประวัติการขายดักได้ <strong>150 คู่ SKU×ร้าน</strong> ที่ให้ส่วนลดเกินเพดานที่ตั้งไว้เองเฉลี่ย <strong>9.9 จุด%</strong> (หนักสุดเกิน 180 จุด%) คิดเป็น <strong>เงินรั่ว 9.3% ของมูลค่าสินค้ากลุ่มนั้น</strong> · และ reorder engine บน dashboard (จุดสั่งซ้ำ, safety stock, days-of-cover, ปริมาณที่ควรสั่ง) แทนการเปิดหลายตารางมานั่งเทียบเอง — ครอบ <strong>2,971 SKU ชี้ 373 ตัวที่ต้องสั่งทันที</strong> และจับ PO ค้างที่ระบบเคยนับผิดว่าเป็นของกำลังมา",
        "<strong>เปลี่ยนระบบคลังของ vendor เป็นแอปที่เขียนเอง และปิดช่องว่างที่ ERP ทำไม่ได้</strong> — WMS: จากโค้ดบรรทัดแรกถึง production deploy ใน <strong>15 วัน</strong> · พนักงาน (รวมคนไม่ใช้ภาษาไทย) เรียนไม่ถึง 5 นาทีก็ใช้ได้ ทุกการหยิบ/นับ/ย้ายลง inventory ledger พร้อม audit trail — ปัจจุบัน <strong>208,189 รายการ · ผู้ใช้จริง 36 คน</strong> รองรับ ~2,000 ออเดอร์/วัน · ระบบสแกน barcode ปิด 2 ช่องว่างของ ERP: แยก <strong>“เคลม” ออกจาก “คืนปกติ” ได้ 21,152 พัสดุ</strong> และตอบได้ว่าของกลับถึงคลังหรือยัง (การมองเห็นขากลับ 26.8% → 98.0%) — เห็นของที่กำลังเดินทางกลับอีก <strong>2,661 พัสดุ</strong> ที่เดิมมองไม่เห็น",
        "<strong>สร้าง AI system ที่ใช้จริงใน production ไม่ใช่แค่ใช้ AI ช่วยเขียนโค้ด</strong> — ผู้ช่วยข้อมูลที่พนักงานถามเป็นภาษาไทยแล้วได้ Excel/dashboard จากคลังข้อมูลจริง (บังคับ SELECT-only ระดับฐานข้อมูล + guard ข้อมูลส่วนบุคคล), chat widget แบบ function-calling บน dashboard และบอทสรุปสถานะระบบใน Telegram",
        "<strong>ลดค่าใช้จ่ายคลาวด์รายเดือนแบบวัดผลได้ และทำให้ข้อมูลบริษัทมีที่สำรองเป็นครั้งแรก</strong> — ย้าย warehouse และระบบตั้งเวลางานออกจากบริการคลาวด์รายเดือน 2 ตัว มาอยู่บนเครื่องที่ดูแลเอง จบในไตรมาสเดียวโดย dashboard ไม่ดาวน์เลย · วัดจากบิล Azure จริง: <strong>ค่าฐานข้อมูลรายเดือนลดลง 44%</strong> ทั้งที่มีงานใหม่ของระบบสแกนเข้ามาในบิลเดียวกัน และ <strong>Data Factory เหลือ 0 ตั้งแต่วันปลดระวาง</strong> · การตัดสินใจย้ายมาจากการวัด ไม่ใช่ความรู้สึก — batch เดิมติด IO 98% บนคลาวด์ ใช้เวลา 2 ชม. เทียบกับ 20 นาทีบนเครื่องโลคอล และการเพิ่ม vCore ทดสอบแล้วว่าแพงขึ้นโดยไม่คุ้ม · ทำ backup รายคืนตัวแรกของ warehouse พร้อมตรวจสอบผลทุกครั้ง · <strong>ดูแลระบบ production ทั้ง 7 ตัว</strong> ตั้งแต่ออกแบบ สร้าง deploy จนถึง on-call"
      ],
      travelerRoles: [
        "Accounting · ERP &amp; BI · Operations (multiple hats)"
      ],
      travelerBullets: [
        "<strong>จบปัญหาสต็อกไม่ตรงกันข้ามช่องทางขาย</strong> — วาง master data ของ ERP ตั้งแต่ศูนย์: ผังบัญชี, ข้อมูลลูกค้า/ผู้ขาย และมาตรฐาน SKU เดียวกันทุก marketplace ให้ finance กับ sales ใช้ตัวเลขชุดเดียวกัน (Ecount ERP)",
        "<strong>ปิดปัญหาตัวเลข sales กับ finance ไม่ตรงกันที่เกิดซ้ำทุกเดือน</strong> — สร้างรายงานรายเดือนกลางที่สองทีมใช้กระทบยอดร่วมกัน (Power BI, Power Query)"
      ],
      projectTaglines: [
        "Data platform เต็มระบบ — PostgreSQL warehouse 52 GB เก็บ 70 ล้านแถว, 199 dbt models ใต้ 494 tests, รับข้อมูลจาก 8 source systems ครอบ 4 ช่องทางขาย เสิร์ฟ 5 dashboard พร้อมผู้ช่วย AI",
        "WMS ที่เขียนเองมาแทนระบบเดิม — แผนที่คลังกดดูของได้ คนงานต่างชาติเรียนไม่ถึง 5 นาทีก็ใช้ได้ track ของได้ทุกการเคลื่อนไหว",
        "ระบบ barcode scanner หลายคนใช้พร้อมกันแบบ real-time พร้อมบังคับลำดับงาน",
        "ระบบ AI อ่านไฟล์ invoice PDF ของ e-commerce ไทย แล้วลงข้อมูลเป็น structured records ให้อัตโนมัติ"
      ],
      projectBullets: [
        [
          "ดึงข้อมูลจาก <strong>8 source systems</strong> (Shopee, Lazada, TikTok, Facebook, JST OMS, DHL, Flash Express, internal WMS/BSN) ผ่าน <strong>Python + Playwright RPA framework</strong> และ direct database pull ลงใน <strong>Azure Blob data lake</strong> → <strong>dbt medallion</strong> warehouse 5 ชั้น — แทนการที่เคยมีคนนั่งโหลดไฟล์ทุกเช้า",
          "orchestrate ด้วย <strong>Apache Airflow ใน Docker</strong> — 11 DAGs พร้อม custom Bronze/dbt operators, <strong>494 dbt data tests</strong>, source-freshness checks และ <strong>Telegram alert</strong> ที่ยิงทั้งตอน pipeline fail <em>และ</em> ตอนที่รันจบเขียวแต่ข้อมูลไม่ขยับ",
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
        procurement: "ดู Purchase Plan",
        wms: "ดูแผนที่คลัง",
        bsn: "ดู Barcode scanner"
      }
    }
  };
