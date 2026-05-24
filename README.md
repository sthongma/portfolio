# Portfolio — Sahatsawat Thongma

## Files

- [`resume.html`](resume.html) — bilingual EN/TH resume → [live](https://sthongma.github.io/portfolio/resume.html)
- [`resume.pdf`](resume.pdf) — printable PDF version for ATS uploads → [live](https://sthongma.github.io/portfolio/resume.pdf)
- [`resume-i18n.js`](resume-i18n.js) — EN/TH content keys for the resume
- [`mockups/fulfillment.html`](mockups/fulfillment.html) — fulfillment dashboard mock → [live](https://sthongma.github.io/portfolio/mockups/fulfillment.html)
- [`mockups/sales-insights.html`](mockups/sales-insights.html) — sales insights dashboard mock → [live](https://sthongma.github.io/portfolio/mockups/sales-insights.html)
- [`mockups/wms.html`](mockups/wms.html) — warehouse map mock → [live](https://sthongma.github.io/portfolio/mockups/wms.html)
- [`mockups/bsn.html`](mockups/bsn.html) — barcode scanner mock → [live](https://sthongma.github.io/portfolio/mockups/bsn.html)

## Regenerating the PDF

After editing `resume.html`, regenerate the PDF with headless Chrome:

```sh
chrome --headless=new --disable-gpu --no-pdf-header-footer \
  --print-to-pdf=resume.pdf \
  "file://$(pwd)/resume.html"
```
