import json
import logging
from pathlib import Path
from fpdf import FPDF

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class ExecutiveAnalyticsPDF(FPDF):
    def __init__(self):
        super().__init__()
        # Disable auto page break to prevent collision with manual table pagination logic
        self.set_auto_page_break(auto=False)

    def header(self):
        self.set_font("Helvetica", "B", 15)
        self.set_text_color(30, 41, 59)
        self.cell(
            0,
            8,
            "Campus Telemetry - Monthly Executive Summary",
            new_x="LMARGIN",
            new_y="NEXT",
            align="C",
        )

        self.set_font("Helvetica", "I", 9)
        self.set_text_color(100, 116, 139)
        self.cell(
            0,
            5,
            "Automated Monthly Performance & Engagement Report",
            new_x="LMARGIN",
            new_y="NEXT",
            align="C",
        )
        self.ln(4)

        self.set_draw_color(226, 232, 240)
        self.set_line_width(0.4)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(6)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(148, 163, 184)
        self.cell(0, 10, f"Page {self.page_no()}", align="C")

    def chapter_title(self, title: str):
        self.set_font("Helvetica", "B", 10)
        self.set_fill_color(241, 245, 249)
        self.set_text_color(15, 23, 42)
        self.cell(0, 7, f"  {title.upper()}", new_x="LMARGIN", new_y="NEXT", fill=True)
        self.ln(3)

    def draw_table(self, headers, rows, col_widths, alignments=None):
        if not rows:
            self.set_font("Helvetica", "I", 9)
            self.set_text_color(100, 116, 139)
            self.cell(
                0,
                8,
                "No data available for this section.",
                new_x="LMARGIN",
                new_y="NEXT",
                align="C",
            )
            self.ln(4)
            return

        if alignments is None:
            alignments = ["C"] * len(headers)

        self.set_font("Helvetica", "B", 8.5)
        self.set_fill_color(30, 41, 59)
        self.set_text_color(255, 255, 255)
        self.set_draw_color(203, 213, 225)

        for i, h in enumerate(headers):
            self.cell(
                col_widths[i],
                7,
                str(h),
                border=1,
                align="C",
                fill=True,
                new_x="RIGHT",
                new_y="TOP",
            )
        self.ln()

        self.set_font("Helvetica", "", 8.5)
        for r_idx, row in enumerate(rows):
            # Safe manual pagination
            if self.get_y() > 270:
                self.add_page()
                self.set_font("Helvetica", "B", 8.5)
                self.set_fill_color(30, 41, 59)
                self.set_text_color(255, 255, 255)
                for i, h in enumerate(headers):
                    self.cell(
                        col_widths[i],
                        7,
                        str(h),
                        border=1,
                        align="C",
                        fill=True,
                        new_x="RIGHT",
                        new_y="TOP",
                    )
                self.ln()
                self.set_font("Helvetica", "", 8.5)

            if r_idx % 2 == 0:
                self.set_fill_color(255, 255, 255)
            else:
                self.set_fill_color(248, 250, 252)

            self.set_text_color(51, 65, 85)

            for i, item in enumerate(row):
                val_str = str(item)
                # CRITICAL: Prevent UnicodeEncodeError on raw user input for FPDF core fonts
                val_str = val_str.encode("latin-1", "replace").decode("latin-1")

                # Dynamic character limit proportional to column width (~0.55 chars per mm)
                max_chars = max(10, int(col_widths[i] * 0.55))
                if len(val_str) > max_chars:
                    val_str = val_str[: max_chars - 3] + "..."

                self.cell(
                    col_widths[i],
                    6,
                    val_str,
                    border=1,
                    align=alignments[i],
                    fill=True,
                    new_x="RIGHT",
                    new_y="TOP",
                )
            self.ln()
        self.ln(6)


def _fmt(val, is_pct=False, default="N/A"):
    if val is None:
        return default
    try:
        num = float(val)
        if is_pct:
            return f"{num:.1f}%"
        return f"{int(num):,}" if num.is_integer() else f"{num:,.2f}"
    except (ValueError, TypeError):
        return str(val)


def generate_monthly_pdf(
    json_cache_path: str = "analytics_cache.json",
    output_pdf: str = "Monthly_Summary_Report.pdf",
):
    path = Path(json_cache_path)
    if not path.exists():
        logging.error(f"Cache file not found at path: {json_cache_path}")
        return

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse cache JSON: {e}")
        return

    monthly_data = data.get("monthly", {})
    students = monthly_data.get("students", [])[:25]
    events = monthly_data.get("events", [])[:12]
    posts = monthly_data.get("posts", [])[:15]

    pdf = ExecutiveAnalyticsPDF()
    pdf.add_page()

    # --- SECTION 1: TOP STUDENTS LEADERBOARD ---
    pdf.chapter_title("Top 25 Student Leaderboard")
    student_headers = [
        "Rank",
        "Student / User",
        "Class Year",
        "Events Attended",
        "Total Score",
    ]
    student_widths = [15, 65, 30, 40, 40]
    student_aligns = ["C", "L", "C", "R", "R"]

    student_rows = [
        [
            idx + 1,
            s.get("display_name") or s.get("user_id", "N/A"),
            s.get("college_year", "N/A"),
            _fmt(s.get("attend_count")),
            _fmt(s.get("total_score")),
        ]
        for idx, s in enumerate(students)
    ]
    pdf.draw_table(student_headers, student_rows, student_widths, student_aligns)

    # --- SECTION 2: EVENT PERFORMANCE SUMMARY ---
    if pdf.get_y() > 200:
        pdf.add_page()

    pdf.chapter_title("Monthly Event Performance & Turnout")
    event_headers = ["Event Name / ID", "Views", "RSVPs", "Attended", "Turnout %"]
    event_widths = [60, 30, 30, 35, 35]
    event_aligns = ["L", "R", "R", "R", "R"]

    event_rows = [
        [
            e.get("name") or e.get("target_id", "N/A"),
            _fmt(e.get("views")),
            _fmt(e.get("total_rsvps")),
            _fmt(e.get("actual_attended")),
            _fmt(e.get("turnout_vs_rsvp_pct"), is_pct=True),
        ]
        for e in events
    ]
    pdf.draw_table(event_headers, event_rows, event_widths, event_aligns)

    # --- SECTION 3: TOP POST ENGAGEMENT ---
    if pdf.get_y() > 200:
        pdf.add_page()

    pdf.chapter_title("Top 15 Post Engagements")
    post_headers = [
        "Post Content / ID",
        "Views",
        "Likes",
        "Comments",
        "Engagement %",
    ]
    post_widths = [60, 30, 30, 35, 35]
    post_aligns = ["L", "R", "R", "R", "R"]

    post_rows = [
        [
            p.get("content_snippet") or p.get("target_id", "N/A"),
            _fmt(p.get("views")),
            _fmt(p.get("likes")),
            _fmt(p.get("comment_views")),
            _fmt(p.get("engagement_rate_pct"), is_pct=True),
        ]
        for p in posts
    ]
    pdf.draw_table(post_headers, post_rows, post_widths, post_aligns)

    # Fault-tolerant write
    try:
        pdf.output(output_pdf)
        logging.info(f"Successfully generated executive PDF report: {output_pdf}")
    except PermissionError:
        logging.error(
            f"Permission denied: {output_pdf} is currently open or locked by another process."
        )
    except Exception as e:
        logging.error(f"Failed to write PDF file: {e}")