import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone, timedelta
from rich.console import Console


class GSheetClient:
    def __init__(self, creds_path, sheet_url):
        self.console = Console()
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(creds_path, scopes=scope)
        self.client = gspread.authorize(creds)
        self.sheet = self.client.open_by_url(sheet_url).sheet1

        # Force header creation and formatting
        current_headers = self.sheet.row_values(1)
        expected_headers = ["Timestamp", "Channel", "Message Text", "Message Link"]
        self.console.print(f"Current headers: {current_headers}")
        self.console.print(f"Expected headers: {expected_headers}")
        if current_headers != expected_headers:
            self.console.print("Updating headers...")
            self.sheet.update(
                "A1:D1", [expected_headers], value_input_option="USER_ENTERED"
            )
        self.format_sheet()  # Always apply formatting

    def append_message(self, message):
        """Add message to top of sheet with deduplication check"""
        existing = self.sheet.col_values(4)  # Link column
        if message["link"] not in existing:
            # Updated timestamp format to ISO 8601 with UTC+5 offset
            timestamp = message["timestamp"].astimezone(timezone(timedelta(hours=5)))
            formatted_timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[
                :-3
            ] + timestamp.strftime("%z")
            formatted_timestamp = (
                formatted_timestamp[:-2] + ":" + formatted_timestamp[-2:]
            )

            self.sheet.insert_row(
                [
                    formatted_timestamp,
                    message["channel"],
                    message["text"],
                    message["link"],
                ],
                index=2,
            )  # Insert after header

    def batch_append(self, messages):
        """Enhanced batch append with better deduplication and ISO 8601 timestamp"""
        # Get existing links with timestamp
        existing_data = self.sheet.get_all_records()
        existing_links = {
            row["Message Link"]: row["Timestamp"] for row in existing_data
        }

        # Deduplicate considering both link and timestamp
        new_messages = [
            msg
            for msg in messages
            if msg["link"] not in existing_links
            or existing_links[msg["link"]]
            != msg["timestamp"].strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            + msg["timestamp"].strftime("%z")[:-2]
            + ":"
            + msg["timestamp"].strftime("%z")[-2:]
        ]

        if new_messages:
            # Sort by timestamp descending
            sorted_messages = sorted(
                new_messages, key=lambda x: x["timestamp"], reverse=True
            )

            # Prepare rows with formatted timestamp
            rows = []
            for msg in sorted_messages:
                try:
                    # Convert to UTC+5 and format as ISO 8601
                    timestamp = msg["timestamp"].astimezone(
                        timezone(timedelta(hours=5))
                    )
                    formatted_timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[
                        :-3
                    ] + timestamp.strftime("%z")
                    formatted_timestamp = (
                        formatted_timestamp[:-2] + ":" + formatted_timestamp[-2:]
                    )

                    rows.append(
                        [
                            formatted_timestamp,
                            msg["channel"][:50],  # Truncate long channel names
                            msg["text"].replace("\n", " ")[:495],  # Clean text
                            msg["link"],
                        ]
                    )
                except Exception as e:
                    self.console.print(f"[red]Error formatting message: {str(e)}[/]")
                    continue

            # Batch insert with error handling
            try:
                if rows:
                    self.sheet.insert_rows(rows, row=2)
                    return len(rows)
            except gspread.exceptions.APIError as e:
                self.console.print(f"[red]Insert error: {str(e)}[/]")
                return 0

        return len(new_messages)

    def format_sheet(self):
        """Enhanced sheet formatting with professional styling"""
        # Header formatting
        header_format = {
            "textFormat": {
                "bold": True,
                "fontSize": 12,
                "foregroundColor": {"red": 1, "green": 1, "blue": 1},
            },
            "backgroundColor": {"red": 0.15, "green": 0.35, "blue": 0.65},
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
            "padding": {"top": 10, "bottom": 10},
        }
        self.sheet.format("A1:D1", header_format)

        # Set optimal column widths
        # Column width requests (A=220px, B=180px, C=500px, D=350px) - increased A to accommodate longer timestamp
        column_requests = [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": self.sheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": i,
                        "endIndex": i + 1,
                    },
                    "properties": {"pixelSize": size},
                    "fields": "pixelSize",
                }
            }
            for i, size in enumerate([220, 180, 500, 350])
        ]
        self.sheet.spreadsheet.batch_update({"requests": column_requests})

        # Format timestamp column
        self.sheet.format(
            "A2:A",
            {
                "numberFormat": {
                    "type": "TEXT"  # Use TEXT to preserve exact ISO 8601 format
                },
                "horizontalAlignment": "LEFT",
                "textFormat": {"fontFamily": "Roboto Mono"},
            },
        )

        # Rest of the formatting remains the same as in the previous version
        # (Channel, Message Text, Message Link formatting...)

        # Add frozen header and filter view
        self.sheet.freeze(rows=1)
        self.sheet.set_basic_filter()  # Apply filter to entire sheet

        # Set optimal row heights
        row_height_request = {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": self.sheet.id,
                    "dimension": "ROWS",
                    "startIndex": 1,  # Start from row 2
                },
                "properties": {"pixelSize": 30},
                "fields": "pixelSize",
            }
        }
        self.sheet.spreadsheet.batch_update({"requests": [row_height_request]})
