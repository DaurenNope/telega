# Telegram Project Update Analyzer

This project listens to specified Telegram channels, analyzes messages using the Gemini API to identify crypto project updates, and saves the structured data to Supabase.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <your-repo-url>
    cd telega
    ```

2.  **Create a Python virtual environment (optional but recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Environment Variables:**
    *   Copy `.env.example` to `.env`:
        ```bash
        cp .env.example .env
        ```
    *   Edit the `.env` file and fill in your actual credentials:
        *   `TELEGRAM_API_ID` and `TELEGRAM_API_HASH`: Get from [my.telegram.org](https://my.telegram.org/apps).
        *   `TELEGRAM_SESSION_NAME`: A name for your session file (e.g., `telegram_scraper`).
        *   `GEMINI_API_KEY`: Your Google AI API key.
        *   `SUPABASE_URL` and `SUPABASE_SERVICE_KEY`: Your Supabase project URL and service role key.
        *   `NOTIFICATION_BOT_TOKEN` and `NOTIFICATION_CHAT_ID`: (Optional) For sending crash/status notifications via a Telegram bot.

5.  **Configure Channels:**
    *   Copy `channels.example.txt` to `channels.txt`:
        ```bash
        cp channels.example.txt channels.txt
        ```
    *   Edit `channels.txt` and add the target Telegram channel usernames or invite links, one per line.

6.  **(Optional) Google Sheets Tester:**
    *   If using `tests/test_analyzer_gsheet.py`, place your Google Cloud service account key file named `google_creds.json` in the `telega/tests/` directory.
    *   Ensure the service account has access to the Google Sheet specified in the script.
    *   **Important:** Add `tests/google_creds.json` to your `.gitignore` file if you haven't already via the pattern.

## Running the Application

```bash
python src/main.py
```

Follow the interactive menu prompts:
*   **Connect:** The first time you run, you may need to authenticate your Telegram account by entering your phone number and a code sent via Telegram.
*   **Scrape History (Option 1):** Fetches historical messages from specified channels and analyzes them.
*   **Start Listener (Option 2):** Runs continuously, listening for new messages in the specified channels and analyzing them in real-time.
*   **Clean Channel List (Option 4):** Removes duplicates from `channels.txt`.
*   **Exit (Option 6):** Stops the application.

## Running Tests (Optional)

To run the Google Sheet based test:

```bash
cd tests
python test_analyzer_gsheet.py [number_of_rows_to_test]
cd ..
```
Replace `[number_of_rows_to_test]` with the number of rows you want to process from the sheet (optional, defaults to all).

### Running the Supabase Analyzer Test (Optional)

This test fetches the most recent messages directly from your Supabase database and runs the analyzer on them.

```bash
cd tests
# Test latest 10 rows (default)
python test_analyzer_supabase.py

# Test latest 50 rows
python test_analyzer_supabase.py 50

# Test latest N rows
python test_analyzer_supabase.py N

cd ..
```

## Features

*   Connects to Telegram using your account via the Telethon library.
*   Scrapes message history from specified Telegram entities (chats, channels).
*   Extracts key message data: Timestamp, Sender, Message Content, View Count, Forward Count.
*   Connects to Google Sheets using a Service Account.
*   Appends scraped data to a specified Google Sheet.
*   Formats the timestamp column in Google Sheets as a proper Date/Time format.
*   Handles Telegram session validation (detects multi-IP usage and prompts for re-authentication).
*   Uses environment variables for secure configuration management.
*   Provides an interactive command-line interface for ease of use.

## Prerequisites

Before you begin, ensure you have the following:

1.  **Python:** Version 3.8 or higher recommended.
2.  **Git:** For cloning the repository (if applicable) and version control.
3.  **Telegram Account:** An active Telegram account.
4.  **Telegram API Credentials:**
    *   Go to [https://my.telegram.org/apps](https://my.telegram.org/apps).
    *   Log in with your Telegram account.
    *   Create a new application (fill in basic details).
    *   Note down your `api_id` and `api_hash`. **Keep these secret!**
5.  **Google Cloud Platform Project:**
    *   Create a project at [https://console.cloud.google.com/](https://console.cloud.google.com/).
    *   Enable the **Google Sheets API** for your project.
6.  **Google Service Account Credentials:**
    *   In your Google Cloud Project, navigate to "IAM & Admin" -> "Service Accounts".
    *   Create a new Service Account. Give it a name (e.g., `telegram-sheets-updater`).
    *   Grant this Service Account the "Editor" role (or a more restricted role if you prefer, ensuring it can edit Sheets).
    *   Create a key for this Service Account (choose JSON format). A JSON file will be downloaded. **Keep this file secure!**
7.  **Google Sheet:**
    *   Create a new Google Sheet where you want to store the data.
    *   **Share** this sheet with the **email address** of the Service Account you created in the previous step (it looks like `your-service-account-name@your-project-id.iam.gserviceaccount.com`). Grant it "Editor" permissions.
    *   Note down the **URL** of your Google Sheet.

## Installation

1.  **Clone the Repository (if applicable):**
    ```bash
    git clone <your-repository-url>
    cd <your-repository-name>
    ```
    *(Replace `<your-repository-url>` and `<your-repository-name>`)*

2.  **Create a Virtual Environment (Recommended):**
    ```bash
    python -m venv venv
    # On Windows
    venv\Scripts\activate
    # On macOS/Linux
    source venv/bin/activate
    ```

3.  **Install Dependencies:**
    Create a `requirements.txt` file (if you don't have one) with the necessary libraries:
    ```txt
    python-dotenv
    telethon
    gspread
    google-auth-oauthlib
    google-auth-httplib2
    rich
    # Add any other specific libraries your project uses
    ```
    Then install them:
    ```bash
    pip install -r requirements.txt
    ```

## Configuration

1.  **Create `.env` file:**
    Make a copy of the `.env.example` file (you should create this example file) and name it `.env`.
    ```bash
    cp .env.example .env
    ```
    *(If you don't have `.env.example`, just create a new file named `.env`)*

2.  **Populate `.env`:**
    Open the `.env` file and fill in your credentials:

    ```dotenv
    # Telegram API Credentials
    API_ID=YOUR_TELEGRAM_API_ID
    API_HASH=YOUR_TELEGRAM_API_HASH
    SESSION_NAME=my_telegram_session # Or any name you prefer for the session file

    # Google Sheets Credentials
    # Path relative to the project root where you saved the Service Account JSON key
    GSHEET_CREDENTIALS=path/to/your/service-account-key.json
    SHEET_URL=YOUR_GOOGLE_SHEET_URL
    ```

    *   **IMPORTANT:** Ensure the `GSHEET_CREDENTIALS` path is correct relative to where you run the script (usually the project root).
    *   **NEVER commit your `.env` file or your Service Account JSON file to Git.** Make sure they are listed in your `.gitignore` file.

## Usage

1.  **Run the Script:**
    Navigate to the project's root directory in your terminal (where the `src` folder and `.env` file are) and run:
    ```bash
    python src/main.py
    ```

2.  **First Run - Telegram Authentication:**
    The first time you run the script, Telethon will need to authenticate your Telegram account. It will prompt you for:
    *   Your phone number (international format, e.g., +1234567890).
    *   The login code sent to your Telegram account.
    *   Your two-factor authentication password (if enabled).
    A session file (`<SESSION_NAME>.session`) will be created to store your login details for future runs.

3.  **Interactive Menu:**
    Once connected, you should see an interactive menu allowing you to:
    *   List your recent chats/channels.
    *   Select a chat/channel to scrape.
    *   Specify the number of messages to retrieve.
    *   Start the scraping process.
    *   (Potentially) Enter a continuous listening mode.

## Running Continuously (Deployment on a Server)

To run this script continuously in the background on a server:

1.  **Transfer Files:** Upload the entire project folder (excluding `.env` initially, transfer that securely) to your server.
2.  **Install Prerequisites:** Install Python, Git, and any system dependencies on the server.
3.  **Setup:** Follow the Installation and Configuration steps on the server. Ensure the `.env` file and the Google Service Account JSON file are present and correctly configured.
4.  **Use a Process Manager:** To keep the script running reliably (even after disconnects or reboots), use a tool like:
    *   **`screen` or `tmux`:** Simple terminal multiplexers. You start the script within a session and detach, leaving it running. (e.g., `screen -S telegram_scraper`, run `python src/main.py`, then detach with `Ctrl+A D`).
    *   **`systemd`:** (Linux) Create a service unit file to manage the script as a system service. This is more robust for automatic restarts.
    *   **`supervisor`:** Another popular process control system.

    *Example using `screen`:*
    ```bash
    screen -S telegram_scraper  # Start a new screen session
    cd /path/to/your/project  # Navigate to your project
    source venv/bin/activate   # Activate virtual environment
    python src/main.py         # Run your script
    # Press Ctrl+A then D to detach from the screen session
    ```
    You can reattach later using `screen -r telegram_scraper`.

## Important Notes

*   **Telegram Session File (`.session`):** Telegram restricts sessions to a single IP address. If you run the script from a different IP than the one used to create the session, you'll get an error. The script attempts to handle this by deleting the invalid session file and prompting you to re-authenticate upon restart.
*   **API Rate Limits:** Be mindful of potential rate limits imposed by both Telegram and Google Sheets APIs, especially when scraping large amounts of data or running frequently.
*   **Error Handling:** Basic error handling is included, but you may want to enhance it for production use (e.g., more detailed logging, specific exception handling).
*   **Security:** Keep your `.env` file, your `.session` file, and your Google Service Account JSON key secure and **never** commit them to version control. Ensure your `.gitignore` file correctly lists them.

## `.gitignore`

Ensure your `.gitignore` file includes at least the following:
