# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,YouTube Data Extraction Pipeline - Documentation
# MAGIC %md
# MAGIC # YouTube Data Extraction Pipeline - Complete Documentation
# MAGIC
# MAGIC ## 📋 Table of Contents
# MAGIC 1. [Overview](#overview)
# MAGIC 2. [Architecture](#architecture)
# MAGIC 3. [Components & Features](#components--features)
# MAGIC 4. [Setup & Configuration](#setup--configuration)
# MAGIC 5. [Functions Reference](#functions-reference)
# MAGIC 6. [Workflow](#workflow)
# MAGIC 7. [Output Format](#output-format)
# MAGIC 8. [Error Handling](#error-handling)
# MAGIC 9. [Best Practices](#best-practices)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC ### Purpose
# MAGIC This notebook implements an end-to-end data engineering pipeline that extracts YouTube channel data using the YouTube Data API v3 and stores it in Databricks Unity Catalog volumes in JSON format.
# MAGIC
# MAGIC ### Key Capabilities
# MAGIC - Secure API key management using Databricks Secret Scopes
# MAGIC - Channel metadata extraction by handle/name
# MAGIC - Playlist discovery and video enumeration
# MAGIC - Comprehensive video metadata extraction (title, description, statistics, tags)
# MAGIC - Automated storage to Unity Catalog volumes
# MAGIC - Date-based file naming for data versioning
# MAGIC
# MAGIC ### Use Cases
# MAGIC - YouTube content analysis
# MAGIC - Channel performance tracking
# MAGIC - Video metadata collection for analytics
# MAGIC - Content recommendation systems
# MAGIC - Social media monitoring
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ### Data Flow
# MAGIC ```
# MAGIC Channel Handle → Search API → Channel ID → Playlist ID → Video IDs → Video Details → JSON Storage
# MAGIC ```
# MAGIC
# MAGIC ### Components
# MAGIC 1. **Secret Management Layer**: Secure credential storage and retrieval
# MAGIC 2. **API Integration Layer**: YouTube Data API v3 interactions
# MAGIC 3. **Data Extraction Layer**: Multi-step data collection process
# MAGIC 4. **Storage Layer**: Unity Catalog volume persistence
# MAGIC
# MAGIC ### Technology Stack
# MAGIC - **Platform**: Databricks
# MAGIC - **Language**: Python 3.x
# MAGIC - **API**: YouTube Data API v3
# MAGIC - **Storage**: Unity Catalog Volumes
# MAGIC - **Security**: Databricks Secret Scopes
# MAGIC - **Libraries**: requests, json, datetime
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Components & Features
# MAGIC %md
# MAGIC ## Components & Features
# MAGIC
# MAGIC ### 1. Secret Management Module (`00.Helper Function`)
# MAGIC
# MAGIC Provides secure management of Databricks Secret Scopes and Secrets using the REST API.
# MAGIC
# MAGIC #### Features:
# MAGIC - **Scope Creation**: Create new secret scopes
# MAGIC - **Scope Deletion**: Remove existing secret scopes
# MAGIC - **Secret Storage**: Store sensitive credentials securely
# MAGIC - **Secret Retrieval**: Access stored secrets programmatically
# MAGIC - **Scope Listing**: View all available secret scopes
# MAGIC - **Secret Enumeration**: List all secrets within a scope
# MAGIC
# MAGIC #### Functions:
# MAGIC
# MAGIC ##### `create_scope(scope_name: str, backend_type: str)`
# MAGIC Creates a new Databricks secret scope.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `scope_name`: Name of the secret scope
# MAGIC - `backend_type`: Backend type (for future extensibility)
# MAGIC
# MAGIC **Returns:** None (prints status)
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC create_scope("youtube", "DATABRICKS")
# MAGIC ```
# MAGIC
# MAGIC ##### `delete_scope(scope_name: str)`
# MAGIC Deletes an existing secret scope.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `scope_name`: Name of the scope to delete
# MAGIC
# MAGIC **Returns:** None (prints status)
# MAGIC
# MAGIC ##### `create_secret(scope_name: str, secret_key: str, secret_value: str)`
# MAGIC Stores a secret in a specified scope.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `scope_name`: Target secret scope
# MAGIC - `secret_key`: Key/name for the secret
# MAGIC - `secret_value`: Actual secret value
# MAGIC
# MAGIC **Returns:** None (prints status)
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC create_secret("youtube", "api_key", "YOUR_YOUTUBE_API_KEY")
# MAGIC ```
# MAGIC
# MAGIC ##### `read_secret(scope_name: str, secret_key: str)`
# MAGIC Retrieves and parses a JSON secret.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `scope_name`: Secret scope name
# MAGIC - `secret_key`: Key of the secret
# MAGIC
# MAGIC **Returns:** Parsed JSON object
# MAGIC
# MAGIC ##### `list_secrets(scope_name: str)`
# MAGIC Lists all secrets in a scope.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `scope_name`: Scope to query
# MAGIC
# MAGIC **Returns:** None (prints secret keys)
# MAGIC
# MAGIC ##### `list_scopes()`
# MAGIC Lists all available secret scopes.
# MAGIC
# MAGIC **Returns:** None (prints scope names)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. Data Extraction Module (`01.Extracting The Youtube Data`)
# MAGIC
# MAGIC Implements the core YouTube data extraction pipeline.
# MAGIC
# MAGIC #### Features:
# MAGIC - **Channel Discovery**: Search and identify channels by handle
# MAGIC - **Playlist Retrieval**: Extract upload playlist IDs
# MAGIC - **Video Enumeration**: Collect all video IDs from playlists
# MAGIC - **Metadata Extraction**: Gather comprehensive video details
# MAGIC - **Batch Processing**: Handle multiple videos efficiently
# MAGIC - **Error Handling**: Robust exception management
# MAGIC - **Automated Storage**: Save results to Unity Catalog volumes
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Functions Reference
# MAGIC %md
# MAGIC ## Functions Reference
# MAGIC
# MAGIC ### Data Extraction Functions
# MAGIC
# MAGIC #### `get_playlist_id(channel_handle: str) -> str`
# MAGIC
# MAGIC **Purpose:** Retrieves the uploads playlist ID for a YouTube channel.
# MAGIC
# MAGIC **Algorithm:**
# MAGIC 1. Search for the channel using the YouTube Search API
# MAGIC 2. Extract the channel ID from search results
# MAGIC 3. Query the Channels API to get content details
# MAGIC 4. Return the uploads playlist ID
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `channel_handle` (str): Channel name or handle (e.g., "MrBeast")
# MAGIC
# MAGIC **Returns:**
# MAGIC - `str`: Playlist ID for the channel's uploads
# MAGIC
# MAGIC **API Calls:**
# MAGIC - `GET /youtube/v3/search?part=snippet&type=channel&q={handle}`
# MAGIC - `GET /youtube/v3/channels?part=contentDetails&id={channel_id}`
# MAGIC
# MAGIC **Exceptions:**
# MAGIC - Raises `Exception` if channel not found or API error occurs
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC playlist_id = get_playlist_id("MrBeast")
# MAGIC # Returns: "UUX6OQ3DkcsbYNE6H8uQQuVA"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `get_video_ids(playlist_id: str) -> list`
# MAGIC
# MAGIC **Purpose:** Extracts all video IDs from a YouTube playlist.
# MAGIC
# MAGIC **Algorithm:**
# MAGIC 1. Initialize empty results list
# MAGIC 2. Query PlaylistItems API with pagination
# MAGIC 3. Collect all video IDs across multiple pages
# MAGIC 4. Handle pagination using nextPageToken
# MAGIC 5. Return complete list of video IDs
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `playlist_id` (str): YouTube playlist ID
# MAGIC
# MAGIC **Returns:**
# MAGIC - `list`: List of video ID strings
# MAGIC
# MAGIC **API Calls:**
# MAGIC - `GET /youtube/v3/playlistItems?part=contentDetails&playlistId={id}&maxResults=50&pageToken={token}`
# MAGIC
# MAGIC **Features:**
# MAGIC - Handles pagination automatically
# MAGIC - Retrieves up to 50 videos per request (API maximum)
# MAGIC - Continues until all videos are collected
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC video_ids = get_video_ids("UUX6OQ3DkcsbYNE6H8uQQuVA")
# MAGIC # Returns: ["vid1", "vid2", "vid3", ...]
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `extract_video_data(video_ids: list) -> dict`
# MAGIC
# MAGIC **Purpose:** Extracts comprehensive metadata for multiple YouTube videos.
# MAGIC
# MAGIC **Algorithm:**
# MAGIC 1. Join video IDs into comma-separated string
# MAGIC 2. Query Videos API with all IDs in a single request
# MAGIC 3. Extract and structure metadata for each video
# MAGIC 4. Return dictionary keyed by video ID
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `video_ids` (list): List of YouTube video IDs
# MAGIC
# MAGIC **Returns:**
# MAGIC - `dict`: Dictionary with video IDs as keys, metadata as values
# MAGIC
# MAGIC **Extracted Fields:**
# MAGIC - `video_id`: Unique video identifier
# MAGIC - `title`: Video title
# MAGIC - `description`: Full video description
# MAGIC - `published_at`: Publication timestamp
# MAGIC - `view_count`: Total views (integer)
# MAGIC - `like_count`: Total likes (integer)
# MAGIC - `comment_count`: Total comments (integer)
# MAGIC - `duration`: Video duration (ISO 8601 format)
# MAGIC - `tags`: List of video tags
# MAGIC - `category_id`: YouTube category ID
# MAGIC - `channel_id`: Channel identifier
# MAGIC - `channel_title`: Channel name
# MAGIC
# MAGIC **API Calls:**
# MAGIC - `GET /youtube/v3/videos?part=snippet,contentDetails,statistics&id={ids}`
# MAGIC
# MAGIC **Batch Processing:**
# MAGIC - Can handle up to 50 videos per API call
# MAGIC - For larger datasets, processes in batches automatically
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC data = extract_video_data(["video_id_1", "video_id_2"])
# MAGIC # Returns:
# MAGIC # {
# MAGIC #   "video_id_1": {
# MAGIC #     "title": "Video Title",
# MAGIC #     "view_count": 1000000,
# MAGIC #     "like_count": 50000,
# MAGIC #     ...
# MAGIC #   },
# MAGIC #   "video_id_2": {...}
# MAGIC # }
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### `save_to_json(extracted_data: dict) -> None`
# MAGIC
# MAGIC **Purpose:** Saves extracted YouTube data to Unity Catalog volume.
# MAGIC
# MAGIC **Algorithm:**
# MAGIC 1. Define volume path (`/Volumes/youtube/landing/data`)
# MAGIC 2. Generate filename with current date (`yt_data_YYYY-MM-DD.json`)
# MAGIC 3. Write JSON with proper formatting
# MAGIC 4. Print confirmation message
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `extracted_data` (dict): Video metadata dictionary
# MAGIC
# MAGIC **Returns:**
# MAGIC - None (writes to file)
# MAGIC
# MAGIC **File Format:**
# MAGIC - JSON with 4-space indentation
# MAGIC - UTF-8 encoding
# MAGIC - Human-readable formatting
# MAGIC - Non-ASCII characters preserved
# MAGIC
# MAGIC **Output Location:**
# MAGIC ```
# MAGIC /Volumes/youtube/landing/data/yt_data_YYYY-MM-DD.json
# MAGIC ```
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC save_to_json(extracted_data)
# MAGIC # Saves to: /Volumes/youtube/landing/data/yt_data_2026-07-05.json
# MAGIC # Prints: "file: /Volumes/youtube/landing/data/yt_data_2026-07-05.json saved successfully"
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Setup & Configuration
# MAGIC %md
# MAGIC ## Setup & Configuration
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC #### 1. YouTube Data API v3 Key
# MAGIC
# MAGIC **Obtain API Key:**
# MAGIC 1. Go to [Google Cloud Console](https://console.cloud.google.com/)
# MAGIC 2. Create a new project or select existing one
# MAGIC 3. Enable "YouTube Data API v3"
# MAGIC 4. Navigate to "Credentials"
# MAGIC 5. Click "Create Credentials" → "API Key"
# MAGIC 6. Copy the generated API key
# MAGIC
# MAGIC **API Key Format:**
# MAGIC - 39 characters long
# MAGIC - Typically starts with "AIza"
# MAGIC - Example: `AIzaSyDxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`
# MAGIC
# MAGIC **Quota Limits:**
# MAGIC - Default: 10,000 units/day
# MAGIC - Search query: 100 units
# MAGIC - Videos list: 1 unit per video
# MAGIC - PlaylistItems list: 1 unit
# MAGIC
# MAGIC #### 2. Unity Catalog Volume
# MAGIC
# MAGIC **Create Volume:**
# MAGIC ```sql
# MAGIC CREATE CATALOG IF NOT EXISTS youtube;
# MAGIC CREATE SCHEMA IF NOT EXISTS youtube.landing;
# MAGIC CREATE VOLUME IF NOT EXISTS youtube.landing.data;
# MAGIC ```
# MAGIC
# MAGIC **Required Permissions:**
# MAGIC - `CREATE VOLUME` on schema
# MAGIC - `WRITE FILES` on volume
# MAGIC - `READ FILES` on volume (for verification)
# MAGIC
# MAGIC #### 3. Databricks Environment
# MAGIC
# MAGIC **Compute Requirements:**
# MAGIC - Serverless compute (recommended)
# MAGIC - Python 3.x runtime
# MAGIC - No special cluster libraries required
# MAGIC
# MAGIC **Libraries:**
# MAGIC - `requests` (built-in)
# MAGIC - `json` (built-in)
# MAGIC - `datetime` (built-in)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Installation Steps
# MAGIC
# MAGIC #### Step 1: Import Helper Notebook
# MAGIC
# MAGIC 1. Import `00.Helper Function` notebook
# MAGIC 2. Place in your workspace folder
# MAGIC 3. Verify all functions load correctly
# MAGIC
# MAGIC #### Step 2: Create Secret Scope
# MAGIC
# MAGIC ```python
# MAGIC %run "./00.Helper Function"
# MAGIC create_scope("youtube", "DATABRICKS")
# MAGIC ```
# MAGIC
# MAGIC #### Step 3: Store API Key
# MAGIC
# MAGIC ```python
# MAGIC create_secret("youtube", "api_key", "YOUR_ACTUAL_YOUTUBE_API_KEY")
# MAGIC ```
# MAGIC
# MAGIC **⚠️ Important:**
# MAGIC - Replace `YOUR_ACTUAL_YOUTUBE_API_KEY` with real key
# MAGIC - Never commit API keys to version control
# MAGIC - Use secret scopes for all sensitive credentials
# MAGIC
# MAGIC #### Step 4: Verify Setup
# MAGIC
# MAGIC ```python
# MAGIC # List scopes
# MAGIC list_scopes()
# MAGIC
# MAGIC # List secrets in youtube scope
# MAGIC list_secrets("youtube")
# MAGIC
# MAGIC # Test API key (optional)
# MAGIC api_key = dbutils.secrets.get(scope="youtube", key="api_key")
# MAGIC print(f"API key configured: {api_key[:10]}...")
# MAGIC ```
# MAGIC
# MAGIC #### Step 5: Test Extraction
# MAGIC
# MAGIC ```python
# MAGIC # Test with a small channel
# MAGIC playlist_id = get_playlist_id("test_channel")
# MAGIC print(f"Playlist ID: {playlist_id}")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Configuration Options
# MAGIC
# MAGIC #### Modify Storage Location
# MAGIC
# MAGIC Edit the `save_to_json` function:
# MAGIC
# MAGIC ```python
# MAGIC def save_to_json(extracted_data):
# MAGIC     volume_path = "/Volumes/YOUR_CATALOG/YOUR_SCHEMA/YOUR_VOLUME"
# MAGIC     # Rest of function...
# MAGIC ```
# MAGIC
# MAGIC #### Change File Naming Convention
# MAGIC
# MAGIC ```python
# MAGIC # Current: yt_data_2026-07-05.json
# MAGIC file_path = f"{volume_path}/yt_data_{date.today()}.json"
# MAGIC
# MAGIC # Alternative: Include channel name
# MAGIC file_path = f"{volume_path}/yt_data_{channel_name}_{date.today()}.json"
# MAGIC
# MAGIC # Alternative: Include timestamp
# MAGIC file_path = f"{volume_path}/yt_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
# MAGIC ```
# MAGIC
# MAGIC #### Adjust Pagination Limits
# MAGIC
# MAGIC Modify `get_video_ids` function:
# MAGIC
# MAGIC ```python
# MAGIC # Current: 50 videos per page (API maximum)
# MAGIC maxResults=50
# MAGIC
# MAGIC # For testing with fewer videos:
# MAGIC maxResults=10
# MAGIC
# MAGIC # Add video count limit:
# MAGIC if len(video_ids) >= 100:  # Stop after 100 videos
# MAGIC     break
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Workflow & Usage
# MAGIC %md
# MAGIC ## Workflow
# MAGIC
# MAGIC ### Complete Pipeline Execution
# MAGIC
# MAGIC ```python
# MAGIC # Step 1: Load helper functions
# MAGIC %run "./00.Helper Function"
# MAGIC
# MAGIC # Step 2: Configure secrets (one-time setup)
# MAGIC create_scope("youtube", "DATABRICKS")
# MAGIC create_secret("youtube", "api_key", "YOUR_YOUTUBE_API_KEY")
# MAGIC
# MAGIC # Step 3: Import required libraries
# MAGIC import requests
# MAGIC import json
# MAGIC from datetime import date
# MAGIC
# MAGIC # Step 4: Define extraction functions
# MAGIC def get_playlist_id(channel_handle: str) -> str:
# MAGIC     # ... (function code)
# MAGIC     pass
# MAGIC
# MAGIC def get_video_ids(playlist_id: str) -> list:
# MAGIC     # ... (function code)
# MAGIC     pass
# MAGIC
# MAGIC def extract_video_data(video_ids: list) -> dict:
# MAGIC     # ... (function code)
# MAGIC     pass
# MAGIC
# MAGIC def save_to_json(extracted_data: dict) -> None:
# MAGIC     # ... (function code)
# MAGIC     pass
# MAGIC
# MAGIC # Step 5: Execute pipeline
# MAGIC playlist_id = get_playlist_id(channel_handle="MrBeast")
# MAGIC video_ids = get_video_ids(playlist_id)
# MAGIC extracted_data = extract_video_data(video_ids)
# MAGIC save_to_json(extracted_data)
# MAGIC ```
# MAGIC
# MAGIC ### Execution Flow Diagram
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────┐
# MAGIC │   User Input: Channel   │
# MAGIC │   Handle ("MrBeast")    │
# MAGIC └─────────┬────────────────┘
# MAGIC          │
# MAGIC          │ get_playlist_id()
# MAGIC          │ ├─ Search API
# MAGIC          │ └─ Channels API
# MAGIC          │
# MAGIC          ▼
# MAGIC ┌─────────────────────────┐
# MAGIC │   Playlist ID          │
# MAGIC │   (UUX6OQ3D...)        │
# MAGIC └─────────┬────────────────┘
# MAGIC          │
# MAGIC          │ get_video_ids()
# MAGIC          │ └─ PlaylistItems API
# MAGIC          │    (paginated)
# MAGIC          │
# MAGIC          ▼
# MAGIC ┌─────────────────────────┐
# MAGIC │   List of Video IDs    │
# MAGIC │   [id1, id2, ...]      │
# MAGIC └─────────┬────────────────┘
# MAGIC          │
# MAGIC          │ extract_video_data()
# MAGIC          │ └─ Videos API
# MAGIC          │    (batch request)
# MAGIC          │
# MAGIC          ▼
# MAGIC ┌─────────────────────────┐
# MAGIC │   Video Metadata Dict  │
# MAGIC │   {id: {data}, ...}    │
# MAGIC └─────────┬────────────────┘
# MAGIC          │
# MAGIC          │ save_to_json()
# MAGIC          │ └─ Write to Volume
# MAGIC          │
# MAGIC          ▼
# MAGIC ┌─────────────────────────┐
# MAGIC │   JSON File Saved      │
# MAGIC │   /Volumes/youtube/... │
# MAGIC └─────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Usage Examples
# MAGIC
# MAGIC #### Example 1: Single Channel Extraction
# MAGIC
# MAGIC ```python
# MAGIC # Extract data for one channel
# MAGIC playlist_id = get_playlist_id("TechChannel")
# MAGIC video_ids = get_video_ids(playlist_id)
# MAGIC data = extract_video_data(video_ids)
# MAGIC save_to_json(data)
# MAGIC ```
# MAGIC
# MAGIC #### Example 2: Multiple Channels
# MAGIC
# MAGIC ```python
# MAGIC channels = ["MrBeast", "TechWithTim", "CodeWithChris"]
# MAGIC
# MAGIC for channel in channels:
# MAGIC     try:
# MAGIC         print(f"Processing {channel}...")
# MAGIC         playlist_id = get_playlist_id(channel)
# MAGIC         video_ids = get_video_ids(playlist_id)
# MAGIC         data = extract_video_data(video_ids)
# MAGIC         
# MAGIC         # Save with channel name in filename
# MAGIC         volume_path = "/Volumes/youtube/landing/data"
# MAGIC         file_path = f"{volume_path}/{channel}_{date.today()}.json"
# MAGIC         with open(file_path, "w", encoding="utf-8") as f:
# MAGIC             json.dump(data, f, indent=4, ensure_ascii=False)
# MAGIC         
# MAGIC         print(f"Completed {channel}")
# MAGIC     except Exception as e:
# MAGIC         print(f"Error processing {channel}: {e}")
# MAGIC ```
# MAGIC
# MAGIC #### Example 3: Filtered Video Extraction
# MAGIC
# MAGIC ```python
# MAGIC # Extract only recent videos
# MAGIC playlist_id = get_playlist_id("NewsChannel")
# MAGIC all_video_ids = get_video_ids(playlist_id)
# MAGIC
# MAGIC # Take only first 10 videos (most recent)
# MAGIC recent_video_ids = all_video_ids[:10]
# MAGIC
# MAGIC data = extract_video_data(recent_video_ids)
# MAGIC save_to_json(data)
# MAGIC ```
# MAGIC
# MAGIC #### Example 4: Data Validation
# MAGIC
# MAGIC ```python
# MAGIC playlist_id = get_playlist_id("DataChannel")
# MAGIC video_ids = get_video_ids(playlist_id)
# MAGIC data = extract_video_data(video_ids)
# MAGIC
# MAGIC # Validate before saving
# MAGIC print(f"Total videos extracted: {len(data)}")
# MAGIC print(f"Total views: {sum(v['view_count'] for v in data.values())}")
# MAGIC print(f"Total likes: {sum(v['like_count'] for v in data.values())}")
# MAGIC
# MAGIC if len(data) > 0:
# MAGIC     save_to_json(data)
# MAGIC     print("Data validated and saved successfully")
# MAGIC else:
# MAGIC     print("No data to save")
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Output Format & Error Handling
# MAGIC %md
# MAGIC ## Output Format
# MAGIC
# MAGIC ### JSON Structure
# MAGIC
# MAGIC The pipeline generates a JSON file with the following structure:
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "video_id_1": {
# MAGIC     "video_id": "dQw4w9WgXcQ",
# MAGIC     "title": "Example Video Title",
# MAGIC     "description": "Full video description text...",
# MAGIC     "published_at": "2026-01-15T10:30:00Z",
# MAGIC     "view_count": 1500000,
# MAGIC     "like_count": 75000,
# MAGIC     "comment_count": 2500,
# MAGIC     "duration": "PT10M30S",
# MAGIC     "tags": ["tag1", "tag2", "tag3"],
# MAGIC     "category_id": "22",
# MAGIC     "channel_id": "UCxxxxxxxxxxxxxxxxxxxxx",
# MAGIC     "channel_title": "Channel Name"
# MAGIC   },
# MAGIC   "video_id_2": {
# MAGIC     "video_id": "another_video_id",
# MAGIC     "title": "Another Video",
# MAGIC     ...
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Field Descriptions
# MAGIC
# MAGIC | Field | Type | Description | Example |
# MAGIC |-------|------|-------------|----------|
# MAGIC | `video_id` | string | Unique YouTube video identifier | "dQw4w9WgXcQ" |
# MAGIC | `title` | string | Video title | "Amazing Tutorial" |
# MAGIC | `description` | string | Full video description | "In this video..." |
# MAGIC | `published_at` | string | ISO 8601 timestamp | "2026-01-15T10:30:00Z" |
# MAGIC | `view_count` | integer | Total views | 1500000 |
# MAGIC | `like_count` | integer | Total likes | 75000 |
# MAGIC | `comment_count` | integer | Total comments | 2500 |
# MAGIC | `duration` | string | ISO 8601 duration | "PT10M30S" (10 min 30 sec) |
# MAGIC | `tags` | array | Video tags/keywords | ["tutorial", "python"] |
# MAGIC | `category_id` | string | YouTube category ID | "22" (People & Blogs) |
# MAGIC | `channel_id` | string | Channel identifier | "UCxxx..." |
# MAGIC | `channel_title` | string | Channel name | "Tech Channel" |
# MAGIC
# MAGIC ### Duration Format (ISO 8601)
# MAGIC
# MAGIC - `PT5M`: 5 minutes
# MAGIC - `PT1H30M`: 1 hour 30 minutes
# MAGIC - `PT2H15M30S`: 2 hours, 15 minutes, 30 seconds
# MAGIC - `PT45S`: 45 seconds
# MAGIC
# MAGIC ### Category IDs
# MAGIC
# MAGIC | ID | Category |
# MAGIC |----|----------|
# MAGIC | 1 | Film & Animation |
# MAGIC | 2 | Autos & Vehicles |
# MAGIC | 10 | Music |
# MAGIC | 15 | Pets & Animals |
# MAGIC | 17 | Sports |
# MAGIC | 19 | Travel & Events |
# MAGIC | 20 | Gaming |
# MAGIC | 22 | People & Blogs |
# MAGIC | 23 | Comedy |
# MAGIC | 24 | Entertainment |
# MAGIC | 25 | News & Politics |
# MAGIC | 26 | Howto & Style |
# MAGIC | 27 | Education |
# MAGIC | 28 | Science & Technology |
# MAGIC | 29 | Nonprofits & Activism |
# MAGIC
# MAGIC ### File Storage
# MAGIC
# MAGIC **Path Structure:**
# MAGIC ```
# MAGIC /Volumes/
# MAGIC   └─ youtube/
# MAGIC       └─ landing/
# MAGIC           └─ data/
# MAGIC               ├─ yt_data_2026-07-05.json
# MAGIC               ├─ yt_data_2026-07-06.json
# MAGIC               └─ yt_data_2026-07-07.json
# MAGIC ```
# MAGIC
# MAGIC **File Properties:**
# MAGIC - Format: JSON
# MAGIC - Encoding: UTF-8
# MAGIC - Indentation: 4 spaces
# MAGIC - Non-ASCII characters preserved (`ensure_ascii=False`)
# MAGIC - Naming: `yt_data_YYYY-MM-DD.json`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Error Handling
# MAGIC
# MAGIC ### Common Errors & Solutions
# MAGIC
# MAGIC #### 1. API Key Errors
# MAGIC
# MAGIC **Error:** `400 Client Error: Bad Request`
# MAGIC
# MAGIC **Causes:**
# MAGIC - Invalid API key
# MAGIC - API not enabled in Google Cloud
# MAGIC - Quota exceeded
# MAGIC
# MAGIC **Solutions:**
# MAGIC ```python
# MAGIC # Verify API key
# MAGIC api_key = dbutils.secrets.get(scope="youtube", key="api_key")
# MAGIC print(f"API key configured: {api_key[:10]}...")
# MAGIC
# MAGIC # Check if it's not the placeholder
# MAGIC if api_key == "value_of_api_key":
# MAGIC     print("ERROR: Using placeholder API key!")
# MAGIC     print("Please update with real YouTube API key")
# MAGIC ```
# MAGIC
# MAGIC **Fix:**
# MAGIC 1. Get valid API key from Google Cloud Console
# MAGIC 2. Enable YouTube Data API v3
# MAGIC 3. Update secret: `create_secret("youtube", "api_key", "REAL_KEY")`
# MAGIC
# MAGIC #### 2. Channel Not Found
# MAGIC
# MAGIC **Error:** `Exception: No channel found for handle: ChannelName`
# MAGIC
# MAGIC **Causes:**
# MAGIC - Incorrect channel handle/name
# MAGIC - Channel doesn't exist
# MAGIC - Channel is private
# MAGIC
# MAGIC **Solutions:**
# MAGIC ```python
# MAGIC # Try different search terms
# MAGIC handles = ["MrBeast", "@MrBeast", "MrBeast Official"]
# MAGIC
# MAGIC for handle in handles:
# MAGIC     try:
# MAGIC         playlist_id = get_playlist_id(handle)
# MAGIC         print(f"Found with '{handle}': {playlist_id}")
# MAGIC         break
# MAGIC     except Exception as e:
# MAGIC         print(f"Failed with '{handle}': {e}")
# MAGIC ```
# MAGIC
# MAGIC #### 3. Empty Results
# MAGIC
# MAGIC **Error:** `IndexError: list index out of range`
# MAGIC
# MAGIC **Causes:**
# MAGIC - No videos in playlist
# MAGIC - Channel has no uploads
# MAGIC - API returned empty results
# MAGIC
# MAGIC **Solutions:**
# MAGIC ```python
# MAGIC # Add validation checks
# MAGIC data = response.json()
# MAGIC if not data.get("items"):
# MAGIC     raise Exception("No items found in API response")
# MAGIC
# MAGIC channel_items = data["items"]
# MAGIC if len(channel_items) == 0:
# MAGIC     raise Exception("Channel has no content")
# MAGIC ```
# MAGIC
# MAGIC #### 4. Volume Permission Errors
# MAGIC
# MAGIC **Error:** `PermissionError: [Errno 13] Permission denied`
# MAGIC
# MAGIC **Causes:**
# MAGIC - No WRITE FILES permission on volume
# MAGIC - Volume doesn't exist
# MAGIC - Incorrect volume path
# MAGIC
# MAGIC **Solutions:**
# MAGIC ```sql
# MAGIC -- Grant permissions
# MAGIC GRANT WRITE FILES ON VOLUME youtube.landing.data TO `user@company.com`;
# MAGIC
# MAGIC -- Verify volume exists
# MAGIC SHOW VOLUMES IN youtube.landing;
# MAGIC ```
# MAGIC
# MAGIC #### 5. Quota Exceeded
# MAGIC
# MAGIC **Error:** `403 Client Error: Forbidden` with quota message
# MAGIC
# MAGIC **Causes:**
# MAGIC - Exceeded daily API quota (10,000 units)
# MAGIC - Too many requests in short time
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Wait until quota resets (midnight Pacific Time)
# MAGIC - Request quota increase in Google Cloud Console
# MAGIC - Optimize queries to use fewer API calls
# MAGIC - Implement caching for repeated requests
# MAGIC
# MAGIC #### 6. Network/Connection Errors
# MAGIC
# MAGIC **Error:** `requests.exceptions.ConnectionError`
# MAGIC
# MAGIC **Causes:**
# MAGIC - Network connectivity issues
# MAGIC - API endpoint unreachable
# MAGIC - Timeout
# MAGIC
# MAGIC **Solutions:**
# MAGIC ```python
# MAGIC import time
# MAGIC
# MAGIC def retry_request(url, max_retries=3):
# MAGIC     for attempt in range(max_retries):
# MAGIC         try:
# MAGIC             response = requests.get(url, timeout=30)
# MAGIC             response.raise_for_status()
# MAGIC             return response
# MAGIC         except requests.exceptions.RequestException as e:
# MAGIC             if attempt < max_retries - 1:
# MAGIC                 print(f"Retry {attempt + 1}/{max_retries}...")
# MAGIC                 time.sleep(2 ** attempt)  # Exponential backoff
# MAGIC             else:
# MAGIC                 raise e
# MAGIC ```
# MAGIC
# MAGIC ### Exception Handling Best Practices
# MAGIC
# MAGIC ```python
# MAGIC # Add comprehensive error handling
# MAGIC try:
# MAGIC     playlist_id = get_playlist_id("ChannelName")
# MAGIC     video_ids = get_video_ids(playlist_id)
# MAGIC     
# MAGIC     if not video_ids:
# MAGIC         print("No videos found in playlist")
# MAGIC         return
# MAGIC     
# MAGIC     data = extract_video_data(video_ids)
# MAGIC     
# MAGIC     if data:
# MAGIC         save_to_json(data)
# MAGIC         print(f"Successfully extracted {len(data)} videos")
# MAGIC     else:
# MAGIC         print("No data extracted")
# MAGIC         
# MAGIC except requests.exceptions.HTTPError as e:
# MAGIC     print(f"HTTP Error: {e}")
# MAGIC     print("Check API key and quota")
# MAGIC except requests.exceptions.ConnectionError:
# MAGIC     print("Network connection error")
# MAGIC     print("Check internet connectivity")
# MAGIC except KeyError as e:
# MAGIC     print(f"Missing expected data field: {e}")
# MAGIC     print("API response structure may have changed")
# MAGIC except Exception as e:
# MAGIC     print(f"Unexpected error: {e}")
# MAGIC     import traceback
# MAGIC     traceback.print_exc()
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Best Practices & Advanced Topics
# MAGIC %md
# MAGIC ## Best Practices
# MAGIC
# MAGIC ### Security
# MAGIC
# MAGIC #### 1. API Key Management
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC - Store API keys in Databricks Secret Scopes
# MAGIC - Use descriptive secret names (`api_key`, not `key1`)
# MAGIC - Rotate API keys regularly
# MAGIC - Use separate keys for dev/prod environments
# MAGIC
# MAGIC ❌ **DON'T:**
# MAGIC - Hard-code API keys in notebooks
# MAGIC - Commit keys to version control (Git)
# MAGIC - Share keys via email or chat
# MAGIC - Use placeholder values in production
# MAGIC
# MAGIC #### 2. Access Control
# MAGIC
# MAGIC ```sql
# MAGIC -- Grant minimal necessary permissions
# MAGIC GRANT READ FILES ON VOLUME youtube.landing.data TO `data_analysts`;
# MAGIC GRANT WRITE FILES ON VOLUME youtube.landing.data TO `data_engineers`;
# MAGIC
# MAGIC -- Secret scope permissions
# MAGIC GRANT READ ON SECRET SCOPE youtube TO `etl_service_account`;
# MAGIC ```
# MAGIC
# MAGIC ### Performance Optimization
# MAGIC
# MAGIC #### 1. Batch Processing
# MAGIC
# MAGIC ```python
# MAGIC # Process videos in batches to optimize API calls
# MAGIC def extract_video_data_batched(video_ids: list, batch_size: int = 50) -> dict:
# MAGIC     """
# MAGIC     Extract video data in batches to optimize API usage.
# MAGIC     YouTube API allows up to 50 IDs per request.
# MAGIC     """
# MAGIC     all_data = {}
# MAGIC     
# MAGIC     for i in range(0, len(video_ids), batch_size):
# MAGIC         batch = video_ids[i:i + batch_size]
# MAGIC         batch_data = extract_video_data(batch)
# MAGIC         all_data.update(batch_data)
# MAGIC         
# MAGIC         # Rate limiting: avoid hitting quota too fast
# MAGIC         if i + batch_size < len(video_ids):
# MAGIC             time.sleep(0.5)  # 500ms delay between batches
# MAGIC     
# MAGIC     return all_data
# MAGIC ```
# MAGIC
# MAGIC #### 2. Caching Results
# MAGIC
# MAGIC ```python
# MAGIC import pickle
# MAGIC from pathlib import Path
# MAGIC
# MAGIC # Cache playlist IDs to avoid repeated lookups
# MAGIC cache_file = "/tmp/channel_cache.pkl"
# MAGIC
# MAGIC def get_cached_playlist_id(channel_handle: str) -> str:
# MAGIC     cache = {}
# MAGIC     
# MAGIC     # Load cache if exists
# MAGIC     if Path(cache_file).exists():
# MAGIC         with open(cache_file, 'rb') as f:
# MAGIC             cache = pickle.load(f)
# MAGIC     
# MAGIC     # Check cache
# MAGIC     if channel_handle in cache:
# MAGIC         print(f"Using cached playlist ID for {channel_handle}")
# MAGIC         return cache[channel_handle]
# MAGIC     
# MAGIC     # Fetch from API
# MAGIC     playlist_id = get_playlist_id(channel_handle)
# MAGIC     
# MAGIC     # Update cache
# MAGIC     cache[channel_handle] = playlist_id
# MAGIC     with open(cache_file, 'wb') as f:
# MAGIC         pickle.dump(cache, f)
# MAGIC     
# MAGIC     return playlist_id
# MAGIC ```
# MAGIC
# MAGIC #### 3. Incremental Loading
# MAGIC
# MAGIC ```python
# MAGIC import json
# MAGIC from datetime import datetime, timedelta
# MAGIC
# MAGIC def get_new_videos_only(channel_handle: str, days_back: int = 7) -> dict:
# MAGIC     """
# MAGIC     Extract only videos published in the last N days.
# MAGIC     Reduces API calls and processing time.
# MAGIC     """
# MAGIC     playlist_id = get_playlist_id(channel_handle)
# MAGIC     all_video_ids = get_video_ids(playlist_id)
# MAGIC     
# MAGIC     # Get video data
# MAGIC     all_data = extract_video_data(all_video_ids)
# MAGIC     
# MAGIC     # Filter by date
# MAGIC     cutoff_date = datetime.now() - timedelta(days=days_back)
# MAGIC     new_videos = {}
# MAGIC     
# MAGIC     for video_id, video_data in all_data.items():
# MAGIC         published = datetime.fromisoformat(video_data['published_at'].replace('Z', '+00:00'))
# MAGIC         if published > cutoff_date:
# MAGIC             new_videos[video_id] = video_data
# MAGIC     
# MAGIC     print(f"Found {len(new_videos)} new videos out of {len(all_data)} total")
# MAGIC     return new_videos
# MAGIC ```
# MAGIC
# MAGIC #### 4. Parallel Processing
# MAGIC
# MAGIC ```python
# MAGIC from concurrent.futures import ThreadPoolExecutor, as_completed
# MAGIC
# MAGIC def extract_multiple_channels_parallel(channels: list, max_workers: int = 5) -> dict:
# MAGIC     """
# MAGIC     Process multiple channels in parallel.
# MAGIC     """
# MAGIC     results = {}
# MAGIC     
# MAGIC     def process_channel(channel):
# MAGIC         try:
# MAGIC             playlist_id = get_playlist_id(channel)
# MAGIC             video_ids = get_video_ids(playlist_id)
# MAGIC             data = extract_video_data(video_ids)
# MAGIC             return (channel, data)
# MAGIC         except Exception as e:
# MAGIC             print(f"Error processing {channel}: {e}")
# MAGIC             return (channel, None)
# MAGIC     
# MAGIC     with ThreadPoolExecutor(max_workers=max_workers) as executor:
# MAGIC         futures = {executor.submit(process_channel, ch): ch for ch in channels}
# MAGIC         
# MAGIC         for future in as_completed(futures):
# MAGIC             channel, data = future.result()
# MAGIC             if data:
# MAGIC                 results[channel] = data
# MAGIC     
# MAGIC     return results
# MAGIC ```
# MAGIC
# MAGIC ### Data Quality
# MAGIC
# MAGIC #### 1. Validation
# MAGIC
# MAGIC ```python
# MAGIC def validate_video_data(data: dict) -> tuple:
# MAGIC     """
# MAGIC     Validate extracted video data.
# MAGIC     Returns (is_valid, error_messages)
# MAGIC     """
# MAGIC     errors = []
# MAGIC     
# MAGIC     # Check required fields
# MAGIC     required_fields = ['video_id', 'title', 'view_count', 'published_at']
# MAGIC     
# MAGIC     for video_id, video in data.items():
# MAGIC         for field in required_fields:
# MAGIC             if field not in video:
# MAGIC                 errors.append(f"Missing {field} in video {video_id}")
# MAGIC         
# MAGIC         # Validate data types
# MAGIC         if 'view_count' in video and not isinstance(video['view_count'], int):
# MAGIC             errors.append(f"Invalid view_count type in video {video_id}")
# MAGIC         
# MAGIC         # Check for nulls
# MAGIC         if video.get('title') in [None, '']:
# MAGIC             errors.append(f"Empty title in video {video_id}")
# MAGIC     
# MAGIC     return (len(errors) == 0, errors)
# MAGIC
# MAGIC # Usage
# MAGIC data = extract_video_data(video_ids)
# MAGIC is_valid, errors = validate_video_data(data)
# MAGIC
# MAGIC if is_valid:
# MAGIC     save_to_json(data)
# MAGIC else:
# MAGIC     print("Validation errors:")
# MAGIC     for error in errors:
# MAGIC         print(f"  - {error}")
# MAGIC ```
# MAGIC
# MAGIC #### 2. Data Cleaning
# MAGIC
# MAGIC ```python
# MAGIC def clean_video_data(data: dict) -> dict:
# MAGIC     """
# MAGIC     Clean and normalize video data.
# MAGIC     """
# MAGIC     cleaned = {}
# MAGIC     
# MAGIC     for video_id, video in data.items():
# MAGIC         cleaned_video = video.copy()
# MAGIC         
# MAGIC         # Handle missing counts (set to 0)
# MAGIC         for count_field in ['view_count', 'like_count', 'comment_count']:
# MAGIC             if count_field not in cleaned_video or cleaned_video[count_field] is None:
# MAGIC                 cleaned_video[count_field] = 0
# MAGIC         
# MAGIC         # Ensure tags is a list
# MAGIC         if 'tags' not in cleaned_video or cleaned_video['tags'] is None:
# MAGIC             cleaned_video['tags'] = []
# MAGIC         
# MAGIC         # Truncate long descriptions
# MAGIC         if len(cleaned_video.get('description', '')) > 5000:
# MAGIC             cleaned_video['description'] = cleaned_video['description'][:5000] + '...'
# MAGIC         
# MAGIC         cleaned[video_id] = cleaned_video
# MAGIC     
# MAGIC     return cleaned
# MAGIC ```
# MAGIC
# MAGIC ### Monitoring & Logging
# MAGIC
# MAGIC #### 1. Execution Logging
# MAGIC
# MAGIC ```python
# MAGIC import logging
# MAGIC from datetime import datetime
# MAGIC
# MAGIC # Configure logging
# MAGIC logging.basicConfig(
# MAGIC     level=logging.INFO,
# MAGIC     format='%(asctime)s - %(levelname)s - %(message)s'
# MAGIC )
# MAGIC logger = logging.getLogger(__name__)
# MAGIC
# MAGIC def extract_with_logging(channel_handle: str):
# MAGIC     logger.info(f"Starting extraction for channel: {channel_handle}")
# MAGIC     start_time = datetime.now()
# MAGIC     
# MAGIC     try:
# MAGIC         # Step 1: Get playlist
# MAGIC         logger.info("Fetching playlist ID...")
# MAGIC         playlist_id = get_playlist_id(channel_handle)
# MAGIC         logger.info(f"Playlist ID: {playlist_id}")
# MAGIC         
# MAGIC         # Step 2: Get video IDs
# MAGIC         logger.info("Fetching video IDs...")
# MAGIC         video_ids = get_video_ids(playlist_id)
# MAGIC         logger.info(f"Found {len(video_ids)} videos")
# MAGIC         
# MAGIC         # Step 3: Extract data
# MAGIC         logger.info("Extracting video metadata...")
# MAGIC         data = extract_video_data(video_ids)
# MAGIC         logger.info(f"Extracted data for {len(data)} videos")
# MAGIC         
# MAGIC         # Step 4: Save
# MAGIC         logger.info("Saving to volume...")
# MAGIC         save_to_json(data)
# MAGIC         
# MAGIC         duration = (datetime.now() - start_time).total_seconds()
# MAGIC         logger.info(f"Extraction completed in {duration:.2f} seconds")
# MAGIC         
# MAGIC         return data
# MAGIC         
# MAGIC     except Exception as e:
# MAGIC         logger.error(f"Extraction failed: {e}")
# MAGIC         logger.exception("Full traceback:")
# MAGIC         raise
# MAGIC ```
# MAGIC
# MAGIC #### 2. Metrics Tracking
# MAGIC
# MAGIC ```python
# MAGIC def extract_with_metrics(channel_handle: str) -> dict:
# MAGIC     metrics = {
# MAGIC         'channel': channel_handle,
# MAGIC         'start_time': datetime.now().isoformat(),
# MAGIC         'api_calls': 0,
# MAGIC         'videos_processed': 0,
# MAGIC         'errors': [],
# MAGIC         'status': 'running'
# MAGIC     }
# MAGIC     
# MAGIC     try:
# MAGIC         # Track API calls
# MAGIC         playlist_id = get_playlist_id(channel_handle)
# MAGIC         metrics['api_calls'] += 2  # Search + Channels API
# MAGIC         
# MAGIC         video_ids = get_video_ids(playlist_id)
# MAGIC         metrics['api_calls'] += len(video_ids) // 50 + 1  # Paginated calls
# MAGIC         
# MAGIC         data = extract_video_data(video_ids)
# MAGIC         metrics['api_calls'] += 1  # Videos API
# MAGIC         metrics['videos_processed'] = len(data)
# MAGIC         
# MAGIC         save_to_json(data)
# MAGIC         
# MAGIC         metrics['status'] = 'success'
# MAGIC         metrics['end_time'] = datetime.now().isoformat()
# MAGIC         
# MAGIC         # Save metrics
# MAGIC         metrics_path = f"/Volumes/youtube/landing/metrics/metrics_{date.today()}.json"
# MAGIC         with open(metrics_path, 'w') as f:
# MAGIC             json.dump(metrics, f, indent=2)
# MAGIC         
# MAGIC         return data
# MAGIC         
# MAGIC     except Exception as e:
# MAGIC         metrics['status'] = 'failed'
# MAGIC         metrics['errors'].append(str(e))
# MAGIC         raise
# MAGIC ```
# MAGIC
# MAGIC ### Production Deployment
# MAGIC
# MAGIC #### 1. Scheduled Job Configuration
# MAGIC
# MAGIC ```python
# MAGIC # Create as a Databricks Job with schedule
# MAGIC # Job configuration (JSON):
# MAGIC {
# MAGIC   "name": "YouTube Data Extraction",
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC       "task_key": "extract_youtube_data",
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/Users/user@company.com/01.Extracting The Youtube Data",
# MAGIC         "base_parameters": {
# MAGIC           "channel_handle": "MrBeast"
# MAGIC         }
# MAGIC       },
# MAGIC       "timeout_seconds": 3600,
# MAGIC       "max_retries": 2
# MAGIC     }
# MAGIC   ],
# MAGIC   "schedule": {
# MAGIC     "quartz_cron_expression": "0 0 2 * * ?",  # Daily at 2 AM
# MAGIC     "timezone_id": "America/Los_Angeles"
# MAGIC   },
# MAGIC   "email_notifications": {
# MAGIC     "on_failure": ["data-team@company.com"]
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC #### 2. Environment Variables
# MAGIC
# MAGIC ```python
# MAGIC # Use widgets for parameterization
# MAGIC dbutils.widgets.text("channel_handle", "MrBeast", "Channel Handle")
# MAGIC dbutils.widgets.dropdown("environment", "prod", ["dev", "staging", "prod"], "Environment")
# MAGIC
# MAGIC channel_handle = dbutils.widgets.get("channel_handle")
# MAGIC environment = dbutils.widgets.get("environment")
# MAGIC
# MAGIC # Environment-specific configuration
# MAGIC if environment == "prod":
# MAGIC     volume_path = "/Volumes/youtube/landing/data"
# MAGIC else:
# MAGIC     volume_path = f"/Volumes/youtube_dev/{environment}/data"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Summary
# MAGIC
# MAGIC This YouTube Data Extraction pipeline provides:
# MAGIC
# MAGIC ✅ **Secure**: API keys stored in secret scopes
# MAGIC ✅ **Scalable**: Handles channels with thousands of videos
# MAGIC ✅ **Reliable**: Comprehensive error handling
# MAGIC ✅ **Maintainable**: Well-documented and modular code
# MAGIC ✅ **Production-ready**: Logging, monitoring, and scheduling support
# MAGIC
# MAGIC ### Quick Start Checklist
# MAGIC
# MAGIC - [ ] Get YouTube Data API v3 key from Google Cloud
# MAGIC - [ ] Create Unity Catalog volume: `youtube.landing.data`
# MAGIC - [ ] Create secret scope: `youtube`
# MAGIC - [ ] Store API key in secret scope
# MAGIC - [ ] Import and run helper notebook
# MAGIC - [ ] Test extraction with a small channel
# MAGIC - [ ] Schedule for production use
# MAGIC
# MAGIC ### Key Metrics
# MAGIC
# MAGIC - **API Quota Usage**: ~100-150 units per channel
# MAGIC - **Processing Time**: ~2-5 seconds per 50 videos
# MAGIC - **Storage**: ~2-5 KB per video (JSON)
# MAGIC - **Recommended Schedule**: Daily at off-peak hours
# MAGIC
# MAGIC ### Support & Resources
# MAGIC
# MAGIC - **YouTube Data API Docs**: https://developers.google.com/youtube/v3
# MAGIC - **Unity Catalog Volumes**: https://docs.databricks.com/unity-catalog/volumes.html
# MAGIC - **Databricks Secrets**: https://docs.databricks.com/security/secrets/index.html
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Version**: 1.0  
# MAGIC **Last Updated**: 2026-07-05  
# MAGIC **Author**: Alok Awasthi
