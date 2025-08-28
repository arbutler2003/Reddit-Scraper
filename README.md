Reddit-Scraper (soon to be improved) is a Python based automation tool that monitors local subreddits for relevant keywords. When a match is discovered in a post/comment, a alert is dispatched via email.

Features

- Reddit Feed Monitoring: Uses Reddit's API stream.
- Keyword Matching: Scans Reddit's stream for specified keywords.
- Notification: Sends alerts via email when a post/comment matches specified keywords.
- Duplicate Prevention: Tracks previously handled post/comments to prevent sending duplicate alerts.

Technology
Language: Python 3.9
Libraries:

- praw: Python Reddit API wrapper.
- python-dotenv: Managing environment variables and secrets.
