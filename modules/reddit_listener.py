"""
This is the Reddit listener module.

This module connects to the Reddit API using PRAW for listening to Reddit posts and
comments from specified subreddits.
"""

import os  # For environment variable management.
import time  # Used for the 'sleep' function to pause the script during reconnection.
import logging  # Structured logging.
import praw  # Python Reddit API Wrapper (PRAW).
import prawcore  # Used for specific exception handling.
from dotenv import load_dotenv  # Load secrets from a local .env file.


# Configure basic logging.
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

def initialize_reddit():
    """
    Initializes the Reddit instance using PRAW.
    Loads API credentials from .env and throws an exception if credentials are missing.

    Raises:
        ValueError: When any required environment variables are missing.
        RuntimeError: When credentials/network prevent successful authentication.
    Returns:
        praw.Reddit: A ready-to-use authenticated Reddit client.
    """
    # Load environment variables from .env into process environment.
    load_dotenv()

    required_env_vars = [
        'REDDIT_CLIENT_ID',
        'REDDIT_CLIENT_SECRET',
        'REDDIT_USER_AGENT',
        'REDDIT_USERNAME',
        'REDDIT_PASSWORD',
    ]

    logger.info('Authenticating with Reddit...')

    # Construct the PRAW client.
    reddit = praw.Reddit(
        client_id=os.getenv('REDDIT_CLIENT_ID'),
        client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
        user_agent=os.getenv('REDDIT_USER_AGENT'),
        username=os.getenv('REDDIT_USERNAME'),
        password=os.getenv('REDDIT_PASSWORD'),
    )

    # Validate credentials.
    try:
        me = reddit.user.me()
        logger.info(f'Authentication successful. Logged in as {me}')
    except (
        prawcore.exceptions.OAuthException,
        prawcore.exceptions.InvalidToken,
        prawcore.exceptions.Forbidden,
        prawcore.exceptions.BadRequest,
        prawcore.exceptions.ResponseException,
        prawcore.exceptions.RequestException,
        prawcore.exceptions.ServerError,
    ) as e:
        raise RuntimeError(f"Failed to verify Reddit authentication: {e}") from e

    return reddit

def stream_reddit_activity(reddit, sub_reddits):
    """
    Yields a combined stream of new submissions and comments from one or more subreddits.

    This function is designed to run indefinitely (until interrupted) and includes
    resilience features like exponential backoff for transient errors.

    Args:
        reddit (praw.Reddit): An authenticated Reddit client.
        sub_reddits (list[str]): Subreddit names (without the 'r/' prefix).

    Yields:
        praw.models.Submission | praw.models.Comment: Next item from the monitored subreddits.

    Notes:
        - For long-running services, prefer logging over print to preserve context and levels.
        - Backoff with jitter helps avoid retry storms when Reddit is under load.
        - Use KeyboardInterrupt (Ctrl+C) to stop gracefully when running as a script.
    """
    # Convert a list like ['a', 'b'] into 'a+b' which Reddit accepts as a multi-subreddit.
    subreddit_string = '+'.join(s.strip() for s in sub_reddits)

    # PRAW object that exposes .stream.submissions() and .stream.comments().
    subreddit = reddit.subreddit(subreddit_string)

    # Retry/backoff settings (tune these based on your operational needs).
    backoff_seconds = 5  # initial backoff after the first transient error
    max_backoff_seconds = 180  # cap to prevent excessive waiting
    while True:
        try:
            logger.info(f'Starting stream from subreddits: {subreddit_string}')

            # Non-blocking streams: pause_after=0 yields None when no items are available,
            # allowing us to alternate between submissions and comments.
            submissions_stream = subreddit.stream.submissions(skip_existing=True, pause_after=0)
            comments_stream = subreddit.stream.comments(skip_existing=True, pause_after=0)

            # Reset backoff since (re)starting the streams succeeded.
            backoff_seconds = 5

            # Round-robin: drain available items from each stream; sleep briefly if none are available.
            while True:
                # Submissions: None means the stream currently has no more items to yield.
                for submission in submissions_stream:
                    if submission is None:
                        break
                    yield submission

                # Comments: same semantics as above.
                for comment in comments_stream:
                    if comment is None:
                        break
                    yield comment

                # Prevent a tight loop when no new items are currently available.
                time.sleep(0.5)

        except KeyboardInterrupt:
            # Let the caller (or __main__) handle shutdown; this keeps cleanup straightforward.
            logger.info("Stream interrupted by user. Shutting down.")
            raise
        except (prawcore.exceptions.Forbidden, prawcore.exceptions.NotFound) as e:
            # Likely invalid or private subreddit(s); these will not self-heal by retrying.
            logger.error(f'Access issue for subreddits "{subreddit_string}": {e}. Stopping stream.')
            raise
        except (prawcore.exceptions.OAuthException, prawcore.exceptions.InvalidToken) as e:
            # Auth issues typically require operator intervention (refresh/rotate creds).
            logger.error(f'Authentication error while streaming: {e}. Stopping stream.')
            raise
        except (
            prawcore.exceptions.RequestException,   # network hiccups/timeouts
            prawcore.exceptions.ResponseException,  # unexpected HTTP issues
            prawcore.exceptions.ServerError,        # 5xx from Reddit
            # praw.exceptions.APIException,         # optionally include if you inspect and decide it’s transient
        ) as e:
            # Transient conditions: apply exponential backoff with a small jitter to avoid synchronized retries.
            logger.warning(f'Error streaming from Reddit: {e}')
            jitter = 0.5 + (time.time() % 1) * 0.5  # small, deterministic jitter in [0.5, 1.0)
            sleep_for = min(max_backoff_seconds, int(backoff_seconds * (1.5 + jitter)))
            logger.info(f'Reconnecting in {sleep_for} seconds...')
            time.sleep(sleep_for)
            backoff_seconds = min(max_backoff_seconds, max(5, sleep_for * 2))
        except Exception as e:
            # Catch-all: log and retry after a fixed delay. Consider alerting if this repeats.
            logger.error(f'Unexpected error: {e}')
            logger.info('Reconnecting in 15 seconds...')
            time.sleep(15)

# Tests the module in isolation. Does not run when imported.
if __name__ == '__main__':
    # In script mode, ensure the root logger emits INFO. Tweak in production as needed.
    logging.getLogger().setLevel(logging.INFO)
    logger.info('Running reddit_listener.py as a standalone script.')
    try:
        reddit_instance = initialize_reddit()

        # Example subreddits to monitor. Replace with your targets or wire via config.
        test_subreddits = ['smallbusiness', 'learnpython']

        # Stream indefinitely and print a compact summary for each item.
        for item in stream_reddit_activity(reddit_instance, test_subreddits):
            # Keep prints short; avoid logging full bodies to reduce log noise.
            if isinstance(item, praw.models.Submission):
                print('-' * 40)
                print(f'New Post in r/{item.subreddit.display_name}:')
                print(f'  Title: {item.title}')
                print(f'  URL: https://www.reddit.com{item.permalink}')
            elif isinstance(item, praw.models.Comment):
                print('-' * 40)
                print(f'New Comment in r/{item.subreddit.display_name}:')
                print(f'  Comment: {item.body[:80]}...')
                print(f'  URL: https://www.reddit.com{item.permalink}')
    except ValueError as e:
        # Configuration issues should be explicit so operators can fix .env or env vars.
        logger.error(f'Configuration Error: {e}')
    except RuntimeError as e:
        # Authentication failures: typically require changing credentials or app type/scopes.
        logger.error(str(e))
    except KeyboardInterrupt:
        # Graceful shutdown path (e.g., Docker stop, Ctrl+C).
        logger.info("Shut down by user.")
    except Exception as e:
        # Unexpected crash path; include stack trace for diagnosis.
        logger.exception(f'An unexpected error occurred: {e}')
