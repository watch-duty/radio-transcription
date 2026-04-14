# /// script
# dependencies = [
#   "asyncpg",
# ]
# ///

import asyncio
import datetime
import uuid
import asyncpg

async def main():
    try:
        conn = await asyncpg.connect(
            host="localhost",
            port=5432,
            user="postgres",
            password="postgres",
            database="postgres"
        )
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        print("Make sure docker compose is running and port 5432 is mapped.")
        return

    # Create a test feed
    feed_name = f"thousands-test-feed-{uuid.uuid4().hex[:8]}"
    try:
        feed_id = await conn.fetchval(
            "INSERT INTO feeds (name, source_type) VALUES ($1, $2) RETURNING id",
            feed_name,
            "bcfy_feeds",
        )
        print(f"Created feed '{feed_name}' with ID: {feed_id}")
    except Exception as e:
        print(f"Failed to create feed: {e}")
        await conn.close()
        return

    # Insert thousands of transcripts
    count = 2000
    print(f"Inserting {count} transcripts...")

    data = []
    base_time = datetime.datetime.now(datetime.timezone.utc)

    for i in range(count):
        transmission_id = uuid.uuid4()
        # Make timestamps distinct to test ordering (newest first)
        ts = base_time - datetime.timedelta(seconds=i)
        data.append((
            transmission_id,
            feed_id,
            f"Transcript {i}: Engine 81, EMS code 3, 1575, South Winchester Boulevard, Campbell area.",
            ts, # start_timestamp
            ts + datetime.timedelta(seconds=8), # end_timestamp
            False, # missing_prior_context
            False, # missing_post_context
            [f"gs://ingestion-canonical-bucket/echo/{feed_id}/20260407/Santa_Clara_Co_Fire_Disp_20260407_130126.flac"], # source_audio_uris
            f"gs://ingestion-canonical-bucket/stitched/lossless/{feed_id}/2026/04/07/20260407T130126Z.flac", # canonical_audio_uri
            datetime.timedelta(0), # start_audio_offset
            datetime.timedelta(0), # end_audio_offset
            [] # evaluation_decisions
        ))

    try:
        await conn.executemany("""
            INSERT INTO transcripts (
                transmission_id,
                feed_id,
                transcript,
                start_timestamp,
                end_timestamp,
                missing_prior_context,
                missing_post_context,
                source_audio_uris,
                canonical_audio_uri,
                start_audio_offset,
                end_audio_offset,
                evaluation_decisions
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """, data)
        print("Successfully inserted transcripts!")
        print(f"\nTo test in UI, search for transcripts with Feed ID: {feed_id}")
    except Exception as e:
        print(f"Failed to insert transcripts: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
