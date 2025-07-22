import asyncio
import os
import uvloop
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from typing import Dict, Any

from faststream import Logger
from faststream.redis import RedisBroker, ListSub
from faststream.asgi import AsgiFastStream
import redis.asyncio as aioredis

from models import (
    ReportGenerationMessage,
    ReportMetadata,
    ReportStatus,
)
from report_processor import ReportProcessor


REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
REPORT_METADATA_KEY = "report:metadata:{report_id}"
REPORT_DATA_KEY = "report:data:{report_id}"
REPORT_STATUS_KEY = "report:status:{report_id}"
REPORT_QUEUE = "report_generation_queue"

broker = RedisBroker(REDIS_URL)

redis_client = None
report_processor = ReportProcessor()

active_reports: Dict[str, asyncio.Task] = {}


@asynccontextmanager
async def lifespan():
    global redis_client

    redis_client = await aioredis.from_url(REDIS_URL, decode_responses=True)
    cleanup_task = asyncio.create_task(periodic_cleanup())

    yield

    cleanup_task.cancel()

    for task in active_reports.values():
        task.cancel()

    await redis_client.close()


app = AsgiFastStream(broker, lifespan=lifespan)


@broker.subscriber(list=ListSub(REPORT_QUEUE), no_ack=False)
async def handle_report_generation(message: ReportGenerationMessage, logger: Logger):
    report_id = message.report_id

    task = asyncio.create_task(process_report(message, logger))
    active_reports[report_id] = task

    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception:
        pass


@broker.subscriber("report_cancellations")
async def handle_report_cancellation(message: Dict[str, Any], logger: Logger):
    if message.get("action") == "cancel":
        report_id = message.get("report_id")

        if report_id in active_reports:
            logger.info(f"Cancelling report {report_id}")
            task = active_reports[report_id]
            task.cancel()

            try:
                await asyncio.wait_for(task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass


async def update_report_status(
    report_id: str,
    status: ReportStatus,
    progress: int = None,
    message: str = None,
    error: str = None,
):
    metadata_key = REPORT_METADATA_KEY.format(report_id=report_id)
    metadata_json = await redis_client.get(metadata_key)

    if not metadata_json:
        return

    metadata = ReportMetadata.model_validate_json(metadata_json)
    metadata.status = status
    metadata.progress = progress
    metadata.message = message
    metadata.updated_at = datetime.now(timezone.utc)

    if status == ReportStatus.COMPLETED:
        metadata.completed_at = datetime.now(timezone.utc)
    elif status == ReportStatus.FAILED:
        metadata.error = error

    ttl = await redis_client.ttl(metadata_key)

    if ttl > 0:
        await redis_client.setex(metadata_key, ttl, metadata.model_dump_json())


async def progress_callback(report_id: int, progress: int, message_text: str):
    if report_id not in active_reports:
        raise asyncio.CancelledError("Report cancelled")

    await update_report_status(
        report_id, ReportStatus.PROCESSING, progress=progress, message=message_text
    )


async def process_report(message: ReportGenerationMessage, logger: Logger):
    report_id = message.report_id

    try:
        await update_report_status(
            report_id,
            ReportStatus.PROCESSING,
            progress=0,
            message="Starting report processing",
        )

        logger.info(f"Processing report {report_id} of type {message.report_type}")

        csv_data, row_count = await report_processor.generate_report(
            message.report_id, message.report_type, message.filters, progress_callback
        )

        data_key = REPORT_DATA_KEY.format(report_id=report_id)
        await redis_client.setex(data_key, timedelta(hours=message.ttl_hours), csv_data)

        metadata_key = REPORT_METADATA_KEY.format(report_id=report_id)
        metadata_json = await redis_client.get(metadata_key)

        if metadata_json:
            metadata = ReportMetadata.model_validate_json(metadata_json)
            metadata.status = ReportStatus.COMPLETED
            metadata.completed_at = datetime.now(timezone.utc)
            metadata.updated_at = datetime.now(timezone.utc)
            metadata.file_size = len(csv_data.encode("utf-8"))
            metadata.row_count = row_count

            ttl = await redis_client.ttl(metadata_key)

            if ttl > 0:
                await redis_client.setex(metadata_key, ttl, metadata.model_dump_json())

        await update_report_status(
            report_id,
            ReportStatus.COMPLETED,
            progress=100,
            message=f"Report completed with {row_count} rows",
        )

        logger.info(f"Report {report_id} completed successfully with {row_count} rows")

    except asyncio.CancelledError:
        logger.warning(f"Report {report_id} was cancelled")
        await update_report_status(
            report_id,
            ReportStatus.CANCELLED,
            message="Report generation cancelled by user",
        )
        raise

    except Exception as e:
        logger.error(f"Error processing report {report_id}: {str(e)}")
        await update_report_status(report_id, ReportStatus.FAILED, error=str(e))
        raise

    finally:
        active_reports.pop(report_id, None)


async def periodic_cleanup():
    while True:
        try:
            await asyncio.sleep(3600)

            pattern = REPORT_METADATA_KEY.format(report_id="*")
            keys = await redis_client.keys(pattern)

            cleaned = 0

            for key in keys:
                metadata_json = await redis_client.get(key)

                if metadata_json:
                    metadata = ReportMetadata.model_validate_json(metadata_json)

                    if datetime.now(timezone.utc) > metadata.expires_at:
                        report_id = metadata.report_id

                        await redis_client.delete(
                            REPORT_METADATA_KEY.format(report_id=report_id),
                            REPORT_DATA_KEY.format(report_id=report_id),
                            REPORT_STATUS_KEY.format(report_id=report_id),
                        )

                        cleaned += 1

            if cleaned > 0:
                print(f"Cleaned up {cleaned} expired reports")

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Error in cleanup task: {e}")


if __name__ == "__main__":
    uvloop.run(app.run())
