import asyncio
import csv
import io
import random
from datetime import datetime, timedelta
from typing import Dict, Any
from models import ReportType


async def _process_pub_blocks(report_id, filters, progress_callback):
    output = io.StringIO()
    writer = csv.writer(output)

    headers = [
        "Date",
        "Pub Blopcks",
    ]

    writer.writerow(headers)

    start_date = datetime.strptime(
        filters.get("start_date", "2024-01-01"), "%Y-%m-%d"
    ).date()
    end_date = datetime.strptime(
        filters.get("end_date", "2024-12-31"), "%Y-%m-%d"
    ).date()

    blocks = ["A", "B", "C", "D", "E"]

    rows = []
    days = (end_date - start_date).days + 1

    for i in range(min(days * 5, 100)):
        if progress_callback and i % 10 == 0:
            progress = 20 + int((i / min(days * 5, 10)) * 60)

            await progress_callback(report_id, progress, f"Processing row {i}")

        date = start_date + timedelta(days=random.randint(0, days - 1))
        pub_block = random.choice(blocks)

        rows.append([date, pub_block])

        if i % 50 == 0:
            await asyncio.sleep(0.1)

    rows.sort(key=lambda x: x[0])

    for row in rows:
        writer.writerow(row)

    csv_data = output.getvalue()

    return csv_data, len(rows)


async def _process_orders_by_day(report_id, filters, progress_callback):
    output = io.StringIO()
    writer = csv.writer(output)

    headers = [
        "Date",
        "Order ID",
    ]

    writer.writerow(headers)

    start_date = datetime.strptime(
        filters.get("start_date", "2024-01-01"), "%Y-%m-%d"
    ).date()
    end_date = datetime.strptime(
        filters.get("end_date", "2024-12-31"), "%Y-%m-%d"
    ).date()

    rows = []
    days = (end_date - start_date).days + 1

    for i in range(min(days * 5, 100)):
        if progress_callback and i % 10 == 0:
            progress = 20 + int((i / min(days * 5, 10)) * 60)

            await progress_callback(report_id, progress, f"Processing row {i}")

            await asyncio.sleep(5)

        date = start_date + timedelta(days=random.randint(0, days - 1))
        order_id = f"ORD-{random.randint(10000, 99999)}"

        rows.append([date, order_id])

        if i % 50 == 0:
            await asyncio.sleep(0.1)

    rows.sort(key=lambda x: x[0])

    for row in rows:
        writer.writerow(row)

    csv_data = output.getvalue()

    return csv_data, len(rows)


REPORT_TYPES = {
    ReportType.PUBLISHER_BLOCKS: _process_pub_blocks,
    ReportType.ORDERS_BY_DAY: _process_orders_by_day,
}


class ReportProcessor:
    async def generate_report(
        self,
        report_id: int,
        report_type: ReportType,
        filters: Dict[str, Any],
        progress_callback=None,
    ) -> tuple[str, int]:
        if progress_callback:
            await progress_callback(report_id, 10, "Starting report generation")

        await asyncio.sleep(random.uniform(2, 5))

        process_func = REPORT_TYPES.get(report_type)

        if not process_func:
            raise ValueError(f"Unknown report type: {report_type}")

        csv_data, row_count = await process_func(report_id, filters, progress_callback)

        if progress_callback:
            await progress_callback(report_id, 100, "Report generation completed")

        return csv_data, row_count
