import os
import json
import redis
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from models import (
    ReportRequest,
    ReportStatus,
    ReportMetadata,
    ReportGenerationMessage,
    ReportType,
)

app = Flask(__name__)
CORS(app)

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
REPORT_METADATA_KEY = "report:metadata:{report_id}"
REPORT_DATA_KEY = "report:data:{report_id}"
REPORT_QUEUE = "report_generation_queue"

redis_client = redis.from_url(REDIS_URL, decode_responses=True)


@app.route("/reports/generate", methods=["POST"])
def generate_report():
    try:
        data = request.json
        report_request = ReportRequest(
            report_type=data["report_type"],
            filters=data.get("filters", {}),
            user_id=data["user_id"],
            ttl_hours=data.get("ttl_hours", 24),
        )

        report_type = ReportType(report_request.report_type)

        expires_at = datetime.now(timezone.utc) + timedelta(
            hours=report_request.ttl_hours
        )

        metadata = ReportMetadata(
            report_id=report_request.report_id,
            report_type=report_type,
            status=ReportStatus.PENDING,
            user_id=report_request.user_id,
            progress=0,
            message="Report Accepted",
            filters=report_request.filters,
            created_at=report_request.created_at,
            updated_at=report_request.created_at,
            ttl_hours=report_request.ttl_hours,
            expires_at=expires_at,
        )

        metadata_key = REPORT_METADATA_KEY.format(report_id=report_request.report_id)

        redis_client.setex(
            metadata_key,
            timedelta(hours=report_request.ttl_hours + 1),
            metadata.model_dump_json(),
        )

        message = ReportGenerationMessage(
            report_id=report_request.report_id,
            report_type=report_request.report_type,
            filters=report_request.filters,
            user_id=report_request.user_id,
            ttl_hours=report_request.ttl_hours,
        )

        redis_client.lpush(REPORT_QUEUE, message.model_dump_json())

        return jsonify(
            {
                "report_id": report_request.report_id,
                "status": ReportStatus.PENDING,
                "message": "Report generation request submitted successfully",
            }
        ), 202

    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/reports/<report_id>/status", methods=["GET"])
def get_report_status(report_id):
    try:
        metadata_key = REPORT_METADATA_KEY.format(report_id=report_id)
        metadata_json = redis_client.get(metadata_key)

        if not metadata_json:
            return jsonify({"error": "Report not found"}), 404

        metadata = ReportMetadata.model_validate_json(metadata_json)

        response = {
            "report_id": metadata.report_id,
            "status": metadata.status,
            "progress": metadata.progress,
            "message": metadata.message,
            "created_at": metadata.created_at.isoformat(),
            "updated_at": metadata.updated_at.isoformat(),
            "expires_at": metadata.expires_at.isoformat(),
        }

        if metadata.status == ReportStatus.COMPLETED:
            response.update(
                {
                    "completed_at": metadata.completed_at.isoformat()
                    if metadata.completed_at
                    else None,
                    "file_size": metadata.file_size,
                    "row_count": metadata.row_count,
                }
            )
        elif metadata.status == ReportStatus.FAILED:
            response["error"] = metadata.error

        return jsonify(response), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/reports/<report_id>", methods=["GET"])
def get_report(report_id):
    try:
        metadata_key = REPORT_METADATA_KEY.format(report_id=report_id)
        metadata_json = redis_client.get(metadata_key)

        if not metadata_json:
            return jsonify({"error": "Report not found"}), 404

        metadata = ReportMetadata.model_validate_json(metadata_json)

        if metadata.status != ReportStatus.COMPLETED:
            return jsonify(
                {"error": f"Report is not ready. Current status: {metadata.status}"}
            ), 400

        data_key = REPORT_DATA_KEY.format(report_id=report_id)
        report_data = redis_client.get(data_key)

        if not report_data:
            return jsonify({"error": "Report data not found"}), 404

        return Response(
            report_data,
            mimetype="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=report_{report_id}.csv"
            },
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/reports/<report_id>", methods=["DELETE"])
def cancel_report(report_id):
    try:
        metadata_key = REPORT_METADATA_KEY.format(report_id=report_id)
        metadata_json = redis_client.get(metadata_key)

        if not metadata_json:
            return jsonify({"error": "Report not found"}), 404

        metadata = ReportMetadata.model_validate_json(metadata_json)

        if metadata.status in [
            ReportStatus.COMPLETED,
            ReportStatus.FAILED,
            ReportStatus.CANCELLED,
        ]:
            return jsonify(
                {"error": f"Cannot cancel report with status: {metadata.status}"}
            ), 400

        metadata.status = ReportStatus.CANCELLED
        metadata.updated_at = datetime.utcnow()

        redis_client.setex(metadata_key, timedelta(hours=1), metadata.model_dump_json())

        cancel_message = {"action": "cancel", "report_id": report_id}
        redis_client.publish("report_cancellations", json.dumps(cancel_message))

        return jsonify(
            {
                "report_id": report_id,
                "status": "cancelled",
                "message": "Report cancellation requested",
            }
        ), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/reports", methods=["GET"])
def list_reports():
    try:
        user_id = request.args.get("user_id")
        if not user_id:
            return jsonify({"error": "user_id parameter is required"}), 400

        pattern = REPORT_METADATA_KEY.format(report_id="*")
        keys = redis_client.keys(pattern)

        reports = []
        for key in keys:
            metadata_json = redis_client.get(key)

            if metadata_json:
                metadata = ReportMetadata.model_validate_json(metadata_json)

                if metadata.user_id == user_id:
                    reports.append(
                        {
                            "report_id": metadata.report_id,
                            "report_type": metadata.report_type,
                            "status": metadata.status,
                            "created_at": metadata.created_at.isoformat(),
                            "expires_at": metadata.expires_at.isoformat(),
                        }
                    )

        reports.sort(key=lambda x: x["created_at"], reverse=True)

        return jsonify({"reports": reports}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
