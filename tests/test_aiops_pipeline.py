import json
import runpy

from src.anomaly_detector import AnomalyDetector
from src.aiops_pipeline import run_pipeline
from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.event_topic import EventTopic


def test_normal_record_is_not_anomaly():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 120,
        "cpu_percent": 42,
        "memory_percent": 51,
        "log_level": "INFO",
        "message": "Payment request processed successfully"
    }

    assert detector.detect(record) is None


def test_anomalous_record_is_detected():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:05:00",
        "service": "payment-service",
        "response_time_ms": 610,
        "cpu_percent": 75,
        "memory_percent": 70,
        "log_level": "ERROR",
        "message": "Payment service timeout"
    }

    event = detector.detect(record)

    assert event is not None
    assert event["type"] == "ANOMALY"


def test_producer_publishes_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    assert producer.publish(event)
    assert len(topic.get_messages()) == 1


def test_consumer_receives_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)
    consumer = EventConsumer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    producer.publish(event)

    messages = consumer.consume()

    assert len(messages) == 1


def test_detect_warning_log_as_anomaly():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:10:00",
        "service": "audit-service",
        "response_time_ms": 120,
        "cpu_percent": 30,
        "memory_percent": 40,
        "log_level": "WARNING",
        "message": "degraded performance"
    }

    event = detector.detect(record)

    assert event is not None
    assert event["type"] == "ANOMALY"
    assert "Error log detected" in event["reasons"]


def test_run_pipeline_publishes_and_consumes_detected_anomalies(tmp_path):
    data_file = tmp_path / "service_data.json"
    data_file.write_text(json.dumps([
        {
            "timestamp": "2026-09-20T10:00:00",
            "service": "payment-service",
            "response_time_ms": 120,
            "cpu_percent": 42,
            "memory_percent": 51,
            "log_level": "INFO",
            "message": "ok"
        },
        {
            "timestamp": "2026-09-20T10:01:00",
            "service": "payment-service",
            "response_time_ms": 600,
            "cpu_percent": 85,
            "memory_percent": 70,
            "log_level": "ERROR",
            "message": "timeout"
        }
    ]), encoding="utf-8")

    result = run_pipeline(str(data_file))

    assert result["records_processed"] == 2
    assert len(result["anomalies_detected"]) == 1
    assert len(result["events_consumed"]) == 1
    assert result["events_consumed"][0]["service"] == "payment-service"


def test_event_producer_rejects_falsey_event_and_topic_can_clear():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    assert producer.publish(None) is False
    assert topic.get_messages() == []

    producer.publish({"type": "ANOMALY", "service": "payment-service"})
    assert len(topic.get_messages()) == 1

    topic.clear()
    assert topic.get_messages() == []


def test_aiops_pipeline_main_entrypoint_runs_from_module(monkeypatch):
    monkeypatch.chdir("/workspaces/github-skills-challenge")
    runpy.run_module("src.aiops_pipeline", run_name="__main__")