"""
Entrypoint: starts the streaming query and keeps it running until interrupted.

Run the event simulator (src/data_generator/event_stream_simulator.py) in a
separate terminal to feed this job continuously.
"""
from pipeline.config import StreamingConfig
from pipeline.logger import get_logger
from pipeline.spark_session import get_spark
from pipeline.streaming_job import build_and_start_stream

logger = get_logger(__name__)


def main():
    config = StreamingConfig()
    spark = get_spark()
    query = build_and_start_stream(spark, config)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("shutdown requested, stopping stream gracefully")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
