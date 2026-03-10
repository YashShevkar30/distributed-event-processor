"""
Main entry point for the Distributed Event Processor.
Starts the consumer worker with optional metrics server.
"""

import click
import structlog
from processor.config import get_settings
from processor.consumer import EventConsumer
from processor.metrics import start_metrics_server

logger = structlog.get_logger()


@click.group()
def cli():
    """Distributed Event Processor — Redis Streams consumer worker."""
    pass


@cli.command()
@click.option("--consumer-name", default=None, help="Override consumer name")
@click.option("--no-metrics", is_flag=True, help="Disable Prometheus metrics server")
def start(consumer_name: str, no_metrics: bool):
    """Start the event processor worker."""
    settings = get_settings()

    if consumer_name:
        settings.consumer_name = consumer_name

    click.echo(f"Starting Event Processor: {settings.consumer_name}")
    click.echo(f"  Stream: {settings.stream_name}")
    click.echo(f"  Group:  {settings.consumer_group}")
    click.echo(f"  Redis:  {settings.redis_url}")

    if not no_metrics:
        start_metrics_server(settings.metrics_port)
        click.echo(f"  Metrics: http://0.0.0.0:{settings.metrics_port}/metrics")

    consumer = EventConsumer(settings=settings)
    consumer.start()


@cli.command()
@click.option("--count", default=10, help="Number of DLQ entries to show")
def dlq_list(count: int):
    """List entries in the dead-letter queue."""
    import redis as redis_lib
    settings = get_settings()
    r = redis_lib.from_url(settings.redis_url, decode_responses=True)

    from processor.dead_letter import DeadLetterQueue
    dlq = DeadLetterQueue(r, settings.dlq_stream_name)

    entries = dlq.list_dlq(count=count)
    if not entries:
        click.echo("Dead-letter queue is empty.")
        return

    click.echo(f"DLQ entries ({len(entries)}):")
    for entry in entries:
        click.echo(f"  [{entry.get('dlq_id')}] "
                    f"error={entry.get('error_message', 'N/A')}, "
                    f"retries={entry.get('retry_count', '?')}")


@cli.command()
@click.argument("dlq_id")
def dlq_replay(dlq_id: str):
    """Replay a specific DLQ event back to the main stream."""
    import redis as redis_lib
    settings = get_settings()
    r = redis_lib.from_url(settings.redis_url, decode_responses=True)

    from processor.dead_letter import DeadLetterQueue
    dlq = DeadLetterQueue(r, settings.dlq_stream_name)

    new_id = dlq.replay_event(dlq_id)
    if new_id:
        click.echo(f"Replayed DLQ entry {dlq_id} → new message ID: {new_id}")
    else:
        click.echo(f"DLQ entry {dlq_id} not found.")


if __name__ == "__main__":
    cli()
