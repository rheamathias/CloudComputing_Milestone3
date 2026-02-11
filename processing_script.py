import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json


class FilterMissing(beam.DoFn):
    def process(self, element):
        # Decode bytes to JSON
        record = json.loads(element.decode('utf-8'))

        # Remove records with missing values
        if record.get("pressure") is None:
            return
        if record.get("temperature") is None:
            return

        yield record


class ConvertUnits(beam.DoFn):
    def process(self, record):
        # Convert pressure: kPa -> psi
        record["pressure"] = record["pressure"] / 6.895

        # Convert temperature: C -> F
        record["temperature"] = record["temperature"] * 1.8 + 32

        # Convert back to bytes for Pub/Sub
        yield json.dumps(record).encode('utf-8')


def run():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)

    known_args, pipeline_args = parser.parse_known_args()

    options = PipelineOptions(pipeline_args, streaming=True)

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=known_args.input)
            | "FilterMissingValues" >> beam.ParDo(FilterMissing())
            | "ConvertUnits" >> beam.ParDo(ConvertUnits())
            | "WriteToPubSub" >> beam.io.WriteToPubSub(topic=known_args.output)
        )


if __name__ == '__main__':
    run()
