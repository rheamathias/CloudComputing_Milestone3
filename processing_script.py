import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import math


class FilterMissing(beam.DoFn):
    def process(self, element):
        # Convert bytes to dictionary
        record = json.loads(element.decode('utf-8'))

        pressure = record.get("pressure")
        temperature = record.get("temperature")

        # Filter None values
        if pressure is None or temperature is None:
            return

        # Filter NaN values (from pandas CSV)
        if isinstance(pressure, float) and math.isnan(pressure):
            return

        if isinstance(temperature, float) and math.isnan(temperature):
            return

        yield record


class ConvertUnits(beam.DoFn):
    def process(self, record):
        # Convert pressure from kPa to psi
        record["pressure"] = record["pressure"] / 6.895

        # Convert temperature from Celsius to Fahrenheit
        record["temperature"] = record["temperature"] * 1.8 + 32

        # Convert back to bytes for Pub/Sub
        yield json.dumps(record).encode('utf-8')


def run():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help="Input Pub/Sub topic")
    parser.add_argument('--output', required=True, help="Output Pub/Sub topic")

    known_args, pipeline_args = parser.parse_known_args()

    # Enable streaming mode
    options = PipelineOptions(
        pipeline_args,
        streaming=True,
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
                topic=known_args.input
            )
            | "FilterMissingValues" >> beam.ParDo(FilterMissing())
            | "ConvertUnits" >> beam.ParDo(ConvertUnits())
            | "WriteToPubSub" >> beam.io.WriteToPubSub(
                topic=known_args.output
            )
        )


if __name__ == '__main__':
    run()
