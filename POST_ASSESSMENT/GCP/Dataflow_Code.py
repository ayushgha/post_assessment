import logging
import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText

def run(argv=None):

    p = beam.Pipeline(options=PipelineOptions())


    class Printer(beam.DoFn):
        def process(self, element):
            print element

    class Transaction(beam.DoFn):
        def process(self, element):
            
            t=[]
            t=element.split(',')	
            if t[0]!='Place':
                return [{"Place": t[0],"Gender": t[1],"Year" : t[2],"Name" : t[3][::-1],"Number" : int(t[4])*22}]
				


    data_from_source = (p
                        | 'Read the source file' >> ReadFromText('gs://group4-bucket/Group4Data1.csv')
                        | 'Clean the items' >> beam.ParDo(Transaction())
                        )

    project_id = "pe-training"  # replace with your project ID
    dataset_id = 'group4dataset1'  # replace with your dataset ID
    table_id = 'ayushtable'  # replace with your table ID
    table_schema = ('Place:STRING,Gender:STRING,Year:INTEGER,Name:STRING,Number:INTEGER')

    # Persist to BigQuery
    # WriteToBigQuery accepts the data as list of JSON objects
    data_from_source | 'Write' >> beam.io.WriteToBigQuery(
                    table=table_id,
                    dataset=dataset_id,
                    project=project_id,
                    schema=table_schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    batch_size=int(100)
                    )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logger = logging.getLogger().setLevel(logging.INFO)
    run()
