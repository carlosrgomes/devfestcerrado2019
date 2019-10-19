import argparse
import logging

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from apache_beam.runners.portability import fn_api_runner
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability import python_urns
from google.cloud import vision
from google.cloud.vision import types
from google.cloud.vision import enums




import json


class Vison(beam.DoFn):
    

    def process(self, element):
        
        filename = element[0]
        client = vision.ImageAnnotatorClient()
        image = types.Image()
        image.source.image_uri = filename
        response = client.label_detection(image=image)
        
        labels = response.label_annotations

        row = {'id': element[1] , 'label1': labels[0].description ,  'label2': labels[1].description, 'label3': labels[2].description  }
        return row



def run(argv=None):
    # python -m dataflowdevfest --input gs://devfest-carros/  --temp_location gs://devfest-temp/ --runner DataflowRunner --project devfestcerrado2019  --requirements_file requirements.txt


    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        dest='input',
                        help='image dataset location.',
                        required=True)


    known_args, pipeline_args = parser.parse_known_args(argv)
    

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:

        
      

        output = (
            p | "Get_all_files" >> beam.io.fileio.MatchFiles(known_args.input + '*.jpg')
            | "Read_all" >> beam.io.fileio.ReadMatches()
            | "Transform Path" >> beam.Map(lambda file: (file.metadata.path, file.metadata.path.split('/')[-1].replace(".jpg", "")))
            | "GET Vison API" >> beam.ParDo(Vison())
            | 'Write to BigQuery' >> beam.io.Write( beam.io.BigQuerySink(
             # The table name is a required argument for the BigQuery sink.
             # In this case we use the value passed in from the command line.
             dataset='demodataflow',
             table='tabelademo',
             # Here we use the simplest way of defining a schema:
             # fieldName:fieldType
             schema='id:STRING,label1:STRING,label2:STRING,label3:STRING',
             # Creates the table in BigQuery if it does not yet exist.
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             # Deletes all data in the BigQuery table before writing.
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
         
        )
        

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
