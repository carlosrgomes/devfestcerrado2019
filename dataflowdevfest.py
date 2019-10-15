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
# import os
#python2.7 -m dataflowdevfest --input gs://devfest-carros/  --temp_location gs://devfest-temp/ --runner DataflowRunner --project devfestcerrado2019
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/diego/Documentos/Test Google/Credenciais/ml_demo3/credentials/dev/service_account.json"
# export GOOGLE_APPLICATION_CREDENTIALS="/home/diego/Documentos/Test_Google/Credenciais/ml_demo3/credentials/dev/service_account.json"

path_gs = 'gs://cloud_ml_data/images_jsons/'


class Vison(beam.DoFn):
    



    def process(self, element):
        
        filename = element[0]
        client = vision.ImageAnnotatorClient()
        image = types.Image()
        image.source.image_uri = filename
        response = client.label_detection(image=image)
        print(response)




class Shapes(beam.DoFn):

    def set_img_path(self, gs):
        self.gs = gs
    
    def process(self, element):

        path_element = element[0]
        data = element[1]

        shapes = data['shapes']
        width = data['imageWidth']
        height = data['imageHeight']

        image_name = path_element.split('/')[-1]
        #path_img_file = self.gs + image_name
        path_img_file = path_gs + image_name

        for shape in shapes:
            yield  {'shape' : shape,
                    'width' : width,
                    'height' : height,
                    'path_img_file' : path_img_file}


def run(argv=None):
    # python -m dataflowdevfest --input gs://devfest-carros/  --temp_location gs://devfest-temp/ --runner DataflowRunner --project devfestcerrado2019  --requirements_file requirements.txt



    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        dest='input',
                        help='image dataset location.',
                        required=True)


    known_args, pipeline_args = parser.parse_known_args(argv)
    # pipeline_args.extend([
    #     '--job_name=job-test'
    # ])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:

        def printer1(x):
            print(x)
            return x 
      

        output = (
            p | "Get_all_files" >> beam.io.fileio.MatchFiles(known_args.input + '*.jpg')
            | "Read_all" >> beam.io.fileio.ReadMatches()
            | "Transform Path" >> beam.Map(lambda file: (file.metadata.path, file.metadata.path.split('/')[-1].replace(".jpg", "")))
          #  | beam.Map(printer1)
            | "GET Vison API" >> beam.ParDo(Vison())
          #  | beam.Map(printer2)
          #  | "Format_output" >> beam.ParDo(FormatOutput())
        )
        
        #output | WriteToText(known_args.output, file_name_suffix='.csv', header='set, img_path, label, x1, y1, " " , " " , x2, y2, " " , " "', shard_name_template='')

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
