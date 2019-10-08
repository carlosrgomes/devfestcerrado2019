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

import json
# import os
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/diego/Documentos/Test Google/Credenciais/ml_demo3/credentials/dev/service_account.json"
# export GOOGLE_APPLICATION_CREDENTIALS="/home/diego/Documentos/Test_Google/Credenciais/ml_demo3/credentials/dev/service_account.json"

path_gs = 'gs://cloud_ml_data/images_jsons/'

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


class FormatOutput(beam.DoFn):
    def process(self, element):

        path_img_file = element['path_img_file']
        shape = element['shape']
        width = element['width']
        height = element['height']

        points = shape['points']
        label = shape['label']

        top_left_x = points[0][0] / width
        bottom_right_x = points[1][0] / width
        top_left_y = points[0][1] / height
        bottom_right_y = points[1][1] / height

        # Guarantes the points are not out of range [0,1]
        top_left_x = 1.0 if top_left_x > 1.0 else top_left_x
        bottom_right_x = 1.0 if bottom_right_x > 1.0 else bottom_right_x
        top_left_y = 1.0 if top_left_y > 1.0 else top_left_y
        bottom_right_y = 1.0 if bottom_right_y > 1.0 else bottom_right_y

        top_left_x = 0.0 if top_left_x < 0.0 else top_left_x
        bottom_right_x = 0.0 if bottom_right_x < 0.0 else bottom_right_x
        top_left_y = 0.0 if top_left_y < 0.0 else top_left_y
        bottom_right_y = 0.0 if bottom_right_y < 0.0 else bottom_right_y

        return ['UNASSIGNED' + ',' + str(path_img_file).replace('.json', '.jpg') + ',' + str(label) + ',' + str(top_left_x) + ',' + str(top_left_y) + ',' + '' + ',' + '' + ',' + str(bottom_right_x) + ',' + str(bottom_right_y) + ',' + '' + ',' + '']


def run(argv=None):
    # python -m dataflowdevfest --input gs://devfest-carros/  --temp_location gs://devfest-temp/ --runner DataflowRunner --project devfestcerrado2019


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
            | beam.Map(printer1)
          #  | "Get_all_shapes" >> beam.ParDo(Shapes())
          #  | beam.Map(printer2)
          #  | "Format_output" >> beam.ParDo(FormatOutput())
        )
        
        #output | WriteToText(known_args.output, file_name_suffix='.csv', header='set, img_path, label, x1, y1, " " , " " , x2, y2, " " , " "', shard_name_template='')

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
