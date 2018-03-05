#!/usr/bin/env python

import apache_beam as beam
import re
import csv

def my_grep(line, term):
   if re.match( r'^' + re.escape(term), line):
      yield line


PROJECT='dataflow-helloworld'
BUCKET='raw-data-upload-bucket01'

#DoFN Class
class ParseCsvFn(beam.DoFn):

  def process(self, element):
   row = list(csv.reader([element]))[0]
   yield {
       'Name': row[0],
       'number': row[1]

   }

def run():
   argv = [
      '--project={0}'.format(PROJECT),
      '--job_name=DailyImport',
      '--save_main_session',
      '--staging_location=gs://{0}/test/'.format(BUCKET),
      '--temp_location=gs://{0}/test/'.format(BUCKET),
      '--runner=DataflowRunner'
   ]


   p = beam.Pipeline(argv=argv)
   input = 'gs://{0}/daily-dumps/*.csv'.format(BUCKET)
   output_prefix = 'gs://{0}/output'.format(BUCKET)


   # find all lines that contain the searchTerm
   (p
      | 'Read Uploaded Files' >> beam.io.ReadFromText(input)
      | 'PFM' >> beam.ParDo(ParseCsvFn())
      | 'Save Output' >> beam.io.WriteToText(output_prefix)
   )

   p.run()

if __name__ == '__main__':
   run()

