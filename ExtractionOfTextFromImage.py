import boto3
import json
import sys
from pprint import pprint
import io
from io import BytesIO
from PIL import Image, ImageDraw, ImageFont

def main(file_name='image1.jpeg'):
    table_csv = get_table_csv_results(file_name)
    print(table_csv)
    output_file = 'output.csv'

    # replace content
    with open(output_file, "wt") as fout:
        fout.write(table_csv)

    # show the results
    print('CSV OUTPUT FILE: ', output_file)

def get_table_csv_results(file_name):
    #Amazon Textract
    textract= boto3.client(
        service_name='textract',
        region_name='us-east-2')

    #Amazon s3
    s3=boto3.resource('s3')
    s3object=s3.Object('formdata',file_name)      #formdata is the bucketname
    s3response=s3object.get();
    stream=io.BytesIO(s3response['Body'].read())
    image=Image.open(stream)
    try:
        image_binary = stream.getvalue()
        response = textract.analyze_document(Document={'Bytes': image_binary},FeatureTypes=['TABLES'])
        blocks_map = {}
        table_blocks = []
        for block in response['Blocks']:
            blocks_map[block['Id']] = block
            if block['BlockType'] == "TABLE":
                table_blocks.append(block)
        csv = ''
        for index, table in enumerate(table_blocks):
            csv += generate_table_csv(table, blocks_map, index + 1)
            csv += '\n\n'


    except Exception as e:
        print (e)
    return csv;

def generate_table_csv(table_result, blocks_map, table_index):
    rows = get_rows_columns_map(table_result, blocks_map)

    table_id = 'Table_' + str(table_index)

    # get cells.
    csv = 'Table: {0}\n\n'.format(table_id)

    for row_index, cols in rows.items():

        for col_index, text in cols.items():
            csv += '{}'.format(text) + ","
        csv += '\n'

    csv += '\n\n\n'
    print(csv);
    return csv


def get_rows_columns_map(table_result, blocks_map):
    rows = {}
    for relationship in table_result['Relationships']:
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                cell = blocks_map[child_id]
                if cell['BlockType'] == 'CELL':
                    row_index = cell['RowIndex']
                    col_index = cell['ColumnIndex']
                    if row_index not in rows:
                        # create new row
                        rows[row_index] = {}
                        if col_index == 2:
                            rows[row_index][1]=''
                    # get the text value
                    rows[row_index][col_index] = get_text(cell, blocks_map)

    return rows

def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] =='SELECTED':
                            text +=  'X '
    return text

if __name__ == "__main__":
    main()