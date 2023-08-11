import boto3

def create_csv_classifier():
    glue = boto3.client('glue')
    response = glue.create_classifier(CsvClassifier={
        'Name': 'csv_classifier',
        'Delimiter': ',',
        'QuoteSymbol': '"',
        'ContainsHeader': 'PRESENT',
        'Header': [
            'UserId', 'ProductId' ,'Rating', 'Timestamp'
        ]
    })

create_csv_classifier()