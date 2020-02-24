import boto3

resource_dynamodb = boto3.resource('dynamodb')


def handler(event, context):
    arquivo = event['Records'][0]['s3']['object']['key']
    evento = event['Records'][0]['eventName'].split(':')
    evento = evento[1]
    main(arquivo,evento)


def extrai_arquivo(arquivo):
    atributos = arquivo[:-4]
    atributos = tuple(atributos.split('-'))
    return (atributos)


def atualiza_database(atributos,evento):
    table = resource_dynamodb.Table('PhotoColletion')
    if evento == 'Put':
        response = table.put_item(
            Item={
                'id': int(atributos[0]),
                'assunto': atributos[1],
                'colecao': atributos[2],
                'descricao': atributos[3]
            }
        )
    if evento == 'Delete':
        response = table.delete_item(
            Key={
                'id': int(atributos[0])
            }
        )

    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print(f'DB atualizado: {atributos}')
    else:
        print('ERRO de atualizacao')

def main(arquivo,evento):
    atributos = extrai_arquivo(arquivo)
    atualiza_database(atributos, evento)
