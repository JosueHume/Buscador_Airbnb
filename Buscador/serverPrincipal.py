import json
import requests
import pymongo
from flask import Flask, request
from datetime import datetime
from bson import ObjectId, Decimal128

def ler_arquivo_configuracao(nome_arquivo):
    with open(nome_arquivo, 'r') as arquivo:
        configuracoes = json.load(arquivo)
    return configuracoes

def serialize_obj(obj):
    if isinstance(obj, (datetime, ObjectId, Decimal128)):
        return str(obj)
    raise TypeError(f" Erro no Objeto JSON: {obj.__class__.__name__}")

def configurar_servidor(configuracoes):
    id_servidor = configuracoes['id']
    porta = configuracoes['port']
    uri_mongodb = configuracoes['mongo_uri']
    nome_banco_dados = configuracoes['database']
    vizinhos = configuracoes['vizinhos']
    
    client = pymongo.MongoClient(uri_mongodb)
    banco_dados = client[nome_banco_dados]
    collection = banco_dados['review']
    
    app = Flask(__name__)
    
    @app.route('/buscar', methods=['GET'])
    def buscar():
        id_objeto = request.args.get('id')

        objeto_local = collection.find_one({'_id': id_objeto})
        if objeto_local:
            objeto_local['servidor'] = id_servidor  

            mensagem = f'Objeto encontrado no servidor de código {id_servidor}, Porta {porta}'
            print(mensagem)

            return json.dumps(objeto_local, default=serialize_obj)

        for vizinho in vizinhos:
            url_vizinho = f"http://localhost:{vizinho['port']}/buscar?id={id_objeto}"
            try:
                response = requests.get(url_vizinho)
                if response.status_code == 200:
                    return response.json()
            except requests.exceptions.ConnectionError:
                continue

        mensagem_vazio = 'Atenção: Nenhum vizinho possui o objeto.'
        return json.dumps({'message': mensagem_vazio})

    @app.route('/adicionar', methods=['POST'])
    def adicionar():
        novo_objeto = request.json
        collection.insert_one(novo_objeto)
        return 'Objeto adicionado com sucesso!'
    
    if __name__ == '__main__':
        app.run(port=porta)

arquivo_configuracao = 'vizinhos.json'
configuracoes = ler_arquivo_configuracao(arquivo_configuracao)
configurar_servidor(configuracoes)
