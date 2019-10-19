# Projeto de exemplo para palestra: Processando Grande Volumes de Dados com Dataflow, BigQuery, Vison

Esse projeto tem como objetivo mostar como utilizar o Google Cloud Dataflow integrando entre os produtos:

Google Cloud Storage
Google Cloud Dataflow
Google Cloud Vison
Google Cloud BigQuery



### Pré requisitos

1. O SDK do Apache Beam para Python requer o Python versão 2.7.x com o pip instalado. Para se certificar de que você está com o Python 2.7.x funcionando e o pip instalado, execute:

```
python2.7 --version
python2.7 -m pip --version

```

2. Instale um ambiente virtual do Python para fazer os experimentos iniciais. Se você não tem a virtualenvversão 13.1.0 ou mais recente, as etapas de instalação para seu sistema operacional podem ser encontradas em Como instalar e usar a ferramenta virtualenv.
Para configurar e ativar um novo ambiente virtual, execute:

```
python2.7 -m virtualenv env
source env/bin/activate

```


3. Instale as dependências

```
pip install apache-beam[gcp]
pip install -r requrements.txt
```

4. Execute o script do dataflow

```
python -m dataflowdevfest --input gs://nome-do-seu-storage/  --temp_location gs://nome-do-seu-storage-temp/ --runner DataflowRunner --project nomedoseuprojeto  --requirements_file requirements.txt
```


