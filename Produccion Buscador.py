# import main Flask class and request object
from flask import Flask, request, jsonify
import requests
import json
import tokens
import time
from flask_cors import CORS
import os


# create the Flask app
app = Flask(__name__)
CORS(app)

###################################################

##################### DEV ZONE ####################
###################################################
"""
os.environ['url_elastic'] = "192.168.133.216"
os.environ['elastic_port'] = "9200"

os.environ['server_ip'] = "192.168.133.216"
os.environ['server_port'] = "5010"


os.environ['timeout'] = "5"
"""
###################################################
###################################################
dafault_lang = "es_MX"

elastic_ip = os.environ['url_elastic']
elastic_port = os.environ['elastic_port']

server_ip = os.environ['server_ip']
server_port = os.environ['server_port']

############################## Salud / Health  ##############################

@app.route('/health')
def health():
    return "Alive!"

############################## Buscador de Documentos  ##############################

@app.route('/ccv/browser')
def browser():

    headers = request.headers
    auth = headers.get("X-Api-Key")
    if auth == tokens.ccv_token:
        #return jsonify({"message": "OK: Authorized"}), 200
        pass
    else:
        return jsonify({"message": "ERROR: Unauthorized"}), 401

    try:
        query = str(request.args.get('query'))
    except:
        return [{"error" :"Favor de ingresar al menos una palabra en 'query' como parametro"}]

    ############################## Documentos que contengan ##############################

    #print(len(query))
    datos = len(query)

    #return str(datos)

    if len(query) == 0:
        return [{"error" :"Favor de ingresar al menos una palabra"}]

    ############################################################################################
    #Aqui se debe insertar al indice de TOP Query si la palabra coincide con algunas almacenadas
    ############################################################################################

    try:

        timestamp = str(time.time())
        timestamp = timestamp.split(".")[0]

        texto = query
        texto = texto.upper()
        texto = texto.split(" ")

        url_top_search = "http://{}:{}/ccv_top_search/query/".format(elastic_ip,elastic_port)

        headers = {
            'Authorization': tokens.elastic_token,
            'Content-Type': 'application/json'
        }

        
        #print(texto)

        for palabra in texto:

            try:
                if palabra in tokens.diccionario_top:
                    print("Esta en diccionario")
                    #Se agrega al indice de querys

                    payload = { "timestamp": timestamp, "query": palabra}
                    response = requests.request("POST", url_top_search, headers=headers, json=payload, timeout=int(os.environ['timeout']))
                    print(response.text)
                    
                else:
                    pass
            except:
                pass

    except:
        pass

    ############################################################################################
        

    url = "http://{}:{}/new_sitio_ccv/_search".format(elastic_ip,elastic_port)
    frase = query
    query_json = []     

    for item in frase.split():
        try:
            query_json.append({"span_multi":{"match":{"fuzzy":{"content":{"fuzziness": "4","value": "{}".format(item)}}}}})
        except:
            pass

    payload = json.dumps({"query": {"span_near": {"clauses": query_json ,"slop": 4,"in_order": "true"}},"highlight" : {"pre_tags" : ["<b>"] , "post_tags" : ["</b>"], "fields" : {"content":{}}}})

    print(payload)

    # Token de Acceso a Elastic 
    headers = {
      'Authorization': tokens.elastic_token,
      'Content-Type': 'application/json'
    }

    response = requests.request("GET", url, headers=headers, data=payload, timeout=int(os.environ['timeout']))

    #print(response.text)

    response = json.loads(response.text)

    query_response = []

    try:
        for result in response['hits']['hits']:
            #print(result)
            #os.environ['url_base']

            base_url = os.environ['url_base']

            result_id = result['_id']
            result_score = result['_score']
            result_content = result['_source']['content']
            result_extension = result['_source']['file']['extension']
            result_highlight = result['highlight']['content'][0]

            try:
                result_filename = result['_source']['file']['filename']

                result_url = result_filename.rsplit("@")[0]
                result_url = "{}{}".format(base_url,result_url)

            except:
                result_url = result['_source']['file']['url']

            try:
                result_filename = result['_source']['file']['filename']
                result_filename = result_filename.rsplit("@")[1]
            except:
                result_filename = result['_source']['file']['filename']


            query_response.append(
                {
                "id":"{}".format(result_id),
                "score" : "{}".format(result_score),
                #"content" : "{}".format(result_content),
                "extension" : "{}".format(result_extension),
                "filename" : "{}".format(result_filename),
                "url" : "{}".format(result_url),
                "highlight" : "{}".format(result_highlight)
                }
              )

    except Exception as e:
        print("Entro a un error : {}".format(e))
        query_response = []

    return query_response

############################## Categorias Mas Buscadas ##############################

"""
@app.route('/ccv/top')
def top_search():

    headers = request.headers
    auth = headers.get("X-Api-Key")

    if auth == tokens.ccv_token:
        #return jsonify({"message": "OK: Authorized"}), 200
        pass
    else:
        return jsonify({"message": "ERROR: Unauthorized"}), 401


    ############################## Top Palabras que contengan ##############################
    try:

        query_response = []
        items = []

        timestamp = str(time.time())
        timestamp = timestamp.split(".")[0]

        # Tiempo de una semana atras 
        diference = 604800

        now = int(timestamp) + 10
        past = int(timestamp) - diference

        url_top = "http://{}:{}/ccv_top_search/query/_search?".format(elastic_ip,elastic_port)

        payload = {
            "query": {
                "range" : {
                    "timestamp": {
                        "gte" : "{}".format(past),
                        "lte" : "{}".format(now)
                    }
                }
            }
        }

        # Token de Acceso a Elastic 
        headers = {
          'Authorization': tokens.elastic_token,
          'Content-Type': 'application/json'
        }

        response = requests.request("GET", url_top, headers=headers, json=payload, timeout=int(os.environ['timeout']))

        #print(response.text)

        response = json.loads(response.text)

        for item in response['hits']['hits']:

            try:

                hits = item['_source']['query']
                #print(hits)

                if hits in items:
                    pass
                else:
                    items.append(hits)

            except:
                pass

        print(items)


    except Exception as e:
        print("Entro a un error : {}".format(e))
        items = []

    return items

    #return 'Query String Example {}'.format(language)
"""
############################## Indice de Pagina CCV Web #############################

@app.route('/ccv/web')
def web():

    headers = request.headers
    auth = headers.get("X-Api-Key")

    if auth == tokens.ccv_token:
        #return jsonify({"message": "OK: Authorized"}), 200
        pass
    else:
        return jsonify({"message": "ERROR: Unauthorized"}), 401

    #################################################
    try:
        query = str(request.args.get('query'))
    except:
        return [{"error" :"Favor de ingresar al menos una palabra en 'query' como parametro" }]

    #print(len(query))
    datos = len(query)

    #return str(datos)

    if len(query) == 0:
        return [{"error" :"Favor de ingresar al menos una palabra" }]

    #print("OK")

    #################################################

    
    try:

        lang = str(request.args.get('lang'))

        #print(lang)

        if lang == "None":
            lang = dafault_lang.lower()
        else:
            lang = lang.lower()

    except:
        lang = dafault_lang.lower()

    

    print("lang : ",lang)
    dataArray = []

    try:

        url = "http://{}:{}/ccv_web_{}/query/_search?pretty=true".format(elastic_ip,elastic_port,lang)


        payload={
          "query": {
            "multi_match": {
              "query": "{}".format(query),
              "fields": [ "title","description"],
              "fuzziness": 2
            }
          },
          "highlight" : {"pre_tags" : ["<b>"] , "post_tags" : ["</b>"], "fields" : {"title":{},"description":{}}}
        }


        headers = {
          'Authorization': tokens.elastic_token,
          'Content-Type': 'application/json'
        }

        response = requests.request("GET", url, headers=headers, json=payload, timeout=int(os.environ['timeout']))
        response = json.loads(response.text)

        #print(response)
        #return response

        ######################################################################
        ##### Todas las posibles variables que puede regresar el buscador ####
        ######################################################################
        """
        "id"
        "location"
        "title"
        "description"
        "urlBuscador"
        "links"
        "lang"
        "subtitle"
        "year"
        "url"
        """
        ######################################################################
        ######################################################################

        raw_response = response['hits']['hits']

        #print(raw_response)

        #return "OK"

        #dataArray["raw_response"] = raw_response

        #responseArray = []

        for item in response['hits']['hits']:

            try:
                
                try:
                    web_id = item['_source']['id']
                except:
                    web_id = ""

                try:
                    web_location = item['_source']['location']
                except:
                    web_location = ""

                try:
                    web_title = item['_source']['title']
                except:
                    web_title = ""

                try:
                    web_description = item['_source']['description']
                except:
                    web_description = ""

                try:
                    web_urlBuscador = item['_source']['urlBuscador']
                except:
                    web_urlBuscador = ""

                try:
                    web_links = item['_source']['links']
                except:
                    web_links = ""

                try:
                    web_lang = item['_source']['lang']
                except:
                    web_lang = ""

                try:
                    web_subtitle = item['_source']['subtitle']
                except:
                    web_subtitle = ""

                try:
                    web_year = item['_source']['year']
                except:
                    web_year = ""

                try:
                    web_url = item['_source']['url']
                except:
                    web_url = ""



                try:
                    web_highlight_description = item['highlight']['description']
                except:
                    web_highlight_description = []

                try:
                    web_highlight_title = item['highlight']['title']
                except:
                    web_highlight_title = []


                dataArray.append(
                    {
                        "id":web_id,
                        "location":web_location,
                        "title":web_title,
                        "description":web_description,
                        "urlBuscador":web_urlBuscador,
                        "links":web_links,
                        "lang":web_lang,
                        "subtitle":web_subtitle,
                        "year":web_year,
                        "url":web_url,                    
                    })

            except Exception as e:
                print(e)

        #dataArray["response"] = responseArray
        #dataArray.append({"raw_response":raw_response})

        return dataArray
    
    except Exception as e:
        print(e)
        return dataArray

##################################################

if __name__ == '__main__':
    # run app in debug mode on port 5000
    app.run(host="{}".format(server_ip),debug=True, port="{}".format(server_port))
