from QRDecoderClass import QRdecode
from QREncoder import QRencode
import json
import requests
import falcon

class Falcon_frontend(object):
    def __init__(self):
        self.encoder = QRencode()
        self.decoder = QRdecode()
    def on_get(self, req, resp):
        """Handles GET requests"""
        d = falcon.uri.parse_query_string(req.query_string)
        req_type = d['type']
        req_data = d['data']
        if req_type == "encode":
            if len(req_data) > 22 or len(req_data) == 0:
                res = "\nCannot deal with the request.\n"
            else:
                # res = "encode"
                res = self.encoder.encode(req_data)
        elif req_type == "decode":
            _, res = self.decoder.decode(req_data)
        else:
            res = "\nCannot detect request type.\n"

        resp.status = falcon.HTTP_200  # This is the default status
        resp.body = res

    def on_post(self, req, resp):
        d = falcon.uri.parse_query_string(req.query_string)
        req_type = d['type']
        req_data = d['data']
        if req_type == "encode":
            if len(req_data) > 22 or len(req_data) == 0:
                res = "\nCannot deal with the request.\n"
            else:
                # res = "encode"
                res = self.encoder.encode(req_data)
        elif req_type == "decode":
            _, res = self.decoder.decode(req_data)
        else:
            res = "\nCannot detect request type.\n"

        resp.status = falcon.HTTP_200  # This is the default status
        resp.body = res


# falcon.API instances are callable WSGI apps
app = falcon.API()
app.req_options.auto_parse_form_urlencoded = True
# Resources are represented by long-lived class instances
cc_fuzzy = Falcon_frontend()

# things will handle all requests to the '/things' URL path
app.add_route('/q1', cc_fuzzy)
