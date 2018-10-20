from flask import Flask, request, Response
from QRDecoder import QRdecode
from QREncoder import QRencode
import json
import requests

app = Flask(__name__, static_url_path='')


@app.route("/", methods=['GET', 'POST'])
def handle():
    if request.method == 'GET':
        input = request.get_json()
        type = input["type"]
        data = input["data"]
        if (type == "encode"):
            if len(data) > 22 or len(data) == 0:
                res = "\nCannot deal with the request.\n"
            else:
                # res = "encode"
                res = QRencode(data)
        elif type == "decode":
            # res = "decode"
            # res = data
            _, res = QRdecode(data)
        else:
            res = "\nCannot detect request type.\n"
        return Response(res, mimetype='application/json')

if __name__ == '__main__':
    app.run(threaded=True, debug=True, host="0.0.0.0", port=2333)