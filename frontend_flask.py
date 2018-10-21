from flask import Flask, request, Response
from QRDecoder import QRdecode
from QREncoder import QRencode
import json
import requests

app = Flask(__name__, static_url_path='')


@app.route("/q1", methods=['GET', 'POST'])
def handle():
    if request.method == 'GET':
        type = request.args.get('type')
        data = request.args.get('data')
        # res = data
        # print("###", type)
        # print("###", data)
        # type = input["type"]
        # data = input["data"]
        if (type == "encode"):
            if len(data) <= 22:
                # res = "encode"
                res = QRencode().encode(data)
        else:
        # elif type == "decode":
            # res = "decode"
            # res = data
            _, res = QRdecode(data)
        # else:
        #     res = "\nCannot detect request type.\n"
        return Response(res, mimetype='application/json')

if __name__ == '__main__':
    app.run(threaded=False, debug=False, host="0.0.0.0", port=80)
