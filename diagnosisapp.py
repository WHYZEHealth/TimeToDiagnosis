import pandas as pd
import numpy as np
from flask import Flask
from TimeToDiagnosis  import TimeToDiagnosisCharts

app  = Flask(__name__)
app.add_url_rule('/ceocharts/diagnosis',methods = ['POST', 'GET'],view_func=TimeToDiagnosisCharts.restructure)

#### Main Function
if __name__ == "__main__":
    app.run(port=7013,debug=True, host='0.0.0.0')