import requests
import datetime

class WebScrapingRequest():
    def __init__(self, url, headers=None):
        self.url = url
        self.headers = headers
        self.r = None
        self.output = None
    
    def __repr__(self) -> str:
        return self.url

    def __str__(self) -> str:
        return self.url

    def get_request(self):
        '''
            Point to url and make a request.
        '''
        self.r = requests.get(self.url, headers=self.headers)
    
    def get_output(self):
        '''
            Jsonify request.
        '''
        try:
            self.output = self.r.json()
        except:
            self.output = None
        return self.output

class WebScrapingProcess():
    def __init__(self, input, meta_data, translation=None):
        self.input = self._input_check(input)
        self.meta_data = meta_data
        self.translation = translation
        self.preprocessed = None
        self.translated = None
        self.posprocessed = None
    
    def _input_check(self, input):
        return input

    def _preprocess(self):
        self.preprocessed = self.input
    
    def _translate(self):
        if self.translation:
            result = []
            for item in self.preprocessed:
                aux = {}
                for k,v in self.translation.items():
                    aux[v] = item.get(k, None)
                result.append(aux)
            self.translated = result
        else:
            self.translated = self.preprocessed
    
    def _posprocess(self):
        self.posprocessed = self.translated
    
    def runprocess(self):
        self._preprocess()
        self._translate()
        self._posprocess()
    
    def output(self):
        return self.posprocessed

class WebScrapingDBSchema():
    def __init__(self, input, schema):
        self.input = input
        self.schema = schema
        self.output = None
    
    def process_schema(self):
        '''
            Make a SQL string.
        '''
        output = []
        for item in self.input:
            pkg = []
            for sch in self.schema:
                if sch in ('created_at', 'updated_at'):
                    pkg.append(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                else:
                    pkg.append(item.get(sch, None)) # 'NULL'
            output.append(pkg)
        self.output = output
    
    def get_output(self):
        return self.output

class WebScrapingProcess_LM01(WebScrapingProcess):
    def _preprocess(self):
        input = self.input.get('products').copy()
        for i, _ in enumerate(input):
            input[i]['seller_id'] = self.meta_data['id']
            input[i]['seller_name'] = self.meta_data['name']
            input[i]['category_1'] = self.meta_data['categorias']['1']
            input[i]['category_2'] = self.meta_data['categorias']['2']
            input[i]['category_3'] = self.meta_data['categorias']['3']
            input[i]['category_4'] = self.meta_data['categorias']['4']
            input[i]['seller_source_url'] = self.meta_data['url']
            input[i]['unit'] = 'un' if input[i].get('unit', 'un') == 'cada' else input[i].get('unit', 'un')
            input[i]['id'] = str(input[i].get('id'))
            try:
                input[i]['price'] = float(input[i]['price']['to']['integers'] + '.' + input[i]['price']['to']['decimals'])
            except:
                input[i]['price'] = None

        self.preprocessed = input