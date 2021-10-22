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
    def __init__(self, input, translation=None):
        self.input = self._input_check(input)
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
            for item in self.input:
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
        input = self.input.copy()
        for i, _ in enumerate(input):
            input[i]['seller_id'] = 1
            input[i]['seller_name'] = 'Leroy Merlin'
            input[i]['category_1'] = 'Construcao'
            input[i]['category_2'] = 'Materiais Basicos'
            input[i]['category_3'] = 'Cimento'
            input[i]['seller_source_url'] = 'https://www.leroymerlin.com.br/api/boitata/v1/categories/67c4a6736f44e69d84093b50/products'
            input[i]['unit'] = 'un' if input[i].get('unit', 'un') == 'cada' else input[i].get('unit', 'un')
            input[i]['id'] = str(input[i].get('id'))
            try:
                input[i]['price'] = float(input[i]['price']['to']['integers'] + '.' + input[i]['price']['to']['decimals'])
            except:
                input[i]['price'] = None

        self.preprocessed = input