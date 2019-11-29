from elasticsearch.helpers import bulk


class ESSink(object):
    def __init__(self, df, es):
        self.df = df
        self.es = es
    def to_es(self, recall_bucket):
        ACTIONS = []
        self.df.apply(lambda x:ACTIONS.append(
            {"_index": recall_bucket,
            "_type": recall_bucket,
            "_source":eval(x.to_json(orient='index'))}),axis=1)
        self.es.indices.exists(index=recall_bucket) and self.es.indices.delete(index=recall_bucket)
        success, _ =  bulk(self.es, ACTIONS, index=recall_bucket, raise_on_error=True)
        self.es.indices.refresh(index=recall_bucket)
